package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bogem/id3v2/v2"
)

type Song struct {
	Nombre  string `json:"nombre"`
	Titulo  string `json:"titulo"`
	Artista string `json:"artista"`
}

const (
	// Rango leído para MP3/FLAC (el tag/los bloques de metadatos van al principio)
	headFetchSize int64 = 2 * 1024 * 1024 // 2MB
	// Rango leído para WAV (Serato/rekordbox/Traktor suelen meter el chunk id3
	// DESPUÉS del audio, casi al final del archivo)
	tailFetchSize int64 = 3 * 1024 * 1024 // 3MB
	// Rango ligero usado solo para el escaneo inicial de /songs
	scanHeadSize int64 = 512 * 1024
	scanTailSize int64 = 512 * 1024
)

var (
	s3Client    *s3.Client
	bucketName  string
	cacheMutex  sync.Mutex
	cachedSongs []Song
	cacheLoaded bool

	coverCacheMutex sync.Mutex
	coverCache      = map[string]coverCacheEntry{}
)

type coverCacheEntry struct {
	mime  string
	data  []byte
	found bool
}

func initS3() {
	bucketName = os.Getenv("R2_BUCKET_NAME")
	accountID := os.Getenv("R2_ACCOUNT_ID")
	accessKey := os.Getenv("R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("R2_SECRET_ACCESS_KEY")

	if accountID == "" || bucketName == "" {
		log.Println("Aviso: Variables de entorno de R2 no configuradas.")
		return
	}

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID),
		}, nil
	})

	cfg, err := awscfg.LoadDefaultConfig(context.TODO(),
		awscfg.WithEndpointResolverWithOptions(r2Resolver),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awscfg.WithRegion("auto"),
	)
	if err != nil {
		log.Fatalf("Error al cargar configuración de AWS/R2: %v", err)
	}

	s3Client = s3.NewFromConfig(cfg)
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Range")
		w.Header().Set("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, Content-Length")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// --- Utilidades de lectura parcial contra R2/S3 ---

func fetchRange(ctx context.Context, key string, rangeHeader string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}
	res, err := s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

func getObjectSize(ctx context.Context, key string) (int64, error) {
	head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(head.ContentLength), nil
}

// findID3Signature busca la firma real de un tag ID3v2 ("ID3" + byte de versión
// 2/3/4) dentro de un buffer de bytes. No depende de que el chunk "id3 " de WAV
// esté bien alineado: busca el tag ID3v2 en sí, esté donde esté.
func findID3Signature(data []byte) int {
	search := []byte("ID3")
	start := 0
	for {
		idx := bytes.Index(data[start:], search)
		if idx == -1 {
			return -1
		}
		pos := start + idx
		if pos+3 < len(data) {
			ver := data[pos+3]
			if ver == 2 || ver == 3 || ver == 4 {
				return pos
			}
		}
		start = pos + 1
		if start >= len(data) {
			return -1
		}
	}
}

func parseID3TitleArtist(data []byte) (title, artist string, ok bool) {
	idx := findID3Signature(data)
	if idx == -1 {
		return "", "", false
	}
	tag, err := id3v2.ParseReader(bytes.NewReader(data[idx:]), id3v2.Options{Parse: true})
	if err != nil {
		return "", "", false
	}
	defer tag.Close()
	return tag.Title(), tag.Artist(), true
}

func extractPictureFromID3Bytes(data []byte) (mime string, pic []byte) {
	idx := findID3Signature(data)
	if idx == -1 {
		return "", nil
	}
	tag, err := id3v2.ParseReader(bytes.NewReader(data[idx:]), id3v2.Options{Parse: true})
	if err != nil {
		return "", nil
	}
	defer tag.Close()
	for _, f := range tag.GetFrames(tag.CommonID("Attached picture")) {
		if p, ok := f.(id3v2.PictureFrame); ok {
			return p.MimeType, p.Picture
		}
	}
	return "", nil
}

// extractFlacPictureFromBytes recorre los METADATA BLOCKs de un FLAC (van al
// principio del archivo, antes del audio) buscando el bloque PICTURE (tipo 6).
func extractFlacPictureFromBytes(data []byte) (mime string, pic []byte) {
	if len(data) < 4 || string(data[:4]) != "fLaC" {
		return "", nil
	}
	offset := 4
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		header := data[offset]
		isLast := (header & 0x80) != 0
		blockType := header & 0x7F
		length := int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		if offset+length > len(data) {
			break
		}

		if blockType == 6 {
			blockData := data[offset : offset+length]
			for i := 0; i+3 < len(blockData); i++ {
				if blockData[i] == 0xFF && blockData[i+1] == 0xD8 {
					return "image/jpeg", blockData[i:]
				}
				if blockData[i] == 0x89 && blockData[i+1] == 0x50 && blockData[i+2] == 0x4E && blockData[i+3] == 0x47 {
					return "image/png", blockData[i:]
				}
			}
		}

		if isLast {
			break
		}
		offset += length
	}
	return "", nil
}

func getSongs(w http.ResponseWriter, r *http.Request) {
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	cacheMutex.Lock()
	if cacheLoaded {
		cacheMutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cachedSongs)
		return
	}
	cacheMutex.Unlock()

	ctx := context.TODO()

	output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		http.Error(w, "Error al listar canciones de la nube: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var songs []Song
	for _, obj := range output.Contents {
		name := aws.ToString(obj.Key)
		ext := strings.ToLower(filepath.Ext(name))

		if strings.Contains(name, "Multipart") || (ext != ".mp3" && ext != ".flac" && ext != ".wav") {
			continue
		}

		title := strings.TrimSuffix(name, ext)
		artist := "Cloud Library"
		size := aws.ToInt64(obj.Size)

		// Lectura ligera en RAM para el escaneo inicial
		head, err := fetchRange(ctx, name, fmt.Sprintf("bytes=0-%d", scanHeadSize-1))
		if err == nil && len(head) > 0 {
			switch ext {
			case ".mp3":
				if t, a, ok := parseID3TitleArtist(head); ok {
					if t != "" {
						title = t
					}
					if a != "" {
						artist = a
					}
				}
			case ".wav":
				t, a, ok := parseID3TitleArtist(head)
				if !ok && size > 0 {
					// El id3 casi siempre queda al final en grabaciones largas de DJ,
					// fuera del rango de cabecera que acabamos de leer.
					start := size - scanTailSize
					if start < 0 {
						start = 0
					}
					if tail, terr := fetchRange(ctx, name, fmt.Sprintf("bytes=%d-%d", start, size-1)); terr == nil {
						t, a, ok = parseID3TitleArtist(tail)
					}
				}
				if ok {
					if t != "" {
						title = t
					}
					if a != "" {
						artist = a
					}
				}
			}
		}

		songs = append(songs, Song{
			Nombre:  name,
			Titulo:  title,
			Artista: artist,
		})
	}

	cacheMutex.Lock()
	cachedSongs = songs
	cacheLoaded = true
	cacheMutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(songs)
}

func playSong(w http.ResponseWriter, r *http.Request) {
	songName := r.URL.Query().Get("song")
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(songName),
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	result, err := s3Client.GetObject(context.TODO(), input)
	if err != nil {
		http.Error(w, "Archivo no encontrado en la nube", http.StatusNotFound)
		return
	}
	defer result.Body.Close()

	if result.ContentType != nil {
		w.Header().Set("Content-Type", *result.ContentType)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	if result.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *result.ContentLength))
	}
	if result.ContentRange != nil {
		w.Header().Set("Content-Range", *result.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	}

	io.Copy(w, result.Body)
}

// getCover ya NO descarga el archivo completo: pide solo el rango donde
// realmente suele vivir el tag/los metadatos, y cachea el resultado en memoria
// para que las siguientes peticiones de la misma canción sean instantáneas.
func getCover(w http.ResponseWriter, r *http.Request) {
	songName := r.URL.Query().Get("song")
	ext := strings.ToLower(filepath.Ext(songName))
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	coverCacheMutex.Lock()
	if cached, ok := coverCache[songName]; ok {
		coverCacheMutex.Unlock()
		if !cached.found {
			http.Error(w, "No cover found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", cached.mime)
		w.Write(cached.data)
		return
	}
	coverCacheMutex.Unlock()

	ctx := context.TODO()
	var mime string
	var pic []byte

	switch ext {
	case ".mp3":
		if head, err := fetchRange(ctx, songName, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
			mime, pic = extractPictureFromID3Bytes(head)
		}

	case ".wav":
		if size, err := getObjectSize(ctx, songName); err == nil {
			start := size - tailFetchSize
			if start < 0 {
				start = 0
			}
			if tail, terr := fetchRange(ctx, songName, fmt.Sprintf("bytes=%d-%d", start, size-1)); terr == nil {
				mime, pic = extractPictureFromID3Bytes(tail)
			}
		}
		if pic == nil {
			// Fallback por si algún WAV concreto sí lo trae al principio
			if head, err := fetchRange(ctx, songName, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
				mime, pic = extractPictureFromID3Bytes(head)
			}
		}

	case ".flac":
		if head, err := fetchRange(ctx, songName, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
			mime, pic = extractFlacPictureFromBytes(head)
		}
	}

	coverCacheMutex.Lock()
	coverCache[songName] = coverCacheEntry{mime: mime, data: pic, found: pic != nil}
	coverCacheMutex.Unlock()

	if pic == nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Write(pic)
}

func main() {
	initS3()

	http.HandleFunc("/songs", enableCORS(getSongs))
	http.HandleFunc("/play", enableCORS(playSong))
	http.HandleFunc("/cover", enableCORS(getCover))
	http.Handle("/", http.FileServer(http.Dir(".")))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Servidor Go escuchando en el puerto %s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
