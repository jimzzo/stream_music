package main

import (
	"bytes"
	"context"
	"encoding/binary"
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

var (
	s3Client   *s3.Client
	bucketName string
	
	cacheMutex   sync.Mutex
	cachedSongs  []Song
	cacheLoaded  = false
)

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

// Extrae el bloque ID3v2 interno de un archivo WAV con contenedor RIFF
func extractWavID3Chunk(filePath string) string {
	file, err := os.Open(filePath)
	if err != nil {
		return ""
	}
	defer file.Close()

	// Leer cabecera RIFF (12 bytes: "RIFF" + size + "WAVE")
	header := make([]byte, 12)
	if _, err := io.ReadFull(file, header); err != nil {
		return ""
	}

	if string(header[:4]) != "RIFF" || string(header[8:]) != "WAVE" {
		return ""
	}

	// Buscar chunks iterativamente dentro del RIFF
	for {
		chunkHeader := make([]byte, 8)
		if _, err := io.ReadFull(file, chunkHeader); err != nil {
			break
		}

		chunkID := string(chunkHeader[:4])
		chunkSize := binary.LittleEndian.Uint32(chunkHeader[4:8])

		// Los chunks de metadatos ID3 en archivos WAV suelen llamarse "id3 " o "ID3 "
		if chunkID == "id3 " || chunkID == "ID3 " {
			id3Data := make([]byte, chunkSize)
			if _, err := io.ReadFull(file, id3Data); err != nil {
				break
			}

			// Escribir el bloque ID3 extraído a un archivo temporal puro para que id3v2 lo lea sin problemas
			tmpTagFile, err := os.CreateTemp("", "wav-id3-*.tmp")
			if err != nil {
				return ""
			}
			tmpTagPath := tmpTagFile.Name()
			tmpTagFile.Write(id3Data)
			tmpTagFile.Close()
			return tmpTagPath
		}

		// Saltar al siguiente chunk (asegurando alineación par en RIFF)
		toSkip := int64(chunkSize)
		if toSkip%2 != 0 {
			toSkip++
		}
		if _, err := file.Seek(toSkip, io.SeekCurrent); err != nil {
			break
		}
	}
	return ""
}

// Descarga el archivo completo a un disco temporal por streaming (seguro para RAM en Render) y devuelve la ruta del archivo temporal
func downloadToTempFile(ctx context.Context, key string) (string, error) {
	res, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	tmpFile, err := os.CreateTemp("", "media-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmpFile.Name()

	_, copyErr := io.Copy(tmpFile, res.Body)
	tmpFile.Close()

	if copyErr != nil {
		os.Remove(tmpName)
		return "", copyErr
	}

	return tmpName, nil
}

func getSongs(w http.ResponseWriter, r *http.Request) {
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	cacheMutex.Lock()
	if cacheLoaded {
		log.Println("[CACHE] Sirviendo lista de canciones desde la memoria caché.")
		cacheMutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cachedSongs)
		return
	}
	cacheMutex.Unlock()

	log.Println("[CACHE] Escaneando canciones en Cloudflare R2 por primera vez...")
	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
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

		// Descargamos el archivo completo a disco temporal para asegurar carátulas e integridad total
		tmpPath, err := downloadToTempFile(context.TODO(), name)
		if err == nil {
			defer os.Remove(tmpPath)

			if ext == ".mp3" {
				tag, err := id3v2.Open(tmpPath, id3v2.Options{Parse: true})
				if err == nil {
					defer tag.Close()
					if t := tag.Title(); t != "" {
						title = t
					}
					if a := tag.Artist(); a != "" {
						artist = a
					}
				}
			} else if ext == ".wav" {
				// Parseo especializado para WAV con RIFF ID3v2.3
				tagPath := extractWavID3Chunk(tmpPath)
				if tagPath != "" {
					defer os.Remove(tagPath)
					tag, err := id3v2.Open(tagPath, id3v2.Options{Parse: true})
					if err == nil {
						defer tag.Close()
						if t := tag.Title(); t != "" {
							title = t
						}
						if a := tag.Artist(); a != "" {
							artist = a
						}
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

func getCover(w http.ResponseWriter, r *http.Request) {
	songName := r.URL.Query().Get("song")
	ext := strings.ToLower(filepath.Ext(songName))
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	// Descargar archivo completo a disco temporal para que las imágenes grandes no salgan cortadas
	tmpPath, err := downloadToTempFile(context.TODO(), songName)
	if err != nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}
	defer os.Remove(tmpPath)

	if ext == ".mp3" {
		tag, err := id3v2.Open(tmpPath, id3v2.Options{Parse: true})
		if err == nil {
			defer tag.Close()
			pictures := tag.GetFrames(tag.CommonID("Attached picture"))
			for _, f := range pictures {
				if pic, ok := f.(id3v2.PictureFrame); ok {
					w.Header().Set("Content-Type", pic.MimeType)
					w.Write(pic.Picture)
					return
				}
			}
		}
	} else if ext == ".wav" {
		tagPath := extractWavID3Chunk(tmpPath)
		if tagPath != "" {
			defer os.Remove(tagPath)
			tag, err := id3v2.Open(tagPath, id3v2.Options{Parse: true})
			if err == nil {
				defer tag.Close()
				pictures := tag.GetFrames(tag.CommonID("Attached picture"))
				for _, f := range pictures {
					if pic, ok := f.(id3v2.PictureFrame); ok {
						w.Header().Set("Content-Type", pic.MimeType)
						w.Write(pic.Picture)
						return
					}
				}
			}
		}
	} else if ext == ".flac" {
		fBytes, err := os.ReadFile(tmpPath)
		if err == nil && len(fBytes) > 4 && string(fBytes[:4]) == "fLaC" {
			offset := 4
			for offset < len(fBytes) {
				if offset+4 > len(fBytes) {
					break
				}
				header := fBytes[offset]
				isLast := (header & 0x80) != 0
				blockType := header & 0x7F

				length := int(fBytes[offset+1])<<16 | int(fBytes[offset+2])<<8 | int(fBytes[offset+3])
				offset += 4

				if offset+length > len(fBytes) {
					break
				}

				if blockType == 6 {
					blockData := fBytes[offset : offset+length]
					for i := 0; i < len(blockData)-4; i++ {
						if (blockData[i] == 0xFF && blockData[i+1] == 0xD8) ||
							(blockData[i] == 0x89 && blockData[i+1] == 0x50 && blockData[i+2] == 0x4E && blockData[i+3] == 0x47) {
							mType := "image/jpeg"
							if blockData[i] == 0x89 {
								mType = "image/png"
							}
							w.Header().Set("Content-Type", mType)
							w.Write(blockData[i:])
							return
						}
					}
				}

				if isLast {
					break
				}
				offset += length
			}
		}
	}

	http.Error(w, "No cover found", http.StatusNotFound)
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
