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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bogem/id3v2/v2"
)

type Song struct {
	Nombre  string `json:"nombre"`
	Titulo  string `json:"titulo"`
	Artista string `json:"artista"`
}

// ManifestEntry es lo que se guarda en R2 (_manifest/manifest.json) para no
// tener que volver a extraer metadatos de un archivo que no ha cambiado.
type ManifestEntry struct {
	Nombre   string `json:"nombre"`
	Titulo   string `json:"titulo"`
	Artista  string `json:"artista"`
	ETag     string `json:"etag"`
	CoverKey string `json:"coverKey,omitempty"`
	// Version identifica con qué lógica de extracción se generó esta entrada.
	// Si el código cambia la forma de leer metadatos (metadataVersion sube),
	// todas las entradas antiguas dejan de "hacer match" aunque el ETag no
	// haya cambiado, y se reprocesan solas en la siguiente reconciliación.
	Version int `json:"version"`
}

const (
	// Rango leído para MP3/FLAC (el tag/los bloques de metadatos van al principio)
	headFetchSize int64 = 3 * 1024 * 1024 // 2MB
	// Rango leído para WAV (Serato/rekordbox/Traktor/Mp3tag suelen meter el
	// chunk id3 DESPUÉS del audio, casi al final del archivo). Título, artista
	// y carátula se extraen SIEMPRE del mismo buffer/rango para evitar que uno
	// se encuentre y el otro no por usar ventanas de tamaño distinto.
	tailFetchSize int64 = 3 * 1024 * 1024 // 3MB

	manifestKey = "_manifest/manifest.json"
	coverPrefix = "_manifest/covers/"

	reconcileInterval = 10 * time.Minute

	// Sube este número cada vez que cambies la lógica de extractMetadata para
	// forzar un reprocesado automático de toda la librería en el siguiente
	// despliegue, sin tener que tocar nada a mano en R2.
	metadataVersion = 2
)

var (
	s3Client   *s3.Client
	bucketName string

	cacheMutex  sync.Mutex
	cachedSongs []Song
	cacheLoaded bool

	manifestMutex sync.Mutex
	manifestCache = map[string]ManifestEntry{}

	reconcileMutex sync.Mutex
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

// --- Utilidades de lectura/escritura parcial contra R2/S3 ---

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

func getObjectInfo(ctx context.Context, key string) (size int64, etag string, err error) {
	head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, "", err
	}
	return aws.ToInt64(head.ContentLength), aws.ToString(head.ETag), nil
}

func listAllObjects(ctx context.Context) ([]types.Object, error) {
	var all []types.Object
	var token *string
	for {
		out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, out.Contents...)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return all, nil
}

// --- Extracción de metadatos ID3 / FLAC ---

// findID3Signature busca la firma real de un tag ID3v2 ("ID3" + byte de
// versión 2/3/4) dentro de un buffer de bytes. No depende de que el chunk
// "id3 " de WAV esté bien alineado: busca el tag ID3v2 en sí, esté donde esté.
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

// extractMetadata lee cada rango UNA sola vez y saca título, artista y
// carátula del mismo buffer, para MP3/FLAC (cabecera) y WAV (cola, con
// fallback a cabecera). Antes se usaban ventanas de tamaño distinto para el
// título (pequeña) y la carátula (grande): si el tag+carátula quedaba a
// caballo de esa frontera, la carátula se encontraba pero el título no (o
// viceversa). Al usar un único buffer más grande para ambos, ese desajuste
// desaparece.
func extractMetadata(ctx context.Context, name, ext string, size int64) (title, artist, mime string, pic []byte) {
	title = strings.TrimSuffix(name, ext)
	artist = "Cloud Library"

	applyFrom := func(data []byte) bool {
		found := false
		if t, a, ok := parseID3TitleArtist(data); ok {
			if t != "" {
				title = t
			}
			if a != "" {
				artist = a
			}
			found = true
		}
		if m, p := extractPictureFromID3Bytes(data); p != nil {
			mime, pic = m, p
			found = true
		}
		return found
	}

	switch ext {
	case ".mp3":
		if head, err := fetchRange(ctx, name, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
			applyFrom(head)
		}

	case ".wav":
		found := false
		if size > 0 {
			start := size - tailFetchSize
			if start < 0 {
				start = 0
			}
			if tail, err := fetchRange(ctx, name, fmt.Sprintf("bytes=%d-%d", start, size-1)); err == nil {
				found = applyFrom(tail)
			}
		}
		// Fallback: algunos WAV (o algunas configuraciones de Mp3tag) escriben
		// el tag al principio del archivo en vez de al final.
		if !found {
			if head, err := fetchRange(ctx, name, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
				applyFrom(head)
			}
		}

	case ".flac":
		if head, err := fetchRange(ctx, name, fmt.Sprintf("bytes=0-%d", headFetchSize-1)); err == nil {
			mime, pic = extractFlacPictureFromBytes(head)
		}
	}

	return
}

func coverKeyFor(name, mime string) string {
	ext := ".jpg"
	if mime == "image/png" {
		ext = ".png"
	}
	safe := strings.NewReplacer("/", "_", " ", "_").Replace(name)
	return coverPrefix + safe + ext
}

// --- Manifiesto persistido en R2 ---

func loadManifest(ctx context.Context) map[string]ManifestEntry {
	manifest := map[string]ManifestEntry{}
	res, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(manifestKey),
	})
	if err != nil {
		return manifest // no existe todavía (primer arranque) o error de red: empezamos de cero
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return manifest
	}
	var entries []ManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return manifest
	}
	for _, e := range entries {
		manifest[e.Nombre] = e
	}
	return manifest
}

func saveManifest(ctx context.Context, manifest map[string]ManifestEntry) error {
	entries := make([]ManifestEntry, 0, len(manifest))
	for _, e := range manifest {
		entries = append(entries, e)
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(manifestKey),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

func persistManifestAsync() {
	manifestMutex.Lock()
	snapshot := make(map[string]ManifestEntry, len(manifestCache))
	for k, v := range manifestCache {
		snapshot[k] = v
	}
	manifestMutex.Unlock()
	if err := saveManifest(context.Background(), snapshot); err != nil {
		log.Println("No se pudo guardar el manifest en R2:", err)
	}
}

// reconcileLibrary compara el bucket con el manifest guardado en R2 y solo
// procesa (extrae metadatos/carátula) los archivos nuevos o modificados,
// detectando cambios por ETag. Completamente automático: se llama sola al
// arrancar, cada reconcileInterval en segundo plano, y de forma perezosa la
// primera vez que alguien pide /songs.
func reconcileLibrary(ctx context.Context) {
	reconcileMutex.Lock()
	defer reconcileMutex.Unlock()

	if s3Client == nil {
		return
	}

	objects, err := listAllObjects(ctx)
	if err != nil {
		log.Println("Reconcile: error listando el bucket:", err)
		return
	}

	oldManifest := loadManifest(ctx)
	newManifest := make(map[string]ManifestEntry, len(objects))
	var songs []Song
	changed := false

	for _, obj := range objects {
		name := aws.ToString(obj.Key)
		if strings.HasPrefix(name, coverPrefix) || name == manifestKey {
			continue
		}
		ext := strings.ToLower(filepath.Ext(name))
		if strings.Contains(name, "Multipart") || (ext != ".mp3" && ext != ".flac" && ext != ".wav") {
			continue
		}

		etag := aws.ToString(obj.ETag)
		size := aws.ToInt64(obj.Size)

		if existing, ok := oldManifest[name]; ok && existing.ETag == etag && existing.Version == metadataVersion {
			newManifest[name] = existing
			songs = append(songs, Song{Nombre: name, Titulo: existing.Titulo, Artista: existing.Artista})
			continue
		}

		// Archivo nuevo, modificado, o generado con una versión anterior de la
		// lógica de extracción: (re)procesarlo.
		changed = true
		title, artist, mime, pic := extractMetadata(ctx, name, ext, size)

		coverKey := ""
		if pic != nil {
			coverKey = coverKeyFor(name, mime)
			if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(bucketName),
				Key:         aws.String(coverKey),
				Body:        bytes.NewReader(pic),
				ContentType: aws.String(mime),
			}); err != nil {
				log.Println("Reconcile: no se pudo guardar la carátula de", name, ":", err)
				coverKey = ""
			}
		}

		newManifest[name] = ManifestEntry{Nombre: name, Titulo: title, Artista: artist, ETag: etag, CoverKey: coverKey, Version: metadataVersion}
		songs = append(songs, Song{Nombre: name, Titulo: title, Artista: artist})
	}

	// Limpieza: canciones que ya no están en el bucket (borradas) pierden su
	// carátula huérfana y su entrada en el manifest.
	for key, old := range oldManifest {
		if _, ok := newManifest[key]; !ok {
			changed = true
			if old.CoverKey != "" {
				_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(old.CoverKey),
				})
			}
		}
	}

	cacheMutex.Lock()
	cachedSongs = songs
	cacheLoaded = true
	cacheMutex.Unlock()

	manifestMutex.Lock()
	manifestCache = newManifest
	manifestMutex.Unlock()

	if changed {
		if err := saveManifest(ctx, newManifest); err != nil {
			log.Println("Reconcile: no se pudo guardar el manifest:", err)
		}
	}
}

func getSongs(w http.ResponseWriter, r *http.Request) {
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

	cacheMutex.Lock()
	loaded := cacheLoaded
	cacheMutex.Unlock()

	if !loaded {
		// Primera vez desde que arrancó el servidor: reconcilia ya mismo para
		// poder responder con datos completos (las siguientes veces esto ya
		// está resuelto por el ticker en segundo plano).
		reconcileLibrary(r.Context())
	}

	cacheMutex.Lock()
	songs := cachedSongs
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

func serveObject(ctx context.Context, w http.ResponseWriter, key string) bool {
	res, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return false
	}
	defer res.Body.Close()
	if res.ContentType != nil {
		w.Header().Set("Content-Type", *res.ContentType)
	}
	io.Copy(w, res.Body)
	return true
}

// getCover sirve la carátula ya indexada en el manifest (una simple lectura
// de un archivo de imagen pequeño en R2, sin tocar el audio). Si la canción
// es tan nueva que todavía no pasó por reconcileLibrary, la extrae al vuelo
// y la dejar registrada para que la próxima petición ya sea instantánea.
func getCover(w http.ResponseWriter, r *http.Request) {
	songName := r.URL.Query().Get("song")
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}
	ctx := r.Context()

	manifestMutex.Lock()
	entry, known := manifestCache[songName]
	manifestMutex.Unlock()

	if known {
		if entry.CoverKey == "" {
			http.Error(w, "No cover found", http.StatusNotFound)
			return
		}
		if serveObject(ctx, w, entry.CoverKey) {
			return
		}
		// La carátula desapareció de R2 por algún motivo: seguimos al fallback.
	}

	ext := strings.ToLower(filepath.Ext(songName))
	size, etag, _ := getObjectInfo(ctx, songName)
	title, artist, mime, pic := extractMetadata(ctx, songName, ext, size)

	coverKey := ""
	if pic != nil {
		coverKey = coverKeyFor(songName, mime)
		if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucketName),
			Key:         aws.String(coverKey),
			Body:        bytes.NewReader(pic),
			ContentType: aws.String(mime),
		}); err != nil {
			coverKey = ""
		}
	}

	manifestMutex.Lock()
	manifestCache[songName] = ManifestEntry{Nombre: songName, Titulo: title, Artista: artist, ETag: etag, CoverKey: coverKey, Version: metadataVersion}
	manifestMutex.Unlock()
	go persistManifestAsync()

	if pic == nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Write(pic)
}

func main() {
	initS3()

	if s3Client != nil {
		// Reconciliación inicial en segundo plano (no bloquea el arranque del
		// servidor) y refresco periódico automático, sin intervención manual.
		go reconcileLibrary(context.Background())
		go func() {
			ticker := time.NewTicker(reconcileInterval)
			defer ticker.Stop()
			for range ticker.C {
				reconcileLibrary(context.Background())
			}
		}()
	}

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
