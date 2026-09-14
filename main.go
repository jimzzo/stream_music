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
	s3Client    *s3.Client
	bucketName  string
	cacheMutex  sync.Mutex
	cachedSongs []Song
	cacheLoaded = false
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

// Extrae el bloque ID3v2 directamente desde un búfer en memoria RAM (Cero uso de disco)
func extractWavID3FromBytes(data []byte) io.Reader {
	if len(data) < 12 {
		return nil
	}
	if string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil
	}

	offset := 12
	for offset+8 <= len(data) {
		chunkID := string(data[offset : offset+4])
		chunkSize := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		offset += 8

		if offset+int(chunkSize) > len(data) {
			break
		}

		if chunkID == "id3 " || chunkID == "ID3 " {
			id3Bytes := data[offset : offset+int(chunkSize)]
			return bytes.NewReader(id3Bytes)
		}

		offset += int(chunkSize)
		if chunkSize%2 != 0 {
			offset++
		}
	}
	return nil
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

		// Pedimos solo los primeros 512 KB en memoria RAM para leer metadatos rápidamente sin saturar nada
		res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(name),
			Range:  aws.String("bytes=0-524288"),
		})

		if err == nil {
			buf, readErr := io.ReadAll(res.Body)
			res.Body.Close()

			if readErr == nil && len(buf) > 0 {
				if ext == ".mp3" {
					tag, err := id3v2.ParseReader(bytes.NewReader(buf), id3v2.Options{Parse: true})
					if err == nil {
						if t := tag.Title(); t != "" {
							title = t
						}
						if a := tag.Artist(); a != "" {
							artist = a
						}
					}
				} else if ext == ".wav" {
					if tagReader := extractWavID3FromBytes(buf); tagReader != nil {
						tag, err := id3v2.ParseReader(tagReader, id3v2.Options{Parse: true})
						if err == nil {
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

	// Para las carátulas leemos un rango mayor (1 MB) directamente en memoria RAM sin usar archivos temporales en disco
	res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(songName),
		Range:  aws.String("bytes=0-1048576"),
	})
	if err != nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}
	defer res.Body.Close()

	buf, err := io.ReadAll(res.Body)
	if err != nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}

	if ext == ".mp3" {
		tag, err := id3v2.ParseReader(bytes.NewReader(buf), id3v2.Options{Parse: true})
		if err == nil {
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
		if tagReader := extractWavID3FromBytes(buf); tagReader != nil {
			tag, err := id3v2.ParseReader(tagReader, id3v2.Options{Parse: true})
			if err == nil {
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
		if len(buf) > 4 && string(buf[:4]) == "fLaC" {
			offset := 4
			for offset < len(buf) {
				if offset+4 > len(buf) {
					break
				}
				header := buf[offset]
				isLast := (header & 0x80) != 0
				blockType := header & 0x7F

				length := int(buf[offset+1])<<16 | int(buf[offset+2])<<8 | int(buf[offset+3])
				offset += 4

				if offset+length > len(buf) {
					break
				}

				if blockType == 6 {
					blockData := buf[offset : offset+length]
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
