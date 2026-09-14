package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

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

var s3Client *s3.Client
var bucketName string

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

func getSongs(w http.ResponseWriter, r *http.Request) {
	if s3Client == nil {
		http.Error(w, "Cloud storage no configurado", http.StatusInternalServerError)
		return
	}

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

		headRes, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(name),
		})
		
		var fileSize int64 = 0
		if err == nil && headRes.ContentLength != nil {
			fileSize = *headRes.ContentLength
		}

		log.Printf("[DEBUG] Analizando archivo: '%s' | Tamaño: %d bytes", name, fileSize)

		// Como R2 no permite rangos múltiples con comas, descargamos los ÚLTIMOS 2 MB del archivo 
		// que es exactamente donde Mp3tag guarda los metadatos y carátulas en los WAV gigantes.
		var rangeStr string
		if fileSize > 2097152 {
			start := fileSize - 2097152
			rangeStr = fmt.Sprintf("bytes=%d-%d", start, fileSize-1)
		} else if fileSize > 0 {
			rangeStr = fmt.Sprintf("bytes=0-%d", fileSize-1)
		} else {
			songs = append(songs, Song{Nombre: name, Titulo: title, Artista: artist})
			continue
		}

		res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(name),
			Range:  aws.String(rangeStr),
		})
		if err != nil {
			log.Printf("[DEBUG ERROR] Falló rango en %s: %v", name, err)
			songs = append(songs, Song{Nombre: name, Titulo: title, Artista: artist})
			continue
		}

		data, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			songs = append(songs, Song{Nombre: name, Titulo: title, Artista: artist})
			continue
		}

		// Abrimos el bloque final con id3v2
		tmpFile, err := os.CreateTemp("", "meta-*.tmp")
		if err == nil {
			tmpName := tmpFile.Name()
			tmpFile.Write(data)
			tmpFile.Close()
			defer os.Remove(tmpName)

			tag, err := id3v2.Open(tmpName, id3v2.Options{Parse: true})
			if err == nil {
				defer tag.Close()
				t := tag.Title()
				a := tag.Artist()
				log.Printf("[ID3V2 OK] %s -> Título: '%s' | Artista: '%s'", name, t, a)
				if t != "" {
					title = t
				}
				if a != "" {
					artist = a
				}
			} else {
				log.Printf("[ID3V2 WARN] No se detectó ID3 al final de %s: %v", name, err)
			}
		}

		songs = append(songs, Song{
			Nombre:  name,
			Titulo:  title,
			Artista: artist,
		})
	}

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

	headRes, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(songName),
	})
	
	var fileSize int64 = 0
	if err == nil && headRes.ContentLength != nil {
		fileSize = *headRes.ContentLength
	}

	var rangeStr string
	if fileSize > 2097152 {
		start := fileSize - 2097152
		rangeStr = fmt.Sprintf("bytes=%d-%d", start, fileSize-1)
	} else if fileSize > 0 {
		rangeStr = fmt.Sprintf("bytes=0-%d", fileSize-1)
	} else {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}

	res, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(songName),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		http.Error(w, "No cover found", http.StatusNotFound)
		return
	}

	if ext == ".mp3" || ext == ".wav" {
		tmpFile, err := os.CreateTemp("", "cover-*.tmp")
		if err == nil {
			tmpName := tmpFile.Name()
			tmpFile.Write(data)
			tmpFile.Close()
			defer os.Remove(tmpName)

			tag, err := id3v2.Open(tmpName, id3v2.Options{Parse: true})
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
	}

	if ext == ".flac" {
		if len(data) > 4 && string(data[:4]) == "fLaC" {
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
