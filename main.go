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

// getSongs lista los archivos de la nube y extrae metadatos descargando una pequeña porción inicial del archivo
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
		if ext != ".mp3" && ext != ".flac" && ext != ".wav" {
			continue
		}

		title := strings.TrimSuffix(name, ext)
		artist := "Cloud Library"

		// Si es MP3, podemos leer metadatos descargando los primeros 128KB del archivo desde R2 de forma eficiente
		if ext == ".mp3" {
			rangeInput := &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(name),
				Range:  aws.String("bytes=0-131071"), // Primeros 128 KB
			}
			res, err := s3Client.GetObject(context.TODO(), rangeInput)
			if err == nil {
				tmpFile, err := os.CreateTemp("", "meta-*.mp3")
				if err == nil {
					tmpName := tmpFile.Name()
					io.Copy(tmpFile, res.Body)
					tmpFile.Close()
					res.Body.Close()
					defer os.Remove(tmpName)

					tag, err := id3v2.Open(tmpName, id3v2.Options{Parse: true})
					if err == nil {
						if t := tag.Title(); t != "" {
							title = t
						}
						if a := tag.Artist(); a != "" {
							artist = a
						}
						tag.Close()
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(songs)
}

// playSong transmite las canciones de la nube con soporte de Range Requests
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

	// Descargamos una porción inicial para buscar la carátula integrada en el MP3
	if ext == ".mp3" {
		rangeInput := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(songName),
			Range:  aws.String("bytes=0-524288"), // Primeros 512 KB donde suele estar la carátula
		}
		res, err := s3Client.GetObject(context.TODO(), rangeInput)
		if err == nil {
			tmpFile, err := os.CreateTemp("", "cover-*.mp3")
			if err == nil {
				tmpName := tmpFile.Name()
				io.Copy(tmpFile, res.Body)
				tmpFile.Close()
				res.Body.Close()
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
