package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bogem/id3v2/v2"
)

type Song struct {
	Nombre  string `json:"nombre"`
	Titulo  string `json:"titulo"`
	Artista string `json:"artista"`
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func getSongs(w http.ResponseWriter, r *http.Request) {
	musicDir := "./musica"
	files, err := os.ReadDir(musicDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("No se encuentra la carpeta 'musica' en: %s", musicDir), http.StatusInternalServerError)
		return
	}

	var songs []Song
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".mp3" && ext != ".flac" && ext != ".wav" {
			continue
		}

		title := strings.TrimSuffix(name, ext)
		artist := "Música Local"
		filePath := filepath.Join(musicDir, name)

		// 1. Extraer metadatos para MP3
		if ext == ".mp3" {
			tag, err := id3v2.Open(filePath, id3v2.Options{Parse: true})
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

		// 2. Extraer metadatos para WAV (leyendo el bloque ID3 dentro de la estructura RIFF)
		if ext == ".wav" {
			data, err := os.ReadFile(filePath)
			if err == nil && len(data) > 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WAVE" {
				offset := 12
				for offset < len(data)-8 {
					chunkID := string(data[offset : offset+4])
					chunkSize := int(data[offset+4]) | int(data[offset+5])<<8 | int(data[offset+6])<<16 | int(data[offset+7])<<24

					if chunkID == "ID3 " || chunkID == "id3 " {
						if offset+8+chunkSize <= len(data) {
							tagData := data[offset+8 : offset+8+chunkSize]
							tmpFile, err := os.CreateTemp("", "wav-id3-*.tmp")
							if err == nil {
								tmpName := tmpFile.Name()
								tmpFile.Write(tagData)
								tmpFile.Close()
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
						break
					}

					offset += 8 + chunkSize
					if chunkSize%2 != 0 {
						offset++
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

func playSong(w http.ResponseWriter, r *http.Request) {
	musicDir := "./musica"
	songName := r.URL.Query().Get("song")
	filePath := filepath.Join(musicDir, songName)

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "Archivo no encontrado", http.StatusNotFound)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		http.Error(w, "Error interno", http.StatusInternalServerError)
		return
	}

	http.ServeContent(w, r, stat.Name(), stat.ModTime(), file)
}

func getCover(w http.ResponseWriter, r *http.Request) {
	musicDir := "./musica"
	songName := r.URL.Query().Get("song")
	filePath := filepath.Join(musicDir, songName)
	ext := strings.ToLower(filepath.Ext(songName))

	// 1. Carátula para MP3
	if ext == ".mp3" {
		tag, err := id3v2.Open(filePath, id3v2.Options{Parse: true})
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

	// 2. Carátula para WAV
	if ext == ".wav" {
		data, err := os.ReadFile(filePath)
		if err == nil && len(data) > 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WAVE" {
			offset := 12
			for offset < len(data)-8 {
				chunkID := string(data[offset : offset+4])
				chunkSize := int(data[offset+4]) | int(data[offset+5])<<8 | int(data[offset+6])<<16 | int(data[offset+7])<<24

				if chunkID == "ID3 " || chunkID == "id3 " {
					if offset+8+chunkSize <= len(data) {
						tagData := data[offset+8 : offset+8+chunkSize]
						tmpFile, err := os.CreateTemp("", "wav-id3-*.tmp")
						if err == nil {
							tmpName := tmpFile.Name()
							tmpFile.Write(tagData)
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
				}

				offset += 8 + chunkSize
				if chunkSize%2 != 0 {
					offset++
				}
			}
		}
	}

	// 3. Carátula para FLAC
	if ext == ".flac" {
		data, err := os.ReadFile(filePath)
		if err == nil && len(data) > 4 && string(data[:4]) == "fLaC" {
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
	http.HandleFunc("/songs", enableCORS(getSongs))
	http.HandleFunc("/play", enableCORS(playSong))
	http.HandleFunc("/cover", enableCORS(getCover))
	http.Handle("/", http.FileServer(http.Dir(".")))

	fmt.Println("Servidor Go escuchando en http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}