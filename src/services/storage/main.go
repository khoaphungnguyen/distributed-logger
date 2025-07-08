package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogEntry should match the struct used by ingestor
type LogEntry struct {
	Timestamp    string `json:"timestamp"`
	Level        string `json:"level"`
	Message      string `json:"message"`
	Service      string `json:"service"`
	PartitionKey string `json:"partition_key,omitempty"`
}

var (
	dataDir      = "./data"
	mu           sync.Mutex
	partitionMap = make(map[string]*os.File) // partitionKey -> file
)

// Partition by service and hour
func getPartitionKey(entry LogEntry) string {
	t := time.Now()
	if entry.Timestamp != "" {
		parsed, err := time.Parse(time.RFC3339, entry.Timestamp)
		if err == nil {
			t = parsed
		}
	}
	return fmt.Sprintf("%s_%s", entry.Service, t.Format("2006-01-02-15"))
}

// Write log entry to a partition file
func writeLog(entry LogEntry) error {
	mu.Lock()
	defer mu.Unlock()
	partition := entry.PartitionKey
	if partition == "" {
		partition = getPartitionKey(entry)
	}
	f, ok := partitionMap[partition]
	if !ok {
		filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
		var err error
		f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		partitionMap[partition] = f
	}
	b, _ := json.Marshal(entry)
	_, err := f.Write(append(b, '\n'))
	return err
}

// HTTP handler for log ingestion
func logHandler(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid log entry", 400)
		return
	}
	if entry.PartitionKey == "" {
		entry.PartitionKey = getPartitionKey(entry)
	}
	if err := writeLog(entry); err != nil {
		http.Error(w, "Failed to write log", 500)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HTTP handler for querying logs (very basic, for demo)
func queryHandler(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	partition := r.URL.Query().Get("partition")
	if service == "" || partition == "" {
		http.Error(w, "Missing service or partition", 400)
		return
	}
	filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s_%s.log", service, partition))
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Try gzipped version
		filePath += ".gz"
	}
	f, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "Partition not found", 404)
		return
	}
	defer f.Close()
	http.ServeContent(w, r, filePath, time.Now(), f)
}

// Compress files older than 3 days
func compressOldFiles(olderThanDays int) {
	for {
		files, _ := filepath.Glob(filepath.Join(dataDir, "partition_*.log"))
		cutoff := time.Now().AddDate(0, 0, -olderThanDays)
		for _, file := range files {
			fi, err := os.Stat(file)
			if err != nil || fi.ModTime().After(cutoff) {
				continue
			}
			gzFile := file + ".gz"
			in, err := os.Open(file)
			if err != nil {
				continue
			}
			out, err := os.Create(gzFile)
			if err != nil {
				in.Close()
				continue
			}
			gz := gzip.NewWriter(out)
			_, err = io.Copy(gz, in)
			gz.Close()
			in.Close()
			out.Close()
			if err == nil {
				os.Remove(file)
			}
		}
		time.Sleep(12 * time.Hour)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	os.MkdirAll(dataDir, 0755)
	go compressOldFiles(3) // Compress files older than 3 days
	http.HandleFunc("/ingest", logHandler)
	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/health", healthHandler)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	addr := ln.Addr().String()
	log.Printf("Storage service listening on %s", addr)
	log.Fatal(http.Serve(ln, nil))
}
