package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type LogEntry struct {
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	Message    string `json:"message"`
	Service    string `json:"service"`
	Hostname   string `json:"hostname,omitempty"`
	AppVersion string `json:"app_version,omitempty"`
}

var (
	dataDir            = "./data"
	partitionIndex     = make(map[string][]string) // service -> []partition files
	indexMu            sync.RWMutex
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR")
	selfAddr           string

	processCount int64
	totalBytes   int64
	totalLatency int64
	latencyCount int64
	droppedLogs  int64
	rateTicker   = time.NewTicker(1 * time.Second)

	partitionBuffers = make(map[string]chan LogEntry)
	bufferMu         sync.Mutex
)

func getPartitionBuffer(partition string) chan LogEntry {
	bufferMu.Lock()
	defer bufferMu.Unlock()
	buf, ok := partitionBuffers[partition]
	if !ok {
		buf = make(chan LogEntry, 1000)
		partitionBuffers[partition] = buf
		go partitionBufferWriter(partition, buf)
		// --- Add this to update the index ---
		parts := strings.Split(partition, "_")
		if len(parts) >= 2 {
			service := parts[0]
			filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
			addToPartitionIndex(service, filePath)
		}
	}
	return buf
}
func partitionBufferWriter(partition string, buf chan LogEntry) {
	filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to open file for partition %s: %v", partition, err)
		return
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	for entry := range buf {
		encoder.Encode(entry)
		atomic.AddInt64(&processCount, 1)
	}
}

func writeLogBatchBuffered(entries []LogEntry) {
	for _, entry := range entries {
		partition := getPartitionKey(entry)
		buf := getPartitionBuffer(partition)
		buf <- entry
	}
}

func metricsLogger() {
	for range rateTicker.C {
		rate := atomic.LoadInt64(&processCount)
		bytesPerSec := atomic.LoadInt64(&totalBytes)
		totalLat := atomic.LoadInt64(&totalLatency)
		latCount := atomic.LoadInt64(&latencyCount)
		dropped := atomic.LoadInt64(&droppedLogs)

		avgLatency := float64(0)
		if latCount > 0 {
			avgLatency = float64(totalLat) / float64(latCount)
		}
		numGoroutines := runtime.NumGoroutine()

		log.Printf("[METRIC] Logs/sec: %d, MB/s: %.2f, Latency: %.2fµs, Goroutines: %d, Dropped: %d",
			rate, float64(bytesPerSec)/(1024*1024), avgLatency, numGoroutines, dropped)

		atomic.StoreInt64(&processCount, 0)
		atomic.StoreInt64(&totalBytes, 0)
		atomic.StoreInt64(&totalLatency, 0)
		atomic.StoreInt64(&latencyCount, 0)
	}
}

func buildPartitionIndex() {
	indexMu.Lock()
	defer indexMu.Unlock()
	partitionIndex = make(map[string][]string)
	files, _ := filepath.Glob(filepath.Join(dataDir, "partition_*.log"))
	for _, file := range files {
		base := filepath.Base(file)
		parts := strings.Split(base, "_")
		if len(parts) < 3 {
			continue
		}
		service := parts[1]
		partitionIndex[service] = append(partitionIndex[service], file)
	}
}

func addToPartitionIndex(service, file string) {
	indexMu.Lock()
	defer indexMu.Unlock()
	for _, f := range partitionIndex[service] {
		if f == file {
			return // already indexed
		}
	}
	partitionIndex[service] = append(partitionIndex[service], file)
}

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

func batchLogHandler(w http.ResponseWriter, r *http.Request) {
	var entries []LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
		http.Error(w, "Invalid batch payload", 400)
		return
	}
	writeLogBatchBuffered(entries)
	w.WriteHeader(http.StatusOK)
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST supported", http.StatusMethodNotAllowed)
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Parse time range if provided
	var startTime, endTime time.Time
	var err error
	if req.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, req.StartTime)
		if err != nil {
			http.Error(w, "invalid start_time", http.StatusBadRequest)
			return
		}
	}
	if req.EndTime != "" {
		endTime, err = time.Parse(time.RFC3339, req.EndTime)
		if err != nil {
			http.Error(w, "invalid end_time", http.StatusBadRequest)
			return
		}
	}

	// Use index to get relevant files
	var files []string
	indexMu.RLock()
	if req.Service != "" {
		files = append(files, partitionIndex[req.Service]...)
	} else {
		for _, fs := range partitionIndex {
			files = append(files, fs...)
		}
	}
	indexMu.RUnlock()

	// Filter files by time range if provided
	filteredFiles := files
	if !startTime.IsZero() || !endTime.IsZero() {
		filteredFiles = nil
		for _, file := range files {
			base := filepath.Base(file)
			parts := strings.Split(base, "_")
			if len(parts) < 3 {
				continue
			}
			timePart := strings.TrimSuffix(parts[len(parts)-1], ".log")
			fileTime, err := time.Parse("2006-01-02-15", timePart)
			if err != nil {
				continue
			}
			if (!startTime.IsZero() && fileTime.Before(startTime)) ||
				(!endTime.IsZero() && fileTime.After(endTime)) {
				continue
			}
			filteredFiles = append(filteredFiles, file)
		}
	}

	resultCh := make(chan []LogEntry, len(filteredFiles))
	var wg sync.WaitGroup

	// Limit concurrency to avoid too many open files
	maxConcurrency := 4
	sem := make(chan struct{}, maxConcurrency)

	for _, file := range filteredFiles {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var localResults []LogEntry
			f, err := os.Open(file)
			if err != nil {
				return
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				var entry LogEntry
				if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
					continue
				}
				if req.Level != "" && entry.Level != req.Level {
					continue
				}
				if !startTime.IsZero() || !endTime.IsZero() {
					entryTime, err := time.Parse(time.RFC3339, entry.Timestamp)
					if err != nil {
						continue
					}
					if !startTime.IsZero() && entryTime.Before(startTime) {
						continue
					}
					if !endTime.IsZero() && entryTime.After(endTime) {
						continue
					}
				}
				localResults = append(localResults, entry)
				// Early exit if limit reached
				if req.Limit > 0 && len(localResults) >= req.Limit {
					break
				}
			}
			f.Close()
			resultCh <- localResults
		}(file)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var results []LogEntry
collectLoop:
	for res := range resultCh {
		for _, entry := range res {
			results = append(results, entry)
			if req.Limit > 0 && len(results) >= req.Limit {
				break collectLoop
			}
		}
	}

	json.NewEncoder(w).Encode(QueryResponse{Results: results})
}

type QueryRequest struct {
	Service   string `json:"service,omitempty"`
	Level     string `json:"level,omitempty"`
	StartTime string `json:"start_time,omitempty"`
	EndTime   string `json:"end_time,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}
type QueryResponse struct {
	Results []LogEntry `json:"results"`
}

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

func registerWithClusterManager(addr string) {
	if clusterManagerAddr == "" {
		log.Println("CLUSTER_MANAGER_ADDR not set, skipping registration")
		return
	}
	body, _ := json.Marshal(map[string]string{
		"address": addr,
		"type":    "storage",
	})
	_, err := http.Post(clusterManagerAddr+"/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("Failed to register with cluster manager: %v", err)
	} else {
		log.Printf("Registered with cluster manager as %s", addr)
	}
}

func unregisterWithClusterManager(addr string) {
	if clusterManagerAddr == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{
		"address": addr,
	})
	http.Post(clusterManagerAddr+"/nodes/unregister", "application/json", bytes.NewReader(body))
	log.Printf("Unregistered from cluster manager")
}

func main() {
	os.MkdirAll(dataDir, 0755)
	buildPartitionIndex()
	go compressOldFiles(3)
	go metricsLogger()
	http.HandleFunc("/ingest/batch", batchLogHandler)
	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/cluster/health", healthHandler)

	storageName := os.Getenv("STORAGE_NAME")
	if storageName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal("STORAGE_NAME must be set and hostname could not be determined")
		}
		storageName = hostname
	}

	ln, err := net.Listen("tcp", ":0") // random port
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		log.Fatalf("Failed to parse port: %v", err)
	}
	addr := fmt.Sprintf("http://%s:%s", storageName, port)
	selfAddr = addr

	registerWithClusterManager(selfAddr)
	defer unregisterWithClusterManager(selfAddr)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		unregisterWithClusterManager(selfAddr)
		os.Exit(0)
	}()

	log.Printf("Storage service listening on %s", addr)
	log.Fatal(http.Serve(ln, nil))
}
