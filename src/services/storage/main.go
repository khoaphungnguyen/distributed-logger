package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// --- Consistent Hash Ring (copy from ingestor/query or share as a package) ---
type hashRing struct {
	nodes    []string
	replicas int
	keys     []uint32
	hashMap  map[uint32]string
}

func newHashRing(nodes []string, replicas int) *hashRing {
	hr := &hashRing{
		nodes:    nodes,
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
	hr.generate()
	return hr
}

func (hr *hashRing) generate() {
	hr.keys = nil
	hr.hashMap = make(map[uint32]string)
	for _, node := range hr.nodes {
		for i := 0; i < hr.replicas; i++ {
			key := fmt.Sprintf("%s#%d", node, i)
			hash := crc32.ChecksumIEEE([]byte(key))
			hr.keys = append(hr.keys, hash)
			hr.hashMap[hash] = node
		}
	}
	sort.Slice(hr.keys, func(i, j int) bool { return hr.keys[i] < hr.keys[j] })
}

func (hr *hashRing) getNodes(key string, n int) []string {
	if len(hr.keys) == 0 || n <= 0 {
		return nil
	}
	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(hr.keys), func(i int) bool { return hr.keys[i] >= hash })
	result := make([]string, 0, n)
	seen := make(map[string]bool)
	for i := 0; len(result) < n && i < len(hr.keys); i++ {
		node := hr.hashMap[hr.keys[(idx+i)%len(hr.keys)]]
		if !seen[node] {
			result = append(result, node)
			seen[node] = true
		}
	}
	return result
}

// Add global variables for ring and storage nodes
var (
	storageNodes      []string
	storageNodesMutex sync.RWMutex
	storageRing       *hashRing
	replicaCount      = 3 // Should match ingestor/query
)

// Update storageNodes and ring periodically (call this in main)
func updateStorageNodes() {
	resp, err := http.Get(clusterManagerAddr + "/storage-nodes")
	if err != nil {
		log.Printf("Failed to get storage nodes: %v", err)
		return
	}
	defer resp.Body.Close()
	var nodes []struct {
		Address   string `json:"address"`
		IsHealthy bool   `json:"is_healthy"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		log.Printf("Failed to decode storage nodes: %v", err)
		return
	}
	var addrs []string
	for _, n := range nodes {
		if n.IsHealthy {
			addrs = append(addrs, n.Address)
		}
	}
	storageNodesMutex.Lock()
	storageNodes = addrs
	storageRing = newHashRing(addrs, 100)
	storageNodesMutex.Unlock()
}

func periodicallyUpdateStorageNodes() {
	for {
		updateStorageNodes()
		time.Sleep(2 * time.Second)
	}
}

// --- /get_entry endpoint for peer repair ---
func getEntryHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Service string `json:"service"`
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	indexMu.RLock()
	files := partitionIndex[req.Service]
	indexMu.RUnlock()
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				continue
			}
			if entry.Service == req.Service && entry.Level == req.Level && entry.Message == req.Message {
				json.NewEncoder(w).Encode(entry)
				f.Close()
				return
			}
		}
		f.Close()
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// --- Read Repair Engine (background anti-entropy) ---
func readRepairEngine() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		storageNodesMutex.RLock()
		ring := storageRing
		nodes := append([]string{}, storageNodes...)
		storageNodesMutex.RUnlock()
		if ring == nil || len(nodes) == 0 {
			continue
		}
		indexMu.RLock()
		for _, files := range partitionIndex {
			for _, file := range files {
				f, err := os.Open(file)
				if err != nil {
					continue
				}
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					var entry LogEntry
					if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
						continue
					}
					// Compute responsible replicas for this entry
					replicas := ring.getNodes(entry.Service, replicaCount)
					for _, peer := range replicas {
						if peer == selfAddr {
							continue // skip self
						}
						go func(peer string, entry LogEntry) {
							reqBody, _ := json.Marshal(map[string]string{
								"service": entry.Service,
								"level":   entry.Level,
								"message": entry.Message,
							})
							resp, err := http.Post(peer+"/get_entry", "application/json", bytes.NewReader(reqBody))
							if err != nil {
								return
							}
							defer resp.Body.Close()
							if resp.StatusCode != http.StatusOK {
								return
							}
							var peerEntry LogEntry
							if err := json.NewDecoder(resp.Body).Decode(&peerEntry); err != nil {
								return
							}
							// If peer has newer, repair self
							if peerEntry.Timestamp > entry.Timestamp {
								repairHandlerInternal(peerEntry)
							}
							// If self is newer, repair peer
							if entry.Timestamp > peerEntry.Timestamp {
								repairBody, _ := json.Marshal(entry)
								http.Post(peer+"/repair", "application/json", bytes.NewReader(repairBody))
							}
						}(peer, entry)
					}
				}
				f.Close()
			}
		}
		indexMu.RUnlock()
	}
}

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

// Internal repair logic (reuse your repairHandler logic)
func repairHandlerInternal(entry LogEntry) {
	partition := getPartitionKey(entry)
	filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
	// ...same logic as in repairHandler to update file...
	var updatedEntries []LogEntry
	found := false
	f, err := os.Open(filePath)
	if err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var existing LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &existing); err != nil {
				continue
			}
			if existing.Message == entry.Message && existing.Service == entry.Service && existing.Level == entry.Level {
				if entry.Timestamp > existing.Timestamp {
					updatedEntries = append(updatedEntries, entry)
				} else {
					updatedEntries = append(updatedEntries, existing)
				}
				found = true
			} else {
				updatedEntries = append(updatedEntries, existing)
			}
		}
		f.Close()
	}
	if !found {
		updatedEntries = append(updatedEntries, entry)
	}
	tmpFile := filePath + ".tmp"
	out, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	enc := json.NewEncoder(out)
	for _, e := range updatedEntries {
		enc.Encode(e)
	}
	out.Close()
	os.Rename(tmpFile, filePath)
}

// Add this handler function:
func repairHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST supported", http.StatusMethodNotAllowed)
		return
	}
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	partition := getPartitionKey(entry)
	filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))

	// Read all entries, replace if same key and incoming is newer
	var updatedEntries []LogEntry
	found := false

	// Try to open the file, but it's ok if it doesn't exist yet
	f, err := os.Open(filePath)
	if err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var existing LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &existing); err != nil {
				continue
			}
			// Use composite key for uniqueness
			if existing.Message == entry.Message && existing.Service == entry.Service && existing.Level == entry.Level {
				// Compare timestamps
				if entry.Timestamp > existing.Timestamp {
					updatedEntries = append(updatedEntries, entry)
				} else {
					updatedEntries = append(updatedEntries, existing)
				}
				found = true
			} else {
				updatedEntries = append(updatedEntries, existing)
			}
		}
		f.Close()
	}
	if !found {
		updatedEntries = append(updatedEntries, entry)
	}

	// Rewrite the file with updated entries
	tmpFile := filePath + ".tmp"
	out, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	enc := json.NewEncoder(out)
	for _, e := range updatedEntries {
		enc.Encode(e)
	}
	out.Close()
	os.Rename(tmpFile, filePath)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("repaired"))
}

func main() {
	os.MkdirAll(dataDir, 0755)
	buildPartitionIndex()
	go compressOldFiles(3)
	go periodicallyUpdateStorageNodes()
	go readRepairEngine()
	//go metricsLogger()
	http.HandleFunc("/ingest/batch", batchLogHandler)
	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/cluster/health", healthHandler)
	http.HandleFunc("/repair", repairHandler)
	http.HandleFunc("/get_entry", getEntryHandler)

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
