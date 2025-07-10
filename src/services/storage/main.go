package main

import (
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type LogEntry struct {
	Timestamp    string `json:"timestamp"`
	Level        string `json:"level"`
	Message      string `json:"message"`
	Service      string `json:"service"`
	PartitionKey string `json:"partition_key,omitempty"`
}

var (
	dataDir            = "./data"
	partitionMap       = make(map[string]*os.File) // partitionKey -> file
	partitionLocks     = make(map[string]*sync.Mutex)
	locksMu            sync.Mutex
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR")
	selfAddr           string
)

var (
	processCount int64
	totalBytes   int64
	totalLatency int64
	latencyCount int64
	droppedLogs  int64
	rateTicker   = time.NewTicker(1 * time.Second)
)

// Add this function:
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

func getPartitionLock(partition string) *sync.Mutex {
	locksMu.Lock()
	defer locksMu.Unlock()
	l, ok := partitionLocks[partition]
	if !ok {
		l = &sync.Mutex{}
		partitionLocks[partition] = l
	}
	return l
}

// Update writeLog:
func writeLog(entry LogEntry) error {
	start := time.Now()
	partition := entry.PartitionKey
	if partition == "" {
		partition = getPartitionKey(entry)
	}
	lock := getPartitionLock(partition)
	lock.Lock()
	defer lock.Unlock()
	f, ok := partitionMap[partition]
	if !ok {
		filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
		var err error
		f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			atomic.AddInt64(&droppedLogs, 1)
			return err
		}
		partitionMap[partition] = f
	}
	b, _ := json.Marshal(entry)
	_, err := f.Write(append(b, '\n'))
	if err == nil {
		atomic.AddInt64(&processCount, 1)
		atomic.AddInt64(&totalBytes, int64(len(b)))
		atomic.AddInt64(&totalLatency, time.Since(start).Microseconds())
		atomic.AddInt64(&latencyCount, 1)
	} else {
		atomic.AddInt64(&droppedLogs, 1)
	}
	return err
}

// Update writeLogBatch:
func writeLogBatch(entries []LogEntry) error {
	start := time.Now()
	partitioned := make(map[string][]LogEntry)
	for _, entry := range entries {
		partition := entry.PartitionKey
		if partition == "" {
			partition = getPartitionKey(entry)
		}
		partitioned[partition] = append(partitioned[partition], entry)
	}
	for partition, group := range partitioned {
		lock := getPartitionLock(partition)
		lock.Lock()
		f, ok := partitionMap[partition]
		if !ok {
			filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s.log", partition))
			var err error
			f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				lock.Unlock()
				atomic.AddInt64(&droppedLogs, int64(len(group)))
				return err
			}
			partitionMap[partition] = f
		}
		var buf bytes.Buffer
		for _, entry := range group {
			b, _ := json.Marshal(entry)
			buf.Write(b)
			buf.WriteByte('\n')
		}
		n, err := f.Write(buf.Bytes())
		if err == nil {
			atomic.AddInt64(&processCount, int64(len(group)))
			atomic.AddInt64(&totalBytes, int64(n))
			atomic.AddInt64(&totalLatency, time.Since(start).Microseconds())
			atomic.AddInt64(&latencyCount, int64(len(group)))
		} else {
			atomic.AddInt64(&droppedLogs, int64(len(group)))
			lock.Unlock()
			return err
		}
		lock.Unlock()
	}
	return nil
}

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

func batchLogHandler(w http.ResponseWriter, r *http.Request) {
	var entries []LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
		http.Error(w, "Invalid batch payload", 400)
		return
	}
	if err := writeLogBatch(entries); err != nil {
		http.Error(w, "Failed to write batch", 500)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	partition := r.URL.Query().Get("partition")
	if service == "" || partition == "" {
		http.Error(w, "Missing service or partition", 400)
		return
	}
	filePath := filepath.Join(dataDir, fmt.Sprintf("partition_%s_%s.log", service, partition))
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
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
	go compressOldFiles(3)
	//go metricsLogger()
	http.HandleFunc("/ingest", logHandler)
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
