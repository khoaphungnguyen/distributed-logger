// =========================
// main.go (Go TCP + UDP Server + Web Metrics)
// =========================
package main

import (
	"bufio"
	"bytes"
	logpb "go-ingestor/proto"
	"hash/crc32"
	"sync/atomic"

	// "compress/gzip" // REMOVE this line
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Service   string `json:"service"`
}

func (e *LogEntry) Validate() error {
	if e.Timestamp == "" {
		return fmt.Errorf("missing timestamp")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return fmt.Errorf("invalid timestamp format: %v", err)
	}
	if e.Level == "" {
		return fmt.Errorf("missing level")
	}
	allowedLevels := map[string]bool{"INFO": true, "WARN": true, "ERROR": true, "DEBUG": true}
	if !allowedLevels[e.Level] {
		return fmt.Errorf("invalid level: %s", e.Level)
	}
	if e.Message == "" {
		return fmt.Errorf("missing message")
	}
	if len(e.Message) > 2048 {
		return fmt.Errorf("message too long")
	}
	if e.Service == "" {
		return fmt.Errorf("missing service")
	}
	if len(e.Service) > 128 {
		return fmt.Errorf("service name too long")
	}
	return nil
}

type MetricsSnapshot struct {
	LogsPerSec    int64
	MBPerSec      float64
	LatencyUs     float64
	QueueLength   int64
	FileRotations int32
	Dropped       int64
}

var lastMetrics MetricsSnapshot
var metricsMutex sync.Mutex

const numWriters = 12

var logChans [numWriters]chan LogEntry

// Each writer has its own file and zstd writer
var logFiles [numWriters]*os.File
var zstdWriters [numWriters]*zstd.Encoder
var writerLocks [numWriters]sync.Mutex

var (
	logFile           *os.File
	mutex             sync.Mutex
	processCount      int64
	rotationSize      = int64(50 * 1024 * 1024) // 50MB
	rateTicker        = time.NewTicker(1 * time.Second)
	totalBytes        int64
	totalLatency      int64
	latencyCount      int64
	queueLength       int64
	fileRotationCount int32
	maxQueueLength    int64
	droppedLogs       int64
	logFormat         string // "json" or "proto"
)

func main() {
	setupFile()
	for i := 0; i < numWriters; i++ {
		logChans[i] = make(chan LogEntry, 200000)
		go writerWorker(i)
	}
	go metricsLogger()
	go startTCPServerJSON() // JSON on :3001
	go startTCPServerProto()
	go startUDPServer()
	go startHTTPServer()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down, flushing logs...")
	for i := 0; i < numWriters; i++ {
		close(logChans[i])
	}
	for i := 0; i < numWriters; i++ {
		closeLogFile(i)
	}
}

func setupFile() {
	dataDir := "./data"
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
}

// --- New: JSON TCP server on :3001 ---
func startTCPServerJSON() {
	cert, err := tls.LoadX509KeyPair("./certs/cert.pem", "./certs/key.pem")
	if err != nil {
		log.Fatalf("Failed to load TLS certs: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	listener, err := tls.Listen("tcp", ":3001", config)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port 3001: %v", err)
	}
	log.Println("TLS TCP (JSON) log ingestor listening on :3001")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP Accept error: %v", err)
			continue
		}
		go handleConnectionJSON(conn)
	}
}

// --- New: Proto TCP server on :3003 ---
func startTCPServerProto() {
	cert, err := tls.LoadX509KeyPair("./certs/cert.pem", "./certs/key.pem")
	if err != nil {
		log.Fatalf("Failed to load TLS certs: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	listener, err := tls.Listen("tcp", ":3003", config)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port 3003: %v", err)
	}
	log.Println("TLS TCP (PROTOBUF) log ingestor listening on :3003")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP Accept error: %v", err)
			continue
		}
		go handleConnectionProto(conn)
	}
}

// --- JSON UDP server on :3002 ---
func startUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", ":3002")
	if err != nil {
		log.Fatalf("Failed to resolve UDP port: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP port 3002: %v", err)
	}
	log.Println("UDP log ingestor listening on :3002")

	buf := make([]byte, 65535)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}

		scanner := bufio.NewScanner(bytes.NewReader(buf[:n]))
		for scanner.Scan() {
			start := time.Now()
			var entry LogEntry
			line := scanner.Bytes()
			if err := json.Unmarshal(line, &entry); err != nil {
				log.Printf("Invalid UDP log format: %v", err)
				continue
			}
			if err := entry.Validate(); err != nil {
				log.Printf("Invalid log entry: %v", err)
				continue
			}
			enqueueLog(entry)
			recordLatency(time.Since(start))
		}
		if err := scanner.Err(); err != nil {
			log.Printf("UDP scanner error: %v", err)
		}
	}
}

// --- Handler for JSON ---
func handleConnectionJSON(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		start := time.Now()
		var entry LogEntry
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("Invalid TCP JSON log format: %v", err)
			continue
		}
		if err := entry.Validate(); err != nil {
			log.Printf("Invalid log entry: %v", err)
			continue
		}
		enqueueLog(entry)
		recordLatency(time.Since(start))
	}
	if err := scanner.Err(); err != nil {
		log.Printf("TCP scanner error: %v", err)
	}
}

// --- Handler for Proto ---
func handleConnectionProto(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 64*1024)
	for {
		// Always try to parse as many messages as possible
		for {
			if len(buf) < 4 {
				break
			}
			msgLen := binary.BigEndian.Uint32(buf[:4])
			if msgLen == 0 || msgLen > 10*1024 {
				log.Printf("Invalid proto message length: %d", msgLen)
				return
			}
			if len(buf) < int(4+msgLen) {
				break
			}
			start := time.Now()
			msgBuf := buf[4 : 4+msgLen]
			var pbEntry logpb.LogEntry
			if err := proto.Unmarshal(msgBuf, &pbEntry); err != nil {
				log.Printf("Invalid TCP proto log format: %v", err)
				buf = buf[4+msgLen:]
				continue
			}
			entry := LogEntry{
				Timestamp: pbEntry.Timestamp,
				Level:     pbEntry.Level,
				Message:   pbEntry.Message,
				Service:   pbEntry.Service,
			}
			if err := entry.Validate(); err != nil {
				log.Printf("Invalid log entry: %v", err)
				buf = buf[4+msgLen:]
				continue
			}
			enqueueLog(entry)
			recordLatency(time.Since(start))
			buf = buf[4+msgLen:]
		}
		// Read more data if not enough for a full message
		n, err := conn.Read(tmp)
		if err != nil {
			return
		}
		buf = append(buf, tmp[:n]...)
	}
}

// Hash function to distribute logs
func hash(entry LogEntry) int {
	return int(crc32.ChecksumIEEE([]byte(entry.Timestamp+entry.Service))) % numWriters
}

func enqueueLog(entry LogEntry) {
	idx := hash(entry)
	select {
	case logChans[idx] <- entry:
		// enqueued
	default:
		atomic.AddInt64(&droppedLogs, 1)
		if atomic.AddInt64(&droppedLogs, 1)%1000 == 0 {
			log.Printf("[OVERLOAD] Log dropped! Total dropped: %d", atomic.LoadInt64(&droppedLogs))
		}
	}
}

func writerWorker(idx int) {
	rotateLogFile(idx)
	buffer := make([]LogEntry, 0, 100000)
	flushTimer := time.NewTicker(500 * time.Millisecond)
	defer flushTimer.Stop()

	for {
		select {
		case entry, ok := <-logChans[idx]:
			if !ok {
				flushLogs(idx, buffer)
				return
			}
			buffer = append(buffer, entry)
			atomic.AddInt64(&processCount, 1)
			atomic.StoreInt64(&queueLength, int64(len(logChans[idx])))
			if len(buffer) >= 20000 || shouldRotate(idx) {
				flushLogs(idx, buffer)
				buffer = buffer[:0]
			}
		case <-flushTimer.C:
			if len(buffer) > 0 {
				flushLogs(idx, buffer)
				buffer = buffer[:0]
			}
		}
	}
}

func flushLogs(idx int, logs []LogEntry) {
	writerLocks[idx].Lock()
	defer writerLocks[idx].Unlock()
	rotateIfNeeded(idx)
	for _, entry := range logs {
		line, _ := json.Marshal(entry)
		zstdWriters[idx].Write([]byte(string(line) + "\n"))
		atomic.AddInt64(&totalBytes, int64(len(line)+1))
	}
	zstdWriters[idx].Flush()
}

func shouldRotate(idx int) bool {
	if logFiles[idx] == nil {
		return true
	}
	info, err := logFiles[idx].Stat()
	return err != nil || info.Size() >= rotationSize
}

func rotateIfNeeded(idx int) {
	if shouldRotate(idx) {
		rotateLogFile(idx)
	}
}

func rotateLogFile(idx int) {
	closeLogFile(idx)
	timestamp := time.Now().Format("20060102_150405")
	filePath := filepath.Join("./data", fmt.Sprintf("log_%d_%s.jsonl.zst", idx, timestamp))
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	logFiles[idx] = f
	encoder, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatalf("Failed to create zstd writer: %v", err)
	}
	zstdWriters[idx] = encoder
	fileRotationCount++
	log.Printf("Started new log file: %s", filePath)
}

func closeLogFile(idx int) {
	if zstdWriters[idx] != nil {
		zstdWriters[idx].Close()
	}
	if logFiles[idx] != nil {
		logFiles[idx].Close()
	}
}

func metricsLogger() {
	for range rateTicker.C {
		metricsMutex.Lock()
		rate := atomic.LoadInt64(&processCount)
		bytesPerSec := atomic.LoadInt64(&totalBytes)
		totalLat := atomic.LoadInt64(&totalLatency)
		latCount := atomic.LoadInt64(&latencyCount)
		queueLen := atomic.LoadInt64(&queueLength)
		maxQueue := atomic.LoadInt64(&maxQueueLength)
		rotations := atomic.LoadInt32((*int32)(&fileRotationCount))
		dropped := atomic.LoadInt64(&droppedLogs)

		avgLatency := float64(0)
		if latCount > 0 {
			avgLatency = float64(totalLat) / float64(latCount)
		}
		if queueLen > maxQueue {
			atomic.StoreInt64(&maxQueueLength, queueLen)
			maxQueue = queueLen
		}
		numGoroutines := runtime.NumGoroutine()

		// Save snapshot for HTTP
		lastMetrics = MetricsSnapshot{
			LogsPerSec:    rate,
			MBPerSec:      float64(bytesPerSec) / (1024 * 1024),
			LatencyUs:     avgLatency,
			QueueLength:   queueLen,
			FileRotations: rotations,
			Dropped:       dropped,
		}

		// Reset counters
		atomic.StoreInt64(&processCount, 0)
		atomic.StoreInt64(&totalBytes, 0)
		atomic.StoreInt64(&totalLatency, 0)
		atomic.StoreInt64(&latencyCount, 0)

		var totalFileSize int64
		for i := 0; i < numWriters; i++ {
			if logFiles[i] != nil {
				info, err := logFiles[i].Stat()
				if err == nil {
					totalFileSize += info.Size()
				}
			}
		}

		log.Printf("[METRIC] Logs/sec: %d, MB/s: %.2f, Latency: %.2fµs, Queue: %d (max: %d), Rotations: %d, Goroutines: %d, FileSize: %.2fMB, Dropped: %d",
			rate, float64(bytesPerSec)/(1024*1024), avgLatency, queueLen, maxQueue, rotations, numGoroutines, float64(totalFileSize)/(1024*1024), dropped)
		metricsMutex.Unlock()
	}
}

func recordLatency(d time.Duration) {
	atomic.AddInt64(&totalLatency, d.Microseconds())
	atomic.AddInt64(&latencyCount, 1)
}

func startHTTPServer() {
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsMutex.Lock()
		snap := lastMetrics
		metricsMutex.Unlock()
		fmt.Fprintf(w, `{"logs_per_sec": %d, "mb_per_sec": %.2f, "latency_us": %.2f, "queue_length": %d, "file_rotations": %d, "dropped": %d}`,
			snap.LogsPerSec,
			snap.MBPerSec,
			snap.LatencyUs,
			snap.QueueLength,
			snap.FileRotations,
			snap.Dropped,
		)
	})
	log.Println("Dashboard available at http://localhost:3000")
	http.ListenAndServe(":3000", nil)
}
