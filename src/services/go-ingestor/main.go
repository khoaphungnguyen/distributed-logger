// =========================
// main.go (Go TCP + UDP Server + Web Metrics)
// =========================
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Service   string `json:"service"`
}

var (
	logChan           = make(chan LogEntry, 100000)
	logFile           *os.File
	gzipWriter        *gzip.Writer
	mutex             sync.Mutex
	processCount      int
	rotationSize      = int64(5 * 1024 * 1024) // 5MB
	rateTicker        = time.NewTicker(1 * time.Second)
	flushLock         sync.Mutex
	totalBytes        int64
	totalLatency      int64
	latencyCount      int64
	queueLength       int
	fileRotationCount int
)

func main() {
	rotateLogFile()
	defer closeLogFile()

	go writerWorker()
	go metricsLogger()
	go startTCPServer()
	go startUDPServer()
	go startHTTPServer()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down, flushing logs...")
	close(logChan)
}

func startTCPServer() {
	cert, err := tls.LoadX509KeyPair("./certs/cert.pem", "./certs/key.pem")
	if err != nil {
		log.Fatalf("Failed to load TLS certs: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	listener, err := tls.Listen("tcp", ":3000", config)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port 3000: %v", err)
	}
	log.Println("TLS TCP log ingestor listening on :3000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP Accept error: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func startUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", ":3001")
	if err != nil {
		log.Fatalf("Failed to resolve UDP port: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP port 3001: %v", err)
	}
	log.Println("UDP log ingestor listening on :3001")

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
			if entry.Timestamp == "" {
				entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
			}
			logChan <- entry
			recordLatency(time.Since(start))
		}
		if err := scanner.Err(); err != nil {
			log.Printf("UDP scanner error: %v", err)
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		start := time.Now()
		var entry LogEntry
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("Invalid TCP log format: %v", err)
			continue
		}
		if entry.Timestamp == "" {
			entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}
		logChan <- entry
		recordLatency(time.Since(start))
	}
	if err := scanner.Err(); err != nil {
		log.Printf("TCP scanner error: %v", err)
	}
}

func writerWorker() {
	buffer := make([]LogEntry, 0, 1000)
	flushTimer := time.NewTicker(5 * time.Second)

	for {
		select {
		case entry, ok := <-logChan:
			if !ok {
				flushLogs(buffer)
				return
			}
			buffer = append(buffer, entry)
			mutex.Lock()
			processCount++
			queueLength = len(logChan)
			mutex.Unlock()

			if len(buffer) >= 1000 || shouldRotate() {
				flushLogs(buffer)
				buffer = buffer[:0]
			}
		case <-flushTimer.C:
			if len(buffer) > 0 {
				flushLogs(buffer)
				buffer = buffer[:0]
			}
		}
	}
}

func flushLogs(logs []LogEntry) {
	flushLock.Lock()
	defer flushLock.Unlock()

	rotateIfNeeded()
	for _, entry := range logs {
		line, _ := json.Marshal(entry)
		gzipWriter.Write([]byte(string(line) + "\n"))
		totalBytes += int64(len(line)) + 1
	}
	gzipWriter.Flush()
}

func shouldRotate() bool {
	if logFile == nil {
		return true
	}
	info, err := logFile.Stat()
	return err != nil || info.Size() >= rotationSize
}

func rotateIfNeeded() {
	if shouldRotate() {
		rotateLogFile()
	}
}

func rotateLogFile() {
	closeLogFile()
	timestamp := time.Now().Format("20060102_150405")
	filePath := filepath.Join("./data", fmt.Sprintf("log_%s.jsonl.gz", timestamp))
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	logFile = f
	gzipWriter = gzip.NewWriter(logFile)
	fileRotationCount++
	log.Printf("Started new log file: %s", filePath)
}

func closeLogFile() {
	if gzipWriter != nil {
		gzipWriter.Close()
	}
	if logFile != nil {
		logFile.Close()
	}
}

func metricsLogger() {
	for range rateTicker.C {
		mutex.Lock()
		rate := processCount
		bytesPerSec := totalBytes
		avgLatency := float64(0)
		if latencyCount > 0 {
			avgLatency = float64(totalLatency) / float64(latencyCount)
		}
		queueLen := queueLength
		rotations := fileRotationCount

		processCount = 0
		totalBytes = 0
		totalLatency = 0
		latencyCount = 0
		mutex.Unlock()

		log.Printf("[METRIC] Logs/sec: %d, MB/s: %.2f, Latency: %.2fµs, Queue: %d, Rotations: %d",
			rate, float64(bytesPerSec)/(1024*1024), avgLatency, queueLen, rotations)
	}
}
func recordLatency(d time.Duration) {
	mutex.Lock()
	totalLatency += d.Microseconds()
	latencyCount++
	mutex.Unlock()
}

func startHTTPServer() {
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		fmt.Fprintf(w, `{"logs_per_sec": %d, "mb_per_sec": %.2f, "latency_us": %.2f, "queue_length": %d, "file_rotations": %d}`,
			processCount,
			float64(totalBytes)/(1024*1024),
			float64(totalLatency)/float64(latencyCount),
			queueLength,
			fileRotationCount)
	})
	log.Println("Metrics available at http://localhost:8080/metrics")
	http.ListenAndServe(":8080", nil)
}
