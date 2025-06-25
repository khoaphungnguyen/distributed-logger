// =========================
// main.go (Go TCP Server)
// =========================
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"net"
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
	logChan      = make(chan LogEntry, 100000)
	logFile      *os.File
	gzipWriter   *gzip.Writer
	mutex        sync.Mutex
	processCount int
	rotationSize = int64(5 * 1024 * 1024) // 5MB
	rateTicker   = time.NewTicker(1 * time.Second)
	flushLock    sync.Mutex
)

func main() {
	rotateLogFile()
	defer closeLogFile()

	go writerWorker()
	go metricsLogger()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down, flushing logs...")
		close(logChan)
		os.Exit(0)
	}()

	listener, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen on port 3000: %v", err)
	}
	log.Println("TCP log ingestor listening on :3000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var entry LogEntry
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("Invalid log format: %v", err)
			continue
		}
		if entry.Timestamp == "" {
			entry.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}
		logChan <- entry
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error: %v", err)
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
		processCount = 0
		mutex.Unlock()
		log.Printf("[METRIC] Logs processed: %d logs/sec", rate)
	}
}
