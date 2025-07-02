package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
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

	"github.com/hamba/avro/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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

// --- Metrics ---
type MetricsSnapshot struct {
	LogsPerSec    int64
	MBPerSec      float64
	LatencyUs     float64
	QueueLength   int64
	FileRotations int32
	Dropped       int64
}

var (
	lastMetrics       MetricsSnapshot
	metricsMutex      sync.Mutex
	processCount      int64
	totalBytes        int64
	totalLatency      int64
	latencyCount      int64
	queueLength       int64
	maxQueueLength    int64
	fileRotationCount int32
	droppedLogs       int64
	rateTicker        = time.NewTicker(1 * time.Second)
)

// --- Schema Registry Integration ---
var (
	jsonSchema   *gojsonschema.Schema
	avroCodec    avro.Schema
	protoMsgDesc protoreflect.MessageDescriptor
)

func fetchAndCacheSchemas() {
	// JSON Schema
	resp, err := http.Get("http://go-schema-register:8000/schema/get?format=json&name=LogEntry")
	if err != nil {
		log.Fatalf("Failed to fetch JSON schema: %v", err)
	}
	defer resp.Body.Close()
	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Fatalf("Failed to decode JSON schema: %v", err)
	}
	jsonSchema, err = gojsonschema.NewSchema(gojsonschema.NewStringLoader(result.Schema))
	if err != nil {
		log.Fatalf("Failed to parse JSON schema: %v", err)
	}

	// Avro Schema
	resp, err = http.Get("http://go-schema-register:8000/schema/get?format=avro&name=LogEntry")
	if err != nil {
		log.Fatalf("Failed to fetch Avro schema: %v", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Fatalf("Failed to decode Avro schema: %v", err)
	}
	avroCodec, err = avro.Parse(result.Schema)
	if err != nil {
		log.Fatalf("Failed to parse Avro schema: %v", err)
	}

	// Protobuf Descriptor
	resp, err = http.Get("http://go-schema-register:8000/schema/descriptor?name=LogEntry")
	if err != nil {
		log.Fatalf("Failed to fetch proto descriptor: %v", err)
	}
	defer resp.Body.Close()
	descBytes, _ := ioutil.ReadAll(resp.Body)
	protoDesc := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(descBytes, protoDesc); err != nil {
		log.Fatalf("Failed to unmarshal proto descriptor: %v", err)
	}
	for _, fdProto := range protoDesc.File {
		fd, err := protodesc.NewFile(fdProto, nil)
		if err != nil {
			continue
		}
		if md := fd.Messages().ByName("LogEntry"); md != nil {
			protoMsgDesc = md
			break
		}
	}
}

// --- File and Writer Management ---
const numWriters = 12

var (
	logChans     [numWriters]chan LogEntry
	logFiles     [numWriters]*os.File
	zstdWriters  [numWriters]*zstd.Encoder
	writerLocks  [numWriters]sync.Mutex
	rotationSize = int64(50 * 1024 * 1024) // 50MB
)

func setupFile() {
	dataDir := "./data"
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
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

// --- Log Processing ---
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

// --- Metrics ---
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

		lastMetrics = MetricsSnapshot{
			LogsPerSec:    rate,
			MBPerSec:      float64(bytesPerSec) / (1024 * 1024),
			LatencyUs:     avgLatency,
			QueueLength:   queueLen,
			FileRotations: rotations,
			Dropped:       dropped,
		}

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

// --- HTTP Dashboard ---
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

// --- Universal TCP server on :3001 ---
func startTCPServerUniversal() {
	cert, err := tls.LoadX509KeyPair("./certs/cert.pem", "./certs/key.pem")
	if err != nil {
		log.Fatalf("Failed to load TLS certs: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	listener, err := tls.Listen("tcp", ":3001", config)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port 3001: %v", err)
	}
	log.Println("TLS TCP (UNIVERSAL) log ingestor listening on :3001")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP Accept error: %v", err)
			continue
		}
		go handleUniversalConnection(conn)
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

// --- Universal Handler ---
func handleUniversalConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 64*1024)
	for {
		// Read header + length
		for len(buf) < 5 {
			n, err := conn.Read(tmp)
			if err != nil {
				return
			}
			buf = append(buf, tmp[:n]...)
		}
		format := buf[0]
		msgLen := binary.BigEndian.Uint32(buf[1:5])
		if msgLen == 0 || msgLen > 10*1024 {
			log.Printf("Invalid message length: %d", msgLen)
			return
		}
		for len(buf) < int(5+msgLen) {
			n, err := conn.Read(tmp)
			if err != nil {
				return
			}
			buf = append(buf, tmp[:n]...)
		}
		msgBuf := buf[5 : 5+msgLen]
		var entry LogEntry
		var err error
		start := time.Now()

		switch format {
		case 0x01: // JSON
			err = json.Unmarshal(msgBuf, &entry)
			if err == nil && jsonSchema != nil {
				result, err2 := jsonSchema.Validate(gojsonschema.NewBytesLoader(msgBuf))
				if err2 != nil || !result.Valid() {
					log.Printf("JSON schema validation failed: %v", result.Errors())
					err = fmt.Errorf("schema validation failed")
				}
			}
		case 0x02: // Protobuf
			if protoMsgDesc != nil {
				pbEntry := dynamicpb.NewMessage(protoMsgDesc)
				err = proto.Unmarshal(msgBuf, pbEntry)
				if err == nil {
					entry = LogEntry{
						Timestamp: pbEntry.Get(pbEntry.Descriptor().Fields().ByName("timestamp")).String(),
						Level:     pbEntry.Get(pbEntry.Descriptor().Fields().ByName("level")).String(),
						Message:   pbEntry.Get(pbEntry.Descriptor().Fields().ByName("message")).String(),
						Service:   pbEntry.Get(pbEntry.Descriptor().Fields().ByName("service")).String(),
					}
				}
			} else {
				err = fmt.Errorf("proto descriptor not loaded")
			}
		case 0x03: // Avro
			if avroCodec != nil {
				var avroMap map[string]interface{}
				err = avro.Unmarshal(avroCodec, msgBuf, &avroMap)
				if err == nil {
					entry = LogEntry{
						Timestamp: toString(avroMap["timestamp"]),
						Level:     toString(avroMap["level"]),
						Message:   toString(avroMap["message"]),
						Service:   toString(avroMap["service"]),
					}
				}
			} else {
				err = fmt.Errorf("avro schema not loaded")
			}
		case 0x04: // Raw text
			entry = parseRawLog(string(msgBuf))
		default:
			log.Printf("Unknown log format: %d", format)
			buf = buf[5+msgLen:]
			continue
		}
		if err != nil {
			log.Printf("Failed to parse log: %v", err)
			buf = buf[5+msgLen:]
			continue
		}
		if err := entry.Validate(); err != nil {
			log.Printf("Invalid log entry: %v", err)
			buf = buf[5+msgLen:]
			continue
		}
		enqueueLog(entry)
		recordLatency(time.Since(start))
		buf = buf[5+msgLen:]
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

func parseRawLog(line string) LogEntry {
	var ts, level, service, msg string
	parts := strings.SplitN(line, " ", 4)
	if len(parts) == 4 {
		ts = parts[0]
		level = strings.Trim(parts[1], "[]")
		service = strings.Trim(parts[2], "[]")
		msg = parts[3]
	} else {
		ts = time.Now().UTC().Format(time.RFC3339)
		level = "INFO"
		service = "raw"
		msg = line
	}
	return LogEntry{
		Timestamp: ts,
		Level:     level,
		Service:   service,
		Message:   msg,
	}
}

// --- Main ---
func main() {
	setupFile()
	fetchAndCacheSchemas()
	for i := 0; i < numWriters; i++ {
		logChans[i] = make(chan LogEntry, 200000)
		go writerWorker(i)
	}
	go metricsLogger()
	go startTCPServerUniversal()
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
