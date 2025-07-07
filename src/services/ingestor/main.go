package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
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

// --- Cluster Manager Integration ---
var (
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR") // e.g. "http://cluster-manager:5000"
	currentLeaderAddr  string

	selfAddr   = os.Getenv("NODE_ADDR") // e.g. "http://ingestor-1:4001"
	isLeader   bool
	peers      = make(map[string]bool)
	peersMutex sync.Mutex
)

func registerWithClusterManager() {
	body, _ := json.Marshal(map[string]string{"address": selfAddr})
	_, err := http.Post(clusterManagerAddr+"/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Failed to register with cluster manager: %v", err)
	}
	log.Printf("Registered with cluster manager as %s", selfAddr)
}

func unregisterWithClusterManager() {
	body, _ := json.Marshal(map[string]string{"address": selfAddr})
	http.Post(clusterManagerAddr+"/nodes/unregister", "application/json", bytes.NewReader(body))
	log.Printf("Unregistered from cluster manager")
}

func updateLeaderAndPeers() {
	resp, err := http.Get(clusterManagerAddr + "/leader")
	if err != nil {
		log.Printf("Failed to get leader: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Cluster manager /leader error: %s", string(body))
		return
	}
	var data struct {
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode leader response: %v", err)
		return
	}
	currentLeaderAddr = data.Leader
	isLeader = (data.Leader == selfAddr)

	// Update peers (healthy nodes except self)
	resp2, err := http.Get(clusterManagerAddr + "/nodes")
	if err != nil {
		log.Printf("Failed to get nodes: %v", err)
		return
	}
	defer resp2.Body.Close()
	var nodes []struct {
		Address   string `json:"address"`
		IsHealthy bool   `json:"is_healthy"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&nodes); err != nil {
		log.Printf("Failed to decode nodes: %v", err)
		return
	}
	peersMutex.Lock()
	peers = make(map[string]bool)
	for _, n := range nodes {
		if n.Address != selfAddr && n.IsHealthy {
			peers[n.Address] = true
		}
	}
	peersMutex.Unlock()
}

func periodicallyUpdateClusterState() {
	for {
		updateLeaderAndPeers()
		time.Sleep(1 * time.Second)
	}
}

// Replication batching
var (
	replicationQueue    = make(chan LogEntry, 100000)
	replicationBatchSz  = 1000
	replicationInterval = 50 * time.Millisecond
)

func replicationWorker() {
	batch := make([]LogEntry, 0, replicationBatchSz)
	ticker := time.NewTicker(replicationInterval)
	defer ticker.Stop()
	for {
		select {
		case entry := <-replicationQueue:
			batch = append(batch, entry)
			if len(batch) >= replicationBatchSz {
				sendReplicationBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				sendReplicationBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// In sendReplicationBatch, set ReplicatedAt for each entry:
func sendReplicationBatch(batch []LogEntry) {
	peersMutex.Lock()
	defer peersMutex.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	// Set ReplicatedAt for each entry in the batch
	for i := range batch {
		batch[i].ReplicatedAt = now
	}
	data, _ := json.Marshal(batch)
	for addr := range peers {
		go func(addr string) {
			resp, err := http.Post(addr+"/replicate", "application/json", bytes.NewReader(data))
			if err != nil {
				log.Printf("Replication to %s failed: %v", addr, err)
				updateLeaderAndPeers()
				return
			}
			resp.Body.Close()
		}(addr)
	}
}

func replicateLogToPeers(entry LogEntry) {
	select {
	case replicationQueue <- entry:
	default:
		log.Printf("Replication queue full, dropping log")
	}
}

// --- Replication Handler ---
func replicateHandler(w http.ResponseWriter, r *http.Request) {
	if isLeader {
		http.Error(w, "Leader does not accept replication", http.StatusForbidden)
		return
	}
	var entries []LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
		http.Error(w, "Invalid log entry batch", 400)
		return
	}
	for _, entry := range entries {
		enqueueReplicatedLog(entry)
	}
	w.WriteHeader(http.StatusOK)
}

// Add to LogEntry struct:
type LogEntry struct {
	Timestamp    string `json:"timestamp"`
	Level        string `json:"level"`
	Message      string `json:"message"`
	Service      string `json:"service"`
	Hostname     string `json:"hostname,omitempty"`
	Environment  string `json:"environment,omitempty"`
	AppVersion   string `json:"app_version,omitempty"`
	ReceivedAt   string `json:"received_at,omitempty"`
	ReplicatedAt string `json:"replicated_at,omitempty"` // <-- Add this line
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
	allowedLevels := map[string]bool{
		"INFO": true, "WARN": true, "WARNING": true, "ERROR": true, "DEBUG": true,
		"EMERG": true, "ALERT": true, "CRIT": true, "ERR": true, "NOTICE": true,
	}
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
	FormatCounts  map[string]int64
}

var (
	sampleMutex sync.Mutex
	sampleLogs  = map[string][]string{
		"json":     {},
		"proto":    {},
		"avro":     {},
		"raw":      {},
		"syslog":   {},
		"journald": {},
	}
	sampleLimit = 3 // Number of samples to keep per format
)

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
	formatCounts      = map[string]*int64{
		"json":     new(int64),
		"proto":    new(int64),
		"avro":     new(int64),
		"raw":      new(int64),
		"syslog":   new(int64),
		"journald": new(int64),
	}
)

// --- Schema Registry Integration ---
var (
	jsonSchema   *gojsonschema.Schema
	avroCodec    avro.Schema
	protoMsgDesc protoreflect.MessageDescriptor
)

func fetchAndCacheSchemas() {
	// JSON Schema
	resp, err := http.Get("http://schema-validator:8000/schema/get?format=json&name=LogEntry")
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
	resp, err = http.Get("http://schema-validator:8000/schema/get?format=avro&name=LogEntry")
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
	resp, err = http.Get("http://schema-validator:8000/schema/descriptor?name=LogEntry")
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
const numWriters = 4

var (
	logChans     [numWriters]chan LogEntry
	logFiles     [numWriters]*os.File
	zstdWriters  [numWriters]*zstd.Encoder
	writerLocks  [numWriters]sync.Mutex
	rotationSize = int64(50 * 1024 * 1024) // 50MB
	outputFormat string
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
	filePath := filepath.Join("./data", fmt.Sprintf("log_%d_%s.%s.zst", idx, timestamp, outputFormat))
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
		var line string
		switch outputFormat {
		case "csv":
			line = fmt.Sprintf("%q,%q,%q,%q\n", entry.Timestamp, entry.Level, entry.Service, entry.Message)
		case "text":
			line = fmt.Sprintf("%s [%s] [%s] %s\n", entry.Timestamp, entry.Level, entry.Service, entry.Message)
		default: // jsonl
			b, _ := json.Marshal(entry)
			line = string(b) + "\n"
		}
		zstdWriters[idx].Write([]byte(line))
		atomic.AddInt64(&totalBytes, int64(len(line)))
	}
	zstdWriters[idx].Flush()
}

// --- Log Processing ---
func hash(entry LogEntry) int {
	return int(crc32.ChecksumIEEE([]byte(entry.Timestamp+entry.Service))) % numWriters
}

func writerWorker(idx int) {
	rotateLogFile(idx)
	buffer := make([]LogEntry, 0, 100000)
	flushTimer := time.NewTicker(100 * time.Millisecond)
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
			if len(buffer) >= 1000 || shouldRotate(idx) {
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

		formatSnapshot := make(map[string]int64)
		for k, v := range formatCounts {
			formatSnapshot[k] = atomic.LoadInt64(v)
		}

		lastMetrics = MetricsSnapshot{
			LogsPerSec:    rate,
			MBPerSec:      float64(bytesPerSec) / (1024 * 1024),
			LatencyUs:     avgLatency,
			QueueLength:   queueLen,
			FileRotations: rotations,
			Dropped:       dropped,
			FormatCounts:  formatSnapshot,
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

func recordSample(format string, entry LogEntry) {
	sampleMutex.Lock()
	defer sampleMutex.Unlock()
	b, _ := json.Marshal(entry)
	lines := sampleLogs[format]
	lines = append(lines, string(b))
	if len(lines) > sampleLimit {
		lines = lines[len(lines)-sampleLimit:]
	}
	sampleLogs[format] = lines
}

// --- HTTP Dashboard ---
func startHTTPServer() {
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsMutex.Lock()
		snap := lastMetrics
		metricsMutex.Unlock()
		sampleMutex.Lock()
		defer sampleMutex.Unlock()
		fmt.Fprintf(w, `{"logs_per_sec": %d, "mb_per_sec": %.2f, "latency_us": %.2f, "queue_length": %d, "file_rotations": %d, "dropped": %d`,
			snap.LogsPerSec,
			snap.MBPerSec,
			snap.LatencyUs,
			snap.QueueLength,
			snap.FileRotations,
			snap.Dropped,
		)
		for k, v := range snap.FormatCounts {
			fmt.Fprintf(w, `,"%s":%d`, k, v)
		}
		// Add samples
		fmt.Fprintf(w, `,"samples":{`)
		first := true
		for k, v := range sampleLogs {
			if !first {
				fmt.Fprint(w, ",")
			}
			first = false
			fmt.Fprintf(w, `"%s":[`, k)
			for i, s := range v {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, "%q", s)
			}
			fmt.Fprint(w, "]")
		}
		fmt.Fprint(w, "}}")
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
				log.Printf("entry %v", entry)
				log.Printf("Invalid log entry: %v", err)
				continue
			}
			entry.Hostname, _ = os.Hostname()
			entry.Environment = os.Getenv("ENVIRONMENT")
			entry.AppVersion = os.Getenv("APP_VERSION")
			entry.ReceivedAt = time.Now().UTC().Format(time.RFC3339)
			recordSample("json", entry)

			enqueueLog(entry)
			recordLatency(time.Since(start))
			incrementFormatCount("json")
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
		formatType := ""

		switch format {
		case 0x01: // JSON
			formatType = "json"
			err = json.Unmarshal(msgBuf, &entry)
			if err == nil && jsonSchema != nil {
				result, err2 := jsonSchema.Validate(gojsonschema.NewBytesLoader(msgBuf))
				if err2 != nil || !result.Valid() {
					log.Printf("JSON schema validation failed: %v", result.Errors())
					err = fmt.Errorf("schema validation failed")
				}
			}
		case 0x02: // Protobuf
			formatType = "proto"
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
			formatType = "avro"
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
			formatType = detectLogFormat(string(msgBuf))
			switch formatType {
			case "syslog":
				entry = parseSyslog(string(msgBuf))
			case "journald":
				entry = parseJournald(string(msgBuf))
			default:
				entry = parseRawLog(string(msgBuf))
			}
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
			log.Printf("entry before validation: %+v", entry)
			log.Printf("Invalid log entry: %v", err)
			buf = buf[5+msgLen:]
			continue
		}
		entry.ReceivedAt = time.Now().UTC().Format(time.RFC3339)
		entry.Hostname, _ = os.Hostname()
		entry.Environment = os.Getenv("ENVIRONMENT")
		if entry.Environment == "" {
			entry.Environment = "unknown"
		}
		entry.AppVersion = os.Getenv("APP_VERSION")
		if entry.AppVersion == "" {
			entry.AppVersion = "unknown"
		}
		recordSample(formatType, entry)
		enqueueLog(entry)
		recordLatency(time.Since(start))
		incrementFormatCount(formatType)
		buf = buf[5+msgLen:]
	}
}

func incrementFormatCount(format string) {
	if c, ok := formatCounts[format]; ok {
		atomic.AddInt64(c, 1)
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
		// Try to parse as just a message, fallback to now
		log.Printf("parseRawLog: unexpected format: %q", line)
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

func detectLogFormat(line string) string {
	if strings.Contains(line, "journal") || strings.Contains(line, "_SYSTEMD_UNIT") {
		return "journald"
	}
	// Syslog: starts with <PRI>Mon ... (e.g., <6>Jul  2 17:00:01 ...)
	if len(line) > 5 && line[0] == '<' {
		for i := 1; i < len(line) && i < 5; i++ {
			if line[i] == '>' && i > 1 {
				// Looks like <number>
				months := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
				for _, m := range months {
					if strings.HasPrefix(line[i+1:], m) {
						return "syslog"
					}
				}
			}
		}
	}
	return "raw"
}

var syslogRegex = regexp.MustCompile(`^<(\d+)>([A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(\S+)\[(\d+)\]:\s+(.*)$`)
var journaldRegex = regexp.MustCompile(`"PRIORITY"\s*:\s*"(\d+)"`)

func parseSyslog(line string) LogEntry {
	// Example: <6>Jul  2 17:00:01 host service[1234]: message
	m := syslogRegex.FindStringSubmatch(line)
	ts := time.Now().UTC().Format(time.RFC3339)
	level := "INFO"
	service := "syslog"
	msg := line
	if len(m) == 7 {
		pri, _ := strconv.Atoi(m[1])
		level = syslogLevelFromPRI(pri)
		parsedTs := parseSyslogTimestamp(m[2])
		if parsedTs != "" {
			ts = parsedTs
		}
		service = m[4]
		msg = m[6]
	} else {
		log.Printf("Syslog regex did not match: %q", line)
	}
	return LogEntry{
		Timestamp: ts,
		Level:     level,
		Service:   service,
		Message:   msg,
	}
}
func parseSyslogTimestamp(ts string) string {
	// Parse "Jul  2 17:00:01" to RFC3339 using current year
	year := time.Now().Year()
	parsed, err := time.Parse("Jan 2 15:04:05", ts)
	if err != nil {
		parsed, err = time.Parse("Jan  2 15:04:05", ts) // handle double space
		if err != nil {
			return ""
		}
	}
	parsed = parsed.AddDate(year-parsed.Year(), 0, 0)
	return parsed.Format(time.RFC3339)
}

func syslogLevelFromPRI(pri int) string {
	levels := []string{"EMERG", "ALERT", "CRIT", "ERR", "WARNING", "NOTICE", "INFO", "DEBUG"}
	if pri >= 0 && pri < len(levels) {
		return levels[pri]
	}
	return "INFO"
}

func parseJournald(line string) LogEntry {
	// Try to parse JSON
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(line), &m)
	ts := time.Now().UTC().Format(time.RFC3339)
	level := "INFO"
	service := "journald"
	msg := line
	if v, ok := m["PRIORITY"]; ok {
		level = syslogLevelFromPRI(parsePriority(v))
	}
	if v, ok := m["_SYSTEMD_UNIT"]; ok {
		service = fmt.Sprintf("%v", v)
	}
	if v, ok := m["MESSAGE"]; ok {
		msg = fmt.Sprintf("%v", v)
	}
	if v, ok := m["__REALTIME_TIMESTAMP"]; ok {
		ts = parseJournaldTimestamp(v)
	}
	return LogEntry{
		Timestamp: ts,
		Level:     level,
		Service:   service,
		Message:   msg,
	}
}

func parsePriority(v interface{}) int {
	switch vv := v.(type) {
	case string:
		i, _ := strconv.Atoi(vv)
		return i
	case float64:
		return int(vv)
	default:
		return 6 // info
	}
}

func parseJournaldTimestamp(v interface{}) string {
	switch vv := v.(type) {
	case string:
		i, _ := strconv.ParseInt(vv, 10, 64)
		return time.Unix(0, i*1000).Format(time.RFC3339)
	case float64:
		return time.Unix(0, int64(vv)*1000).Format(time.RFC3339)
	default:
		return time.Now().UTC().Format(time.RFC3339)
	}
}
func startClusterHTTPServer() {
	http.HandleFunc("/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	http.HandleFunc("/replicate", replicateHandler) // <-- Register here!

	u, err := url.Parse(selfAddr)
	if err != nil {
		log.Fatalf("Invalid selfAddr: %v", err)
	}
	host := u.Host
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		log.Fatalf("Could not parse port from selfAddr (%s): %v", selfAddr, err)
	}
	addr := ":" + port

	log.Printf("Cluster HTTP endpoints available at %s (listening on %s)", selfAddr, addr)
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatalf("Failed to start cluster HTTP server: %v", err)
		}
	}()
}

var shuttingDown int32

// --- Main ---
func main() {
	var outFmt string
	flag.StringVar(&outFmt, "out", "jsonl", "Output format: jsonl, csv, or text")
	flag.Parse()
	outputFormat = outFmt

	startClusterHTTPServer()
	go replicationWorker()

	// Register with cluster manager
	registerWithClusterManager()
	defer unregisterWithClusterManager()

	// Periodically update leader/peer state
	go periodicallyUpdateClusterState()

	// Start your log ingestion servers (TCP/UDP/HTTP)
	go startTCPServerUniversal()
	go startUDPServer()
	go startHTTPServer()

	setupFile()
	fetchAndCacheSchemas()
	for i := 0; i < numWriters; i++ {
		logChans[i] = make(chan LogEntry, 200000)
		go writerWorker(i)
	}
	go metricsLogger()
	// Only leader should accept logs from clients

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	atomic.StoreInt32(&shuttingDown, 1)
	log.Println("Shutting down, flushing logs...")
	for i := 0; i < numWriters; i++ {
		close(logChans[i])
	}
	for i := 0; i < numWriters; i++ {
		closeLogFile(i)
	}
}

// --- Enqueue Functions ---
func enqueueLog(entry LogEntry) {
	if atomic.LoadInt32(&shuttingDown) == 1 {
		return
	}
	if isLeader {
		idx := hash(entry)
		select {
		case logChans[idx] <- entry:
			// enqueued
			replicateLogToPeers(entry)
		default:
			dropped := atomic.AddInt64(&droppedLogs, 1)
			if dropped%1000 == 0 {
				log.Printf("[OVERLOAD] Log dropped! Total dropped: %d", dropped)
			}
		}
	} else {
		// If not leader, drop or reject logs from clients
		log.Printf("Not leader, dropping log: %+v", entry)
	}
}

// In enqueueReplicatedLog, record latency if ReplicatedAt is set:
func enqueueReplicatedLog(entry LogEntry) {
	if atomic.LoadInt32(&shuttingDown) == 1 {
		return
	}
	idx := hash(entry)
	select {
	case logChans[idx] <- entry:
		// enqueued, counted in metrics
		if entry.ReplicatedAt != "" {
			if t, err := time.Parse(time.RFC3339, entry.ReplicatedAt); err == nil {
				recordLatency(time.Since(t))
			}
		}
	default:
		dropped := atomic.AddInt64(&droppedLogs, 1)
		if dropped%1000 == 0 {
			log.Printf("[OVERLOAD] Log dropped! Total dropped: %d", dropped)
		}
	}
}
