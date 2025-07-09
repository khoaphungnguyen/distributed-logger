package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	batchSize        = 10000                  // Number of logs per batch
	batchFlushTime   = 200 * time.Millisecond // Max wait before sending batch
	batchWorkerCount = 4                      // Number of parallel batch writers

)

// --- Cluster Manager Integration ---
var (
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR") // e.g. "http://cluster-manager:5000"
	currentLeaderAddr  string

	selfAddr string // set at runtime
	isLeader bool

	storageNodes      []string
	storageNodesMutex sync.RWMutex

	batchChan     = make(chan LogEntry, 100000) // Buffer for incoming logs
	batchSendChan = make(chan []LogEntry, batchWorkerCount*2)
)

// Register with dynamic ports
func registerWithClusterManager(addr string, tcpPort, udpPort, healthPort int) {
	body, _ := json.Marshal(map[string]interface{}{
		"address":     addr,
		"type":        "ingestor",
		"tcp_port":    tcpPort,
		"udp_port":    udpPort,
		"health_port": healthPort,
	})
	_, err := http.Post(clusterManagerAddr+"/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Failed to register with cluster manager: %v", err)
	}
	log.Printf("Registered with cluster manager as %s (tcp:%d, udp:%d, health:%d)", addr, tcpPort, udpPort, healthPort)
}

func unregisterWithClusterManager(addr string) {
	body, _ := json.Marshal(map[string]string{"address": addr})
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
}

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
	storageNodesMutex.Unlock()
}

func periodicallyUpdateClusterState() {
	for {
		updateLeaderAndPeers()
		updateStorageNodes()
		time.Sleep(1 * time.Second)
	}
}

// --- LogEntry struct ---
type LogEntry struct {
	Timestamp   string `json:"timestamp"`
	Level       string `json:"level"`
	Message     string `json:"message"`
	Service     string `json:"service"`
	Hostname    string `json:"hostname,omitempty"`
	Environment string `json:"environment,omitempty"`
	AppVersion  string `json:"app_version,omitempty"`
	ReceivedAt  string `json:"received_at,omitempty"`
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

// --- Storage Sharding ---
func pickStorageNode(entry LogEntry) string {
	storageNodesMutex.RLock()
	defer storageNodesMutex.RUnlock()
	if len(storageNodes) == 0 {
		return ""
	}
	key := entry.Service + entry.Timestamp
	h := sha1.Sum([]byte(key))
	idx := int(h[0]) % len(storageNodes)
	return storageNodes[idx]
}

func forwardToStorage(entry LogEntry) error {
	addr := pickStorageNode(entry)
	if addr == "" {
		return fmt.Errorf("no available storage nodes")
	}
	url := addr + "/ingest"
	b, _ := json.Marshal(entry)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("storage error: %s", string(body))
	}
	return nil
}

// --- Metrics ---
type MetricsSnapshot struct {
	LogsPerSec   int64
	MBPerSec     float64
	LatencyUs    float64
	Dropped      int64
	FormatCounts map[string]int64
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
	lastMetrics  MetricsSnapshot
	metricsMutex sync.Mutex
	processCount int64
	totalBytes   int64
	totalLatency int64
	latencyCount int64
	droppedLogs  int64
	rateTicker   = time.NewTicker(1 * time.Second)
	formatCounts = map[string]*int64{
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

// --- Metrics ---
func metricsLogger() {

	for range rateTicker.C {
		metricsMutex.Lock()
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

		formatSnapshot := make(map[string]int64)
		for k, v := range formatCounts {
			formatSnapshot[k] = atomic.LoadInt64(v)
		}

		lastMetrics = MetricsSnapshot{
			LogsPerSec:   rate,
			MBPerSec:     float64(bytesPerSec) / (1024 * 1024),
			LatencyUs:    avgLatency,
			Dropped:      dropped,
			FormatCounts: formatSnapshot,
		}

		atomic.StoreInt64(&processCount, 0)
		atomic.StoreInt64(&totalBytes, 0)
		atomic.StoreInt64(&totalLatency, 0)
		atomic.StoreInt64(&latencyCount, 0)

		log.Printf("[METRIC] Logs/sec: %d, MB/s: %.2f, Latency: %.2fµs, Goroutines: %d, Dropped: %d",
			rate, float64(bytesPerSec)/(1024*1024), avgLatency, numGoroutines, dropped)
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
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsMutex.Lock()
		snap := lastMetrics
		metricsMutex.Unlock()
		sampleMutex.Lock()
		defer sampleMutex.Unlock()
		fmt.Fprintf(w, `{"logs_per_sec": %d, "mb_per_sec": %.2f, "latency_us": %.2f, "dropped": %d`,
			snap.LogsPerSec,
			snap.MBPerSec,
			snap.LatencyUs,
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
	http.ListenAndServe(":3000", mux)
}

// --- Universal TCP server with dynamic port ---
func startTCPServerUniversalOnListener(listener net.Listener) {
	cert, err := tls.LoadX509KeyPair("./certs/cert.pem", "./certs/key.pem")
	if err != nil {
		log.Fatalf("Failed to load TLS certs: %v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	tlsListener := tls.NewListener(listener, config)
	log.Printf("TLS TCP (UNIVERSAL) log ingestor listening on %s", listener.Addr().String())

	for {
		conn, err := tlsListener.Accept()
		if err != nil {
			log.Printf("TCP Accept error: %v", err)
			continue
		}
		// Limit concurrent connections to avoid goroutine explosion
		go handleUniversalConnection(conn)
	}
}

// --- JSON UDP server with dynamic port ---
func startUDPServerOnConn(conn *net.UDPConn) {
	log.Printf("UDP log ingestor listening on %s", conn.LocalAddr().String())

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

			enqueueLog(entry, len(line))
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
		enqueueLog(entry, len(msgBuf))
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

func parseSyslog(line string) LogEntry {
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
	year := time.Now().Year()
	parsed, err := time.Parse("Jan 2 15:04:05", ts)
	if err != nil {
		parsed, err = time.Parse("Jan  2 15:04:05", ts)
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

// --- Main ---
var shuttingDown int32

func main() {
	// Pick random TCP port for log ingestion
	tcpListener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen on TCP: %v", err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port

	// Pick random UDP port for log ingestion
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		log.Fatalf("Failed to resolve UDP: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	udpPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	// Pick random port for cluster HTTP health endpoint
	healthListener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen for health endpoint: %v", err)
	}
	healthPort := healthListener.Addr().(*net.TCPAddr).Port

	hostname, _ := os.Hostname()
	selfAddr = fmt.Sprintf("http://%s", hostname)

	// Register all ports (including health_port)
	registerWithClusterManager(selfAddr, tcpPort, udpPort, healthPort)

	go periodicallyUpdateClusterState()
	go startTCPServerUniversalOnListener(tcpListener)
	go startUDPServerOnConn(udpConn)
	go startHTTPServer()
	go startClusterHTTPServerOnListener(healthListener)
	fetchAndCacheSchemas()
	go metricsLogger()
	go batchDispatcher()
	for i := 0; i < batchWorkerCount; i++ {
		go batchWorker()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	atomic.StoreInt32(&shuttingDown, 1)
	log.Println("Shutting down...")
}

// Use this for the health endpoint
func startClusterHTTPServerOnListener(listener net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	log.Printf("Cluster HTTP endpoints available at %s (listening on %s)", selfAddr, listener.Addr().String())
	err := http.Serve(listener, mux)
	if err != nil {
		log.Fatalf("Failed to start cluster HTTP server: %v", err)
	}
}

// --- Enqueue Functions ---
func enqueueLog(entry LogEntry, entrySize int) {
	if atomic.LoadInt32(&shuttingDown) == 1 {
		return
	}
	if isLeader {
		atomic.AddInt64(&processCount, 1)
		atomic.AddInt64(&totalBytes, int64(entrySize))
		select {
		case batchChan <- entry:
		default:
			atomic.AddInt64(&droppedLogs, 1) // Drop if buffer full
		}
	} else {
		atomic.AddInt64(&droppedLogs, 1)
	}
}

// Forward a batch of logs to the storage nodes
func forwardBatchToStorage(entries []LogEntry) error {
	addr := ""
	if len(entries) > 0 {
		addr = pickStorageNode(entries[0])
	}
	if addr == "" {
		return fmt.Errorf("no available storage nodes")
	}
	url := addr + "/ingest/batch"
	b, _ := json.Marshal(entries)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("storage error: %s", string(body))
	}
	return nil
}

func batchDispatcher() {
	batch := make([]LogEntry, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Send a copy to avoid data race
		batchCopy := make([]LogEntry, len(batch))
		copy(batchCopy, batch)
		batchSendChan <- batchCopy
		batch = batch[:0]
	}
	timer := time.NewTimer(batchFlushTime)
	for {
		select {
		case entry := <-batchChan:
			batch = append(batch, entry)
			if len(batch) >= batchSize {
				flush()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(batchFlushTime)
			}
		case <-timer.C:
			flush()
			timer.Reset(batchFlushTime)
		}
	}
}

func batchWorker() {
	for entries := range batchSendChan {
		if err := forwardBatchToStorage(entries); err != nil {
			log.Printf("Failed to forward batch to storage: %v", err)
		}
	}
}
