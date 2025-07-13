package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
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

// ========== RAFT STATE & LEADER ELECTION ==========

type RaftState string

const (
	StateFollower  RaftState = "follower"
	StateCandidate RaftState = "candidate"
	StateLeader    RaftState = "leader"
)

var (
	raftState      = StateFollower
	raftTerm       = 0
	votedFor       = ""
	raftMutex      sync.Mutex
	peerAddrs      []string
	peerAddrsMutex sync.RWMutex

	lastHeartbeat   = time.Now()
	electionResetCh = make(chan struct{}, 1)
	selfAddr        string
)

// --- Raft Heartbeat Handler ---
func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Term   int    `json:"term"`
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	raftMutex.Lock()
	defer raftMutex.Unlock()
	if req.Term >= raftTerm {
		raftTerm = req.Term
		raftState = StateFollower
		votedFor = req.Leader
		select {
		case electionResetCh <- struct{}{}:
		default:
		}
	}
	w.WriteHeader(http.StatusOK)
}

// --- Raft Heartbeat Sender (Leader only) ---
func sendHeartbeats() {
	for {
		raftMutex.Lock()
		isLeader := raftState == StateLeader
		term := raftTerm
		raftMutex.Unlock()
		if !isLeader {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		peerAddrsMutex.RLock()
		peers := append([]string{}, peerAddrs...)
		peerAddrsMutex.RUnlock()
		for _, peer := range peers {
			go func(addr string) {
				body, _ := json.Marshal(map[string]interface{}{
					"term":   term,
					"leader": selfAddr,
				})
				http.Post(addr+"/raft/heartbeat", "application/json", bytes.NewReader(body))
			}(peer)
		}
		time.Sleep(75 * time.Millisecond)
	}
}

// --- Raft Peer Discovery ---
func updatePeers() {
	resp, err := http.Get(clusterManagerAddr + "/ingestor-nodes")
	if err != nil {
		log.Printf("Failed to get peers: %v", err)
		return
	}
	defer resp.Body.Close()
	var nodes []struct {
		Address    string `json:"address"`
		HealthPort int    `json:"health_port"`
		IsHealthy  bool   `json:"is_healthy"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		log.Printf("Failed to decode peers: %v", err)
		return
	}
	var addrs []string
	for _, n := range nodes {
		if n.IsHealthy && n.Address != selfAddr {
			addrs = append(addrs, n.Address)
		}
	}
	sort.Strings(addrs)
	//log.Println("[RAFT] Discovered peers:", addrs)
	peerAddrsMutex.Lock()
	if !equalStringSlices(peerAddrs, addrs) {
		log.Printf("[RAFT] Peer list updated: %v", addrs)
	}
	peerAddrs = addrs
	peerAddrsMutex.Unlock()
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- Raft Election Loop ---
func raftElectionLoop() {
	timeoutBase := 300
	for {
		timeout := time.Duration(timeoutBase+rand.Intn(150)) * time.Millisecond
		raftMutex.Lock()
		state := raftState
		raftMutex.Unlock()
		if state == StateLeader {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		select {
		case <-electionResetCh:
			// Heartbeat received, reset timer
			continue
		case <-time.After(timeout):
			log.Printf("[RAFT] Election timeout, starting election (term %d)", raftTerm+1)
		}
		raftMutex.Lock()
		raftTerm++
		raftState = StateCandidate
		votedFor = selfAddr
		term := raftTerm
		raftMutex.Unlock()

		peerAddrsMutex.RLock()
		peers := append([]string{}, peerAddrs...)
		peerAddrsMutex.RUnlock()

		var wg sync.WaitGroup
		voteCh := make(chan bool, len(peers))
		for _, peer := range peers {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				ok := requestVote(addr, term, selfAddr)
				voteCh <- ok
			}(peer)
		}
		wg.Wait()
		close(voteCh)
		votes := 1
		for v := range voteCh {
			if v {
				votes++
			}
		}
		if votes > (len(peers)+1)/2 {
			raftMutex.Lock()
			raftState = StateLeader
			log.Printf("[RAFT] Became leader for term %d", raftTerm)
			raftMutex.Unlock()
			notifyClusterManagerOfLeadership()
			go sendHeartbeats()
		} else {
			raftMutex.Lock()
			raftState = StateFollower
			raftMutex.Unlock()
		}
	}
}

// --- Raft Vote Handler ---
func voteHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Term      int    `json:"term"`
		Candidate string `json:"candidate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	raftMutex.Lock()
	defer raftMutex.Unlock()
	granted := false
	if req.Term > raftTerm {
		raftTerm = req.Term
		raftState = StateFollower
		votedFor = ""
	}
	if req.Term == raftTerm && (votedFor == "" || votedFor == req.Candidate) {
		granted = true
		votedFor = req.Candidate
	}
	json.NewEncoder(w).Encode(map[string]bool{"voteGranted": granted})
}

// --- Raft Vote Client ---
func requestVote(addr string, term int, candidate string) bool {
	body, _ := json.Marshal(map[string]interface{}{
		"term":      term,
		"candidate": candidate,
	})
	resp, err := http.Post(addr+"/raft/vote", "application/json", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var res struct{ VoteGranted bool }
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false
	}
	return res.VoteGranted
}

// ========== CLUSTER MANAGER INTEGRATION ==========

var (
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR")
	currentLeaderAddr  string
	isLeader           bool
	storageNodes       []string
	storageNodesMutex  sync.RWMutex
	storageRing        *hashRing
)

func registerWithClusterManager(addr string, tcpPort, udpPort int) {
	body, _ := json.Marshal(map[string]interface{}{
		"address":  addr,
		"type":     "ingestor",
		"tcp_port": tcpPort,
		"udp_port": udpPort,
	})
	_, err := http.Post(clusterManagerAddr+"/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Failed to register with cluster manager: %v", err)
	}
	log.Printf("Registered with cluster manager as %s (tcp:%d, udp:%d)", addr, tcpPort, udpPort)
}

func unregisterWithClusterManager(addr string) {
	body, _ := json.Marshal(map[string]string{"address": addr})
	http.Post(clusterManagerAddr+"/nodes/unregister", "application/json", bytes.NewReader(body))
	log.Printf("Unregistered from cluster manager")
}

func notifyClusterManagerOfLeadership() {
	body, _ := json.Marshal(map[string]interface{}{
		"address": selfAddr,
		"term":    raftTerm,
	})
	http.Post(clusterManagerAddr+"/nodes/raft-leader", "application/json", bytes.NewReader(body))
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
	storageRing = newHashRing(addrs, 100)
	storageNodesMutex.Unlock()
}

func periodicallyUpdateClusterState() {
	for {
		updateStorageNodes()
		updatePeers()
		time.Sleep(200 * time.Millisecond)
	}
}

// ========== CONSISTENT HASHING ==========

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
			hash := crc32Hash(key)
			hr.keys = append(hr.keys, hash)
			hr.hashMap[hash] = node
		}
	}
	sort.Slice(hr.keys, func(i, j int) bool { return hr.keys[i] < hr.keys[j] })
}

// Helper: Get N unique nodes from the hash ring, starting from the hash of the key
func (hr *hashRing) getNodes(key string, n int) []string {
	if len(hr.keys) == 0 || n <= 0 {
		return nil
	}
	hash := crc32Hash(key)
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

func crc32Hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

func pickStorageNodes(entry LogEntry, n int) []string {
	storageNodesMutex.RLock()
	defer storageNodesMutex.RUnlock()
	if storageRing == nil || len(storageNodes) == 0 {
		return nil
	}
	key := entry.Service
	return storageRing.getNodes(key, n)
}

// ========== LOG ENTRY & VALIDATION ==========

type LogEntry struct {
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	Message    string `json:"message"`
	Service    string `json:"service"`
	Hostname   string `json:"hostname,omitempty"`
	AppVersion string `json:"app_version,omitempty"`
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

// ========== BATCHING & FORWARDING ==========

const (
	partitionCount = 4
	batchSize      = 50000
	batchFlushTime = 500 * time.Millisecond
)

var (
	partitionChans      [partitionCount]chan LogEntry
	partitionBatchChans [partitionCount]chan []LogEntry
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

func partitionIndex(entry LogEntry) int {
	h := sha1.Sum([]byte(entry.Service))
	return int(h[0]) % partitionCount
}

func enqueueLog(entry LogEntry) {
	if atomic.LoadInt32(&shuttingDown) == 1 {
		return
	}
	idx := partitionIndex(entry)
	select {
	case partitionChans[idx] <- entry:
	default:
		atomic.AddInt64(&droppedLogs, 1)
	}
}

func partitionBatchDispatcher(idx int) {
	batch := make([]LogEntry, 0, batchSize)
	timer := time.NewTimer(batchFlushTime)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		partitionBatchChans[idx] <- batch
		batch = make([]LogEntry, 0, batchSize)
	}
	for {
		select {
		case entry := <-partitionChans[idx]:
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

func partitionBatchWorker(idx int) {
	replicaCount := len(storageNodes)
	quorum := (replicaCount + 1) / 2

	for entries := range partitionBatchChans[idx] {
		nodeBatches := make(map[string][]LogEntry)
		for _, entry := range entries {
			replicas := pickStorageNodes(entry, replicaCount)
			for _, addr := range replicas {
				nodeBatches[addr] = append(nodeBatches[addr], entry)
			}
		}

		type result struct {
			err  error
			size int
		}
		resultCh := make(chan result, len(nodeBatches))
		var wg sync.WaitGroup

		for addr, logs := range nodeBatches {
			wg.Add(1)
			go func(addr string, logs []LogEntry) {
				defer wg.Done()
				b, _ := json.Marshal(logs)
				url := addr + "/ingest/batch"
				resp, err := httpClient.Post(url, "application/json", bytes.NewReader(b))
				if err != nil {
					resultCh <- result{err: err, size: 0}
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					body, _ := ioutil.ReadAll(resp.Body)
					resultCh <- result{err: fmt.Errorf("storage error: %s", string(body)), size: 0}
					return
				}
				resultCh <- result{err: nil, size: len(b)}
			}(addr, logs)
		}

		acks := 0
		sentBytes := 0
		for i := 0; i < len(nodeBatches); i++ {
			res := <-resultCh
			if res.err == nil {
				acks++
				sentBytes += res.size
				if acks >= quorum {
					break
				}
			}
		}
		wg.Wait()
		if acks >= quorum {
			atomic.AddInt64(&processCount, int64(len(entries)))
			atomic.AddInt64(&totalBytes, int64(sentBytes))
		} else {
			log.Printf("Partition %d: Write quorum failed: got %d/%d acks", idx, acks, quorum)
		}
	}
}

// ========== METRICS & DASHBOARD ==========

var (
	metricsMutex   sync.Mutex
	processCount   int64
	totalBytes     int64
	totalLatency   int64
	latencyCount   int64
	droppedLogs    int64
	rateTicker     = time.NewTicker(1 * time.Second)
	lastLogsPerSec int64 // stores the last calculated logs/sec
	formatCounts   = map[string]*int64{
		"json":     new(int64),
		"proto":    new(int64),
		"avro":     new(int64),
		"raw":      new(int64),
		"syslog":   new(int64),
		"journald": new(int64),
	}
)

func metricsLogger() {
	for range rateTicker.C {

		rate := atomic.LoadInt64(&processCount)
		// bytesPerSec := atomic.LoadInt64(&totalBytes)
		// totalLat := atomic.LoadInt64(&totalLatency)
		// latCount := atomic.LoadInt64(&latencyCount)
		// dropped := atomic.LoadInt64(&droppedLogs)
		// avgLatency := float64(0)
		// if latCount > 0 {
		// 	avgLatency = float64(totalLat) / float64(latCount)
		// }

		// log.Printf("[METRIC] Logs/sec: %d, MB/s: %.2f, Latency: %.2fµs, Dropped: %d",
		// 	rate, float64(bytesPerSec)/(1024*1024), avgLatency, dropped)
		atomic.StoreInt64(&lastLogsPerSec, rate)
		atomic.StoreInt64(&processCount, 0)
		atomic.StoreInt64(&totalBytes, 0)
		atomic.StoreInt64(&totalLatency, 0)
		atomic.StoreInt64(&latencyCount, 0)
	}
}

func recordLatency(d time.Duration) {
	atomic.AddInt64(&totalLatency, d.Microseconds())
	atomic.AddInt64(&latencyCount, 1)
}

func getResourceMetrics() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return map[string]interface{}{
		"cpu_count":  runtime.NumCPU(),
		"goroutines": runtime.NumGoroutine(),
		"mem_alloc":  m.Alloc,
		"mem_sys":    m.Sys,
		"mem_heap":   m.HeapAlloc,
		"pid":        os.Getpid(),
		"hostname":   func() string { h, _ := os.Hostname(); return h }(),
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	// Aggregate current values on-demand
	rate := atomic.LoadInt64(&lastLogsPerSec)
	totalLat := atomic.LoadInt64(&totalLatency)
	latCount := atomic.LoadInt64(&latencyCount)
	dropped := atomic.LoadInt64(&droppedLogs)

	avgLatency := float64(0)
	if latCount > 0 {
		avgLatency = float64(totalLat) / float64(latCount)
	}

	metrics := map[string]interface{}{
		"service_type": "ingestor",
		"service_id":   selfAddr,
		"logs_per_sec": rate,
		"latency_us":   avgLatency,
		"dropped":      dropped,
		"resource":     getResourceMetrics(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// ========== UNIVERSAL LOG INGESTION SERVERS ==========

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
		go handleUniversalConnection(conn)
	}
}

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
			entry.AppVersion = os.Getenv("APP_VERSION")
			enqueueLog(entry)
			recordLatency(time.Since(start))

		}
		if err := scanner.Err(); err != nil {
			log.Printf("UDP scanner error: %v", err)
		}
	}
}

func handleUniversalConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 64*1024)
	for {
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
		entry.Hostname, _ = os.Hostname()
		entry.AppVersion = os.Getenv("APP_VERSION")
		if entry.AppVersion == "" {
			entry.AppVersion = "unknown"
		}
		enqueueLog(entry)
		recordLatency(time.Since(start))
		buf = buf[5+msgLen:]
	}
}

// ========== LOG FORMAT PARSING HELPERS ==========

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

// ========== MAIN ==========

var shuttingDown int32

func main() {
	rand.Seed(time.Now().UnixNano() + int64(os.Getpid()))
	tcpListener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen on TCP: %v", err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port

	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		log.Fatalf("Failed to resolve UDP: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	udpPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	ingestorName := os.Getenv("INGESTOR_NAME")
	if ingestorName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal("STORAGE_NAME must be set and hostname could not be determined")
		}
		ingestorName = hostname
	}
	ln, err := net.Listen("tcp", ":0") // random port
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		log.Fatalf("Failed to parse port: %v", err)
	}
	addr := fmt.Sprintf("http://%s:%s", ingestorName, port)
	selfAddr = addr

	registerWithClusterManager(selfAddr, tcpPort, udpPort)
	go periodicallyUpdateClusterState()
	go raftElectionLoop()
	go startTCPServerUniversalOnListener(tcpListener)
	go startUDPServerOnConn(udpConn)
	go startClusterHTTPServerOnListener(ln)
	fetchAndCacheSchemas()
	go metricsLogger()
	for i := 0; i < partitionCount; i++ {
		partitionChans[i] = make(chan LogEntry, 200000)
		partitionBatchChans[i] = make(chan []LogEntry)
		go partitionBatchDispatcher(i)
		go partitionBatchWorker(i)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	atomic.StoreInt32(&shuttingDown, 1)
	unregisterWithClusterManager(selfAddr)
	log.Println("Shutting down...")
}

// --- Schema Registry Integration ---
var (
	jsonSchema   *gojsonschema.Schema
	avroCodec    avro.Schema
	protoMsgDesc protoreflect.MessageDescriptor
)

func fetchAndCacheSchemas() {
	// Discover schema-validator address from cluster manager
	resp, err := http.Get(clusterManagerAddr + "/schema-service")
	if err != nil {
		log.Fatalf("Failed to get schema service address from cluster manager: %v", err)
	}
	defer resp.Body.Close()
	var schemaInfo struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schemaInfo); err != nil || schemaInfo.Address == "" {
		log.Fatalf("Failed to decode schema service address: %v", err)
	}
	schemaAddr := schemaInfo.Address

	// JSON Schema
	resp, err = http.Get(schemaAddr + "/schema/get?format=json&name=LogEntry")
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
	resp, err = http.Get(schemaAddr + "/schema/get?format=avro&name=LogEntry")
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
	resp, err = http.Get(schemaAddr + "/schema/descriptor?name=LogEntry")
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

func startClusterHTTPServerOnListener(listener net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/raft/vote", voteHandler)
	mux.HandleFunc("/raft/heartbeat", heartbeatHandler)
	mux.HandleFunc("/metrics", metricsHandler)
	log.Printf("Cluster HTTP endpoints available at %s (listening on %s)", selfAddr, listener.Addr().String())
	server := &http.Server{
		Handler:      mux,
		IdleTimeout:  5 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}
	err := server.Serve(listener)
	if err != nil {
		log.Fatalf("Failed to start cluster HTTP server: %v", err)
	}
}
