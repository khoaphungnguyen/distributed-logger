package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

// Rate limiter - allow 1500 requests per second with burst of 2000
var rateLimiter = rate.NewLimiter(1500, 2000)

// Add global metrics counters
var (
	totalQueries    int64
	totalErrors     int64
	totalLatencyUs  int64
	totalLogs       int64
	logsPerSec      int64
	queriesPerSec   int64
	errorsPerSec    int64
	cacheHits       int64
	cacheMisses     int64
	rateLimitErrors int64
	timeoutErrors   int64
	selfAddr        string
)

type LogEntry struct {
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	Message    string `json:"message"`
	Service    string `json:"service"`
	Hostname   string `json:"hostname,omitempty"`
	AppVersion string `json:"app_version,omitempty"`
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

// Cache implementation
type CacheEntry struct {
	Results   []LogEntry
	Timestamp time.Time
	TTL       time.Duration
}

// Circuit breaker for storage nodes
type CircuitBreaker struct {
	failures    int64
	lastFailure time.Time
	state       string // "CLOSED", "OPEN", "HALF_OPEN"
	threshold   int    // failure threshold
	timeout     time.Duration
	mutex       sync.RWMutex
}

func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		threshold: 10,               // Open after 10 failures
		timeout:   30 * time.Second, // Try again after 30 seconds
		state:     "CLOSED",
	}
}

func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	// Check if circuit should be reset
	if cb.state == "OPEN" && time.Since(cb.lastFailure) > cb.timeout {
		cb.state = "HALF_OPEN"
		cb.failures = 0
	}

	// Reject calls if circuit is open
	if cb.state == "OPEN" {
		return fmt.Errorf("circuit breaker open")
	}

	err := fn()
	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		if cb.failures >= int64(cb.threshold) {
			cb.state = "OPEN"
		}
		return err
	}

	// Success - reset if half open
	if cb.state == "HALF_OPEN" {
		cb.state = "CLOSED"
		cb.failures = 0
	}

	return nil
}

var storageCircuitBreaker = NewCircuitBreaker()

type QueryCache struct {
	entries map[string]*CacheEntry
	mutex   sync.RWMutex
	maxSize int
	ttl     time.Duration
}

func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	cache := &QueryCache{
		entries: make(map[string]*CacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
	go cache.cleanup()
	return cache
}

func (qc *QueryCache) generateKey(req QueryRequest) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d", req.Service, req.Level, req.StartTime, req.EndTime, req.Limit)
}

func (qc *QueryCache) Get(req QueryRequest) ([]LogEntry, bool) {
	qc.mutex.RLock()
	defer qc.mutex.RUnlock()

	key := qc.generateKey(req)
	entry, exists := qc.entries[key]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Since(entry.Timestamp) > entry.TTL {
		return nil, false
	}

	return entry.Results, true
}

func (qc *QueryCache) Set(req QueryRequest, results []LogEntry) {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	key := qc.generateKey(req)

	// If cache is at max size, remove oldest entry
	if len(qc.entries) >= qc.maxSize {
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k, v := range qc.entries {
			if first || v.Timestamp.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.Timestamp
				first = false
			}
		}
		delete(qc.entries, oldestKey)
	}

	qc.entries[key] = &CacheEntry{
		Results:   results,
		Timestamp: time.Now(),
		TTL:       qc.ttl,
	}
}

func (qc *QueryCache) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		qc.mutex.Lock()
		for key, entry := range qc.entries {
			if time.Since(entry.Timestamp) > entry.TTL {
				delete(qc.entries, key)
			}
		}
		qc.mutex.Unlock()
	}
}

func (qc *QueryCache) GetStats() (int, int) {
	qc.mutex.RLock()
	defer qc.mutex.RUnlock()
	return len(qc.entries), qc.maxSize
}

// --- Consistent Hash Ring ---
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

// --- End Consistent Hash Ring ---

var (
	storageNodes      []string
	storageNodesMutex sync.RWMutex
	clusterManagerURL = os.Getenv("CLUSTER_MANAGER_ADDR")
	queryCache        *QueryCache
)

var (
	storageRing  *hashRing
	replicaCount int
	readQuorum   int
)

func updateStorageNodes() {
	resp, err := http.Get(clusterManagerURL + "/storage-nodes")
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
	// Dynamically set replicaCount and readQuorum
	n := len(addrs)
	if n >= 3 {
		replicaCount = n / 2
		if replicaCount < 3 {
			replicaCount = 3 // minimum for safety
		}
		readQuorum = (replicaCount + 1) / 2
	} else {
		replicaCount = n
		readQuorum = 1
	}
	storageNodesMutex.Unlock()
}
func periodicallyUpdateStorageNodes() {
	for {
		updateStorageNodes()
		time.Sleep(2 * time.Second)
	}
}

// --- Query Handler with Read Repair ---
func queryHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Rate limiting - check before processing
	if !rateLimiter.Allow() {
		atomic.AddInt64(&totalErrors, 1)
		atomic.AddInt64(&rateLimitErrors, 1)
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	atomic.AddInt64(&totalQueries, 1)

	// Set response headers early for better performance
	w.Header().Set("Content-Type", "application/json")

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		atomic.AddInt64(&totalErrors, 1)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Check cache first
	if cachedResults, found := queryCache.Get(req); found {
		atomic.AddInt64(&cacheHits, 1)
		logsCount := int64(len(cachedResults))
		atomic.AddInt64(&totalLogs, logsCount)

		json.NewEncoder(w).Encode(QueryResponse{Results: cachedResults})

		// Record latency for cache hit - calculate per log latency
		latencyUs := time.Since(start).Microseconds()
		if logsCount > 0 {
			// For cache hits, distribute latency across all logs returned
			atomic.AddInt64(&totalLatencyUs, latencyUs*logsCount)
		}
		return
	}

	atomic.AddInt64(&cacheMisses, 1)

	storageNodesMutex.RLock()
	nodes := append([]string{}, storageNodes...)
	ring := storageRing
	storageNodesMutex.RUnlock()

	if ring == nil || len(nodes) == 0 {
		atomic.AddInt64(&totalErrors, 1)
		http.Error(w, "no storage nodes available", http.StatusServiceUnavailable)
		return
	}

	key := req.Service
	replicas := ring.getNodes(key, replicaCount)

	type replicaResult struct {
		addr    string
		entries []LogEntry
	}
	resultCh := make(chan replicaResult, len(replicas))
	var totalCollected int64
	var wg sync.WaitGroup

	// Use shorter timeout for individual storage requests
	client := &http.Client{Timeout: 5 * time.Second}

	for _, node := range replicas {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			buf, _ := json.Marshal(req)
			resp, err := client.Post(addr+"/query", "application/json", bytes.NewReader(buf))
			if err != nil {
				log.Printf("Query to %s failed: %v", addr, err)
				// Track timeout errors
				if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "context deadline exceeded") {
					atomic.AddInt64(&timeoutErrors, 1)
				}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Query to %s returned status %d", addr, resp.StatusCode)
				return
			}

			var qr QueryResponse
			if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
				log.Printf("Decode from %s failed: %v", addr, err)
				return
			}

			n := int64(len(qr.Results))
			if n > 0 && (req.Limit == 0 || atomic.AddInt64(&totalCollected, n) <= int64(req.Limit)) {
				resultCh <- replicaResult{addr: addr, entries: qr.Results}
			}
		}(node)
	}

	// Use context with timeout for the whole operation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	acks := 0
	resultMap := make(map[string]LogEntry) // key: composite, value: freshest
	type replicaVersion struct {
		addr  string
		entry LogEntry
	}
	replicaResults := make(map[string][]replicaVersion) // key: composite, value: all versions

	logsReturned := 0

collectLoop:
	for {
		select {
		case <-ctx.Done():
			atomic.AddInt64(&totalErrors, 1)
			http.Error(w, "query timeout", http.StatusRequestTimeout)
			return
		case res, ok := <-resultCh:
			if !ok {
				break collectLoop
			}
			acks++
			for _, entry := range res.entries {
				key := entry.Message + "|" + entry.Service + "|" + entry.Level
				replicaResults[key] = append(replicaResults[key], replicaVersion{addr: res.addr, entry: entry})
				existing, ok := resultMap[key]
				if !ok || entry.Timestamp > existing.Timestamp {
					resultMap[key] = entry
				}
				logsReturned++
				if req.Limit > 0 && len(resultMap) >= req.Limit {
					break collectLoop
				}
			}
			if acks >= readQuorum {
				break collectLoop
			}
		}
	}

	// Skip read repair under high load to reduce latency
	// Read Repair is commented out for performance under high load
	/*
		for key, freshest := range resultMap {
			for _, rv := range replicaResults[key] {
				if rv.entry.Timestamp < freshest.Timestamp {
					go func(addr string, entry LogEntry) {
						repairBody, _ := json.Marshal(entry)
						_, err := http.Post(addr+"/repair", "application/json", bytes.NewReader(repairBody))
						if err != nil {
							log.Printf("Read repair to %s failed: %v", addr, err)
						}
					}(rv.addr, freshest)
				}
			}
		}
	*/

	// Convert map to slice for response
	allResults := make([]LogEntry, 0, len(resultMap))
	for _, entry := range resultMap {
		allResults = append(allResults, entry)
	}

	// Only cache results if we actually got some logs (don't cache empty results)
	if len(allResults) > 0 {
		go queryCache.Set(req, allResults)
	}

	json.NewEncoder(w).Encode(QueryResponse{Results: allResults})

	// Record latency and logs - use actual number of logs returned to client
	latencyUs := time.Since(start).Microseconds()
	logsCount := int64(len(allResults))
	if logsCount > 0 {
		// Distribute total query latency across all logs returned
		atomic.AddInt64(&totalLatencyUs, latencyUs*logsCount)
	}
	atomic.AddInt64(&totalLogs, logsCount) // Count actual logs returned to client
}

// Resource metrics helper
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

// Query metrics handler
func queryMetricsHandler(w http.ResponseWriter, r *http.Request) {
	errors := atomic.LoadInt64(&totalErrors)
	latencySum := atomic.LoadInt64(&totalLatencyUs)
	logs := atomic.LoadInt64(&totalLogs)
	hits := atomic.LoadInt64(&cacheHits)
	misses := atomic.LoadInt64(&cacheMisses)
	rateLimitErrs := atomic.LoadInt64(&rateLimitErrors)
	timeoutErrs := atomic.LoadInt64(&timeoutErrors)
	currentLogsPerSec := atomic.LoadInt64(&logsPerSec)
	avgLogLatency := float64(0)

	if logs > 0 {
		avgLogLatency = float64(latencySum) / float64(logs)
	}

	// Debug logging every 100 requests to see what's happening
	totalQueries := atomic.LoadInt64(&totalQueries)
	// if totalQueries%100 == 0 {
	// 	log.Printf("DEBUG: totalLogs=%d, latencySum=%d, logsPerSec=%d, avgLatency=%.2f",
	// 		logs, latencySum, currentLogsPerSec, avgLogLatency)
	// }

	cacheHitRate := float64(0)
	totalCacheRequests := hits + misses
	if totalCacheRequests > 0 {
		cacheHitRate = float64(hits) / float64(totalCacheRequests) * 100
	}

	cacheSize, cacheMaxSize := queryCache.GetStats()

	metrics := map[string]interface{}{
		"service_type":      "query",
		"service_id":        selfAddr,
		"logs_per_sec":      currentLogsPerSec,
		"latency_us":        avgLogLatency,
		"query_errors":      errors,        // Number of failed query requests
		"total_queries":     totalQueries,  // Total query requests processed
		"total_logs":        logs,          // Total logs returned across all queries
		"rate_limit_errors": rateLimitErrs, // Queries rejected due to rate limiting
		"timeout_errors":    timeoutErrs,   // Queries that timed out
		"cache_hit_rate":    cacheHitRate,
		"cache_hits":        hits,
		"cache_misses":      misses,
		"cache_size":        cacheSize,
		"cache_max_size":    cacheMaxSize,
		"resource":          getResourceMetrics(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func cacheFlushHandler(w http.ResponseWriter, r *http.Request) {
	queryCache.mutex.Lock()
	queryCache.entries = make(map[string]*CacheEntry)
	queryCache.mutex.Unlock()

	log.Printf("Cache flushed manually")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("cache flushed"))
}

func registerWithClusterManager(addr string, healthPort int) {
	body, _ := json.Marshal(map[string]interface{}{
		"address":     addr,
		"type":        "query",
		"health_port": healthPort,
	})
	_, err := http.Post(clusterManagerURL+"/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Failed to register with cluster manager: %v", err)
	}
	log.Printf("Registered with cluster manager as %s (health:%d)", addr, healthPort)
}

func unregisterWithClusterManager(addr string) {
	body, _ := json.Marshal(map[string]interface{}{
		"address": addr,
	})
	http.Post(clusterManagerURL+"/nodes/unregister", "application/json", bytes.NewReader(body))
	log.Printf("Unregistered from cluster manager: %s", addr)
}

// Add this function to periodically update per-second rates:
func metricsRateLogger() {
	var lastQueries int64
	var lastErrors int64
	var lastLogs int64
	for range time.Tick(1 * time.Second) {
		currQueries := atomic.LoadInt64(&totalQueries)
		currErrors := atomic.LoadInt64(&totalErrors)
		currLogs := atomic.LoadInt64(&totalLogs)
		atomic.StoreInt64(&queriesPerSec, currQueries-lastQueries)
		atomic.StoreInt64(&errorsPerSec, currErrors-lastErrors)
		atomic.StoreInt64(&logsPerSec, currLogs-lastLogs)
		lastQueries = currQueries
		lastErrors = currErrors
		lastLogs = currLogs
	}
}

func main() {
	// Initialize cache: 10000 entries max, 2 minute TTL for better hit rate under load
	queryCache = NewQueryCache(10000, 2*time.Minute)

	// Clear any existing cache entries that might have empty results
	log.Printf("Starting query service with fresh cache...")

	go periodicallyUpdateStorageNodes()
	go metricsRateLogger()

	port := os.Getenv("QUERY_PORT")
	if port == "" {
		port = "8000"
	}
	hostname, _ := os.Hostname()
	selfAddr = fmt.Sprintf("http://%s:%s", hostname, port)
	healthPort, _ := strconv.Atoi(port)

	registerWithClusterManager(selfAddr, healthPort)

	// Graceful shutdown: unregister on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		unregisterWithClusterManager(selfAddr)
		os.Exit(0)
	}()

	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/metrics", queryMetricsHandler)
	http.HandleFunc("/cluster/health", healthHandler)
	http.HandleFunc("/cache/flush", cacheFlushHandler)

	// Performance optimizations for high load
	srv := &http.Server{
		Addr:           ":" + port,
		ReadTimeout:    30 * time.Second, // Increased timeout
		WriteTimeout:   30 * time.Second, // Increased timeout
		IdleTimeout:    60 * time.Second, // Connection keep-alive
		MaxHeaderBytes: 1 << 20,          // 1MB max header size
	}
	log.Printf("Query service listening on :%s with cache enabled (max: 10000 entries, TTL: 2min)", port)
	log.Fatal(srv.ListenAndServe())
}
