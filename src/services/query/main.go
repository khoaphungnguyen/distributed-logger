package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	storageNodesMutex.RLock()
	nodes := append([]string{}, storageNodes...)
	ring := storageRing
	storageNodesMutex.RUnlock()

	if ring == nil || len(nodes) == 0 {
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

	for _, node := range replicas {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			buf, _ := json.Marshal(req)
			resp, err := http.Post(addr+"/query", "application/json", bytes.NewReader(buf))
			if err != nil {
				log.Printf("Query to %s failed: %v", addr, err)
				return
			}
			defer resp.Body.Close()
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

collectLoop:
	for res := range resultCh {
		acks++
		for _, entry := range res.entries {
			key := entry.Message + "|" + entry.Service + "|" + entry.Level
			replicaResults[key] = append(replicaResults[key], replicaVersion{addr: res.addr, entry: entry})
			existing, ok := resultMap[key]
			if !ok || entry.Timestamp > existing.Timestamp {
				resultMap[key] = entry
			}
			if req.Limit > 0 && len(resultMap) >= req.Limit {
				break collectLoop
			}
		}
		if acks >= readQuorum {
			break
		}
	}

	// Read Repair: For each key, if any replica returned a stale version, send the freshest to that replica
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

	// Convert map to slice for response
	allResults := make([]LogEntry, 0, len(resultMap))
	for _, entry := range resultMap {
		allResults = append(allResults, entry)
	}

	json.NewEncoder(w).Encode(QueryResponse{Results: allResults})
}

func main() {
	go periodicallyUpdateStorageNodes()
	http.HandleFunc("/query", queryHandler)
	port := os.Getenv("QUERY_PORT")
	if port == "" {
		port = "8000"
	}
	srv := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Printf("Query service listening on :%s", port)
	log.Fatal(srv.ListenAndServe())
}
