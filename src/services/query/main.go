package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
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

var (
	storageNodes      []string
	storageNodesMutex sync.RWMutex
	clusterManagerURL = os.Getenv("CLUSTER_MANAGER_ADDR")
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
	storageNodesMutex.Unlock()
}

func periodicallyUpdateStorageNodes() {
	for {
		updateStorageNodes()
		time.Sleep(2 * time.Second)
	}
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	storageNodesMutex.RLock()
	nodes := append([]string{}, storageNodes...)
	storageNodesMutex.RUnlock()

	var wg sync.WaitGroup
	resultCh := make(chan []LogEntry, len(nodes))
	var totalCollected int64

	for _, node := range nodes {
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
			// Only send up to the remaining needed logs
			remaining := int(req.Limit) - int(atomic.LoadInt64(&totalCollected))
			if req.Limit > 0 && remaining > 0 && len(qr.Results) > remaining {
				qr.Results = qr.Results[:remaining]
			}
			n := int64(len(qr.Results))
			if n > 0 && (req.Limit == 0 || atomic.AddInt64(&totalCollected, n) <= int64(req.Limit)) {
				resultCh <- qr.Results
			}
		}(node)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var allResults []LogEntry
collectLoop:
	for res := range resultCh {
		for _, entry := range res {
			allResults = append(allResults, entry)
			if req.Limit > 0 && len(allResults) >= req.Limit {
				break collectLoop
			}
		}
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
