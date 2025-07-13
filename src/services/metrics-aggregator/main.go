package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Node struct {
	Address   string `json:"address"`
	Type      string `json:"type"`
	IsHealthy bool   `json:"is_healthy"`
}

type NodeMetrics struct {
	Node    Node                   `json:"Node"`
	Metrics map[string]interface{} `json:"Metrics"`
	LastErr string                 `json:"LastErr"`
}

var (
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR")
	scrapeInterval     = 1 * time.Second
	nodesMu            sync.RWMutex
	nodeMetrics        = make(map[string]*NodeMetrics) // key: node address
	clusterStatsMu     sync.RWMutex
	clusterStats       map[string]interface{}
	badNodes           []map[string]interface{}
)

func discoverNodes() ([]Node, error) {
	resp, err := http.Get(clusterManagerAddr + "/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var nodes []Node
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func fetchClusterStats() {
	for {
		resp, err := http.Get(clusterManagerAddr + "/metrics")
		if err == nil {
			var stats map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&stats); err == nil {
				clusterStatsMu.Lock()
				clusterStats = stats
				clusterStatsMu.Unlock()
			}
			resp.Body.Close()
		}
		resp, err = http.Get(clusterManagerAddr + "/bad-nodes")
		if err == nil {
			var bad []map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&bad); err == nil {
				clusterStatsMu.Lock()
				badNodes = bad
				clusterStatsMu.Unlock()
			}
			resp.Body.Close()
		}
		time.Sleep(scrapeInterval)
	}
}

func scrapeMetrics(addr string) (map[string]interface{}, error) {
	url := addr + "/metrics"
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("[SCRAPE][ERROR] Failed to GET %s: %v", url, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[SCRAPE][ERROR] Non-200 from %s: %d, body: %s", url, resp.StatusCode, string(body))
		return nil, fmt.Errorf("non-200 status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var metrics map[string]interface{}
	if err := json.Unmarshal(body, &metrics); err != nil {
		log.Printf("[SCRAPE][ERROR] Failed to decode JSON from %s: %v, body: %s", url, err, string(body))
		return nil, fmt.Errorf("metrics not JSON: %v", err)
	}
	return metrics, nil
}

func metricsScraperLoop() {
	for {
		nodes, err := discoverNodes()
		if err != nil {
			log.Printf("[DISCOVERY][ERROR] Failed to discover nodes: %v", err)
			time.Sleep(scrapeInterval)
			continue
		}
		var wg sync.WaitGroup
		for _, node := range nodes {
			if !node.IsHealthy {
				continue
			}
			wg.Add(1)
			go func(node Node) {
				defer wg.Done()
				metrics, err := scrapeMetrics(node.Address)
				nodesMu.Lock()
				defer nodesMu.Unlock()
				if err != nil {
					log.Printf("[NODE][ERROR] Could not scrape metrics from %s: %v", node.Address, err)
					nodeMetrics[node.Address] = &NodeMetrics{Node: node, LastErr: err.Error()}
				} else {
					nodeMetrics[node.Address] = &NodeMetrics{Node: node, Metrics: metrics}
				}
			}(node)
		}
		wg.Wait()
		time.Sleep(scrapeInterval)
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	nodesMu.RLock()
	clusterStatsMu.RLock()
	defer nodesMu.RUnlock()
	defer clusterStatsMu.RUnlock()
	result := map[string]interface{}{
		"cluster_stats": clusterStats,
		"bad_nodes":     badNodes,
		"node_metrics":  nodeMetrics,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func main() {
	go metricsScraperLoop()
	go fetchClusterStats()
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	mux.HandleFunc("/metrics", metricsHandler)
	log.Println("Metrics aggregator running on :3000")
	log.Fatal(http.ListenAndServe(":3000", mux))
}
