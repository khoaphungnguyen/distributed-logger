package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Node struct {
	Address    string    `json:"address"`
	Type       string    `json:"type"` // "ingestor", "storage", "schema", "query", etc.
	TCPPort    int       `json:"tcp_port,omitempty"`
	UDPPort    int       `json:"udp_port,omitempty"`
	HealthPort int       `json:"health_port,omitempty"`
	LastSeen   time.Time `json:"last_seen"`
	IsHealthy  bool      `json:"is_healthy"`
}

var (
	nodes      = make(map[string]*Node)
	nodesMutex sync.Mutex
	leader     string // Address of the current leader (set by Raft)
	term       int    // Election term number (set by Raft)
)

// --- Resource Metrics Helper ---
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

// --- Cluster Metrics Handler ---
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	counts := map[string]int{}
	healthy := map[string]int{}
	badNodes := []map[string]interface{}{}
	for _, n := range nodes {
		counts[n.Type]++
		if n.IsHealthy {
			healthy[n.Type]++
		} else {
			badNodes = append(badNodes, map[string]interface{}{
				"address":   n.Address,
				"type":      n.Type,
				"last_seen": n.LastSeen,
			})
		}
	}
	metrics := map[string]interface{}{
		"service_type":  "cluster-manager",
		"service_id":    "cluster-manager",
		"node_counts":   counts,
		"healthy_nodes": healthy,
		"bad_nodes":     badNodes,
		"leader":        leader,
		"term":          term,
		"resource":      getResourceMetrics(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// --- Expose all nodes (with health info) ---
func nodesHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	list := []*Node{}
	for _, n := range nodes {
		list = append(list, n)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// --- Expose only unhealthy nodes for quick detection ---
func badNodesHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	bad := []*Node{}
	for _, n := range nodes {
		if !n.IsHealthy {
			bad = append(bad, n)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bad)
}

// --- Expose healthy schema service address ---
func schemaServiceHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	for _, n := range nodes {
		if n.Type == "schema" && n.IsHealthy {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"address": n.Address,
			})
			return
		}
	}
	http.Error(w, "no healthy schema service found", 404)
}

// --- Expose healthy query service address ---
func queryServiceHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	for _, n := range nodes {
		if n.Type == "query" && n.IsHealthy {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"address": n.Address,
			})
			return
		}
	}
	http.Error(w, "no healthy query service found", 404)
}

// --- Register a node (requires address and type) ---
func registerHandler(w http.ResponseWriter, r *http.Request) {
	var req Node
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" || req.Type == "" {
		http.Error(w, "invalid request", 400)
		return
	}
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	nodes[req.Address] = &Node{
		Address:    req.Address,
		Type:       req.Type,
		TCPPort:    req.TCPPort,
		UDPPort:    req.UDPPort,
		HealthPort: req.HealthPort,
		LastSeen:   time.Now(),
		IsHealthy:  true,
	}
	w.WriteHeader(http.StatusOK)
}

// --- Unregister a node ---
func unregisterHandler(w http.ResponseWriter, r *http.Request) {
	var req Node
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" {
		http.Error(w, "invalid request", 400)
		return
	}
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	delete(nodes, req.Address)
	if req.Address == leader {
		leader = ""
		term = 0
	}
	w.WriteHeader(http.StatusOK)
}

// --- Return only healthy storage nodes ---
func storageNodesHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	list := []*Node{}
	for _, n := range nodes {
		if n.Type == "storage" && n.IsHealthy {
			list = append(list, n)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// --- Return only healthy ingestor nodes ---
func ingestorNodesHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	list := []*Node{}
	for _, n := range nodes {
		if n.Type == "ingestor" && n.IsHealthy {
			list = append(list, n)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func leaderHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	if leader == "" || nodes[leader] == nil {
		http.Error(w, "no leader", 404)
		return
	}
	n := nodes[leader]

	// Extract host (without scheme)
	host := n.Address
	if strings.HasPrefix(host, "http") {
		u, err := url.Parse(n.Address)
		if err == nil {
			host = u.Hostname()
		}
	}

	resp := struct {
		Leader  string `json:"leader"` // just the host
		Term    int    `json:"term"`
		TCPPort int    `json:"tcp_port,omitempty"`
		UDPPort int    `json:"udp_port,omitempty"`
	}{
		Leader:  host,
		Term:    term,
		TCPPort: n.TCPPort,
		UDPPort: n.UDPPort,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- Health check nodes and clear leader if unhealthy ---
func healthCheckNodes() {
	for {
		time.Sleep(200 * time.Millisecond)
		nodesMutex.Lock()
		for _, node := range nodes {
			healthURL := node.Address
			if node.HealthPort != 0 {
				u, _ := url.Parse(node.Address)
				healthURL = fmt.Sprintf("%s://%s:%d", u.Scheme, u.Hostname(), node.HealthPort)
			}
			healthURL += "/cluster/health"
			resp, err := http.Get(healthURL)
			if err != nil || resp.StatusCode != 200 {
				node.IsHealthy = false
			} else {
				node.IsHealthy = true
				node.LastSeen = time.Now()
			}
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			if node.Address == leader && !node.IsHealthy {
				leader = ""
				term = 0
			}
		}
		nodesMutex.Unlock()
	}
}

// --- Raft Leader Notification Handler ---
func raftLeaderHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Address string `json:"address"`
		Term    int    `json:"term"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" {
		http.Error(w, "invalid request", 400)
		return
	}
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	if req.Term > term || leader == "" || leader != req.Address {
		leader = req.Address
		term = req.Term
		log.Printf("Cluster manager: Updated leader to %s (term %d)", leader, term)
	}
	w.WriteHeader(http.StatusOK)
}

// --- Cluster manager health endpoint ---
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	http.HandleFunc("/nodes/register", registerHandler)
	http.HandleFunc("/nodes/unregister", unregisterHandler)
	http.HandleFunc("/nodes", nodesHandler)
	http.HandleFunc("/bad-nodes", badNodesHandler)
	http.HandleFunc("/storage-nodes", storageNodesHandler)
	http.HandleFunc("/ingestor-nodes", ingestorNodesHandler)
	http.HandleFunc("/leader", leaderHandler)
	http.HandleFunc("/nodes/raft-leader", raftLeaderHandler)
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/cluster/health", healthHandler)
	http.HandleFunc("/schema-service", schemaServiceHandler)
	http.HandleFunc("/query-service", queryServiceHandler)
	go healthCheckNodes()
	addr := os.Getenv("CLUSTER_MANAGER_ADDR")
	if addr == "" {
		addr = ":5000"
	}
	log.Printf("Cluster manager running at %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
