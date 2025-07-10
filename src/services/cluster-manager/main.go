package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Node struct {
	Address    string    `json:"address"`
	Type       string    `json:"type"` // "ingestor" or "storage"
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

// Register a node (requires address and type)
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

// Unregister a node
func unregisterHandler(w http.ResponseWriter, r *http.Request) {
	var req Node
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" {
		http.Error(w, "invalid request", 400)
		return
	}
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	delete(nodes, req.Address)
	// If the leader node is removed, clear leader info
	if req.Address == leader {
		leader = ""
		term = 0
	}
	w.WriteHeader(http.StatusOK)
}

// Return all nodes
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

// Return only healthy storage nodes
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

// Return only healthy ingestor nodes
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

// Return the current leader (as set by Raft notification)
func leaderHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	if leader == "" || nodes[leader] == nil {
		http.Error(w, "no leader", 404)
		return
	}
	n := nodes[leader]
	resp := struct {
		Leader     string `json:"leader"`
		Term       int    `json:"term"`
		TCPPort    int    `json:"tcp_port,omitempty"`
		UDPPort    int    `json:"udp_port,omitempty"`
		HealthPort int    `json:"health_port,omitempty"`
	}{
		Leader:     n.Address,
		Term:       term,
		TCPPort:    n.TCPPort,
		UDPPort:    n.UDPPort,
		HealthPort: n.HealthPort,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Return only the hostname (no port) for the current leader
func leaderHostHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	if leader == "" {
		http.Error(w, "no leader", 404)
		return
	}
	u, err := url.Parse(leader)
	if err != nil {
		http.Error(w, "Invalid leader address", 500)
		return
	}
	hostOnly := u.Hostname()
	scheme := u.Scheme
	resp := struct {
		Host string `json:"host"`
		URL  string `json:"url"`
	}{
		Host: hostOnly,
		URL:  fmt.Sprintf("%s://%s", scheme, hostOnly),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Health check nodes and clear leader if unhealthy
func healthCheckNodes() {
	for {
		time.Sleep(1 * time.Second)
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
			// If the leader node is unhealthy, clear leader info
			if node.Address == leader && !node.IsHealthy {
				leader = ""
				term = 0
			}
		}
		nodesMutex.Unlock()
	}
}

// Dashboard: group by type, show health, last seen, leader, and ports
func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()
	fmt.Fprint(w, `<html><head><title>Cluster Dashboard</title>
    <script>
    setTimeout(function(){ location.reload(); }, 1000);
    </script>
    </head><body>`)
	fmt.Fprint(w, "<h1>Cluster Manager Dashboard</h1>")
	if leader == "" {
		fmt.Fprint(w, "<p><b>No leader elected</b></p>")
	} else {
		fmt.Fprintf(w, "<p><b>Leader:</b> %s (term %d)</p>", leader, term)
	}
	// Group nodes by type
	types := map[string][]*Node{}
	for _, n := range nodes {
		types[n.Type] = append(types[n.Type], n)
	}
	for nodeType, nodeList := range types {
		fmt.Fprintf(w, "<h2>%s Nodes</h2><table border=1><tr><th>Address</th><th>TCP Port</th><th>UDP Port</th><th>Health Port</th><th>Healthy</th><th>Last Seen</th></tr>", strings.Title(nodeType))
		for _, n := range nodeList {
			status := "NO"
			if n.IsHealthy {
				status = "YES"
			}
			fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>", n.Address, n.TCPPort, n.UDPPort, n.HealthPort, status, n.LastSeen.Format(time.RFC3339))
		}
		fmt.Fprint(w, "</table>")
	}
	fmt.Fprint(w, "</body></html>")
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
	// Only update if term is newer or leader is empty
	if req.Term > term || leader == "" || leader != req.Address {
		leader = req.Address
		term = req.Term
		log.Printf("Cluster manager: Updated leader to %s (term %d)", leader, term)
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/", dashboardHandler)
	http.HandleFunc("/nodes/register", registerHandler)
	http.HandleFunc("/nodes/unregister", unregisterHandler)
	http.HandleFunc("/nodes", nodesHandler)
	http.HandleFunc("/storage-nodes", storageNodesHandler)
	http.HandleFunc("/ingestor-nodes", ingestorNodesHandler)
	http.HandleFunc("/leader", leaderHandler)
	http.HandleFunc("/leader-host", leaderHostHandler)
	http.HandleFunc("/nodes/raft-leader", raftLeaderHandler)
	go healthCheckNodes()
	addr := os.Getenv("CLUSTER_MANAGER_ADDR")
	if addr == "" {
		addr = ":5000"
	}
	log.Printf("Cluster manager running at %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
