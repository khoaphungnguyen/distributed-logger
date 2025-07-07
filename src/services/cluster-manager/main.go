package main

import (
    "encoding/json"
    "log"
    "net/http"
    "os"
    "sync"
    "time"
)

type Node struct {
    Address   string    `json:"address"`
    LastSeen  time.Time `json:"last_seen"`
    IsHealthy bool      `json:"is_healthy"`
}

var (
    nodes      = make(map[string]*Node)
    nodesMutex sync.Mutex
    leader     string // Address of the current leader
)

// Register a node and elect leader if needed
func registerHandler(w http.ResponseWriter, r *http.Request) {
    var req Node
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" {
        http.Error(w, "invalid request", 400)
        return
    }
    nodesMutex.Lock()
    defer nodesMutex.Unlock()
    nodes[req.Address] = &Node{
        Address:   req.Address,
        LastSeen:  time.Now(),
        IsHealthy: true,
    }
    // Elect leader if none or leader is not healthy
    if leader == "" || !nodes[leader].IsHealthy {
        leader = electLeader()
    }
    w.WriteHeader(http.StatusOK)
}

// Unregister a node and re-elect leader if needed
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
        leader = electLeader()
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

// Return the current leader
func leaderHandler(w http.ResponseWriter, r *http.Request) {
    nodesMutex.Lock()
    defer nodesMutex.Unlock()
    if leader == "" {
        http.Error(w, "no leader", 404)
        return
    }
    resp := struct {
        Leader string `json:"leader"`
    }{
        Leader: leader,
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}

// Elect the leader: pick the healthy node with the earliest registration (lowest address as a simple rule)
func electLeader() string {
    var best string
    for addr, n := range nodes {
        if n.IsHealthy && (best == "" || addr < best) {
            best = addr
        }
    }
    return best
}

// Health check nodes and re-elect leader if needed
func healthCheckNodes() {
    for {
        time.Sleep(5 * time.Second)
        nodesMutex.Lock()
        changed := false
        for _, node := range nodes {
            resp, err := http.Get(node.Address + "/cluster/health")
            wasHealthy := node.IsHealthy
            if err != nil || resp.StatusCode != 200 {
                node.IsHealthy = false
            } else {
                node.IsHealthy = true
                node.LastSeen = time.Now()
            }
            if node.IsHealthy != wasHealthy {
                changed = true
            }
        }
        // Re-elect leader if current leader is not healthy
        if leader == "" || (nodes[leader] != nil && !nodes[leader].IsHealthy) || changed {
            leader = electLeader()
        }
        nodesMutex.Unlock()
    }
}

func main() {
    http.HandleFunc("/nodes/register", registerHandler)
    http.HandleFunc("/nodes/unregister", unregisterHandler)
    http.HandleFunc("/nodes", nodesHandler)
    http.HandleFunc("/leader", leaderHandler)
    go healthCheckNodes()
    addr := os.Getenv("CLUSTER_MANAGER_ADDR")
    if addr == "" {
        addr = ":5000"
    }
    log.Printf("Cluster manager running at %s", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}