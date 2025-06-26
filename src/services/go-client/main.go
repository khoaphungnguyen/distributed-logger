// =========================
// main.go (Go Log Client TCP + UDP)
// =========================
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

var levels = []string{"DEBUG", "INFO", "WARN", "ERROR"}
var services = []string{"auth", "payment", "api", "db", "notification"}
var messages = []string{
	"User login successful",
	"Payment processed",
	"DB connection established",
	"User not authorized",
	"Email sent to user",
	"Failed to load config",
	"Cache miss",
	"Session expired",
	"Order created",
	"Resource not found",
}

func randomLog() LogEntry {
	return LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     levels[rand.Intn(len(levels))],
		Message:   messages[rand.Intn(len(messages))],
		Service:   services[rand.Intn(len(services))],
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Define command-line flags
	batchSize := flag.Int("batch", 100, "Number of logs per batch")
	intervalMs := flag.Int("interval", 100, "Interval in milliseconds between batches")
	useUDP := flag.Bool("udp", false, "Use UDP instead of TCP")
	flag.Parse()

	var conn net.Conn
	var err error
	if *useUDP {
		conn, err = net.Dial("udp", "go-ingestor:3001")
	} else {
		conn, err = net.Dial("tcp", "go-ingestor:3000")
	}
	if err != nil {
		fmt.Println("Failed to connect to ingestor:", err)
		os.Exit(1)
	}
	defer conn.Close()

	protocol := "TCP"
	if *useUDP {
		protocol = "UDP"
	}
	fmt.Printf("Connected to go-ingestor via %s. Sending %d logs every %dms...\n", protocol, *batchSize, *intervalMs)

	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		var batch []string
		for i := 0; i < *batchSize; i++ {
			entry := randomLog()
			b, _ := json.Marshal(entry)
			batch = append(batch, string(b))
		}
		batchData := strings.Join(batch, "\n") + "\n"
		conn.Write([]byte(batchData))
	}
}
