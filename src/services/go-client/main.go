package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"os/signal"
	"strings"
	"syscall"
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

func sendWithRetry(conn net.Conn, data []byte, maxRetries int) error {
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = conn.Write(data)
		if err == nil {
			return nil
		}
		log.Printf("Send failed (attempt %d/%d): %v", attempt, maxRetries, err)
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Configurable address and port
	address := flag.String("address", "go-ingestor", "Ingestor host address")
	tcpPort := flag.String("tcp-port", "3000", "TCP port")
	udpPort := flag.String("udp-port", "3001", "UDP port")
	batchSize := flag.Int("batch", 100, "Number of logs per batch")
	intervalMs := flag.Int("interval", 100, "Interval in milliseconds between batches")
	useUDP := flag.Bool("udp", false, "Use UDP instead of TCP")
	flag.Parse()

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var conn net.Conn
	var err error
	var addr string
	if *useUDP {
		addr = *address + ":" + *udpPort
		conn, err = net.Dial("udp", addr)
	} else {
		addr = *address + ":" + *tcpPort
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		log.Fatalf("Failed to connect to ingestor: %v", err)
	}
	defer conn.Close()

	protocol := "TCP"
	if *useUDP {
		protocol = "UDP"
	}
	log.Printf("Connected to %s via %s. Sending %d logs every %dms...", addr, protocol, *batchSize, *intervalMs)

	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Metrics
	var sentBatches, failedBatches int

loop:
	for {
		select {
		case <-ctx.Done():
			log.Println("Received shutdown signal, exiting...")
			break loop
		case <-ticker.C:
			var batch []string
			for i := 0; i < *batchSize; i++ {
				entry := randomLog()
				b, err := json.Marshal(entry)
				if err != nil {
					log.Printf("Failed to marshal log entry: %v", err)
					continue
				}
				batch = append(batch, string(b))
			}
			batchData := strings.Join(batch, "\n") + "\n"

			// UDP batch size warning
			if *useUDP && len(batchData) > 1400 {
				log.Printf("Warning: UDP batch size (%d bytes) may exceed safe MTU, consider reducing batch size.", len(batchData))
			}

			err := sendWithRetry(conn, []byte(batchData), 3)
			if err != nil {
				log.Printf("Failed to send batch after retries: %v", err)
				failedBatches++
			} else {
				sentBatches++
			}

			// Print stats every 1000 batches
			if (sentBatches+failedBatches)%1000 == 0 {
				log.Printf("Stats: Sent batches: %d, Failed batches: %d", sentBatches, failedBatches)
				log.Println("Batch Size Len:", len(batchData))
			}
		}
	}
	log.Printf("Final stats: Sent batches: %d, Failed batches: %d", sentBatches, failedBatches)
}
