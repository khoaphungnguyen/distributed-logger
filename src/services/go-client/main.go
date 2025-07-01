package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	logpb "go-client/proto"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
)

const (
	formatJSON  = 0x01
	formatProto = 0x02
	formatAvro  = 0x03
	formatRaw   = 0x04
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

var avroSchema = `{
  "type": "record",
  "name": "LogEntry",
  "fields": [
    {"name": "timestamp", "type": "string"},
    {"name": "level", "type": "string"},
    {"name": "message", "type": "string"},
    {"name": "service", "type": "string"}
  ]
}`

var avroCodec = avro.MustParse(avroSchema)

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
	tcpPort := flag.String("tcp-port", "", "TCP port (auto by format if empty)")
	udpPort := flag.String("udp-port", "3002", "UDP port")
	batchSize := flag.Int("batch", 100, "Number of logs per batch")
	intervalMs := flag.Int("interval", 100, "Interval in milliseconds between batches")
	useUDP := flag.Bool("udp", false, "Use UDP instead of TCP")
	format := flag.String("format", "json", "Log format: json, proto, avro, or raw")
	flag.Parse()

	// Always use universal handler on TCP port 3001
	if !*useUDP {
		*tcpPort = "3001"
	}

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
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.Dial("tcp", addr, tlsConfig)
	}
	if err != nil {
		log.Fatalf("Failed to connect to ingestor: %v", err)
	}
	defer conn.Close()

	protocol := "TCP"
	if *useUDP {
		protocol = "UDP"
	}
	log.Printf("Connected to %s via %s. Sending %d logs every %dms as %s...", addr, protocol, *batchSize, *intervalMs, *format)

	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	var sentBatches, failedBatches int

loop:
	for {
		select {
		case <-ctx.Done():
			log.Println("Received shutdown signal, exiting...")
			break loop
		case <-ticker.C:
			switch {
			case !*useUDP:
				// Always use universal handler for TCP
				var batchBuf = make([]byte, 0, *batchSize*128)
				var header byte
				switch *format {
				case "json":
					header = formatJSON
				case "proto":
					header = formatProto
				case "avro":
					header = formatAvro
				case "raw":
					header = formatRaw
				default:
					header = formatJSON
				}
				for i := 0; i < *batchSize; i++ {
					var b []byte
					var err error
					switch *format {
					case "json":
						entry := randomLog()
						b, err = json.Marshal(entry)
					case "proto":
						entry := randomLog()
						pbEntry := &logpb.LogEntry{
							Timestamp: entry.Timestamp,
							Level:     entry.Level,
							Message:   entry.Message,
							Service:   entry.Service,
						}
						b, err = proto.Marshal(pbEntry)
					case "avro":
						entry := randomLog()
						avroMap := map[string]interface{}{
							"timestamp": entry.Timestamp,
							"level":     entry.Level,
							"message":   entry.Message,
							"service":   entry.Service,
						}
						b, err = avro.Marshal(avroCodec, avroMap)
					case "raw":
						entry := randomLog()
						rawLine := entry.Timestamp + " [" + entry.Level + "] [" + entry.Service + "] " + entry.Message
						b = []byte(rawLine)
					default:
						entry := randomLog()
						b, err = json.Marshal(entry)
					}
					if err != nil {
						log.Printf("Failed to marshal log entry: %v", err)
						continue
					}
					lenBuf := make([]byte, 4)
					binary.BigEndian.PutUint32(lenBuf, uint32(len(b)))
					batchBuf = append(batchBuf, header)
					batchBuf = append(batchBuf, lenBuf...)
					batchBuf = append(batchBuf, b...)
				}
				err = sendWithRetry(conn, batchBuf, 3)
				if err != nil {
					log.Printf("Failed to send universal batch after retries: %v", err)
					failedBatches++
				} else {
					sentBatches++
				}
			default:
				// UDP (always JSON)
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

				const maxUDPPacket = 1400
				chunks := splitBatchForUDP(batch, maxUDPPacket)
				for i, chunk := range chunks {
					chunkData := strings.Join(chunk, "\n") + "\n"
					if len(chunkData) > maxUDPPacket {
						log.Printf("Warning: UDP chunk %d size (%d bytes) exceeds safe MTU", i, len(chunkData))
					}
					err := sendWithRetry(conn, []byte(chunkData), 3)
					if err != nil {
						log.Printf("Failed to send UDP chunk %d after retries: %v", i, err)
						failedBatches++
					} else {
						sentBatches++
					}
				}
			}
		}
	}
	log.Printf("Final stats: Sent batches: %d, Failed batches: %d", sentBatches, failedBatches)
}

// Split batch into slices of strings, each joined chunk <= maxBytes
func splitBatchForUDP(batch []string, maxBytes int) [][]string {
	var result [][]string
	var current []string
	currentLen := 0

	for _, entry := range batch {
		entryLen := len(entry) + 1 // +1 for newline
		if currentLen+entryLen > maxBytes {
			if len(current) > 0 {
				result = append(result, current)
			}
			current = []string{entry}
			currentLen = entryLen
		} else {
			current = append(current, entry)
			currentLen += entryLen
		}
	}
	if len(current) > 0 {
		result = append(result, current)
	}
	return result
}
