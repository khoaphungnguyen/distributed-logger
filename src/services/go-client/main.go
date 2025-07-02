package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	logpb "go-client/proto"

	"github.com/hamba/avro/v2"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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

var syslogLevels = []string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}
var journaldPriorities = []int{0, 1, 2, 3, 4, 5, 6, 7}

// --- Dynamic schema fetching ---
type SchemaInfo struct {
	Format string `json:"format"`
	Name   string `json:"name"`
	Schema string `json:"schema"`
}

func fetchSchema(schemaRegistryURL, format, name string) (string, error) {
	resp, err := http.Get(schemaRegistryURL + "/schema/get?format=" + format + "&name=" + name)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("fetch failed: %s", string(body))
	}
	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Schema, nil
}

func fetchProtoDescriptor(schemaRegistryURL, name string) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf("%s/schema/descriptor?name=%s", schemaRegistryURL, name))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func getProtoMsgDescriptor(descBytes []byte, name string) (protoreflect.MessageDescriptor, error) {
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(descBytes, fds); err != nil {
		return nil, err
	}
	for _, fdProto := range fds.File {
		fd, err := protodesc.NewFile(fdProto, nil)
		if err != nil {
			continue
		}
		if md := fd.Messages().ByName(protoreflect.Name(name)); md != nil {
			return md, nil
		}
	}
	return nil, fmt.Errorf("message descriptor not found")
}

// --- Validation helpers ---
func validateJSONLog(schemaStr string, entry LogEntry) error {
	loader := gojsonschema.NewStringLoader(schemaStr)
	schema, err := gojsonschema.NewSchema(loader)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(entry)
	docLoader := gojsonschema.NewBytesLoader(b)
	result, err := schema.Validate(docLoader)
	if err != nil {
		return err
	}
	if !result.Valid() {
		return fmt.Errorf("validation failed: %v", result.Errors())
	}
	return nil
}

func validateAvroLog(schemaStr string, entry LogEntry) error {
	codec, err := avro.Parse(schemaStr)
	if err != nil {
		return fmt.Errorf("invalid Avro schema: %v", err)
	}
	avroMap := map[string]interface{}{
		"timestamp": entry.Timestamp,
		"level":     entry.Level,
		"message":   entry.Message,
		"service":   entry.Service,
	}
	_, err = avro.Marshal(codec, avroMap)
	if err != nil {
		return fmt.Errorf("Avro validation failed: %v", err)
	}
	return nil
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

// --- Main ---
func main() {
	rand.Seed(time.Now().UnixNano())

	address := flag.String("address", "go-ingestor", "Ingestor host address")
	tcpPort := flag.String("tcp-port", "", "TCP port (auto by format if empty)")
	udpPort := flag.String("udp-port", "3002", "UDP port")
	batchSize := flag.Int("batch", 100, "Number of logs per batch")
	intervalMs := flag.Int("interval", 100, "Interval in milliseconds between batches")
	useUDP := flag.Bool("udp", false, "Use UDP instead of TCP")
	format := flag.String("format", "json", "Log format: json, proto, avro, or raw")
	typeName := flag.String("type", "LogEntry", "Schema type name")
	flag.Parse()

	if !*useUDP {
		*tcpPort = "3001"
	}

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
	schemaRegistryURL := "http://go-schema-register:8000"

	// --- Dynamic schema fetching ---
	jsonSchemaStr, avroSchemaStr := "", ""
	var msgDesc protoreflect.MessageDescriptor

	if *format == "json" {
		jsonSchemaStr, err = fetchSchema(schemaRegistryURL, "json", *typeName)
		if err != nil {
			log.Fatalf("Failed to fetch JSON schema: %v", err)
		}
	}
	if *format == "avro" {
		avroSchemaStr, err = fetchSchema(schemaRegistryURL, "avro", *typeName)
		if err != nil {
			log.Fatalf("Failed to fetch Avro schema: %v", err)
		}
	}
	if *format == "proto" {
		descBytes, err := fetchProtoDescriptor(schemaRegistryURL, *typeName)
		if err != nil {
			log.Fatalf("Failed to fetch proto descriptor: %v", err)
		}
		msgDesc, err = getProtoMsgDescriptor(descBytes, *typeName)
		if err != nil {
			log.Fatalf("Failed to get proto message descriptor: %v", err)
		}
	}

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
						if err := validateJSONLog(jsonSchemaStr, entry); err != nil {
							log.Printf("Log validation failed: %v", err)
							continue
						}
						b, err = json.Marshal(entry)
					case "proto":
						entry := randomLog()
						msg := dynamicpb.NewMessage(msgDesc)
						bjson, _ := json.Marshal(entry)
						if err := protojson.Unmarshal(bjson, msg); err != nil {
							log.Printf("Proto log validation failed: %v", err)
							continue
						}
						pbEntry := &logpb.LogEntry{
							Timestamp: entry.Timestamp,
							Level:     entry.Level,
							Message:   entry.Message,
							Service:   entry.Service,
						}
						b, err = proto.Marshal(pbEntry)
					case "avro":
						entry := randomLog()
						if err := validateAvroLog(avroSchemaStr, entry); err != nil {
							log.Printf("Avro log validation failed: %v", err)
							continue
						}
						avroMap := map[string]interface{}{
							"timestamp": entry.Timestamp,
							"level":     entry.Level,
							"message":   entry.Message,
							"service":   entry.Service,
						}
						codec, _ := avro.Parse(avroSchemaStr)
						b, err = avro.Marshal(codec, avroMap)
					case "raw":
						b, _ = randomAnyLog()
						// log.Printf("Generated random log of type: %s", typ)
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

// Returns a random log as a []byte and a string describing the type ("logentry", "syslog", "journald")
func randomAnyLog() ([]byte, string) {
	entry := randomLog()
	r := rand.Intn(3)
	switch r {
	case 0:
		rawLine := fmt.Sprintf("%s [%s] [%s] %s", entry.Timestamp, entry.Level, entry.Service, entry.Message)
		return []byte(rawLine), "logentry"
	case 1:
		// Syslog: "Jul  2 17:00:01 host service[pid]: message" (with level as prefix)
		level := syslogLevels[rand.Intn(len(syslogLevels))]
		now := time.Now()
		host := "host"
		pid := rand.Intn(10000)
		line := fmt.Sprintf("%s %s %s[%d]: %s", now.Format("Jan  2 15:04:05"), host, entry.Service, pid, entry.Message)
		// Prepend level in a way some syslog daemons do: "<level>"
		line = fmt.Sprintf("<%d>%s", indexOfSyslogLevel(level), line)
		return []byte(line), "syslog"
	case 2:
		// Journald: JSON with PRIORITY and MESSAGE fields
		priority := journaldPriorities[rand.Intn(len(journaldPriorities))]
		journal := map[string]interface{}{
			"__REALTIME_TIMESTAMP": time.Now().UnixNano() / 1000,
			"_HOSTNAME":            "host",
			"PRIORITY":             strconv.Itoa(priority),
			"_SYSTEMD_UNIT":        entry.Service + ".service",
			"MESSAGE":              entry.Message,
		}
		b, _ := json.Marshal(journal)
		return b, "journald"
	default:
		b, _ := json.Marshal(entry)
		return b, "logentry"
	}
}

func indexOfSyslogLevel(level string) int {
	for i, l := range syslogLevels {
		if l == level {
			return i
		}
	}
	return 6 // info
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
