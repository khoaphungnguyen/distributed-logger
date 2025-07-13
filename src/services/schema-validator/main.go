package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Schema struct {
	Format string `json:"format"` // "json", "avro", "proto"
	Name   string `json:"name"`   // e.g. "LogEntry"
	Schema string `json:"schema"` // The schema definition (JSON Schema, Avro schema, or FileDescriptorSet JSON)
}

type ValidateRequest struct {
	Format string      `json:"format"`
	Name   string      `json:"name"`
	Data   interface{} `json:"data"`
}

var (
	schemaStore       = make(map[string]Schema) // key: format:name
	schemaMutex       sync.RWMutex
	jsonSchemaCache   = make(map[string]*gojsonschema.Schema)           // for fast validation
	avroSchemaCache   = make(map[string]avro.Schema)                    // for fast validation
	protoMsgDescCache = make(map[string]protoreflect.MessageDescriptor) // key: name
)

var (
	validationCount     int64
	validationErrors    int64
	validationLatencyUs int64
	validationsPerSec   int64
	errorsPerSec        int64
)

var (
	clusterManagerAddr = os.Getenv("CLUSTER_MANAGER_ADDR")
	selfAddr           string
)

// Register with cluster manager
func registerWithClusterManager(addr string) {
	if clusterManagerAddr == "" {
		log.Println("CLUSTER_MANAGER_ADDR not set, skipping registration")
		return
	}
	body, _ := json.Marshal(map[string]string{
		"address": addr,
		"type":    "schema",
	})
	_, err := http.Post(clusterManagerAddr+"/nodes/register", "application/json", strings.NewReader(string(body)))
	if err != nil {
		log.Printf("Failed to register with cluster manager: %v", err)
	} else {
		log.Printf("Registered with cluster manager as %s", addr)
	}
}

func unregisterWithClusterManager(addr string) {
	if clusterManagerAddr == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{
		"address": addr,
	})
	http.Post(clusterManagerAddr+"/nodes/unregister", "application/json", strings.NewReader(string(body)))
	log.Printf("Unregistered from cluster manager")
}

// --- Metrics Handler ---
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

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	validations := atomic.LoadInt64(&validationCount)
	latencySum := atomic.LoadInt64(&validationLatencyUs)
	avgLatency := float64(0)
	if validations > 0 {
		avgLatency = float64(latencySum) / float64(validations)
	}
	metrics := map[string]interface{}{
		"service_type":        "schema",
		"service_id":          selfAddr,
		"validations_per_sec": atomic.LoadInt64(&validationsPerSec),
		"avg_latency_us":      avgLatency,
		"validation_errors":   atomic.LoadInt64(&validationErrors),
		"resource":            getResourceMetrics(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// --- Load FileDescriptorSet and cache message descriptors ---
func loadProtoDescriptors(descPath string) {
	descBytes, err := ioutil.ReadFile(descPath)
	if err != nil {
		log.Printf("Could not read descriptor set: %v", err)
		return
	}
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(descBytes, fds); err != nil {
		log.Printf("Could not unmarshal descriptor set: %v", err)
		return
	}
	for _, fdProto := range fds.File {
		fd, err := protodesc.NewFile(fdProto, nil)
		if err != nil {
			continue
		}
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			md := msgs.Get(i)
			protoMsgDescCache[string(md.Name())] = md
			log.Printf("Registered proto message descriptor: %s", md.FullName())
		}
	}
}

// --- Register all *.json files in the given directory as schemas ---
func registerSchemasFromDir(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Printf("No schema directory found at %s, skipping auto-registration", dir)
		return
	}
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, file.Name())
		content, err := ioutil.ReadFile(path)
		if err != nil {
			log.Printf("Failed to read schema file %s: %v", path, err)
			continue
		}
		var meta struct {
			Format string `json:"format"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(content, &meta); err != nil || meta.Format == "" || meta.Name == "" {
			log.Printf("Schema file %s missing 'format' or 'name' fields (must be top-level)", path)
			continue
		}
		s := Schema{
			Format: meta.Format,
			Name:   meta.Name,
			Schema: string(content),
		}
		key := s.Format + ":" + s.Name
		schemaMutex.Lock()
		schemaStore[key] = s
		switch s.Format {
		case "json":
			loader := gojsonschema.NewStringLoader(s.Schema)
			parsed, err := gojsonschema.NewSchema(loader)
			if err == nil {
				jsonSchemaCache[key] = parsed
			}
		case "avro":
			parsed, err := avro.Parse(s.Schema)
			if err == nil {
				avroSchemaCache[key] = parsed
			}
		}
		schemaMutex.Unlock()
		log.Printf("Auto-registered schema: %s (from %s)", key, path)
	}
}

// --- Schema Registration Handler ---
func schemaRegisterHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, _ := ioutil.ReadAll(r.Body)
	var s Schema
	if err := json.Unmarshal(body, &s); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	key := s.Format + ":" + s.Name
	schemaMutex.Lock()
	schemaStore[key] = s
	switch s.Format {
	case "json":
		loader := gojsonschema.NewStringLoader(s.Schema)
		parsed, err := gojsonschema.NewSchema(loader)
		if err == nil {
			jsonSchemaCache[key] = parsed
		}
	case "avro":
		parsed, err := avro.Parse(s.Schema)
		if err == nil {
			avroSchemaCache[key] = parsed
		}
	}
	schemaMutex.Unlock()
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Registered schema %s", key)
}

// --- Schema Get Handler ---
func schemaGetHandler(w http.ResponseWriter, r *http.Request) {
	format := r.URL.Query().Get("format")
	name := r.URL.Query().Get("name")
	key := format + ":" + name
	schemaMutex.RLock()
	s, ok := schemaStore[key]
	schemaMutex.RUnlock()
	if !ok {
		http.Error(w, "Schema not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(s)
}

// --- Schema List Handler ---
func schemaListHandler(w http.ResponseWriter, r *http.Request) {
	schemaMutex.RLock()
	defer schemaMutex.RUnlock()
	var schemas []Schema
	for _, s := range schemaStore {
		schemas = append(schemas, s)
	}
	json.NewEncoder(w).Encode(schemas)
}

// --- Schema Descriptor Handler ---
func schemaDescriptorHandler(w http.ResponseWriter, r *http.Request) {
	descBytes, err := ioutil.ReadFile("schema/logentry.desc")
	if err != nil {
		http.Error(w, "Descriptor not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(descBytes)
}

// --- Schema Validate Handler ---
func schemaValidateHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req ValidateRequest
	body, _ := ioutil.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		atomic.AddInt64(&validationErrors, 1)
		atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&validationCount, 1)
	key := req.Format + ":" + req.Name

	switch req.Format {
	case "json":
		schemaMutex.RLock()
		schema, ok := jsonSchemaCache[key]
		schemaMutex.RUnlock()
		if !ok {
			atomic.AddInt64(&validationErrors, 1)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			http.Error(w, "Schema not found", http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(req.Data)
		docLoader := gojsonschema.NewBytesLoader(b)
		result, err := schema.Validate(docLoader)
		if err != nil {
			atomic.AddInt64(&validationErrors, 1)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}
		if !result.Valid() {
			atomic.AddInt64(&validationErrors, 1)
			http.Error(w, fmt.Sprintf("Validation failed: %v", result.Errors()), http.StatusBadRequest)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
		return
	case "avro":
		schemaMutex.RLock()
		avroSchema, ok := avroSchemaCache[key]
		schemaMutex.RUnlock()
		if !ok {
			atomic.AddInt64(&validationErrors, 1)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			http.Error(w, "Schema not found", http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(req.Data)
		var native interface{}
		if err := json.Unmarshal(b, &native); err != nil {
			atomic.AddInt64(&validationErrors, 1)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			http.Error(w, "Invalid Avro data", http.StatusBadRequest)
			return
		}
		_, err := avro.Marshal(avroSchema, native)
		if err != nil {
			atomic.AddInt64(&validationErrors, 1)
			http.Error(w, fmt.Sprintf("Avro validation failed: %v", err), http.StatusBadRequest)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
		return
	case "proto":
		msgDesc, ok := protoMsgDescCache[req.Name]
		if !ok {
			atomic.AddInt64(&validationErrors, 1)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			http.Error(w, "Unknown proto message type", http.StatusBadRequest)
			return
		}
		msg := dynamicpb.NewMessage(msgDesc)
		b, _ := json.Marshal(req.Data)
		if err := protojson.Unmarshal(b, msg); err != nil {
			atomic.AddInt64(&validationErrors, 1)
			http.Error(w, fmt.Sprintf("Protobuf validation failed: %v", err), http.StatusBadRequest)
			atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
		return
	}

	atomic.AddInt64(&validationErrors, 1)
	http.Error(w, "Validation for this format not implemented", http.StatusNotImplemented)

	// Record latency for all validation attempts
	atomic.AddInt64(&validationLatencyUs, time.Since(start).Microseconds())
}

func metricsRateLogger() {
	var lastValidations int64
	var lastErrors int64
	for range time.Tick(1 * time.Second) {
		currValidations := atomic.LoadInt64(&validationCount)
		currErrors := atomic.LoadInt64(&validationErrors)
		atomic.StoreInt64(&validationsPerSec, currValidations-lastValidations)
		atomic.StoreInt64(&errorsPerSec, currErrors-lastErrors)
		lastValidations = currValidations
		lastErrors = currErrors
	}
}

func main() {
	// Load FileDescriptorSet (compiled from .proto) at startup
	loadProtoDescriptors("schema/logentry.desc") // path to your descriptor set

	// Auto-register all schemas from ./schema/ at startup
	registerSchemasFromDir("./schema")

	mux := http.NewServeMux()
	mux.HandleFunc("/schema/register", schemaRegisterHandler)
	mux.HandleFunc("/schema/get", schemaGetHandler)
	mux.HandleFunc("/schema/list", schemaListHandler)
	mux.HandleFunc("/schema/validate", schemaValidateHandler)
	mux.HandleFunc("/schema/descriptor", schemaDescriptorHandler)
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Listen on a random port
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		log.Fatalf("Failed to parse port: %v", err)
	}
	hostname, _ := os.Hostname()
	selfAddr = fmt.Sprintf("http://%s:%s", hostname, port)

	// Register with cluster manager
	registerWithClusterManager(selfAddr)
	defer unregisterWithClusterManager(selfAddr)

	// Start metrics rate logging
	go metricsRateLogger()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		unregisterWithClusterManager(selfAddr)
		os.Exit(0)
	}()

	log.Printf("Schema Registry API available at %s", selfAddr)
	log.Fatal(http.Serve(ln, mux))
}
