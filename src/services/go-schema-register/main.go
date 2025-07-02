package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"sync"

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

func main() {
	// Load FileDescriptorSet (compiled from .proto) at startup
	loadProtoDescriptors("schema/logentry.desc") // path to your descriptor set

	// Auto-register all schemas from ./schema/ at startup
	registerSchemasFromDir("./schema")

	http.HandleFunc("/schema/register", schemaRegisterHandler)
	http.HandleFunc("/schema/get", schemaGetHandler)
	http.HandleFunc("/schema/list", schemaListHandler)
	http.HandleFunc("/schema/validate", schemaValidateHandler)
	http.HandleFunc("/schema/descriptor", schemaDescriptorHandler)
	log.Println("Schema Registry API available at :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

// Load FileDescriptorSet and cache message descriptors
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

// Register all *.json files in the given directory as schemas
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

func schemaListHandler(w http.ResponseWriter, r *http.Request) {
	schemaMutex.RLock()
	defer schemaMutex.RUnlock()
	var schemas []Schema
	for _, s := range schemaStore {
		schemas = append(schemas, s)
	}
	json.NewEncoder(w).Encode(schemas)
}

// GET /schema/descriptor?name=LogEntry
func schemaDescriptorHandler(w http.ResponseWriter, r *http.Request) {
	//name := r.URL.Query().Get("name")
	// For demo, always serve the same descriptor set file
	descBytes, err := ioutil.ReadFile("schema/logentry.desc")
	if err != nil {
		http.Error(w, "Descriptor not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(descBytes)
}

// POST /schema/validate
// Body: { "format": "json|avro|proto", "name": "LogEntry", "data": {...} }
func schemaValidateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req ValidateRequest
	body, _ := ioutil.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	key := req.Format + ":" + req.Name

	switch req.Format {
	case "json":
		schemaMutex.RLock()
		schema, ok := jsonSchemaCache[key]
		schemaMutex.RUnlock()
		if !ok {
			http.Error(w, "Schema not found", http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(req.Data)
		docLoader := gojsonschema.NewBytesLoader(b)
		result, err := schema.Validate(docLoader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}
		if !result.Valid() {
			http.Error(w, fmt.Sprintf("Validation failed: %v", result.Errors()), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		return
	case "avro":
		schemaMutex.RLock()
		avroSchema, ok := avroSchemaCache[key]
		schemaMutex.RUnlock()
		if !ok {
			http.Error(w, "Schema not found", http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(req.Data)
		var native interface{}
		if err := json.Unmarshal(b, &native); err != nil {
			http.Error(w, "Invalid Avro data", http.StatusBadRequest)
			return
		}
		_, err := avro.Marshal(avroSchema, native)
		if err != nil {
			http.Error(w, fmt.Sprintf("Avro validation failed: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		return
	case "proto":
		// Use dynamicpb for runtime validation
		msgDesc, ok := protoMsgDescCache[req.Name]
		if !ok {
			http.Error(w, "Unknown proto message type", http.StatusBadRequest)
			return
		}
		msg := dynamicpb.NewMessage(msgDesc)
		b, _ := json.Marshal(req.Data)
		if err := protojson.Unmarshal(b, msg); err != nil {
			http.Error(w, fmt.Sprintf("Protobuf validation failed: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Validation successful"))
		return
	}

	http.Error(w, "Validation for this format not implemented", http.StatusNotImplemented)
}
