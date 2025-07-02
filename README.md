# Distributed Log Processing System

## Project Overview

We're building a distributed system for processing log data at scale. This repository contains all the code and configuration needed to run the system.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git
- VS Code (recommended)

### Running the Application

1. Clone this repository.
2. Navigate to the project directory.
3. Run `docker-compose up --build` to build and start all services.
4. When finished, press `Ctrl+C` to stop the services.
5. (Optional) Run `docker-compose down` to clean up containers, networks, and volumes.

## Project Structure

- `src/services/`: Contains individual microservices
- `config/`: Configuration files
- `data/`: Data storage (gitignored)
- `docs/`: Documentation
- `tests/`: Test suites

---

## 🔧 Features (Golang-Based)

### ✅ Go Client

- Simulates real-time log generation with random levels, messages, and services
- Supports configurable **batch size**, **send interval**, **address**, and **protocol** (TCP/UDP) via CLI flags
- **Supports JSON, Protobuf, Avro, and Raw formats** for log transmission (`--format json`, `--format proto`, `--format avro`, or `--format raw`)
- **Universal handler:** All formats are sent over a single secure TCP port (`3001`) with a format header for seamless ingestion
- **Schema validation:** Client fetches schemas dynamically from the schema registry and validates logs before sending
- **Graceful shutdown:** Handles SIGINT/SIGTERM for safe exit and resource cleanup
- **UDP batch splitting:** Automatically splits large batches to avoid exceeding safe MTU (1400 bytes)
- **UDP batch size warning:** Warns if any UDP chunk exceeds safe MTU
- **Retry mechanism:** Retries failed batch transmissions up to 3 times
- **Enhanced metrics:** Tracks and logs sent/failed batch counts, with periodic stats output
- **Improved error handling:** Handles marshal errors and connection issues robustly
- **TLS encryption:** Uses TLS for secure TCP log transmission
- **Efficient batching:** Batches multiple messages into a single write for maximum throughput

## ✅ Go Ingestor (Server)

- **TLS-encrypted TCP ingestion** for secure log reception
- **Universal handler:** Accepts JSON, Protobuf, Avro, and Raw logs on a single TCP port (`3001`) using a format header
- **UDP ingestion** for high-throughput, lossy log reception (JSON only, port `3002`)
- Handles each client in a separate goroutine
- **Schema validation:** Server fetches schemas from the schema registry and validates all incoming logs for every format
- Logs are written to file via buffered writer
- Tracks and prints real-time **logs/sec**, **MB/sec**, **latency**, **queue length**, **file rotations**, and **dropped logs**
- Supports **log rotation at 50MB** and **zstd compression** upon rotation
- Buffered channel and writer for asynchronous disk I/O
- Built-in web dashboard (`/`) and `/metrics` endpoint for live stats
- **Highly concurrent:** Each writer operates independently, matching the number of CPU cores

---

## ⚙️ Configuration Options (Client)

You can pass flags to the Go client container to configure its behavior:

| Flag         | Description                                   | Default       |
| ------------ | --------------------------------------------- | ------------- |
| `--batch`    | Number of logs to send per batch              | `100`         |
| `--interval` | Interval in milliseconds between batches      | `1000`        |
| `--address`  | Ingestor host address                         | `go-ingestor` |
| `--tcp-port` | TCP port for ingestion                        | `3001`        |
| `--udp-port` | UDP port for ingestion                        | `3002`        |
| `--udp`      | Use UDP instead of TCP                        | `false`       |
| `--format`   | Log format: `json`, `proto`, `avro`, or `raw` | `json`        |

**Universal handler (default for TCP):**

- All formats (`json`, `proto`, `avro`, `raw`) are sent to TCP port `3001` with a format header.
- UDP is only supported for JSON logs and uses port `3002`.

Example:

```yaml
go-client:
  command: --batch 500 --interval 10 --address go-ingestor --format avro
```

---

## 🔒 Generating TLS Certificates

To enable TLS for secure TCP log transmission, generate self-signed certificates (for testing):

```sh
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
```

Place `cert.pem` and `key.pem` in the appropriate directory (e.g., `src/services/go-ingestor/certs/`).

---

### 🧪 Sample Ingestor Output

Server prints logs processed per second and other metrics:

```
go-ingestor_1  | 2025/06/30 21:59:23 [METRIC] Logs/sec: 1104395, MB/s: 104.41, Latency: 0.47µs, Queue: 0 (max: 85803), Rotations: 12, Goroutines: 23, FileSize: 199.16MB, Dropped: 132248
```

Compressed log files are written to:

```
/app/data/log_0_20250630_214517.jsonl.zst
```

---

## 🚀 Development Milestones

### Day 1 Milestones

- Set up development environment
- Created project structure
- Implemented basic logger service
- 🔄 Added log file output with rotation
- 🌐 Added web interface to view logs and config
- 🚧 Documented configuration and endpoints

### Day 2 Milestones

- ✅ Enhanced log generator with custom fields (e.g., user ID, session token)
- 🔄 Supported multiple output formats: JSON, CSV, and plain text
- ⚡ Introduced burst mode to simulate spikes in log activity
- 🔁 Refined log patterns to reflect realistic event flows and timing
- 📄 Updated documentation for log schema and usage examples

### Day 3 Milestones

- 👁️ Built a real-time log collector using file watchers
- 🧠 Supported multiple log formats (JSON and plain text) with dynamic parsing
- 🔍 Added regex-based filtering for log entry matching
- 👇 Implemented tagging system to categorize entries (e.g., `auth`, `payment`, `api`)
- ⚠️ Tracked and reported parsing errors for malformed log lines
- 💾 Persisted structured collected entries to `collected_logs/collected.jsonl`
- 🚧 Updated Docker Compose to pass collector configuration via CLI arguments

### Day 4 Milestones

- 🧹 Added support for SQLite and CSV as structured output formats alongside JSON
- 📊 Implemented real-time statistics tracking for log levels, tags, and total entries
- ⚠️ Enhanced error handling with live parsing error count
- 🌐 Integrated a built-in web dashboard (`/metrics`) to display live log stats
- 🧵 Used background threading to serve metrics without blocking the collector
- 🚧️ Updated Docker Compose to support `--output-type`, `--filter`, and exposed web port

### Day 5 Milestones

- 🔧 Introduced centralized log storage service (`log-storage`)
- 🔁 Switched to Gunicorn for production-ready performance
- 📂 Implemented `.json.gz` rotation with disk usage tracking
- 📊 Improved web dashboard visuals and removed unnecessary charts
- ⚡ Added real-time ingestion rate per second and source tracking

### Day 6 Milestones

- 🚀 Transitioned to **Golang-based TCP log ingestion** (`go-ingestor`), replacing the original Flask-based ingestor, which could only handle around 150 messages per connection before significant slowdowns.
- 🧱 Built high-performance `go-client` log generator with batching support
- 🔁 Implemented file rotation (5MB max) and Gzip compression upon rollover
- ⚙️ Enabled batching, configurable interval and batch size via CLI flags
- 📈 Server logs ingestion rate (logs/second) in real time
- 🧪 Stress-tested with 2 clients pushing 100,000 logs/sec with no issues
- 🧵 Used Goroutines for connection scaling, non-blocking write pipeline
- 🐋 Updated Docker Compose to support multi-client scale testing

### Day 7 Milestones

- 📡 **Added UDP ingestion support** alongside existing TCP server in `go-ingestor`
- 🔀 **Dual protocol support** (TCP/UDP) running on ports `3000` and `3001`
- ⚖️ Used **Goroutines** and **separate handlers** to process UDP packets efficiently
- 🧪 Validated ingestion consistency with `go-client` supporting `--udp` flag
- 📊 **Enhanced monitoring dashboard** (`/`) with:
  - Logs/sec
  - MB/sec throughput
  - Avg latency (µs)
  - Queue length
  - File rotation count
- 💾 Observed Gzip compression saving **\~95%+ storage** on log files
- 🔍 Simulated realistic log generation: 5% ERROR, 10% WARN, 85% INFO/DEBUG

### Day 8 Milestones

- 🛡️ **Graceful shutdown**: Client now handles SIGINT/SIGTERM for safe exit and resource cleanup
- ⚙️ **Configurable address and port**: Easily set ingestor host and port via CLI flags (`--address`, `--tcp-port`, `--udp-port`)
- 📏 **UDP batch size warning**: Client warns if UDP batch exceeds safe MTU (1400 bytes) to prevent packet loss
- 📈 **Enhanced metrics**: Tracks and logs sent/failed batch counts, with periodic stats output
- 🧹 **Improved error handling**: Handles JSON marshal errors and connection issues robustly
- 🔄 **Retry mechanism**: Retries failed batch transmissions up to 3 times for reliability

### Day 9 Milestones

- 🔒 **TLS encryption for TCP**: All TCP log traffic is now encrypted using TLS certificates
- 📦 **UDP batch splitting**: Client splits UDP batches to avoid exceeding 1400 bytes (safe MTU)
- 🛡️ **Production-ready ingestion**: Secure, reliable log delivery over TCP; UDP supported for high-throughput, lossy scenarios
- 🧪 **Validated secure ingestion**: Confirmed end-to-end encrypted log flow and UDP chunking in multi-client tests

### Day 10 Milestones

- 🚀 **Ultra-high throughput:** The Go ingestor now reliably handles **1 million log messages per second** on a single instance with minimal drops.
- 🏎️ **Optimized concurrency:** Switched all metrics and counters to atomic operations, eliminating global mutex contention for maximum parallelism.
- 🗃️ **Efficient batching:** Increased batch size and flush intervals for disk writes, reducing overhead and improving sustained throughput.
- ⚡ **Compression tuning:** Leveraged zstd with the fastest compression level for high-speed, space-efficient log storage.
- 💾 **Disk I/O improvements:** System tested and tuned for SSD/NVMe and RAM disk scenarios to ensure disk is not a bottleneck.
- 🧪 **Stress-tested:** Validated with synthetic clients at 1M logs/sec, observing only minimal drops under extreme load.
- 📊 **Accurate live metrics:** Real-time dashboard and `/metrics` endpoint now report logs/sec, MB/sec, latency, queue, rotations, and drops using atomic counters.
- 🧵 **Scalable architecture:** Each writer operates independently, matching the number of CPU cores for optimal resource usage.
- 🛡️ **All previous features retained:** Secure TLS TCP, UDP support, log rotation, compression, graceful shutdown, and robust error handling.

### 🚀 Day 11 Milestones

- 🚀 **Full Protobuf support:** Ingestor and client now support high-throughput, length-prefixed Protobuf log streaming with batching.
- 🏷️ **Schema validation for Protobuf:** Protobuf log entries are validated with the same strict schema checks as JSON logs.
- 📊 **Unified metrics:** Both JSON and Protobuf ingestion paths now report accurate, synchronized live metrics.
- 🖥️ **Dashboard improvements:** Web dashboard and `/metrics` endpoint now reflect true logs/sec and other stats for both formats.
- 🏭 **Production-grade ingestion:** System validated at >1M logs/sec with multiple clients, minimal drops, and robust error handling for both formats.
- ⚡ **Latency breakthrough:** Protobuf ingestion latency reduced from 1–2 µs (microseconds) to as low as **0.2 µs** per log entry, surpassing previous JSON performance.

### 🚀 Day 12 Milestones

- 📦 **Avro serialization support:** Both client and ingestor now support Avro log serialization and ingestion.
- 🔄 **Universal handler:** All formats (Raw, JSON, Avro, Protobuf) are now handled on a single secure TCP port (`3001`) using a format header.
- 📝 **Raw log support:** Clients can send human-readable raw log lines; server parses and normalizes them.
- 🛠️ **Unified ingestion pipeline:** No more separate ports for different formats—universal handler simplifies deployment and scaling.
- 🌐 **Validated at scale:** Multiple clients sending mixed formats concurrently, all ingested and normalized with accurate metrics and robust performance.

### 🚀 Day 13 Milestones

- 🗄️ **Schema Registry Service:** Added a dedicated schema registry microservice.
- 📥 **Dynamic schema registration:** Clients and servers can register, fetch, and list schemas for all supported formats (JSON, Avro, Protobuf) via HTTP API.
- 🔄 **Dynamic schema fetching:** Both client and ingestor now fetch schemas from the registry at startup, enabling easy schema evolution and multi-type support.
- 🛡️ **End-to-end schema validation:** Both client and server validate logs against the latest schema from the registry, ensuring data integrity and compatibility.
- 🧩 **Extensible architecture:** System now supports registering and using multiple log types and schema versions, paving the way for future extensibility.

## 🚀 Day 14 Milestones

- 🕵️ **Format detection engine:** Refined and hardened the automatic detection of incoming log formats in the ingestion pipeline. Now robustly distinguishes between syslog, journald, and custom raw log lines, eliminating accidental misclassification (e.g., JSON detected as raw).
- 🔌 **Syslog & journald adapters:** Improved adapters to parse real-world syslog and journald log lines using regex and field extraction, mapping all standard syslog/journald levels and timestamps to the normalized `LogEntry` struct.
- 🔄 **Universal handler integration:** Enhanced the universal handler to seamlessly ingest syslog, journald, and raw formats alongside JSON, Avro, and Protobuf, with accurate format routing and parsing.
- 🛡️ **Schema validation for new formats:** Extended schema validation to cover logs ingested via syslog and journald, ensuring all data conforms to the unified schema and preventing ingestion of incomplete or malformed entries.
- 📤 **Flexible output options:** Continued support for exporting logs in structured text and CSV formats, in addition to JSONL and compressed outputs.
- 📊 **Dashboard enhancements:** Updated the web dashboard and `/metrics` endpoint to display live statistics and sample entries per log format, providing deeper operational insights and real-time visibility into ingested data.
