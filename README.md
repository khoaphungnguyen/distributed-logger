# Distributed Log Processing System

## Project Overview

Distributed Logger is a scalable, high-performance log processing platform for cloud environments. It ingests logs in multiple formats (JSON, Protobuf, Avro, and raw) over secure TCP and UDP, with dynamic schema validation and enrichment. The clustered architecture provides automatic leader election, peer discovery, and efficient log replication for high availability. A dedicated cluster manager handles node registration, health checks, and leader tracking, while clients automatically follow the current leader for seamless failover. Live dashboards and metrics offer real-time operational insight, and the platform is fully containerized for an easy deployment.

**New:**

- **Quorum-based replication and read repair** for strong consistency and self-healing.
- **Write and read quorums** for ingestor and query services.
- **Background anti-entropy engine** for storage nodes.

This repository contains all the code and configuration needed to run the system.

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

| Flag         | Description                                   | Default    |
| ------------ | --------------------------------------------- | ---------- |
| `--batch`    | Number of logs to send per batch              | `100`      |
| `--interval` | Interval in milliseconds between batches      | `1000`     |
| `--address`  | Ingestor host address                         | `ingestor` |
| `--tcp-port` | TCP port for ingestion                        | `3001`     |
| `--udp-port` | UDP port for ingestion                        | `3002`     |
| `--udp`      | Use UDP instead of TCP                        | `false`    |
| `--format`   | Log format: `json`, `proto`, `avro`, or `raw` | `json`     |

**Universal handler (default for TCP):**

- All formats (`json`, `proto`, `avro`, `raw`) are sent to TCP port `3001` with a format header.
- UDP is only supported for JSON logs and uses port `3002`.

Example:

```yaml
client:
  command: --batch 500 --interval 10 --address go-ingestor --format avro
  command: --batch 2000 --interval 10 --address go-ingestor --format proto
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

## Development Milestones

---

### 🚀 Day 1 Milestones: Project Initialization

- Set up development environment
- Created project structure
- Implemented basic logger service
- 🔄 Added log file output with rotation
- 🌐 Added web interface to view logs and config
- 🚧 Documented configuration and endpoints

---

### 🚀 Day 2 Milestones: Enhanced Log Generator

- ✅ Enhanced log generator with custom fields (e.g., user ID, session token)
- 🔄 Supported multiple output formats: JSON, CSV, and plain text
- ⚡ Introduced burst mode to simulate spikes in log activity
- 🔁 Refined log patterns to reflect realistic event flows and timing
- 📄 Updated documentation for log schema and usage examples

---

### 🚀 Day 3 Milestones: Real-Time Log Collection

- 👁️ Built a real-time log collector using file watchers
- 🧠 Supported multiple log formats (JSON and plain text) with dynamic parsing
- 🔍 Added regex-based filtering for log entry matching
- 👇 Implemented tagging system to categorize entries (e.g., `auth`, `payment`, `api`)
- ⚠️ Tracked and reported parsing errors for malformed log lines
- 💾 Persisted structured collected entries to `collected_logs/collected.jsonl`
- 🚧 Updated Docker Compose to pass collector configuration via CLI arguments

---

### 🚀 Day 4 Milestones: Structured Output & Metrics

- 🧹 Added support for SQLite and CSV as structured output formats alongside JSON
- 📊 Implemented real-time statistics tracking for log levels, tags, and total entries
- ⚠️ Enhanced error handling with live parsing error count
- 🌐 Integrated a built-in web dashboard (`/metrics`) to display live log stats
- 🧵 Used background threading to serve metrics without blocking the collector
- 🚧️ Updated Docker Compose to support `--output-type`, `--filter`, and exposed web port

---

### 🚀 Day 5 Milestones: Centralized Storage & Performance

- 🔧 Introduced centralized log storage service (`log-storage`)
- 🔁 Switched to Gunicorn for production-ready performance
- 📂 Implemented `.json.gz` rotation with disk usage tracking
- 📊 Improved web dashboard visuals and removed unnecessary charts
- ⚡ Added real-time ingestion rate per second and source tracking

---

### 🚀 Day 6 Milestones: Go Ingestor & High-Performance Client

- 🚀 Transitioned to **Golang-based TCP log ingestion** (`go-ingestor`), replacing the original Flask-based ingestor, which could only handle around 150 messages per connection before significant slowdowns.
- 🧱 Built high-performance `go-client` log generator with batching support
- 🔁 Implemented file rotation (5MB max) and Gzip compression upon rollover
- ⚙️ Enabled batching, configurable interval and batch size via CLI flags
- 📈 Server logs ingestion rate (logs/second) in real time
- 🧪 Stress-tested with 2 clients pushing 100,000 logs/sec with no issues
- 🧵 Used Goroutines for connection scaling, non-blocking write pipeline
- 🐋 Updated Docker Compose to support multi-client scale testing

---

### 🚀 Day 7 Milestones: UDP Ingestion & Monitoring

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

---

### 🚀 Day 8 Milestones: Graceful Shutdown & Configurability

- 🛡️ **Graceful shutdown**: Client now handles SIGINT/SIGTERM for safe exit and resource cleanup
- ⚙️ **Configurable address and port**: Easily set ingestor host and port via CLI flags (`--address`, `--tcp-port`, `--udp-port`)
- 📏 **UDP batch size warning**: Client warns if UDP batch exceeds safe MTU (1400 bytes) to prevent packet loss
- 📈 **Enhanced metrics**: Tracks and logs sent/failed batch counts, with periodic stats output
- 🧹 **Improved error handling**: Handles JSON marshal errors and connection issues robustly
- 🔄 **Retry mechanism**: Retries failed batch transmissions up to 3 times for reliability

---

### 🚀 Day 9 Milestones: TLS & UDP Improvements

- 🔒 **TLS encryption for TCP**: All TCP log traffic is now encrypted using TLS certificates
- 📦 **UDP batch splitting**: Client splits UDP batches to avoid exceeding 1400 bytes (safe MTU)
- 🛡️ **Production-ready ingestion**: Secure, reliable log delivery over TCP; UDP supported for high-throughput, lossy scenarios
- 🧪 **Validated secure ingestion**: Confirmed end-to-end encrypted log flow and UDP chunking in multi-client tests

---

### 🚀 Day 10 Milestones: Ultra-High Throughput & Optimizations

- 🚀 **Ultra-high throughput:** The Go ingestor now reliably handles **1 million log messages per second** on a single instance with minimal drops.
- 🏎️ **Optimized concurrency:** Switched all metrics and counters to atomic operations, eliminating global mutex contention for maximum parallelism.
- 🗃️ **Efficient batching:** Increased batch size and flush intervals for disk writes, reducing overhead and improving sustained throughput.
- ⚡ **Compression tuning:** Leveraged zstd with the fastest compression level for high-speed, space-efficient log storage.
- 💾 **Disk I/O improvements:** System tested and tuned for SSD/NVMe and RAM disk scenarios to ensure disk is not a bottleneck.
- 🧪 **Stress-tested:** Validated with synthetic clients at 1M logs/sec, observing only minimal drops under extreme load.
- 📊 **Accurate live metrics:** Real-time dashboard and `/metrics` endpoint now report logs/sec, MB/sec, latency, queue, rotations, and drops using atomic counters.
- 🧵 **Scalable architecture:** Each writer operates independently, matching the number of CPU cores for optimal resource usage.
- 🛡️ **All previous features retained:** Secure TLS TCP, UDP support, log rotation, compression, graceful shutdown, and robust error handling.

---

### 🚀 Day 11 Milestones: Protobuf Support & Unified Metrics

- 🚀 **Full Protobuf support:** Ingestor and client now support high-throughput, length-prefixed Protobuf log streaming with batching.
- 🏷️ **Schema validation for Protobuf:** Protobuf log entries are validated with the same strict schema checks as JSON logs.
- 📊 **Unified metrics:** Both JSON and Protobuf ingestion paths now report accurate, synchronized live metrics.
- 🖥️ **Dashboard improvements:** Web dashboard and `/metrics` endpoint now reflect true logs/sec and other stats for both formats.
- 🏭 **Production-grade ingestion:** System validated at >1M logs/sec with multiple clients, minimal drops, and robust error handling for both formats.
- ⚡ **Latency breakthrough:** Protobuf ingestion latency reduced from 1–2 µs (microseconds) to as low as **0.2 µs** per log entry, surpassing previous JSON performance.

---

### 🚀 Day 12 Milestones: Avro & Universal Handler

- 📦 **Avro serialization support:** Both client and ingestor now support Avro log serialization and ingestion.
- 🔄 **Universal handler:** All formats (Raw, JSON, Avro, Protobuf) are now handled on a single secure TCP port (`3001`) using a format header.
- 📝 **Raw log support:** Clients can send human-readable raw log lines; server parses and normalizes them.
- 🛠️ **Unified ingestion pipeline:** No more separate ports for different formats—universal handler simplifies deployment and scaling.
- 🌐 **Validated at scale:** Multiple clients sending mixed formats concurrently, all ingested and normalized with accurate metrics and robust performance.

---

### 🚀 Day 13 Milestones: Schema Registry Integration

- 🗄️ **Schema Registry Service:** Added a dedicated schema registry microservice.
- 📥 **Dynamic schema registration:** Clients and servers can register, fetch, and list schemas for all supported formats (JSON, Avro, Protobuf) via HTTP API.
- 🔄 **Dynamic schema fetching:** Both client and ingestor now fetch schemas from the registry at startup, enabling easy schema evolution and multi-type support.
- 🛡️ **End-to-end schema validation:** Both client and server validate logs against the latest schema from the registry, ensuring data integrity and compatibility.
- 🧩 **Extensible architecture:** System now supports registering and using multiple log types and schema versions, paving the way for future extensibility.

---

### 🚀 Day 14 Milestones: Format Detection & Syslog/Journald

- 🕵️ **Format detection engine:** Refined and hardened the automatic detection of incoming log formats in the ingestion pipeline. Now robustly distinguishes between syslog, journald, and custom raw log lines, eliminating accidental misclassification (e.g., JSON detected as raw).
- 🔌 **Syslog & journald adapters:** Improved adapters to parse real-world syslog and journald log lines using regex and field extraction, mapping all standard syslog/journald levels and timestamps to the normalized `LogEntry` struct.
- 🔄 **Universal handler integration:** Enhanced the universal handler to seamlessly ingest syslog, journald, and raw formats alongside JSON, Avro, and Protobuf, with accurate format routing and parsing.
- 🛡️ **Schema validation for new formats:** Extended schema validation to cover logs ingested via syslog and journald, ensuring all data conforms to the unified schema and preventing ingestion of incomplete or malformed entries.
- 📤 **Flexible output options:** Continued support for exporting logs in structured text and CSV formats, in addition to JSONL and compressed outputs.
- 📊 **Dashboard enhancements:** Updated the web dashboard and `/metrics` endpoint to display live statistics and sample entries per log format, providing deeper operational insights and real-time visibility into ingested data.

---

### 🚀 Day 15 Milestones: Log Enrichment Pipeline

- ✨ **Log enrichment pipeline:** Added an enrichment step to the ingestion pipeline, automatically attaching contextual metadata (such as `hostname`, `environment`, `app_version`, `received_at`, and container/pod info) to every log entry.
- 🏷️ **Configurable enrichment fields:** Enrichment fields are sourced from environment variables and system calls, with sensible defaults for missing values.
- 🔄 **Integrated enrichment in ingestor:** For small-scale deployments, enrichment is performed directly in the ingestor service for simplicity and performance.
- 🧩 **Future-proof design:** The enrichment logic is modular and ready to be moved to a dedicated microservice as the system scales, enabling independent scaling and advanced enrichment strategies.
- 📊 **Dashboard and metrics update:** The web dashboard and `/metrics` endpoint now display sample logs with all enrichment fields, providing full visibility into the enriched log schema.

---

### 🚀 Day 16 Milestones: Cluster Manager & Leader Election

- 🗂️ **Cluster Manager Service:** Introduced a dedicated cluster manager microservice responsible for node registration, health checks, leader election, and peer discovery. All ingestors now register and synchronize their state via the cluster manager.
- 👑 **Replicated Leader Architecture:** The ingestor service now supports leader election and log replication. Only the elected leader ingests logs from clients; followers receive replicated logs from the leader for high availability and durability.
- 🔄 **Peer and Leader Synchronization:** Ingestors automatically update their view of the cluster, leader, and peers in real time, ensuring seamless failover and consistent replication.
- 📦 **Efficient Batched Replication:** Log replication between leader and followers is performed in efficient batches, minimizing network overhead and maximizing throughput.
- 🔁 **Client Leader Awareness:** The client now dynamically discovers and connects to the current leader via the cluster manager, automatically reconnecting if the leader changes or fails.
- 🖥️ **Cluster Dashboard:** The cluster manager exposes a simple web dashboard displaying the current leader and all healthy peers, auto-refreshing every second for real-time cluster visibility and failover simulation.
- 🛡️ **Robustness Improvements:** Enhanced shutdown handling, error recovery, and resource cleanup across all services to ensure stability during failover and rolling updates.

---

### 🚀 Day 17 Milestones: Ingestor/Storage Separation & Partitioned Batching

- 🏗️ **Ingestor/Storage Separation:** The ingestion and storage responsibilities are now handled by dedicated microservices. The ingestor focuses on high-throughput log validation, enrichment, batching, and forwarding, while the storage service is optimized for efficient, concurrent disk writes.
- 🧩 **Partitioned Batching Pipeline:** The ingestor now partitions incoming logs across multiple independent batching pipelines based on log attributes (e.g., service name). Each partition batches logs and forwards them to storage in parallel, maximizing CPU and network utilization.
- ⚖️ **Balanced Storage Load:** Partitioned batching ensures logs are evenly distributed across all available storage nodes, preventing hotspots and enabling true horizontal scalability.
- 🚀 **High-Throughput Storage Writes:** The storage service now supports concurrent, batched writes per partition, dramatically increasing sustained write throughput and reducing lock contention.
- 🔄 **Efficient Batch Transfer:** Ingestor-to-storage communication is performed in large batches over HTTP, minimizing serialization and network overhead.
- 📈 **Accurate Metrics:** Metrics now reflect logs actually delivered to storage, providing a true picture of end-to-end throughput and system health.
- 🛠️ **Pluggable Storage Backend (Ready):** The storage service is designed for easy extension to support alternative backends (e.g., S3, cloud storage, or distributed filesystems) in future milestones.
- 🧪 **Stress-Tested at Scale:** The new architecture has been validated at 1M+ logs/sec with multiple clients and storage nodes, demonstrating robust performance and balanced resource usage.
- 📊 **Dashboard Enhancements:** The dashboard and `/metrics` endpoint now report partition-level stats, storage node health, and end-to-end delivery rates for full operational visibility.

---

### 🚀 Day 18 Milestones: Consistent Hashing, Raft, and Dynamic Rebalancing

- 🔗 **Consistent Hashing for Log Distribution:** Implemented consistent hashing in the ingestor pipeline to ensure logs are evenly and predictably distributed across storage nodes, even as nodes are added or removed. This minimizes data movement and prevents hotspots, enabling seamless horizontal scaling.
- 👑 **Raft-Based Leader Election & Fast Failover:** Integrated Raft for robust, decentralized leader election among ingestor nodes. When the leader fails, a new leader is automatically re-elected within 1 seconds, ensuring minimal disruption. Only the elected leader coordinates cluster-wide tasks, while followers synchronize state and are ready to take over instantly.
- ❤️ **Automated Health Checks:** The cluster manager continuously performs health checks on all registered nodes. If a node (including the leader) becomes unhealthy or unreachable, it is automatically removed from the active peer list, and a new leader is elected if necessary.
- ⚖️ **Dynamic Rebalancing:** When storage nodes join or leave, the consistent hashing ring automatically rebalances log distribution with minimal disruption.
- 🔄 **Seamless Client Reconnection:** Both clients and ingestors automatically detect leader changes and update their connections. Clients reconnect to the new leader within 1 seconds after a failover, ensuring uninterrupted log ingestion and replication. The system quickly recovers from node failures with zero manual intervention.
- 🛡️ **High Availability:** The combination of Raft leader election, rapid failover, and health checks ensures the cluster remains highly available and self-healing, even during rolling updates.

---

### 🚀 Day 19 Milestones: Distributed Query Service

- 🔍 **Distributed Query Service:** Introduced a dedicated query microservice that enables users and tools to efficiently query logs across all storage nodes and partitions.
- ⚡ **Parallel, Partition-Aware Search:** The query service discovers healthy storage nodes from the cluster manager and fans out queries in parallel, aggregating results from all relevant partitions for fast, scalable log retrieval.
- 🧠 **Service & Time-Range Optimization:** Queries are optimized to scan only the relevant files based on service name and time range, dramatically reducing search latency and resource usage.
- 🧵 **Concurrent File Scanning:** Each storage node processes file scans concurrently, leveraging Go's goroutines for high-throughput, low-latency distributed search.
- 🛠️ **Simple REST API:** Exposes a RESTful `/query` endpoint for flexible log search by service, level, and time window, supporting pagination and result limits.
- 🔄 **Dynamic Node Awareness:** The query service automatically tracks changes in the storage cluster, ensuring queries always reach all healthy nodes—even as nodes join or leave.
- 📈 **Cluster-Wide Visibility:** Enables real-time, cluster-wide log analytics and troubleshooting, making it easy to find and aggregate logs from any service or time period, regardless of where they are stored.

---

### 🚀 Day 20 Milestones: Quorum Replication & Read Repair Engine

- 🗂️ **Quorum-Based Replication:** The ingestor now uses a configurable write quorum for log replication to storage nodes, ensuring durability and consistency even in the presence of node failures.
- 🛡️ **Write Quorum for Ingestor:** Logs are only considered successfully stored when a quorum of storage nodes acknowledges the batch, providing strong guarantees against data loss.
- 🔍 **Read Quorum for Query:** The query service now performs quorum reads, collecting results from a configurable number of replicas and merging them to ensure the freshest data is returned.
- 🔄 **Read Repair Engine:** During query operations, if stale data is detected on any replica, the freshest version is immediately sent back to the stale node for repair, ensuring the cluster self-heals over time.
- 🔧 **Background Anti-Entropy (Storage):** Each storage node runs a background repair engine that periodically checks its data against peers, repairing itself or others if staleness is detected, inspired by Dynamo/Cassandra anti-entropy.
- ⚖️ **Dynamic Quorum Configuration:** Both write and read quorums are dynamically calculated based on the number of healthy storage nodes, with sensible minimums for safety and performance.
- 📈 **Consistency & Availability:** These features together provide strong consistency, high availability, and robust self-healing, making the system production-ready for demanding distributed log workloads.
