# Distributed Log Processing System

## Project Overview

We're building a distributed system for processing log data at scale. This repository contains all the code and configuration needed to run the system.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git
- VS Code (recommended)

### Running the Application

1. Clone this repository
2. Navigate to the project directory
3. Run `docker-compose up --build`

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
- Supports configurable **batch size** and **send interval** via CLI flags
- Can scale multiple clients concurrently using Docker Compose

### ✅ Go Ingestor (Server)

- TCP-based ingestion using `net` package for high-throughput log reception
- Handles each client in a separate goroutine
- Logs are written to file via buffered writer
- Tracks and prints real-time **logs/sec** processing rate
- Supports **log rotation at 5MB** and **gzip compression** upon rotation
- Buffered channel and writer for asynchronous disk I/O

---

## ⚙️ Configuration Options (Client)

You can pass flags to the Go client container to configure batch size and interval:

| Flag         | Description                              | Default |
| ------------ | ---------------------------------------- | ------- |
| `--batch`    | Number of logs to send per batch         | `100`   |
| `--interval` | Interval in milliseconds between batches | `1000`  |

Example:

```yaml
go-client:
  command: --batch 500 --interval 10
```

---

### 🧪 Sample Ingestor Output

Server prints logs processed per second:

```
[METRIC] Logs processed: 102345 logs/sec
```

Compressed log files are written to:

```
/app/data/logs_20240622_150000.jsonl.gz
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

- 🚀 Transitioned to **Golang-based TCP log ingestion** (`go-ingestor`)
- 🧱 Built high-performance `go-client` log generator with batching support
- 🔁 Implemented file rotation (5MB max) and Gzip compression upon rollover
- ⚙️ Enabled batching, configurable interval and batch size via CLI flags
- 📈 Server logs ingestion rate (logs/second) in real time
- 🧪 Stress-tested with 2 clients pushing 100,000 logs/sec with no issues
- 🧵 Used Goroutines for connection scaling, non-blocking write pipeline
- 🐋 Updated Docker Compose to support multi-client scale testing

The system now supports **true distributed ingestion** over the network with high throughput and scalability using Go.
