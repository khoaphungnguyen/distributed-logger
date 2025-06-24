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

## 🔧 Features

### ✅ Logger Service

- Logs service heartbeat messages at a configurable frequency
- Logs are written to **both console and rotating file**
- Uses Python’s `logging` module with log level and format controls

### ✅ Log Rotation

- Logs are saved to `logs/app.log`
- When file exceeds **1MB**, it rotates (keeps up to 3 backups)

---

## ⚙️ Configuration Options

You can configure the logger via environment variables in `docker-compose.yml`:

| Variable        | Description                             | Default           |
| --------------- | --------------------------------------- | ----------------- |
| `LOG_LEVEL`     | Logging level (`DEBUG`, `INFO`, etc.)   | `INFO`            |
| `LOG_FREQUENCY` | How often logs are emitted (in seconds) | `5`               |
| `LOG_FORMAT`    | Format string for logs                  | See default below |
| `LOG_FILE`      | Path to log file                        | `logs/app.log`    |

**Example format string:**

```
[%(asctime)s] %(levelname)s: %(message)s
```

You can adjust these values in the `environment:` section of your `docker-compose.yml`.

---

## 🚀 Development Milestones

### Day 1 Milestones

- Set up development environment
- Created project structure
- Implemented basic logger service
- 🔄 Added log file output with rotation
- 🌐 Added web interface to view logs and config
- 🛠 Documented configuration and endpoints

### Day 2 Milestones

- ✅ Enhanced log generator with custom fields (e.g., user ID, session token)
- 🔄 Supported multiple output formats: JSON, CSV, and plain text
- ⚡ Introduced burst mode to simulate spikes in log activity
- 🔁 Refined log patterns to reflect realistic event flows and timing
- 📄 Updated documentation for log schema and usage examples

### Day 3 Milestones

- 👁️‍🗨️ Built a real-time log collector using file watchers
- 🧠 Supported multiple log formats (JSON and plain text) with dynamic parsing
- 🔍 Added regex-based filtering for log entry matching
- 🏷️ Implemented tagging system to categorize entries (e.g., `auth`, `payment`, `api`)
- ⚠️ Tracked and reported parsing errors for malformed log lines
- 💾 Persisted structured collected entries to `collected_logs/collected.jsonl`
- 🛠 Updated Docker Compose to pass collector configuration via CLI arguments
