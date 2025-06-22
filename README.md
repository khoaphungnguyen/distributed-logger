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
4. Visit `http://localhost:8080` to access the web interface

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

### ✅ Web Interface
- Minimal Flask app running on port `8080`
- Endpoints:
  - `/` – Welcome message
  - `/logs` – View the last 50 lines of logs
  - `/config` – View current logger configuration

### ✅ Log Rotation
- Logs are saved to `logs/app.log`
- When file exceeds **1MB**, it rotates (keeps up to 3 backups)

---

## ⚙️ Configuration Options

You can configure the logger via environment variables in `docker-compose.yml`:

| Variable        | Description                              | Default          |
|-----------------|------------------------------------------|------------------|
| `LOG_LEVEL`     | Logging level (`DEBUG`, `INFO`, etc.)    | `INFO`           |
| `LOG_FREQUENCY` | How often logs are emitted (in seconds)  | `5`              |
| `LOG_FORMAT`    | Format string for logs                   | See default below|
| `LOG_FILE`      | Path to log file                         | `logs/app.log`   |

**Example format string**:
```

[%(asctime)s] %(levelname)s: %(message)s

```

You can adjust these values in the `environment:` section of your `docker-compose.yml`.

---

## Day 1 Milestones
- Set up development environment
- Created project structure
- Implemented basic logger service
- 🔄 Added log file output with rotation
- 🌐 Added web interface to view logs and config
- 🛠 Documented configuration and endpoints
