from flask import Flask, request, jsonify, render_template_string
import os
import json
import gzip
import time
import threading
import shutil
from collections import defaultdict, deque
from datetime import datetime

app = Flask(__name__)

STORAGE_DIR = os.environ.get("STORAGE_DIR", "./storage_data")
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 1 * 1024 * 1024))  # Default 1MB
RETENTION_LIMIT = int(os.environ.get("RETENTION_LIMIT", 5))  # Default 5 files

active_logs = defaultdict(list)
source_last_seen = {}
stats = defaultdict(int)
service_stats = defaultdict(lambda: defaultdict(int))
log_buffer = deque()
buffer_lock = threading.Lock()
request_timestamps = deque(maxlen=100)

if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

@app.route("/ingest", methods=["POST"])
def ingest():
    data = request.get_json()
    if not data:
        return "Bad Request", 400

    now = datetime.utcnow()
    source = request.remote_addr
    source_last_seen[source] = now
    request_timestamps.append(time.time())

    entries = [data] if isinstance(data, dict) else data

    with buffer_lock:
        for entry in entries:
            entry["_received"] = now.isoformat()
            log_buffer.append(entry)
            level = entry.get("level", "UNKNOWN").upper()
            service = entry.get("service", "unknown") or "unknown"
            stats[level] += 1
            stats["total"] += 1
            service_stats[service][level] += 1

    return "OK", 200

@app.route("/metrics")
def metrics():
    now_ts = time.time()
    rps = sum(1 for t in request_timestamps if now_ts - t <= 1)
    buffered = len(log_buffer)

    levels = [k for k in stats if k != "total"]
    values = [stats[k] for k in levels]

    services = list(service_stats.keys())
    service_data = {
        svc: [service_stats[svc].get(lvl, 0) for lvl in levels]
        for svc in services if any(service_stats[svc].get(lvl, 0) for lvl in levels)
    }

    disk_usage = shutil.disk_usage(STORAGE_DIR)
    usage_stats = {
        "free": round(disk_usage.free / 1024 / 1024, 2),
        "used": round(disk_usage.used / 1024 / 1024, 2),
        "total": round(disk_usage.total / 1024 / 1024, 2)
    }

    current_files = sorted([f for f in os.listdir(STORAGE_DIR) if f.endswith(".json.gz")])
    current_file_sizes = {
        f: round(os.path.getsize(os.path.join(STORAGE_DIR, f)) / 1024 / 1024, 2)
        for f in current_files
    }

    html = """
    <html><head><title>Log Storage Metrics</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <meta http-equiv="refresh" content="5">
    </head>
    <body>
    <h2>Current Metrics</h2>
    <canvas id="logChart" width="600" height="200"></canvas>
    <script>
        const logCtx = document.getElementById('logChart').getContext('2d');
        const chart = new Chart(logCtx, {
            type: 'bar',
            data: {
                labels: {{ levels | tojson }},
                datasets: [
                    {
                        label: 'Total Log Levels',
                        data: {{ values | tojson }},
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    },
                    {% for svc, svc_data in service_data.items() %}
                    {
                        label: '{{ svc }}',
                        data: {{ svc_data | tojson }},
                        type: 'line'
                    },
                    {% endfor %}
                ]
            },
            options: {
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
    </script>

    <h3>Last Seen Per Source</h3>
    <ul>
    {% for source, timestamp in sources.items() %}
        <li>{{ source }}: {{ timestamp }}</li>
    {% endfor %}
    </ul>

    <h3>Real-Time Stats</h3>
    <ul>
        <li>Buffered Log Entries: {{ buffered }}</li>
        <li>Requests per Second: {{ rps }}</li>
    </ul>

    <h3>Disk Usage</h3>
    <ul>
        <li>Total: {{ usage.total }} MB</li>
        <li>Used: {{ usage.used }} MB</li>
        <li>Free: {{ usage.free }} MB</li>
    </ul>

    <h3>Log Files</h3>
    <ul>
    {% for f, size in files.items() %}
        <li>{{ f }} - {{ size }} MB</li>
    {% endfor %}
    </ul>
    </body></html>
    """

    return render_template_string(
        html,
        stats=stats,
        sources=source_last_seen,
        levels=levels,
        values=values,
        service_data=service_data,
        usage=usage_stats,
        files=current_file_sizes,
        buffered=buffered,
        rps=rps
    )

@app.route("/config")
def config():
    return jsonify({
        "STORAGE_DIR": STORAGE_DIR,
        "MAX_FILE_SIZE": MAX_FILE_SIZE,
        "RETENTION_LIMIT": RETENTION_LIMIT
    })

def rotate_logs():
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    base_path = os.path.join(STORAGE_DIR, f"log_{timestamp}.json.gz")
    with gzip.open(base_path, "wt", encoding="utf-8") as f:
        with buffer_lock:
            if not log_buffer:
                return
            while log_buffer:
                f.write(json.dumps(log_buffer.popleft()) + "\n")
    prune_old_logs()

def prune_old_logs():
    all_logs = sorted([f for f in os.listdir(STORAGE_DIR) if f.endswith(".json.gz")])
    while len(all_logs) > RETENTION_LIMIT:
        oldest = all_logs.pop(0)
        try:
            os.remove(os.path.join(STORAGE_DIR, oldest))
        except Exception:
            pass

def background_rotator():
    while True:
        time.sleep(5)
        with buffer_lock:
            data_to_measure = ''.join(json.dumps(entry) for entry in list(log_buffer))
            if not log_buffer:
                continue
            if len(data_to_measure.encode('utf-8')) < 100:
                continue
            if len(data_to_measure.encode('utf-8')) >= MAX_FILE_SIZE:
                rotate_logs()
            else:
                rotate_logs()

if __name__ == "__main__":
    threading.Thread(target=background_rotator, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
