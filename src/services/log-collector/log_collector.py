import os
import time
import yaml
import logging
import argparse
import re
import json
import csv
import sqlite3
from collections import defaultdict
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from flask import Flask, jsonify
import threading

# Configure logging for our collector service
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LogCollector')

app = Flask(__name__)
collector_stats = {}

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file_path, log_format="text", output_dir=None, filter_regex=None, output_type="json"):
        self.log_file_path = log_file_path
        self.log_format = log_format
        self.output_dir = output_dir
        self.filter_regex = re.compile(filter_regex) if filter_regex else None
        self.last_position = 0
        self.parsing_errors = 0
        self.output_type = output_type
        self.stats = defaultdict(int)
        self.initialize_output()
        self.initialize_position()

    def initialize_position(self):
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'r') as file:
                file.seek(0, os.SEEK_END)
                self.last_position = file.tell()
                logger.info(f"Initialized tracking for {self.log_file_path} at position {self.last_position}")

    def initialize_output(self):
        if not self.output_dir:
            return
        os.makedirs(self.output_dir, exist_ok=True)
        if self.output_type == "csv":
            self.csv_path = os.path.join(self.output_dir, "collected.csv")
            if not os.path.exists(self.csv_path):
                with open(self.csv_path, "w", newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=["timestamp", "level", "service", "user_id", "request_id", "duration", "message", "tag"])
                    writer.writeheader()
        elif self.output_type == "sqlite":
            self.db_path = os.path.join(self.output_dir, "collected.db")
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute('''CREATE TABLE IF NOT EXISTS logs (
                timestamp TEXT,
                level TEXT,
                service TEXT,
                user_id TEXT,
                request_id TEXT,
                duration INTEGER,
                message TEXT,
                tag TEXT
            )''')
            self.conn.commit()

    def parse_line(self, line):
        try:
            if self.log_format == "json":
                return json.loads(line)
            else:
                return {"timestamp": "", "level": "INFO", "service": "", "user_id": "", "request_id": "", "duration": 0, "message": line.strip()}
        except Exception:
            self.parsing_errors += 1
            return None

    def tag_entry(self, entry):
        msg = entry.get("message", "")
        if "login" in msg:
            entry["tag"] = "auth"
        elif "checkout" in msg:
            entry["tag"] = "payment"
        elif "API" in msg:
            entry["tag"] = "api"
        else:
            entry["tag"] = "general"
        return entry

    def update_stats(self, entry):
        self.stats['total'] += 1
        self.stats[entry.get('level', 'UNKNOWN')] += 1
        self.stats[f"tag:{entry.get('tag', 'untagged')}"] += 1
        self.stats['parsing_errors'] = self.parsing_errors
        collector_stats[self.log_file_path] = dict(self.stats)

    def print_stats(self):
        logger.info(f"--- Stats for {self.log_file_path} ---")
        for key, val in self.stats.items():
            logger.info(f"{key}: {val}")
        logger.info(f"parsing_errors: {self.parsing_errors}")
        logger.info("----------------------------------")

    def write_entry(self, entry):
        if not self.output_dir:
            return
        if self.output_type == "json":
            out_path = os.path.join(self.output_dir, "collected.jsonl")
            with open(out_path, "a") as out:
                out.write(json.dumps(entry) + "\n")
        elif self.output_type == "csv":
            with open(self.csv_path, "a", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=entry.keys())
                writer.writerow(entry)
        elif self.output_type == "sqlite":
            values = tuple(entry.get(k) for k in ["timestamp", "level", "service", "user_id", "request_id", "duration", "message", "tag"])
            self.conn.execute("INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", values)
            self.conn.commit()

    def on_modified(self, event):
        if event.src_path == self.log_file_path:
            self.collect_new_logs()

    def collect_new_logs(self):
        try:
            with open(self.log_file_path, 'r') as file:
                file.seek(self.last_position)
                new_content = file.read()

                if new_content:
                    for line in new_content.splitlines():
                        if not line.strip():
                            continue
                        if self.filter_regex and not self.filter_regex.search(line):
                            continue
                        parsed = self.parse_line(line)
                        if not parsed:
                            continue
                        tagged = self.tag_entry(parsed)
                        self.update_stats(tagged)
                        print(f"COLLECTED: {tagged}")
                        self.write_entry(tagged)
                self.last_position = file.tell()
                self.print_stats()
        except Exception as e:
            logger.error(f"Error reading log file: {e}")

class LogCollectorService:
    def __init__(self):
        self.args = self.parse_args()
        self.observers = []
        self.handlers = []
        self.run_config = {
            'log_files': self.args.log_files or [],
            'log_format': self.args.log_format,
            'output_dir': self.args.output_dir,
            'filter': self.args.filter,
            'output_type': self.args.output_type
        }

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--log-files', nargs='*', required=True, help='List of log file paths to monitor')
        parser.add_argument('--log-format', choices=['text', 'json'], default='text')
        parser.add_argument('--output-dir', help='Directory to save collected logs')
        parser.add_argument('--filter', help='Regex to filter log entries')
        parser.add_argument('--output-type', choices=['json', 'csv', 'sqlite'], default='json')
        return parser.parse_args()
    
    def start(self):
        for log_file_path in self.run_config['log_files']:
            if not os.path.exists(log_file_path):
                logger.warning(f"Log file not found: {log_file_path}")
                continue
            handler = LogFileHandler(
                log_file_path,
                log_format=self.run_config['log_format'],
                output_dir=self.run_config['output_dir'],
                filter_regex=self.run_config['filter'],
                output_type=self.run_config['output_type']
            )
            self.handlers.append(handler)
            observer = Observer()
            observer.schedule(handler, os.path.dirname(log_file_path), recursive=False)
            observer.start()
            self.observers.append(observer)
            logger.info(f"Started monitoring: {log_file_path}")

    def stop(self):
        for observer in self.observers:
            observer.stop()
        for observer in self.observers:
            observer.join()
        logger.info("Stopped all log monitoring")

    def run(self):
        threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000)).start()
        try:
            self.start()
            logger.info("Log collector running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping log collector...")
        finally:
            self.stop()

@app.route("/metrics")
def metrics():
    return jsonify(collector_stats)

if __name__ == "__main__":
    collector = LogCollectorService()
    collector.run()
