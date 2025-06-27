import os
import time
import json
import re
import logging
import argparse
import requests
from collections import deque
import threading

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('LogCollector')

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file_path, log_format="text", filter_regex=None, storage_url=None, debug=False, batch_size=20, batch_interval=5):
        self.log_file_path = log_file_path
        self.log_format = log_format
        self.filter_regex = re.compile(filter_regex) if filter_regex else None
        self.last_position = 0
        self.storage_url = storage_url
        self.debug = debug
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.initialize_position()
        self.buffer = deque()
        self.lock = threading.Lock()
        self.start_batch_sender()

    def initialize_position(self):
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'r') as file:
                file.seek(0, os.SEEK_END)
                self.last_position = file.tell()
                logger.info(f"Tracking started at position {self.last_position}")

    def start_batch_sender(self):
        def loop():
            while True:
                time.sleep(self.batch_interval)
                self.flush_batch()
        threading.Thread(target=loop, daemon=True).start()

    def flush_batch(self):
        with self.lock:
            if not self.buffer:
                return
            batch = list(self.buffer)
            self.buffer.clear()
            for _ in range(3):  # retry 3 times
                try:
                    requests.post(self.storage_url, json=batch, timeout=10)
                    break
                except Exception as e:
                    logger.error(f"Retrying batch POST due to: {e}")
                    time.sleep(1)

    def parse_line(self, line):
        try:
            if self.log_format == "json":
                return json.loads(line)
            else:
                return {
                    "timestamp": "",
                    "level": "INFO",
                    "service": "default",
                    "user_id": "",
                    "request_id": "",
                    "duration": 0,
                    "message": line.strip()
                }
        except Exception:
            return None
    # "SERVICES": ["user-service", "payment-service", "inventory-service", "notification-service"],
    def tag_entry(self, entry):
        msg = entry.get("service", "")
        if "user-service" in msg:
            tag = "user"
        elif "payment-service" in msg:
            tag = "payment"
        elif "inventory-service" in msg:
            tag = "inventory"
        elif "notification-service" in msg:
            tag = "notification"
        elif "auth-service" in msg:
            tag = "auth"
        else:
            tag = "general"
        entry["tag"] = tag
        return entry, tag

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
                        tagged, tag = self.tag_entry(parsed)
                        if self.debug:
                            print(f"COLLECTED: {tag}")
                        with self.lock:
                            self.buffer.append(tagged)
                        if len(self.buffer) >= self.batch_size:
                            self.flush_batch()
                self.last_position = file.tell()
        except Exception as e:
            logger.error(f"Error reading log file: {e}")

class LogCollectorService:
    def __init__(self):
        self.args = self.parse_args()
        self.observers = []

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--log-files', nargs='*', required=True, help='Log file paths to monitor')
        parser.add_argument('--log-format', choices=['text', 'json'], default='text')
        parser.add_argument('--filter', help='Regex filter for log messages')
        parser.add_argument('--storage-url', required=True, help='Log storage endpoint')
        parser.add_argument('--debug', action='store_true', help='Enable debug output (prints collected tags)')
        parser.add_argument('--batch-size', type=int, default=20, help='Batch size for sending logs')
        parser.add_argument('--batch-interval', type=int, default=5, help='Batch send interval in seconds')
        return parser.parse_args()

    def run(self):
        for path in self.args.log_files:
            if not os.path.exists(path):
                logger.warning(f"Log file not found: {path}")
                continue
            handler = LogFileHandler(
                log_file_path=path,
                log_format=self.args.log_format,
                filter_regex=self.args.filter,
                storage_url=self.args.storage_url,
                debug=self.args.debug,
                batch_size=self.args.batch_size,
                batch_interval=self.args.batch_interval
            )
            observer = Observer()
            observer.schedule(handler, os.path.dirname(path), recursive=False)
            observer.start()
            self.observers.append(observer)
            logger.info(f"Watching: {path}")

        try:
            logger.info("Log collector running. Ctrl+C to stop.")
            while True:
                time.sleep(2)
        except KeyboardInterrupt:
            logger.info("Stopping...")
            for o in self.observers:
                o.stop()
            for o in self.observers:
                o.join()

if __name__ == "__main__":
    LogCollectorService().run()
