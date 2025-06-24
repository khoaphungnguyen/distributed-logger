import os
import time
import yaml
import logging
import argparse
import re
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging for our collector service
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LogCollector')

class LogFileHandler(FileSystemEventHandler):
    """Handles events for log files we're monitoring."""

    def __init__(self, log_file_path, log_format="text", output_dir=None, filter_regex=None):
        self.log_file_path = log_file_path
        self.log_format = log_format
        self.output_dir = output_dir
        self.filter_regex = re.compile(filter_regex) if filter_regex else None
        self.last_position = 0
        self.parsing_errors = 0
        self.initialize_position()

    def initialize_position(self):
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'r') as file:
                file.seek(0, os.SEEK_END)
                self.last_position = file.tell()
                logger.info(f"Initialized tracking for {self.log_file_path} at position {self.last_position}")
                
    def parse_line(self, line):
        try:
            if self.log_format == "json":
                return json.loads(line)
            else:
                return {"raw": line, "message": line, "level": "INFO"}  # Simplified for plain text
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

    def on_modified(self, event):
        """Called when a file is modified."""
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
                        print(f"COLLECTED: {tagged}")

                        if self.output_dir:
                            os.makedirs(self.output_dir, exist_ok=True)
                            out_path = os.path.join(self.output_dir, "collected.jsonl")
                            with open(out_path, "a") as out:
                                out.write(json.dumps(tagged) + "\n")

                self.last_position = file.tell()
        except Exception as e:
            logger.error(f"Error reading log file: {e}")

class LogCollectorService:
    """Main service class for the log collector."""
    def __init__(self):
        self.args = self.parse_args()
        self.observers = []
        self.handlers = []
        self.run_config = {
            'log_files': self.args.log_files or [],
            'log_format': self.args.log_format,
            'output_dir': self.args.output_dir,
            'filter': self.args.filter
        }

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--log-files', nargs='*', required=True, help='List of log file paths to monitor')
        parser.add_argument('--log-format', choices=['text', 'json'], default='text')
        parser.add_argument('--output-dir', help='Directory to save collected logs')
        parser.add_argument('--filter', help='Regex to filter log entries')
        return parser.parse_args()

    def load_config(self, config_path):
        self.config = {
            'log_files': ['./sample_logs/app.log'],
            'check_interval': 0.5
        }

        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as file:
                    yaml_config = yaml.safe_load(file)
                    if yaml_config:
                        self.config.update(yaml_config)
                        logger.info("Loaded configuration from file")
        except Exception as e:
            logger.error(f"Error loading config: {e}")

        # Apply CLI override
        if self.args.log_files:
            self.config['log_files'] = self.args.log_files

        logger.info(f"Monitoring log files: {self.config['log_files']}")
    
    def start(self):
        """Start monitoring log files."""
        for log_file_path in self.run_config['log_files']:
            if not os.path.exists(log_file_path):
                logger.warning(f"Log file not found: {log_file_path}")
                continue

            handler = LogFileHandler(
                log_file_path,
                log_format=self.run_config['log_format'],
                output_dir=self.run_config['output_dir'],
                filter_regex=self.run_config['filter']
            )
            self.handlers.append(handler)

            observer = Observer()
            observer.schedule(handler, os.path.dirname(log_file_path), recursive=False)
            observer.start()
            self.observers.append(observer)

            logger.info(f"Started monitoring: {log_file_path}")
    
    def stop(self):
        """Stop monitoring log files."""
        for observer in self.observers:
            observer.stop()
        for observer in self.observers:
            observer.join()
        logger.info("Stopped all log monitoring")
        
        
    def run(self):
        """Run the collector service until interrupted."""
        try:
            self.start()
            logger.info("Log collector running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping log collector...")
        finally:
            self.stop()

if __name__ == "__main__":
    collector = LogCollectorService()
    collector.run()