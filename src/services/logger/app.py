from flask import Flask, jsonify
from config import LOG_LEVEL, LOG_FREQUENCY, LOG_FILE
import logging
import time
from config import setup_logger
import threading

web = Flask(__name__)

@web.route("/")
def home():
    return "Logger Service is running. Visit /logs or /config"

@web.route("/logs")
def show_logs():
    try:
        with open(LOG_FILE, "r") as f:
            lines = f.readlines()
        return "<pre>" + "".join(lines[-50:]) + "</pre>"  # last 50 lines
    except Exception as e:
        return f"Error reading logs: {str(e)}", 500

@web.route("/config")
def show_config():
    return jsonify({
        "log_level": LOG_LEVEL,
        "log_frequency": LOG_FREQUENCY,
        "log_file": LOG_FILE
    })

def start_web():
    web.run(host="0.0.0.0", port=8080)


def main():
    setup_logger()
    logging.info("Starting distributed logger service...")

    # Start web server in separate thread
    threading.Thread(target=start_web, daemon=True).start()

    # Main logging loop
    while True:
        logging.info("Logger service is running. This will be part of our distributed system!")
        time.sleep(LOG_FREQUENCY)
        
if __name__ == "__main__":
    main()