FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the log collector script
COPY log_storage.py .

# Create logs directory
RUN mkdir -p /data/storage

# Set default command
#ENTRYPOINT ["python", "log_storage.py"]
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers=4", "log_storage:app"]