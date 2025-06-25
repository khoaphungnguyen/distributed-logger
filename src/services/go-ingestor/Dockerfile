# Use minimal Go image
FROM golang:1.24-alpine

# Set working directory 
WORKDIR /app

# Copy source files
COPY . .

# Build the Go App
RUN go build -o tcp-ingestor main.go

# Expose TCP port
EXPOSE 3000

# Run the TCP server
CMD ["./tcp-ingestor"]
