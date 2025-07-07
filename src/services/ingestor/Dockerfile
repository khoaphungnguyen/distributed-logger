# Use minimal Go image
FROM golang:1.24-alpine

# Set working directory 
WORKDIR /app

# Copy source files
COPY . .

# Build the Go App
RUN go build -o go-ingestor main.go

# Run the TCP server
CMD ["./go-ingestor"]
