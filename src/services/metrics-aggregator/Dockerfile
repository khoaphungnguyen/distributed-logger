# Use minimal Go image
FROM golang:1.24-alpine

# Set working directory 
WORKDIR /app

# Copy source files
COPY . .

# Build the Go App
RUN go build -o metrics_aggregator main.go

# Run the TCP server
CMD ["./metrics_aggregator"]
