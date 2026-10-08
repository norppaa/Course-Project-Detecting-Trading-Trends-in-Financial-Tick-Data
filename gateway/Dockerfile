FROM golang:1.25-alpine AS builder
WORKDIR /app

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Build binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/gateway ./gateway

# Minimal runtime image
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bin/gateway .

EXPOSE 8081
CMD ["./gateway"]
