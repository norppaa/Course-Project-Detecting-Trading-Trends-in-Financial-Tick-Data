FROM golang:1.25-alpine AS builder
WORKDIR /app

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Build binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/producer ./producer

# Minimal runtime image
FROM alpine:3.22
WORKDIR /app
COPY --from=builder /app/bin/producer .
RUN adduser -D -H producer
USER producer

CMD ["./producer"]
