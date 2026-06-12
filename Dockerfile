# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Cache module downloads first
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /aegis-exec ./cmd/server

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM alpine:3.19

# TLS root certs (needed for HTTPS calls to Capital.com) + timezone data
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /aegis-exec ./aegis-exec

# Fly machines expose 8080 by default
EXPOSE 8080

CMD ["./aegis-exec"]
