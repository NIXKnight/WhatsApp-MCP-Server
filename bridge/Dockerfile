# ---------------------------------------------------------------------------
# Build stage: compile the Go binary (pure Go, no CGO dependencies)
# ---------------------------------------------------------------------------
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Cache module downloads in a separate layer
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 go build -o whatsapp-bridge .

# ---------------------------------------------------------------------------
# Runtime stage: minimal Alpine image with only the compiled binary
# ---------------------------------------------------------------------------
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

# Data directory for media storage (writable by any UID)
RUN mkdir /data && chmod 777 /data

COPY --from=builder /build/whatsapp-bridge /usr/local/bin/whatsapp-bridge
COPY --chmod=0555 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/local/bin/whatsapp-bridge"]
