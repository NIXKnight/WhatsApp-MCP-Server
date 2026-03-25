# ---------------------------------------------------------------------------
# Build stage: compile the Go binary with CGO enabled (required by go-sqlite3)
# ---------------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /build

# Cache module downloads in a separate layer
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=1 go build -o whatsapp-bridge .

# ---------------------------------------------------------------------------
# Runtime stage: minimal Alpine image with only the compiled binary
# ---------------------------------------------------------------------------
FROM alpine:3.21

RUN apk add --no-cache ca-certificates sqlite-libs

# Non-root user for security
RUN addgroup -S appuser && adduser -S -G appuser appuser

# Data directory for SQLite database and media storage
RUN mkdir /data && chown appuser:appuser /data

COPY --from=builder /build/whatsapp-bridge /usr/local/bin/whatsapp-bridge

USER appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/whatsapp-bridge"]
