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

# Data directory for SQLite database and media storage (writable by any UID)
RUN mkdir /data && chmod 777 /data

COPY --from=builder /build/whatsapp-bridge /usr/local/bin/whatsapp-bridge

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/whatsapp-bridge"]
