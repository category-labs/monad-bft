# syntax=docker/dockerfile:1.4

FROM golang:1.18-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /go/bin/gossip-testground ./cmd/gossip-testground


FROM alpine:latest

# Create a non-root user and group for security purposes
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy the binary from the builder stage and set the correct ownership
COPY --from=builder --chown=appuser:appgroup /go/bin/gossip-testground /usr/local/bin/gossip-testground

# Switch to the non-root user
USER appuser


# Create a non-root user to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup && \
    chown appuser:appgroup /usr/bin/gossip-testground /usr/bin/entrypoint.sh
USER appuser
ENTRYPOINT ["/usr/local/bin/gossip-testground"]
