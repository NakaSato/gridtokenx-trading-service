# syntax=docker/dockerfile:1
# =============================================================================
# GridTokenX Trading Service - Debian Bookworm Production Image
# =============================================================================
FROM rust:1.89-bookworm AS builder

# Install build dependencies with cache mount
RUN --mount=type=cache,target=/var/lib/apt/lists <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        git \
        curl \
        protobuf-compiler
EOT

# Set working directory
WORKDIR /app

# Copy dependency manifests and project structure
COPY gridtokenx-trading-service/ gridtokenx-trading-service/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/

WORKDIR /app/gridtokenx-trading-service

# Build in release mode with cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/gridtokenx-trading-service/target \
    cargo build --release --bin trading-service && \
    strip target/release/trading-service && \
    cp target/release/trading-service /app/trading-service-bin

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Minimal Debian)
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN --mount=type=cache,target=/var/lib/apt/lists <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tzdata
EOT

# Create non-root user
RUN <<EOT
    groupadd -g 1000 appgroup
    useradd -u 1000 -g appgroup -s /bin/sh appuser
EOT

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/trading-service-bin /app/trading-service
COPY --from=builder /app/gridtokenx-trading-service/migrations /app/migrations

# Ensure appuser owns the directory
RUN chown -R appuser:appgroup /app

# Use non-root user
USER appuser

# Expose gRPC port (5020) and HTTP metrics port (4020)
EXPOSE 5020 4020

# Run the binary
ENTRYPOINT ["/app/trading-service"]
