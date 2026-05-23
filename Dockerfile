# =============================================================================
# GridTokenX Trading Service - Alpine Linux Production Image
# =============================================================================
FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    clang \
    git \
    curl \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the whole project to maintain structure for sqlx migrations
COPY gridtokenx-trading-service/ gridtokenx-trading-service/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/

WORKDIR /app/gridtokenx-trading-service

# Build in release mode
RUN cargo build --release --bin trading-service

# Strip binary to reduce size
RUN strip target/release/trading-service

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Minimal Debian)
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/sh appuser

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/gridtokenx-trading-service/target/release/trading-service /app/trading-service
COPY --from=builder /app/gridtokenx-trading-service/migrations /app/migrations

# Ensure appuser owns the directory
RUN chown -R appuser:appgroup /app

# Use non-root user
USER appuser

# Expose gRPC port (5020) and HTTP metrics port (4020)
EXPOSE 5020 4020

# Run the binary
ENTRYPOINT ["/app/trading-service"]
