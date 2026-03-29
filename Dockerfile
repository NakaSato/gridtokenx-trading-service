# =============================================================================
# GridTokenX Trading Service - Alpine Linux Production Image
# =============================================================================
FROM rust:1.88-bookworm AS builder

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

# Copy standalone workspace file for Docker build
COPY Cargo.docker.toml Cargo.toml
COPY Cargo.lock ./

# Copy all workspace members
COPY gridtokenx-api gridtokenx-api
COPY gridtokenx-iam-service gridtokenx-iam-service
COPY gridtokenx-trading-service gridtokenx-trading-service
COPY gridtokenx-oracle-bridge gridtokenx-oracle-bridge

# Build in release mode
RUN cargo build --release --bin gridtokenx-trading-service

# Strip binary to reduce size
RUN strip /app/target/release/gridtokenx-trading-service

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
COPY --from=builder /app/target/release/gridtokenx-trading-service /app/trading-service

# Expose gRPC port
EXPOSE 50052

# Run the binary
ENTRYPOINT ["/app/trading-service"]
