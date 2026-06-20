# syntax=docker/dockerfile:1.7
# =============================================================================
# GridTokenX Trading Service — distroless image: binary + its shared libs only.
# No Rust toolchain, no target/ in the image (target lives in a BuildKit cache).
# =============================================================================
FROM rust:1.89-bookworm AS builder

# Install build dependencies with cache mount
RUN <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        git \
        curl \
        libprotobuf-dev \
        protobuf-compiler
EOT

# Set working directory
WORKDIR /app

# Copy dependency manifests and project structure
COPY gridtokenx-trading-service/ gridtokenx-trading-service/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/
COPY gridtokenx-iam-service/ gridtokenx-iam-service/
COPY gridtokenx-telemetry/ gridtokenx-telemetry/

WORKDIR /app/gridtokenx-trading-service

# Build in release mode with cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/gridtokenx-trading-service/target \
    cargo build --release --bin trading-service && \
    strip target/release/trading-service && \
    cp target/release/trading-service /app/trading-service-bin

# Collect the binary + its non-glibc shared libs into a flat lib/ folder.
# glibc core + the dynamic loader come from the distroless/cc base — skip them.
RUN set -eux; \
    BIN=/app/trading-service-bin; \
    mkdir -p /out/lib; \
    cp "$BIN" /out/trading-service; \
    ldd "$BIN" | awk '/=>/{print $3} !/=>/{print $1}' | grep -E '^/' | sort -u | while read -r lib; do \
        case "$lib" in \
            */ld-linux*|*/libc.so*|*/libm.so*|*/libpthread*|*/libdl.so*|*/librt.so*) continue;; \
        esac; \
        cp -Lv "$lib" /out/lib/; \
    done

# -----------------------------------------------------------------------------
# Stage 2: Runtime (distroless, non-root uid 65532)
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

# Copy binary, its lib folder, and migrations from the builder stage
COPY --from=builder /out/trading-service /app/trading-service
COPY --from=builder /out/lib/ /app/lib/
COPY --from=builder /app/gridtokenx-trading-service/migrations /app/migrations

ENV LD_LIBRARY_PATH=/app/lib

# Expose gRPC port (5020) and HTTP metrics port (4020)
EXPOSE 5020 4020

# Run the binary
ENTRYPOINT ["/app/trading-service"]
