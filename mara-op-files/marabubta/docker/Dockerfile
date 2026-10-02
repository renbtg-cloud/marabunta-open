# Marabunta - Licensed under the MIT License.
# Marabunta Compute - Multi-stage Docker Build
# Produces minimal images for coordinator, master, and worker components

# =============================================================================
# Stage 1: Build Stage
# =============================================================================
FROM rust:1.75-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock* ./

# Create dummy source to build dependencies
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/coordinator.rs && \
    echo "fn main() {}" > src/bin/master.rs && \
    echo "fn main() {}" > src/bin/worker.rs && \
    echo "fn main() {}" > src/bin/marabunta.rs && \
    echo "fn main() {}" > src/bin/marabunta-worker-windows.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies only (cached layer)
RUN cargo build --release --bins 2>/dev/null || true

# Copy actual source code
COPY src ./src

# Touch source files to invalidate cache and rebuild with real code
RUN touch src/lib.rs src/bin/*.rs

# Build release binaries
RUN cargo build --release --bin marabunta-coordinator --bin marabunta-master --bin marabunta-worker --bin marabunta

# Strip binaries for smaller size (already done by profile, but ensure it)
RUN strip /build/target/release/marabunta-coordinator \
    /build/target/release/marabunta-master \
    /build/target/release/marabunta-worker \
    /build/target/release/marabunta 2>/dev/null || true

# =============================================================================
# Stage 2: Runtime Base Image
# =============================================================================
FROM debian:bookworm-slim AS runtime-base

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1000 -s /bin/false marabunta

# Create necessary directories
RUN mkdir -p /var/lib/marabunta /var/log/marabunta /etc/marabunta \
    && chown -R marabunta:marabunta /var/lib/marabunta /var/log/marabunta /etc/marabunta

# =============================================================================
# Stage 3a: Coordinator Image
# =============================================================================
FROM runtime-base AS coordinator

COPY --from=builder /build/target/release/marabunta-coordinator /usr/local/bin/

USER marabunta
WORKDIR /var/lib/marabunta

# Coordinator listens on port 8080 by default
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["marabunta-coordinator"]
CMD ["--bind", "0.0.0.0:8080"]

# =============================================================================
# Stage 3b: Master Image
# =============================================================================
FROM runtime-base AS master

COPY --from=builder /build/target/release/marabunta-master /usr/local/bin/

USER marabunta
WORKDIR /var/lib/marabunta

# Master listens on port 9000 by default
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

ENTRYPOINT ["marabunta-master"]
CMD ["--bind", "0.0.0.0:9000"]

# =============================================================================
# Stage 3c: Worker Image
# =============================================================================
FROM runtime-base AS worker

COPY --from=builder /build/target/release/marabunta-worker /usr/local/bin/

USER marabunta
WORKDIR /var/lib/marabunta

# Worker doesn't expose ports by default (connects outbound to master)

# Health check - check process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep marabunta-worker || exit 1

ENTRYPOINT ["marabunta-worker"]

# =============================================================================
# Stage 3d: CLI Image (includes all tools)
# =============================================================================
FROM runtime-base AS cli

COPY --from=builder /build/target/release/marabunta /usr/local/bin/
COPY --from=builder /build/target/release/marabunta-coordinator /usr/local/bin/
COPY --from=builder /build/target/release/marabunta-master /usr/local/bin/
COPY --from=builder /build/target/release/marabunta-worker /usr/local/bin/

USER marabunta
WORKDIR /home/marabunta

ENTRYPOINT ["marabunta"]

# =============================================================================
# Default target: All-in-one image
# =============================================================================
FROM cli AS default
