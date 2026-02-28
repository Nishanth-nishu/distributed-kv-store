# ── Build stage ────────────────────────────────────────
FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN mkdir -p build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF \
    && make -j$(nproc)

# ── Runtime stage ──────────────────────────────────────
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/kvstore_node /usr/local/bin/kvstore_node
COPY --from=builder /app/build/kv_cli       /usr/local/bin/kv_cli

# Data directory
RUN mkdir -p /data

EXPOSE 7000

ENTRYPOINT ["kvstore_node"]
CMD ["--node-id", "node1", "--port", "7000", "--data-dir", "/data"]
