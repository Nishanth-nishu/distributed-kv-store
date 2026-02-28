# Distributed Key-Value Store

> A DynamoDB-inspired, horizontally-scalable, fault-tolerant key-value store built from scratch in **C++17**.

## ✨ Features

| Feature | Description |
|---|---|
| **Consistent Hashing** | Hash ring with 150 virtual nodes per physical node; O(log N) key routing |
| **Quorum Replication** | Configurable N/R/W (default: 3/2/2); R+W > N guarantees strong consistency |
| **Write-Ahead Log** | Crash-safe persistence with CRC32 integrity checks |
| **Gossip Protocol** | Decentralized failure detection and membership management |
| **Thread Pool** | Concurrent request handling with shared-mutex reader-writer locks |
| **Read Repair** | Automatically heals stale replicas during quorum reads |
| **Docker Cluster** | One-command 3-node cluster via `docker-compose` |

---

## 🏗️ Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │  TCP (binary protocol)
                    ┌──────▼──────┐
                    │ Coordinator │  ← Routes & orchestrates quorum
                    ├─────────────┤
                    │  Replication │  ← Parallel async W writes, R reads
                    │   Manager   │
                    ├─────────────┤
              ┌─────┤  Hash Ring  ├─────┐
              │     └─────────────┘     │
        ┌─────▼─────┐            ┌──────▼────┐
        │  Storage   │            │  Storage  │
        │  Engine    │            │  Engine   │
        ├────────────┤            ├───────────┤
        │ HashMap    │            │ HashMap   │
        │ + WAL      │            │ + WAL     │
        └────────────┘            └───────────┘
           Node 1                    Node 2
```

### Consistent Hashing

Each key is mapped to a position on a 2³² hash ring via MurmurHash3. Each physical node occupies 150 virtual node positions for balanced distribution. When a node joins/leaves, only ~1/N of keys need to move.

### Quorum Model

With default parameters N=3, R=2, W=2:
- **Writes**: Sent to 3 replicas in parallel, succeeds when 2 acknowledge
- **Reads**: Query 3 replicas, return latest version from 2 responses
- **R + W > N**: Every read overlaps with at least one write → **strong consistency**

### Failure Handling

- **Node Crash**: Gossip protocol detects via heartbeat timeout (5s). Remaining replicas serve reads.
- **Network Partition**: Quorum prevents stale reads; read repair fixes divergence.
- **Crash Recovery**: WAL replay restores in-memory state on restart.

---

## 🚀 Quick Start

### Build from source

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run a single node

```bash
./kvstore_node --node-id node1 --port 7001 --data-dir /tmp/kvstore
```

### Run a 3-node cluster

```bash
# Terminal 1
./kvstore_node --node-id node1 --port 7001 --data-dir /tmp/kv1

# Terminal 2
./kvstore_node --node-id node2 --port 7002 --data-dir /tmp/kv2 \
    --seed localhost:7001

# Terminal 3
./kvstore_node --node-id node3 --port 7003 --data-dir /tmp/kv3 \
    --seed localhost:7001 --seed localhost:7002
```

### Use the CLI client

```bash
./kv_cli --host localhost --port 7001

kvstore> put user:1001 {"name":"Nishanth","role":"SDE"}
  OK
kvstore> get user:1001
  VALUE: {"name":"Nishanth","role":"SDE"}
kvstore> delete user:1001
  OK (deleted)
kvstore> info
  ── Cluster Members (3) ──
    [ALIVE] node1 (0.0.0.0:7001)
    [ALIVE] node2 (0.0.0.0:7002)
    [ALIVE] node3 (0.0.0.0:7003)
```

### Docker cluster

```bash
docker-compose up --build -d    # Start 3-node cluster
docker-compose logs -f          # Watch logs
docker-compose stop node2       # Simulate node failure
docker-compose down             # Tear down
```

---

## 🧪 Testing

### Unit tests

```bash
cd build && ctest --output-on-failure
```

| Test Suite | What it tests |
|---|---|
| `test_consistent_hash` | Ring operations, key distribution uniformity, consistency on add/remove |
| `test_wal` | Append, replay, CRC corruption detection, truncation |
| `test_storage_engine` | CRUD, stale-write rejection, crash recovery, thread safety |
| `test_quorum` | Quorum parameters, replica selection, failure tolerance |

### Integration tests

```bash
python3 tools/cluster_test.py --nodes localhost:7001,localhost:7002,localhost:7003
```

### Load testing

```bash
python3 tools/benchmark.py --host localhost --port 7001 \
    --threads 8 --ops 10000 --ratio 0.5
```

Reports throughput (ops/sec) and latency percentiles (p50, p95, p99).

---

## 📁 Project Structure

```
├── CMakeLists.txt                  # Build configuration
├── Dockerfile                      # Multi-stage Docker build
├── docker-compose.yml              # 3-node cluster deployment
├── src/
│   ├── main.cpp                    # Node entry point
│   ├── common/
│   │   ├── config.h                # Compile-time constants
│   │   ├── types.h                 # Core data types
│   │   ├── logger.h                # Thread-safe logging
│   │   ├── protocol.h             # Wire protocol & TCP helpers
│   │   ├── thread_pool.h          # Fixed-size thread pool
│   │   ├── murmurhash3.h/.cpp     # Hash function
│   ├── storage/
│   │   ├── wal.h/.cpp              # Write-Ahead Log
│   │   ├── storage_engine.h/.cpp   # In-memory store + WAL
│   ├── cluster/
│   │   ├── consistent_hash.h/.cpp  # Hash ring with virtual nodes
│   │   ├── membership.h/.cpp       # Gossip-based membership
│   │   ├── replication.h/.cpp      # Quorum replication
│   ├── server/
│   │   ├── coordinator.h/.cpp      # Request routing
│   │   ├── tcp_server.h/.cpp       # Multi-threaded TCP server
│   └── client/
│       └── kv_client.h/.cpp        # TCP client
├── tools/
│   ├── cli_client.cpp              # Interactive CLI
│   ├── benchmark.py                # Load testing
│   └── cluster_test.py             # Integration tests
└── tests/                          # Google Test unit tests
```

---

## 🏛️ Design Principles

| SOLID Principle | Application |
|---|---|
| **Single Responsibility** | Each class has one job: WAL persists, HashRing routes, StorageEngine stores |
| **Open/Closed** | Storage backend is replaceable (HashMap → LSM-tree) without changing engine API |
| **Liskov Substitution** | Any node can serve any client request (coordinator pattern) |
| **Interface Segregation** | Separate OpTypes for client, internal, and cluster operations |
| **Dependency Inversion** | Components communicate through stable interfaces, not concrete types |

Additional practices:
- **RAII** for all resource management (files, sockets, threads)
- **const-correctness** throughout
- **Smart pointers** — no raw `new`/`delete`
- **Reader-writer locks** — `shared_mutex` maximizes read parallelism

---

## 📊 Performance Targets

| Metric | Target |
|---|---|
| Read latency (p50) | < 5 ms |
| Read latency (p99) | < 20 ms |
| Write latency (p50) | < 10 ms |
| Throughput | > 10,000 ops/sec (3-node cluster) |

---

## 🔮 V2 Roadmap

- [ ] Vector clocks for conflict resolution
- [ ] Anti-entropy repair with Merkle trees
- [ ] LSM-tree storage engine
- [ ] Background compaction
- [ ] Raft-based leader election
- [ ] Hinted handoff for temporary failures

---

## 📜 License

This project is for educational and portfolio purposes. Free to use and modify.
