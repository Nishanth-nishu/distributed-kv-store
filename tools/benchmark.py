#!/usr/bin/env python3
"""
Distributed KV Store — Load Testing & Benchmarking Tool

Measures throughput (ops/sec) and latency (p50, p95, p99) for
PUT, GET, and DELETE operations under concurrent load.

Usage:
    python3 benchmark.py --host localhost --port 7000 \
        --threads 8 --ops 10000 --ratio 0.5
"""

import argparse
import socket
import struct
import time
import threading
import random
import string
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# ═══════════════════════════════════════════════════════
#  Wire Protocol (matches C++ ByteBuffer/protocol.h)
# ═══════════════════════════════════════════════════════

class OpType:
    PUT        = 1
    GET        = 2
    DELETE     = 3
    CLUSTER_INFO = 22


class StatusCode:
    OK         = 0
    NOT_FOUND  = 1
    ERROR      = 2


def encode_string(s: str) -> bytes:
    data = s.encode("utf-8")
    return struct.pack("!I", len(data)) + data


def decode_string(buf: bytes, offset: int) -> Tuple[str, int]:
    length = struct.unpack("!I", buf[offset:offset+4])[0]
    offset += 4
    s = buf[offset:offset+length].decode("utf-8")
    return s, offset + length


def send_message(sock: socket.socket, payload: bytes):
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_message(sock: socket.socket) -> Optional[bytes]:
    header = _recv_exact(sock, 4)
    if not header:
        return None
    length = struct.unpack("!I", header)[0]
    if length == 0:
        return b""
    return _recv_exact(sock, length)


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ═══════════════════════════════════════════════════════
#  Client
# ═══════════════════════════════════════════════════════

class KVClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(10)
        self.sock.connect((self.host, self.port))

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def put(self, key: str, value: str) -> int:
        payload = struct.pack("B", OpType.PUT) + encode_string(key) + encode_string(value)
        send_message(self.sock, payload)
        resp = recv_message(self.sock)
        if resp is None:
            return -1
        return resp[0]  # StatusCode

    def get(self, key: str) -> Tuple[int, Optional[str]]:
        payload = struct.pack("B", OpType.GET) + encode_string(key)
        send_message(self.sock, payload)
        resp = recv_message(self.sock)
        if resp is None:
            return -1, None
        status = resp[0]
        if status == StatusCode.OK:
            val, _ = decode_string(resp, 1)
            return status, val
        return status, None

    def delete(self, key: str) -> int:
        payload = struct.pack("B", OpType.DELETE) + encode_string(key)
        send_message(self.sock, payload)
        resp = recv_message(self.sock)
        if resp is None:
            return -1
        return resp[0]


# ═══════════════════════════════════════════════════════
#  Benchmark
# ═══════════════════════════════════════════════════════

@dataclass
class LatencyStats:
    operation: str = ""
    latencies: List[float] = field(default_factory=list)
    errors: int = 0

    @property
    def count(self) -> int:
        return len(self.latencies)

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p / 100)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def mean(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0

    def display(self):
        if not self.latencies:
            print(f"  {self.operation}: no data")
            return
        print(f"  {self.operation}:")
        print(f"    Count : {self.count}")
        print(f"    Errors: {self.errors}")
        print(f"    Mean  : {self.mean()*1000:.2f} ms")
        print(f"    p50   : {self.percentile(50)*1000:.2f} ms")
        print(f"    p95   : {self.percentile(95)*1000:.2f} ms")
        print(f"    p99   : {self.percentile(99)*1000:.2f} ms")
        print(f"    Min   : {min(self.latencies)*1000:.2f} ms")
        print(f"    Max   : {max(self.latencies)*1000:.2f} ms")


def random_string(length: int = 16) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def worker(host: str, port: int, ops: int, write_ratio: float,
           keys: List[str]) -> Tuple[LatencyStats, LatencyStats]:
    put_stats = LatencyStats(operation="PUT")
    get_stats = LatencyStats(operation="GET")

    client = KVClient(host, port)
    try:
        client.connect()
    except Exception as e:
        print(f"  Worker connect failed: {e}")
        put_stats.errors = ops
        return put_stats, get_stats

    for _ in range(ops):
        if random.random() < write_ratio:
            # PUT
            key = random_string(16)
            value = random_string(64)
            keys.append(key)
            start = time.monotonic()
            status = client.put(key, value)
            elapsed = time.monotonic() - start
            if status == StatusCode.OK:
                put_stats.latencies.append(elapsed)
            else:
                put_stats.errors += 1
        else:
            # GET
            key = random.choice(keys) if keys else random_string(16)
            start = time.monotonic()
            status, _ = client.get(key)
            elapsed = time.monotonic() - start
            if status in (StatusCode.OK, StatusCode.NOT_FOUND):
                get_stats.latencies.append(elapsed)
            else:
                get_stats.errors += 1

    client.close()
    return put_stats, get_stats


def main():
    parser = argparse.ArgumentParser(description="KV Store Benchmark")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--threads", type=int, default=4,
                        help="Number of concurrent workers")
    parser.add_argument("--ops", type=int, default=5000,
                        help="Operations per thread")
    parser.add_argument("--ratio", type=float, default=0.5,
                        help="Write ratio (0.0=all reads, 1.0=all writes)")
    args = parser.parse_args()

    print("\n" + "="*50)
    print("  Distributed KV Store — Benchmark")
    print("="*50)
    print(f"  Target  : {args.host}:{args.port}")
    print(f"  Threads : {args.threads}")
    print(f"  Ops/thr : {args.ops}")
    print(f"  W/R ratio: {args.ratio:.0%} writes / {1-args.ratio:.0%} reads")
    print("="*50 + "\n")

    # Shared key pool for realistic reads
    keys: List[str] = []
    keys_lock = threading.Lock()

    # Pre-seed some keys
    print("  Pre-seeding 100 keys...")
    client = KVClient(args.host, args.port)
    try:
        client.connect()
        for i in range(100):
            key = f"seed_{i}"
            client.put(key, random_string(64))
            keys.append(key)
        client.close()
    except Exception as e:
        print(f"  Pre-seed failed: {e}")
        return

    # Run benchmark
    print("  Running benchmark...\n")
    all_put = LatencyStats(operation="PUT (aggregate)")
    all_get = LatencyStats(operation="GET (aggregate)")

    start_time = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = []
        for _ in range(args.threads):
            futures.append(
                executor.submit(worker, args.host, args.port,
                                args.ops, args.ratio, keys))

        for fut in as_completed(futures):
            put_s, get_s = fut.result()
            all_put.latencies.extend(put_s.latencies)
            all_put.errors += put_s.errors
            all_get.latencies.extend(get_s.latencies)
            all_get.errors += get_s.errors

    elapsed = time.monotonic() - start_time
    total_ops = all_put.count + all_get.count + all_put.errors + all_get.errors

    # Results
    print("="*50)
    print("  RESULTS")
    print("="*50)
    print(f"  Total time    : {elapsed:.2f} s")
    print(f"  Total ops     : {total_ops}")
    print(f"  Throughput    : {total_ops/elapsed:.0f} ops/sec")
    print()
    all_put.display()
    print()
    all_get.display()
    print("="*50 + "\n")


if __name__ == "__main__":
    main()
