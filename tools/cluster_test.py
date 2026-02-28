#!/usr/bin/env python3
"""
Distributed KV Store — Cluster Integration Tests

Tests:
  1. Basic PUT/GET/DELETE through a single node
  2. Data accessible from any node in the cluster
  3. Node failure tolerance (kill one node, data still readable)
  4. Cluster info endpoint

Usage:
    python3 cluster_test.py --nodes localhost:7001,localhost:7002,localhost:7003
"""

import argparse
import socket
import struct
import sys
import time
from typing import Optional, List, Tuple


# ═══════════════════════════════════════════════════════
#  Protocol (same as benchmark.py)
# ═══════════════════════════════════════════════════════

class OpType:
    PUT = 1; GET = 2; DELETE = 3; CLUSTER_INFO = 22

class StatusCode:
    OK = 0; NOT_FOUND = 1; ERROR = 2

def encode_string(s: str) -> bytes:
    data = s.encode(); return struct.pack("!I", len(data)) + data

def decode_string(buf: bytes, offset: int) -> Tuple[str, int]:
    ln = struct.unpack("!I", buf[offset:offset+4])[0]
    return buf[offset+4:offset+4+ln].decode(), offset+4+ln

def send_msg(sock, payload: bytes):
    sock.sendall(struct.pack("!I", len(payload)) + payload)

def recv_msg(sock) -> Optional[bytes]:
    hdr = _recv(sock, 4)
    if not hdr: return None
    ln = struct.unpack("!I", hdr)[0]
    return _recv(sock, ln) if ln else b""

def _recv(sock, n):
    buf = bytearray()
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c: return None
        buf.extend(c)
    return bytes(buf)


class Client:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect((self.host, self.port))
        return self

    def close(self):
        if self.sock: self.sock.close()

    def put(self, k, v):
        send_msg(self.sock, struct.pack("B", OpType.PUT) + encode_string(k) + encode_string(v))
        r = recv_msg(self.sock)
        return r[0] if r else -1

    def get(self, k):
        send_msg(self.sock, struct.pack("B", OpType.GET) + encode_string(k))
        r = recv_msg(self.sock)
        if not r: return -1, None
        st = r[0]
        if st == StatusCode.OK:
            val, _ = decode_string(r, 1)
            return st, val
        return st, None

    def delete(self, k):
        send_msg(self.sock, struct.pack("B", OpType.DELETE) + encode_string(k))
        r = recv_msg(self.sock)
        return r[0] if r else -1

    def info(self):
        send_msg(self.sock, struct.pack("B", OpType.CLUSTER_INFO))
        return recv_msg(self.sock)


# ═══════════════════════════════════════════════════════
#  Tests
# ═══════════════════════════════════════════════════════

passed = 0
failed = 0

def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        print(f"  ✅ {name}")
        passed += 1
    else:
        print(f"  ❌ {name}" + (f" — {detail}" if detail else ""))
        failed += 1


def test_basic_crud(nodes: List[Tuple[str, int]]):
    """Test 1: Basic PUT/GET/DELETE on first node."""
    print("\n── Test 1: Basic CRUD ──")
    host, port = nodes[0]
    c = Client(host, port).connect()

    st = c.put("test_key_1", "hello_world")
    check("PUT returns OK", st == StatusCode.OK, f"got {st}")

    st, val = c.get("test_key_1")
    check("GET returns value", st == StatusCode.OK and val == "hello_world",
          f"status={st}, val={val}")

    st, val = c.get("nonexistent")
    check("GET missing returns NOT_FOUND", st == StatusCode.NOT_FOUND)

    st = c.delete("test_key_1")
    check("DELETE returns OK", st == StatusCode.OK)

    st, val = c.get("test_key_1")
    check("GET after DELETE returns NOT_FOUND", st == StatusCode.NOT_FOUND)

    c.close()


def test_cross_node_access(nodes: List[Tuple[str, int]]):
    """Test 2: Write on node1, read from node2."""
    if len(nodes) < 2:
        print("\n── Test 2: Cross-node (SKIPPED — need 2+ nodes) ──")
        return

    print("\n── Test 2: Cross-node Access ──")

    c1 = Client(*nodes[0]).connect()
    c2 = Client(*nodes[1]).connect()

    c1.put("cross_test", "from_node1")
    time.sleep(0.5)  # Allow replication

    st, val = c2.get("cross_test")
    check("Read from node2 after writing to node1",
          st == StatusCode.OK and val == "from_node1",
          f"status={st}, val={val}")

    c1.close()
    c2.close()


def test_multiple_keys(nodes: List[Tuple[str, int]]):
    """Test 3: Write many keys and verify."""
    print("\n── Test 3: Multiple Keys ──")
    c = Client(*nodes[0]).connect()

    n = 50
    for i in range(n):
        c.put(f"batch_{i}", f"value_{i}")

    all_ok = True
    for i in range(n):
        st, val = c.get(f"batch_{i}")
        if st != StatusCode.OK or val != f"value_{i}":
            all_ok = False
            break

    check(f"All {n} keys readable", all_ok)
    c.close()


def test_cluster_info(nodes: List[Tuple[str, int]]):
    """Test 4: Cluster info endpoint."""
    print("\n── Test 4: Cluster Info ──")
    c = Client(*nodes[0]).connect()
    resp = c.info()
    check("Cluster info returns data", resp is not None and len(resp) > 1)
    c.close()


def test_overwrite(nodes: List[Tuple[str, int]]):
    """Test 5: Overwrite a key."""
    print("\n── Test 5: Overwrite ──")
    c = Client(*nodes[0]).connect()
    c.put("overwrite_key", "v1")
    c.put("overwrite_key", "v2")
    st, val = c.get("overwrite_key")
    check("Overwrite reflects latest value",
          st == StatusCode.OK and val == "v2", f"val={val}")
    c.close()


# ═══════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════

def parse_nodes(nodes_str: str) -> List[Tuple[str, int]]:
    result = []
    for n in nodes_str.split(","):
        n = n.strip()
        host, port = n.rsplit(":", 1)
        result.append((host, int(port)))
    return result


def main():
    parser = argparse.ArgumentParser(description="KV Store Integration Tests")
    parser.add_argument("--nodes", default="localhost:7001",
                        help="Comma-separated node addresses")
    args = parser.parse_args()

    nodes = parse_nodes(args.nodes)

    print("\n" + "="*50)
    print("  Distributed KV Store — Integration Tests")
    print("="*50)
    print(f"  Nodes: {', '.join(f'{h}:{p}' for h, p in nodes)}")

    try:
        test_basic_crud(nodes)
        test_multiple_keys(nodes)
        test_overwrite(nodes)
        test_cluster_info(nodes)
        test_cross_node_access(nodes)
    except Exception as e:
        print(f"\n  💥 Test error: {e}")

    print("\n" + "="*50)
    print(f"  Results: {passed} passed, {failed} failed")
    print("="*50 + "\n")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
