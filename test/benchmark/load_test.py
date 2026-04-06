#!/usr/bin/env python3
"""
SAGE Load Benchmark — Python implementation with proper Ed25519 auth.

Tests:
  1. Memory submission throughput (submissions/sec)
  2. Memory query latency (p50/p95/p99)
  3. Error rate

Auth scheme: SHA-256(json_body_bytes) + struct.pack(">q", unix_timestamp) -> Ed25519 sign -> hex
Headers: X-Agent-ID (hex pubkey), X-Signature (hex sig), X-Timestamp (unix seconds string)
"""

import hashlib
import json
import os
import random
import statistics
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from nacl.signing import SigningKey

BASE_URL = os.environ.get("SAGE_API_URL", "http://localhost:8080")

DOMAINS = ["crypto", "vuln_intel", "challenge_generation", "solver_feedback", "calibration"]
MEM_TYPES = ["fact", "observation", "inference"]


class SageAgent:
    """An agent with Ed25519 credentials for authenticated API requests."""

    def __init__(self, name: str):
        self.name = name
        self.signing_key = SigningKey.generate()
        self.verify_key = self.signing_key.verify_key
        self.agent_id = self.verify_key.encode().hex()

    def sign_request(self, method: str, path: str, body_bytes: bytes, timestamp: int) -> str:
        # Must match auth middleware: SHA-256(method + " " + path + "\n" + body) + BigEndian(ts)
        canonical = f"{method} {path}\n".encode() + body_bytes
        body_hash = hashlib.sha256(canonical).digest()
        ts_bytes = struct.pack(">q", timestamp)
        message = body_hash + ts_bytes
        signed = self.signing_key.sign(message)
        return signed.signature.hex()

    def make_headers(self, method: str, path: str, body_bytes: bytes) -> dict:
        ts = int(time.time())
        sig = self.sign_request(method, path, body_bytes, ts)
        return {
            "Content-Type": "application/json",
            "X-Agent-ID": self.agent_id,
            "X-Signature": sig,
            "X-Timestamp": str(ts),
        }


def submit_memory(client: httpx.Client, agent: SageAgent) -> dict:
    """Submit a single memory and return timing + status info."""
    domain = random.choice(DOMAINS)
    mem_type = random.choice(MEM_TYPES)
    content = f"Benchmark memory {time.time_ns()} - {os.urandom(16).hex()}"

    payload = {
        "content": content,
        "memory_type": mem_type,
        "domain_tag": domain,
        "confidence_score": round(random.uniform(0.5, 1.0), 4),
    }
    body_bytes = json.dumps(payload).encode()
    headers = agent.make_headers("POST", "/v1/memory/submit", body_bytes)

    start = time.monotonic()
    try:
        resp = client.post(f"{BASE_URL}/v1/memory/submit", content=body_bytes, headers=headers)
        elapsed_ms = (time.monotonic() - start) * 1000
        return {
            "type": "submit",
            "status": resp.status_code,
            "elapsed_ms": elapsed_ms,
            "success": resp.status_code == 201,
            "error": None if resp.status_code == 201 else resp.text[:200],
        }
    except Exception as e:
        elapsed_ms = (time.monotonic() - start) * 1000
        return {
            "type": "submit",
            "status": 0,
            "elapsed_ms": elapsed_ms,
            "success": False,
            "error": str(e)[:200],
        }


def query_memory(client: httpx.Client, agent: SageAgent) -> dict:
    """Execute a similarity query and return timing + status info."""
    embedding = [random.uniform(-1, 1) for _ in range(1536)]
    payload = {
        "embedding": embedding,
        "domain_tag": random.choice(DOMAINS),
        "top_k": 10,
    }
    body_bytes = json.dumps(payload).encode()
    headers = agent.make_headers("POST", "/v1/memory/query", body_bytes)

    start = time.monotonic()
    try:
        resp = client.post(f"{BASE_URL}/v1/memory/query", content=body_bytes, headers=headers)
        elapsed_ms = (time.monotonic() - start) * 1000
        return {
            "type": "query",
            "status": resp.status_code,
            "elapsed_ms": elapsed_ms,
            "success": resp.status_code == 200,
            "error": None if resp.status_code == 200 else resp.text[:200],
        }
    except Exception as e:
        elapsed_ms = (time.monotonic() - start) * 1000
        return {
            "type": "query",
            "status": 0,
            "elapsed_ms": elapsed_ms,
            "success": False,
            "error": str(e)[:200],
        }


def percentile(data: list, p: float) -> float:
    """Compute the p-th percentile of a sorted list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def print_stats(label: str, results: list):
    """Print latency percentiles, throughput, and error rate."""
    if not results:
        print(f"\n  [{label}] No results collected.\n")
        return

    total = len(results)
    successes = sum(1 for r in results if r["success"])
    failures = total - successes
    error_rate = (failures / total) * 100 if total > 0 else 0

    latencies = sorted(r["elapsed_ms"] for r in results)
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    avg = statistics.mean(latencies)
    min_l = latencies[0]
    max_l = latencies[-1]

    # Compute throughput from wall-clock time range
    if total > 1:
        # We don't have timestamps per result, so approximate from total time
        total_time_s = sum(r["elapsed_ms"] for r in results) / 1000.0
        # With concurrency, effective throughput is higher
        # We'll compute it from the test harness instead
        rps = "see below"
    else:
        rps = "N/A"

    print(f"\n  [{label}] Results:")
    print(f"    Total requests:  {total}")
    print(f"    Successes:       {successes}")
    print(f"    Failures:        {failures}")
    print(f"    Error rate:      {error_rate:.2f}%")
    print(f"    Latency (ms):")
    print(f"      min:           {min_l:.1f}")
    print(f"      avg:           {avg:.1f}")
    print(f"      p50:           {p50:.1f}")
    print(f"      p95:           {p95:.1f}")
    print(f"      p99:           {p99:.1f}")
    print(f"      max:           {max_l:.1f}")

    # Print first few unique errors if any
    errors = set()
    for r in results:
        if r["error"]:
            errors.add(r["error"])
    if errors:
        print(f"    Sample errors ({len(errors)} unique):")
        for e in list(errors)[:5]:
            print(f"      - {e}")


def run_submit_benchmark(num_agents: int = 5, total_requests: int = 200, concurrency: int = 20):
    """Run the memory submission load test."""
    print(f"\n{'='*70}")
    print(f"  MEMORY SUBMISSION BENCHMARK")
    print(f"  Agents: {num_agents} | Requests: {total_requests} | Concurrency: {concurrency}")
    print(f"{'='*70}")

    agents = [SageAgent(f"bench-submit-{i}") for i in range(num_agents)]
    results = []

    client = httpx.Client(timeout=30.0)
    wall_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(total_requests):
            agent = agents[i % num_agents]
            futures.append(executor.submit(submit_memory, client, agent))

        for future in as_completed(futures):
            results.append(future.result())

    wall_elapsed = time.monotonic() - wall_start
    client.close()

    rps = total_requests / wall_elapsed if wall_elapsed > 0 else 0

    print_stats("SUBMIT", results)
    print(f"    Wall time:       {wall_elapsed:.2f}s")
    print(f"    Throughput:      {rps:.1f} req/s")

    return results, rps


def run_query_benchmark(num_agents: int = 3, total_requests: int = 100, concurrency: int = 10):
    """Run the memory query load test."""
    print(f"\n{'='*70}")
    print(f"  MEMORY QUERY BENCHMARK")
    print(f"  Agents: {num_agents} | Requests: {total_requests} | Concurrency: {concurrency}")
    print(f"{'='*70}")

    agents = [SageAgent(f"bench-query-{i}") for i in range(num_agents)]
    results = []

    client = httpx.Client(timeout=30.0)
    wall_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(total_requests):
            agent = agents[i % num_agents]
            futures.append(executor.submit(query_memory, client, agent))

        for future in as_completed(futures):
            results.append(future.result())

    wall_elapsed = time.monotonic() - wall_start
    client.close()

    rps = total_requests / wall_elapsed if wall_elapsed > 0 else 0

    print_stats("QUERY", results)
    print(f"    Wall time:       {wall_elapsed:.2f}s")
    print(f"    Throughput:      {rps:.1f} req/s")

    return results, rps


def run_mixed_benchmark(num_agents: int = 5, duration_s: int = 30, concurrency: int = 15):
    """Run a mixed workload (70% submit, 30% query) for a fixed duration."""
    print(f"\n{'='*70}")
    print(f"  MIXED WORKLOAD BENCHMARK")
    print(f"  Agents: {num_agents} | Duration: {duration_s}s | Concurrency: {concurrency}")
    print(f"  Mix: 70% submit / 30% query")
    print(f"{'='*70}")

    agents = [SageAgent(f"bench-mixed-{i}") for i in range(num_agents)]
    submit_results = []
    query_results = []

    client = httpx.Client(timeout=30.0)
    wall_start = time.monotonic()
    deadline = wall_start + duration_s

    def worker():
        local_submit = []
        local_query = []
        while time.monotonic() < deadline:
            agent = random.choice(agents)
            if random.random() < 0.7:
                local_submit.append(submit_memory(client, agent))
            else:
                local_query.append(query_memory(client, agent))
        return local_submit, local_query

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        for future in as_completed(futures):
            s, q = future.result()
            submit_results.extend(s)
            query_results.extend(q)

    wall_elapsed = time.monotonic() - wall_start
    client.close()

    total = len(submit_results) + len(query_results)
    rps = total / wall_elapsed if wall_elapsed > 0 else 0

    print_stats("SUBMIT (mixed)", submit_results)
    print_stats("QUERY (mixed)", query_results)
    print(f"\n    Total requests:  {total}")
    print(f"    Wall time:       {wall_elapsed:.2f}s")
    print(f"    Throughput:      {rps:.1f} req/s (combined)")

    return submit_results, query_results, rps


def check_targets(submit_results, submit_rps, query_results):
    """Check results against Phase 1 performance targets."""
    print(f"\n{'='*70}")
    print(f"  PERFORMANCE TARGET CHECK")
    print(f"{'='*70}")

    checks = []

    # Target: 50-200 submissions/sec
    submit_pass = submit_rps >= 50
    checks.append(("Submit throughput >= 50 req/s", submit_pass, f"{submit_rps:.1f} req/s"))

    # Target: <200ms P95 query latency
    if query_results:
        query_latencies = sorted(r["elapsed_ms"] for r in query_results)
        q_p95 = percentile(query_latencies, 95)
        query_pass = q_p95 < 200
        checks.append(("Query P95 < 200ms", query_pass, f"{q_p95:.1f}ms"))
    else:
        checks.append(("Query P95 < 200ms", False, "No data"))

    # Target: <1% error rate
    all_results = (submit_results or []) + (query_results or [])
    if all_results:
        total = len(all_results)
        failures = sum(1 for r in all_results if not r["success"])
        err_rate = (failures / total) * 100
        err_pass = err_rate < 1.0
        checks.append(("Error rate < 1%", err_pass, f"{err_rate:.2f}%"))
    else:
        checks.append(("Error rate < 1%", False, "No data"))

    all_pass = True
    for label, passed, value in checks:
        status = "PASS" if passed else "FAIL"
        print(f"    [{status}] {label}: {value}")
        if not passed:
            all_pass = False

    print()
    return all_pass


def main():
    # Quick connectivity check
    print(f"  Target: {BASE_URL}")
    try:
        r = httpx.get(f"{BASE_URL}/health", timeout=5.0)
        print(f"  Health check: {r.status_code} - {r.text.strip()}")
    except Exception as e:
        print(f"  Health check FAILED: {e}")
        print("  Is the SAGE network running? (make up)")
        sys.exit(1)

    # Quick auth verification with a single request
    print("\n  Verifying Ed25519 auth...")
    agent = SageAgent("auth-test")
    payload = json.dumps({
        "content": "Auth verification test",
        "memory_type": "fact",
        "domain_tag": "crypto",
        "confidence_score": 0.9,
    }).encode()
    headers = agent.make_headers("POST", "/v1/memory/submit", payload)
    try:
        r = httpx.post(f"{BASE_URL}/v1/memory/submit", content=payload, headers=headers, timeout=10.0)
        if r.status_code == 201:
            print(f"  Auth OK - memory submitted: {r.json().get('memory_id', 'unknown')}")
        else:
            print(f"  Auth test returned {r.status_code}: {r.text[:300]}")
            if r.status_code == 401:
                print("  Ed25519 auth is failing. Check signing scheme.")
                sys.exit(1)
    except Exception as e:
        print(f"  Auth test error: {e}")
        sys.exit(1)

    profile = os.environ.get("BENCH_PROFILE", "smoke")

    if profile == "smoke":
        # Quick smoke test
        submit_res, submit_rps = run_submit_benchmark(
            num_agents=2, total_requests=20, concurrency=5
        )
        query_res, query_rps = run_query_benchmark(
            num_agents=2, total_requests=20, concurrency=5
        )
    elif profile == "load":
        # Normal load test
        submit_res, submit_rps = run_submit_benchmark(
            num_agents=5, total_requests=500, concurrency=20
        )
        query_res, query_rps = run_query_benchmark(
            num_agents=5, total_requests=200, concurrency=10
        )
        run_mixed_benchmark(num_agents=5, duration_s=60, concurrency=15)
    elif profile == "stress":
        # Stress test
        submit_res, submit_rps = run_submit_benchmark(
            num_agents=10, total_requests=2000, concurrency=50
        )
        query_res, query_rps = run_query_benchmark(
            num_agents=10, total_requests=500, concurrency=30
        )
        run_mixed_benchmark(num_agents=10, duration_s=120, concurrency=30)
    else:
        print(f"  Unknown profile: {profile}. Use: smoke, load, stress")
        sys.exit(1)

    all_pass = check_targets(submit_res, submit_rps, query_res)

    print(f"\n{'='*70}")
    if all_pass:
        print("  ALL PERFORMANCE TARGETS MET")
    else:
        print("  SOME TARGETS NOT MET (see above)")
    print(f"{'='*70}\n")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
