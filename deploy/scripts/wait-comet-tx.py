#!/usr/bin/env python3
"""Read-only, bounded wait for an exact committed transaction's index entry."""
import json
import re
import sys
import time
from urllib.error import HTTPError
from urllib.request import urlopen


def wait_tx(port, tx_hash, timeout=30):
    if not re.fullmatch(r"[0-9A-Fa-f]{64}", tx_hash):
        raise ValueError("expected an exact 32-byte transaction hash")
    if not 1 <= int(port) <= 65535:
        raise ValueError("invalid loopback RPC port")
    tx_hash = tx_hash.upper()
    url = f"http://127.0.0.1:{int(port)}/tx?hash=0x{tx_hash}&prove=false"
    deadline = time.monotonic() + timeout
    last = "no response"
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=min(3, deadline - time.monotonic())) as response:
                status, raw = response.status, response.read(65537)
        except HTTPError as error:
            with error:
                status, raw = error.code, error.read(65537)
        # Transport failures deliberately propagate; only exact index absence retries.
        if len(raw) > 65536:
            raise ValueError("oversized transaction RPC response")
        last = f"HTTP {status}: {raw.decode('utf-8', errors='replace')}"
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeError) as error:
            raise ValueError(f"invalid transaction RPC JSON: {last}") from error
        if not isinstance(data, dict):
            raise ValueError(f"invalid transaction RPC envelope: {last}")
        error = data.get("error")
        if (status in (200, 500) and isinstance(error, dict)
                and error.get("code") == -32603
                and error.get("message") == "Internal error"
                and error.get("data") == f"tx ({tx_hash}) not found"
                and "result" not in data):
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
            continue
        if status != 200 or error is not None:
            raise ValueError(f"transaction RPC failed: {last}")
        result = data.get("result")
        if not isinstance(result, dict) or result.get("hash", "").upper() != tx_hash:
            raise ValueError(f"transaction RPC hash mismatch: {last}")
        height = result.get("height")
        if not isinstance(height, str) or not re.fullmatch(r"[1-9][0-9]*", height):
            raise ValueError(f"invalid committed transaction height: {last}")
        tx_result = result.get("tx_result")
        if not isinstance(tx_result, dict) or type(tx_result.get("code")) is not int or tx_result["code"] != 0:
            raise ValueError(f"transaction execution failed: {last}")
        return int(height)
    raise TimeoutError(f"transaction index not ready after {timeout}s for {tx_hash}; {last}")


if __name__ == "__main__":
    try:
        print(wait_tx(sys.argv[1], sys.argv[2]))
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
