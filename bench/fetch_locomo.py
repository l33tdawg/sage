#!/usr/bin/env python3
"""Fetch LoCoMo from an explicit immutable upstream commit; verify cached bytes."""
import argparse
import hashlib
import json
import os
import re
import tempfile
import urllib.request
from pathlib import Path


def fetch(path: Path, revision: str) -> dict:
    if not re.fullmatch(r'[0-9a-f]{40}', revision or ''):
        raise ValueError('LOCOMO_DATA_REVISION/--revision must be an immutable 40-character upstream commit SHA')
    url = f'https://raw.githubusercontent.com/snap-research/locomo/{revision}/data/locomo10.json'
    receipt_path = path.with_suffix(path.suffix + '.source.json')
    if path.exists():
        if not receipt_path.exists():
            raise ValueError('Existing dataset has no fetch receipt; use it explicitly via LOCOMO_DATA_PATH or choose a fresh --out path')
        receipt = json.loads(receipt_path.read_text())
        if receipt.get('url') != url or receipt.get('sha256') != hashlib.sha256(path.read_bytes()).hexdigest():
            raise ValueError('Cached dataset revision/hash differs; choose a fresh --out path')
        return receipt
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(20_000_001)
    if len(data) > 20_000_000 or not isinstance(json.loads(data), list):
        raise ValueError('Expected a LoCoMo JSON array under 20 MB')
    receipt = {'url': url, 'upstream_revision': revision, 'sha256': hashlib.sha256(data).hexdigest()}
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        temporary = Path(tmp.name)
        tmp.write(data)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', default=os.environ.get('LOCOMO_DATA_REVISION'))
    parser.add_argument('--out', type=Path, default=Path('bench/locomo/data/locomo10.json'))
    args = parser.parse_args()
    try:
        print(json.dumps(fetch(args.out, args.revision), indent=2))
    except (ValueError, OSError) as exc:
        parser.exit(1, str(exc) + '\n')


if __name__ == '__main__':
    main()
