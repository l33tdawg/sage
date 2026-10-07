"""Signed, fail-closed retrieval benchmark protocol; no operator credential fallback."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
import platform
import secrets
import struct
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from nacl.signing import SigningKey


class BenchmarkError(RuntimeError):
    pass


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_digest(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode())


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parent, stderr=subprocess.DEVNULL).decode().strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def dataset_provenance(rows: list[dict[str, Any]], path: str | None, dataset: str,
                       revision: str | None, split: str = "train") -> dict[str, Any]:
    file_hash = sha256(Path(path).read_bytes()) if path else None
    receipt = None
    if path:
        receipt_path = Path(path).with_suffix(Path(path).suffix + ".source.json")
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if receipt.get("sha256") != file_hash:
                raise BenchmarkError("Dataset fetch receipt does not match the source bytes")
            revision = receipt.get("upstream_revision")
    return {
        "source": str(Path(path).resolve()) if path else dataset,
        "source_file_sha256": file_hash,
        "upstream_revision": revision,
        "fetch_receipt": receipt,
        "split": None if path else split,
        "loaded_rows_sha256": json_digest(rows),
        "loaded_rows": len(rows),
    }


def pinned_revision(name: str) -> str:
    revision = os.environ.get(name, "")
    if len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):
        raise BenchmarkError(f"Set {name} to an immutable upstream 40-character commit SHA, or supply a local dataset file")
    return revision


def add_protocol_arguments(parser: Any) -> None:
    parser.add_argument("--sage-url", default=os.environ.get("SAGE_API_URL"), help="Explicit disposable benchmark node URL (no default)")
    parser.add_argument("--identity", default=os.environ.get("SAGE_BENCH_IDENTITY"), help="Existing approved ordinary Member/Manager raw Ed25519 key file (32-byte seed or 64-byte private key)")
    parser.add_argument("--server-manifest", required=True, help="JSON deployment provenance; must identify server_build_ref, never inferred from harness git SHA")
    parser.add_argument("--expect-reranker", choices=("off", "on"), required=True, help="Required server-observed arm; configured on does not prove each call succeeded")
    parser.add_argument("--commit-timeout", type=float, default=120, help="Seconds to wait for each seed's governed committed state")
    parser.add_argument("--expansion-cache", help="Required with --expand: persist/replay exact variants across reranker arms")


def validate_arguments(args: Any) -> None:
    if not args.sage_url or not args.identity:
        raise BenchmarkError("Provide --sage-url and --identity (or SAGE_API_URL and SAGE_BENCH_IDENTITY); use a disposable benchmark node")
    url = httpx.URL(args.sage_url)
    if (url.scheme not in ("http", "https") or not url.host or url.username or url.password
            or url.path != "/" or url.query or url.fragment):
        raise BenchmarkError("--sage-url must be an HTTP(S) origin without credentials, path, query or fragment")
    if not 0 <= args.expand <= 8 or args.top_k < 10 or args.limit < 0 or not math.isfinite(args.commit_timeout) or args.commit_timeout <= 0:
        raise BenchmarkError("Require --expand 0..8, --top-k >=10 for R@10, --limit >=0 and positive --commit-timeout")
    if args.expand and not args.expansion_cache:
        raise BenchmarkError("--expand requires --expansion-cache so expansion text can be replayed between arms")


class SageBenchmark:
    def __init__(self, client: httpx.Client, key: SigningKey, manifest: dict[str, Any], expected_reranker: bool, commit_timeout: float = 120):
        if not isinstance(manifest, dict) or not isinstance(manifest.get("server_build_ref"), str) or not manifest["server_build_ref"].strip():
            raise BenchmarkError("server manifest requires server_build_ref (exact source commit, binary SHA256 or image digest)")
        self.client, self.key = client, key
        self.agent_id = key.verify_key.encode().hex()
        self.manifest, self.expected_reranker = manifest, expected_reranker
        self.commit_timeout = commit_timeout
        self.run_id = uuid.uuid4().hex
        self.space: dict[str, Any] | None = None
        self.profile: dict[str, Any] = {}
        self.server: dict[str, Any] | None = None
        self.seed_receipts: dict[str, list[dict[str, Any]]] = {}
        self.last_expansions: list[str] = []
        self.write_outcomes: list[dict[str, Any]] = []

    @classmethod
    def from_args(cls, args: Any) -> SageBenchmark:
        validate_arguments(args)
        raw = Path(args.identity).expanduser().read_bytes()
        if len(raw) not in (32, 64):
            raise BenchmarkError("Identity must be an existing raw 32-byte Ed25519 seed or 64-byte private key; no auto-enrollment")
        key = SigningKey(raw[:32])
        if len(raw) == 64 and raw[32:] != key.verify_key.encode():
            raise BenchmarkError("64-byte identity has an invalid public-key suffix")
        manifest = json.loads(Path(args.server_manifest).read_text())
        return cls(httpx.Client(base_url=args.sage_url.rstrip("/"), timeout=65, follow_redirects=False, trust_env=False), key, manifest, args.expect_reranker == "on", args.commit_timeout)

    def request(self, method: str, path: str, body: Any = None, expected_status: int = 200, timeout: float = 65) -> dict[str, Any]:
        raw = b"" if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        timestamp, nonce = int(time.time()), secrets.token_bytes(8)
        message = sha256(f"{method} {path}\n".encode() + raw)
        signed = bytes.fromhex(message) + struct.pack(">q", timestamp) + nonce
        headers = {
            "Content-Type": "application/json", "X-Agent-ID": self.agent_id,
            "X-Timestamp": str(timestamp), "X-Nonce": nonce.hex(),
            "X-Signature": self.key.sign(signed).signature.hex(),
        }
        is_write = method == "POST" and path in ("/v1/domain/register", "/v1/memory/submit")
        outcome = {"path": path, "body_sha256": sha256(raw), "domain": body.get("domain_tag", body.get("name"))} if is_write else None
        try:
            response = self.client.request(method, path, content=raw, headers=headers, timeout=timeout)
        except httpx.HTTPError as exc:
            if outcome is not None:
                self.write_outcomes.append({**outcome, "transport_error": type(exc).__name__, "outcome": "unknown_do_not_resubmit"})
            raise
        if outcome is not None:
            try:
                receipt = response.json()
            except ValueError:
                receipt = {}
            fields = ("memory_id", "tx_hash", "status", "committed", "committed_height", "retryable", "nonce", "embedding_provider", "embedding_queued")
            self.write_outcomes.append({**outcome, "http_status": response.status_code, "receipt": {k: receipt[k] for k in fields if isinstance(receipt, dict) and k in receipt}})
        if response.status_code != expected_status:
            # Never retry a write: 202/timeouts can describe an already-broadcast transaction.
            raise BenchmarkError(f"{method} {path}: expected HTTP {expected_status}, got {response.status_code}; do not resubmit ambiguous writes")
        data = response.json()
        if not isinstance(data, dict):
            raise BenchmarkError(f"{path}: expected a JSON object")
        return data

    def preflight(self) -> None:
        profile = self.request("GET", "/v1/agent/me")
        if (profile.get("agent_id") != self.agent_id or profile.get("role") not in ("member", "manager")
                or profile.get("enrollment_status") != "active" or profile.get("registration_status") != "active"
                or profile.get("approval_required") is not False or profile.get("can_write") is not True
                or profile.get("can_read") is not True or profile.get("clearance", 0) < 1):
            raise BenchmarkError("Benchmark identity must be an approved active ordinary Member/Manager with Read+Write and INTERNAL clearance; Root/Admin are unsupported")
        self.profile = profile
        info = self.request("GET", "/v1/embed/info")
        if (info.get("semantic") is not True or info.get("ready") is not True or info.get("submit_embedding_authoritative") is not True
                or info.get("provider") in (None, "", "hash", "vault-encrypted")):
            raise BenchmarkError("A ready semantic server-authoritative embedder is required; hash vectors cannot measure semantic retrieval")
        self.embed("SAGE retrieval benchmark embedding-space probe")
        if self.space["dimension"] != info.get("dimension"):
            raise BenchmarkError("/v1/embed and /v1/embed/info dimensions disagree")
        self.observe_server()

    def observe_server(self) -> dict[str, Any]:
        health = self.request("GET", "/v1/dashboard/health")
        reranker = health.get("embedder", {}).get("reranker", {})
        if not health.get("version") or type(reranker.get("enabled")) is not bool:
            raise BenchmarkError("Server health must disclose actual version and reranker configuration; do not substitute harness environment")
        if reranker["enabled"] != self.expected_reranker:
            raise BenchmarkError("Observed server reranker setting differs from --expect-reranker")
        observed = {"version": health["version"], "boot_id": health.get("boot_id"), "embedder": health.get("embedder"), "memory_gate": health.get("memory_gate")}
        if self.server is not None and observed != self.server:
            raise BenchmarkError("Observed server configuration/boot changed during the benchmark")
        self.server = observed
        return observed

    def embed(self, text: str) -> list[float]:
        if not isinstance(text, str) or not text.strip():
            raise BenchmarkError("Cannot embed an empty query")
        data = self.request("POST", "/v1/embed", {"text": text})
        vector, dimension = data.get("embedding"), data.get("dimension")
        space = {"embedding_provider": data.get("embedding_provider"), "model": data.get("model"), "dimension": dimension}
        if (not isinstance(vector, list) or type(dimension) is not int or dimension <= 0 or len(vector) != dimension
                or not all(type(v) in (int, float) and math.isfinite(v) for v in vector)
                or not any(v != 0 for v in vector) or not isinstance(space["model"], str) or not space["model"]
                or not isinstance(space["embedding_provider"], str) or not space["embedding_provider"]
                or space["embedding_provider"] in ("hash", "vault-encrypted") or space["model"] == "hash"):
            raise BenchmarkError("Malformed, empty or nonsemantic /v1/embed vector/space metadata")
        if self.space is not None and self.space != space:
            raise BenchmarkError("Server embedding provider/model/dimension changed; aborting mixed-space run")
        self.space = space
        return vector

    def new_domain(self, dataset: str, item: str) -> str:
        domain = f"bench-{dataset}-{self.run_id[:12]}-{sha256(str(item).encode())[:12]}"
        receipt = self.request("POST", "/v1/domain/register", {"name": domain, "description": f"Retrieval benchmark {self.run_id}; item {item}"}, 201)
        if receipt.get("status") != "registered" or not self.valid_hash(receipt.get("tx_hash")):
            raise BenchmarkError("Invalid domain transaction receipt")
        detail = self.request("GET", f"/v1/domain/{domain}")
        if detail.get("domain_name") != domain or detail.get("owner_agent_id") != self.agent_id or type(detail.get("created_height")) is not int or detail["created_height"] <= 0:
            raise BenchmarkError("Fresh domain ownership was not confirmed on-chain")
        self.seed_receipts[domain] = []
        return domain

    @staticmethod
    def valid_hash(value: Any) -> bool:
        return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdefABCDEF" for c in value)

    def seed(self, domain: str, content: str) -> dict[str, Any]:
        if domain not in self.seed_receipts or not content.strip() or len(content.encode()) > 50_000:
            raise BenchmarkError("Seed needs a fresh confirmed domain and nonempty content of at most 50000 UTF-8 bytes")
        receipt = self.request("POST", "/v1/memory/submit", {"content": content, "memory_type": "observation", "domain_tag": domain, "confidence_score": 0.85, "classification": 1}, 201)
        try:
            memory_id = str(uuid.UUID(receipt.get("memory_id", "")))
        except (ValueError, TypeError, AttributeError) as exc:
            raise BenchmarkError("Invalid seed memory ID") from exc
        if (receipt.get("committed") is not True or not self.valid_hash(receipt.get("tx_hash"))
                or type(receipt.get("committed_height")) is not int or receipt["committed_height"] <= 0
                or receipt.get("embedding_queued") is True or receipt.get("embedding_provider") != self.space["embedding_provider"]):
            raise BenchmarkError("Seed receipt is uncommitted, unembedded or in a different embedding space")
        deadline = time.monotonic() + self.commit_timeout
        while True:
            remaining = max(0.001, deadline - time.monotonic())
            detail = self.request("GET", f"/v1/memory/{memory_id}", timeout=min(65, remaining))
            expected = {"memory_id": memory_id, "content": content, "content_hash": sha256(content.encode()), "submitting_agent": self.agent_id, "domain_tag": domain, "memory_type": "observation", "classification": 1}
            if any(detail.get(k) != v for k, v in expected.items()):
                raise BenchmarkError("Seed detail differs from the exact signed content/author/domain/classification")
            if detail.get("status") == "committed":
                verified = {"memory_id": memory_id, "tx_hash": receipt["tx_hash"], "committed_height": receipt["committed_height"], "embedding_provider": receipt["embedding_provider"], "content_sha256": expected["content_hash"], "lifecycle_verified": "committed"}
                self.seed_receipts[domain].append(verified)
                return verified
            if detail.get("status") != "proposed" or time.monotonic() >= deadline:
                raise BenchmarkError(f"Seed {memory_id} did not reach governed committed state before deadline (status={detail.get('status')}); configure the disposable node's voter")
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))

    def hybrid(self, domain: str, question: str, top_k: int, variants: list[str]) -> list[dict[str, Any]]:
        if not self.seed_receipts.get(domain):
            raise BenchmarkError("Cannot score an empty/unverified seed domain")
        self.observe_server()
        body = {"query": question, "embedding": self.embed(question), "embedding_provider": self.space["embedding_provider"], "domain_tag": domain, "top_k": top_k, "status_filter": "committed"}
        if variants:
            body["expansions"] = [{"query": v, "embedding": self.embed(v)} for v in variants]
        self.last_expansions = list(variants)
        data = self.request("POST", "/v1/memory/hybrid", body)
        results = data.get("results")
        if not isinstance(results, list):
            raise BenchmarkError("Hybrid response must contain a results list")
        allowed = {r["memory_id"]: r for r in self.seed_receipts[domain]}
        seen = set()
        for row in results:
            if (not isinstance(row, dict) or row.get("memory_id") not in allowed or row.get("memory_id") in seen
                    or row.get("domain_tag") != domain or row.get("status") != "committed"
                    or not isinstance(row.get("content"), str)
                    or sha256(row["content"].encode()) != allowed[row["memory_id"]]["content_sha256"]):
                raise BenchmarkError("Hybrid result escaped the exact verified seed/domain/content/lifecycle")
            seen.add(row["memory_id"])
        return results

    def metadata(self) -> dict[str, Any]:
        try:
            dirty = bool(subprocess.check_output(
                ["git", "status", "--porcelain"], cwd=Path(__file__).parent,
                stderr=subprocess.DEVNULL,
            ).strip())
        except (OSError, subprocess.CalledProcessError):
            dirty = None
        packages = {}
        for name in ("httpx", "pynacl", "openai", "datasets"):
            module = "nacl" if name == "pynacl" else name
            if importlib.util.find_spec(module) is not None:
                packages[name] = importlib.metadata.version(name)
        return {
            "run_id": self.run_id,
            "harness_git_sha": git_sha(),
            "harness_dirty": dirty,
            "sage_url": str(self.client.base_url),
            "agent_id": self.agent_id,
            "agent_profile_observed": {key: self.profile.get(key) for key in (
                "role", "profile", "home_domain", "enrollment_status", "registration_status",
                "approval_required", "clearance", "capabilities", "can_read", "can_write",
                "access_scope", "on_chain_height",
            )},
            "seed_policy": {"classification": 1, "confidence_score": 0.85,
                            "commit_timeout_seconds": self.commit_timeout},
            "server_observed": self.server,
            "server_manifest_operator_supplied": self.manifest,
            "server_manifest_sha256": json_digest(self.manifest),
            "embedding_space_observed": self.space,
            "seed_receipts": self.seed_receipts,
            "write_outcomes": self.write_outcomes,
            "python": platform.python_version(),
            "packages": packages,
        }



class ExpansionSource:
    """Persist exact expansion text, so on/off reranker comparisons reuse it."""
    def __init__(self, n: int, cache_path: str | None, model: str):
        self.n, self.path, self.model = n, Path(cache_path) if cache_path else None, model
        self.data = {"model": model, "temperature": 0.4, "max_tokens": 200, "variants": {}}
        if self.path and self.path.exists():
            self.data = json.loads(self.path.read_text())
            if any(self.data.get(k) != v for k, v in {"model": model, "temperature": 0.4, "max_tokens": 200}.items()) or not isinstance(self.data.get("variants"), dict):
                raise BenchmarkError("Expansion cache model/parameters do not match this run")

    def variants(self, question: str) -> list[str]:
        if not self.n:
            return []
        key = sha256(question.encode())
        variants = self.data["variants"].get(key)
        if variants is None:
            if not os.environ.get("OPENAI_API_KEY"):
                raise BenchmarkError("Uncached expansion requires OPENAI_API_KEY; --expand 0 and cache replay do not")
            from openai import OpenAI  # optional; no embedding calls are made here
            prompt = (
                f"Generate {self.n} short paraphrase/entity/temporal variants of the question below. "
                "Output ONLY the variants, one per line, no numbering or commentary. "
                "Vary phrasing, surface named entities explicitly, and concretise relative "
                f"time references when context permits.\n\nQuestion: {question}"
            )
            response = OpenAI().chat.completions.create(
                model=self.model, messages=[{"role": "user", "content": prompt}],
                temperature=0.4, max_tokens=200,
            )
            text = response.choices[0].message.content or ""
            variants = list(dict.fromkeys(line.strip().lstrip("-*0123456789.) ").strip() for line in text.splitlines()))
            variants = [v for v in variants if v and v != question][:self.n]
            self.validate(variants, question)
            self.data["variants"][key] = variants
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            temporary.write_text(json.dumps(self.data, indent=2, ensure_ascii=False))
            temporary.replace(self.path)
        self.validate(variants, question)
        return list(variants)

    def validate(self, variants: Any, question: str) -> None:
        if not isinstance(variants, list) or len(variants) != self.n or not all(isinstance(v, str) and v.strip() and v != question for v in variants) or len(set(variants)) != self.n:
            raise BenchmarkError("Expansion arm requires exactly the requested number of distinct nonempty variants; no silent fallback")

    def metadata(self) -> dict[str, Any]:
        return {"n": self.n, "model": self.model if self.n else None, "cache_sha256": sha256(self.path.read_bytes()) if self.n and self.path.exists() else None, "temperature": 0.4 if self.n else None, "max_tokens": 200 if self.n else None}
