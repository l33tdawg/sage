#!/usr/bin/env python3
"""Real Unix sockets and URLSession, plus an isolated governed app-v28 daemon.

Requires macOS. The Swift probe compiles production sources without DEBUG.
Only the daemon's existing v119testfixture governance timing bounds differ from
production. No profile, port, executable, cookie store or key from a live node
is reused. Any missing tool, crashed probe, skipped assertion or timeout fails.
"""
import argparse
import contextlib
import hashlib
import http.server
import json
import os
import signal
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


def status(**changes):
    value = dict(control_protocol=1, daemon_version="12.0.0-beta.1", api_schema=1,
                 min_shell_protocol=1, max_shell_protocol=1,
                 instance_generation="A" * 43, state="ready", ui_origin="http://127.0.0.1:49152")
    value.update(changes)
    return value


def frame(value):
    data = json.dumps(value).encode()
    return struct.pack("!I", len(data)) + data


def receive_frame(conn):
    def exact(count):
        data = b""
        while len(data) < count:
            part = conn.recv(count - len(data))
            if not part:
                raise EOFError("partial control frame")
            data += part
        return data
    size = struct.unpack("!I", exact(4))[0]
    if not 0 < size <= 16384:
        raise AssertionError("invalid probe request length")
    return json.loads(exact(size))


class Qualification:
    def __init__(self, probe, evidence):
        self.probe = str(probe)
        self.evidence = evidence
        self.results = []

    def record(self, name, detail=None):
        self.results.append(dict(name=name, passed=True, detail=detail))
        print(f"PASS {name}", flush=True)

    def run(self, *args, payload=None, timeout=20):
        started = time.monotonic()
        child = subprocess.run([self.probe, *map(str, args)], input=json.dumps(payload) if payload else None,
                               text=True, capture_output=True, timeout=timeout)
        assert child.returncode == 0, f"probe crashed ({child.returncode}): {child.stderr[-3000:]}"
        value = json.loads(child.stdout)
        value["wall_seconds"] = time.monotonic() - started
        return value

    def versions(self):
        accepted = ["11.10.0", "11.19.999", "v11.19.1+build.01", "11.19.0-rc.1",
                    "12.0.0-beta", "12.0.0-beta.1", "v12.0.0-beta.9+build.01"]
        rejected = ["", "v", "dev", "-", "+", "12.0.0", "11.9.9", "11.20.0", "11.23.15",
                    "13.0.0-beta.1", "12.1.0-beta.1", "12.0.0-rc.1", "12.0.0-beta2",
                    "12.0.0-beta.", "12.0.0-beta..1", "12.0.0-beta.01", "12.0.0-beta.1+",
                    "12.0.0-beta.1+build..1", "12.0.0-beta.1+x+y", "012.0.0-beta.1",
                    "12.00.0-beta.1", "12.0.00-beta.1", "12.0.١-beta.1", "12.0.0-beta.α",
                    " 12.0.0-beta.1", "12.0.0-beta.1\n", "12.0-beta.1", "12.0.0.0-beta.1",
                    "11.19.x", "11.19.0+", "11.19.0-", "11.19.0-01", "vv11.19.0"]
        for version in accepted + rejected:
            result = self.run("validate", payload=status(daemon_version=version))
            assert result["ok"] == (version in accepted), f"wrong version disposition: {version!r}: {result}"
        self.record("semantic-version matrix preserves 11.10–19 / 12.0 beta boundary", dict(accepted=len(accepted), rejected=len(rejected)))
        for changes in [dict(control_protocol=2), dict(api_schema=2), dict(min_shell_protocol=2),
                        dict(min_shell_protocol=0), dict(max_shell_protocol=0), dict(state="future"),
                        dict(instance_generation="é" + "A" * 42), dict(instance_generation="A" * 42 + "B"),
                        dict(startup_proof="z" * 64), dict(startup_proof=""), dict(new_field=True)]:
            assert not self.run("validate", payload=status(**changes))["ok"], changes
        for state in ["starting", "locked", "draining", "failed"]:
            value = status(state=state)
            del value["ui_origin"]
            assert self.run("validate", payload=value)["ok"]
            assert not self.run("validate", payload=status(state=state, ui_origin=""))["ok"]
        self.record("closed protocol, generation, startup proof and lifecycle validation")
        for timestamp in ["2026-10-05T18:43:08Z", "2026-10-05T18:43:08.123456789Z", "2026-10-06T02:43:08.123456789+08:00"]:
            value = dict(sage="running", version="12.0.0-beta.1", encrypted=False, vault_locked=False,
                         uptime="1s", chain=dict(block_time=timestamp, app_version="28"))
            result = self.run("decode-health", payload=value)
            assert result["ok"] and result["block_time_decoded"], result
        self.record("shipping dashboard decoder accepts RFC3339Nano with fractions and offsets")

    @contextlib.contextmanager
    def control(self, response, mode=None, run_mode=0o700, socket_mode=0o600):
        # /tmp resolves to /private/tmp; keep AF_UNIX paths below Darwin's limit.
        with tempfile.TemporaryDirectory(prefix="sage-nt-", dir="/private/tmp") as root:
            home = Path(root)
            run = home / "run"
            run.mkdir(mode=0o700)
            endpoint = run / "shell-control.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(endpoint))
            endpoint.chmod(socket_mode)
            run.chmod(run_mode)
            server.listen(1)
            server.settimeout(5)
            failures = []
            stop = threading.Event()

            def serve():
                try:
                    with server.accept()[0] as conn:
                        conn.settimeout(3)
                        if mode == "close":
                            return
                        request = receive_frame(conn)
                        assert request == dict(control_protocol=1, shell_protocol=1, operation="status"), request
                        if mode == "stall":
                            stop.wait(5)
                        elif mode in ("trickle-header", "trickle-body"):
                            data = response
                            if mode == "trickle-body":
                                conn.sendall(data[:4])
                                data = data[4:]
                            for byte in data:
                                if stop.wait(0.45):
                                    break
                                conn.sendall(bytes([byte]))
                        elif mode == "fragment":
                            for byte in response:
                                conn.sendall(bytes([byte]))
                        else:
                            conn.sendall(response)
                except (BrokenPipeError, ConnectionResetError, socket.timeout, EOFError):
                    pass  # The client is expected to reject or time out.
                except Exception as error:
                    failures.append(error)

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            try:
                yield home
            finally:
                stop.set()
                server.close()
                thread.join(timeout=6)
                assert not thread.is_alive(), "control fixture did not stop"
                assert not failures, failures

    def sockets(self):
        with self.control(frame(status()), mode="fragment") as home:
            assert self.run("discover", home)["origin"] == "http://127.0.0.1:49152"
        self.record("fragmented real AF_UNIX status handshake and same-UID peer")
        for name, data, mode in [("zero", struct.pack("!I", 0), None),
                                 ("oversize", struct.pack("!I", 16385), None),
                                 ("partial header", b"\0\0", None),
                                 ("partial body", struct.pack("!I", 40) + b"{}", None),
                                 ("invalid JSON", struct.pack("!I", 1) + b"[", None),
                                 ("unknown field", frame(status(future=True)), None),
                                 ("empty version", frame(status(daemon_version="")), None),
                                 ("immediate close", b"", "close")]:
            with self.control(data, mode) as home:
                assert not self.run("discover", home)["ok"], name
            self.record(f"socket rejects {name} without process death")
        for mode in ["stall", "trickle-header", "trickle-body"]:
            with self.control(frame(status()), mode) as home:
                result = self.run("discover", home, timeout=5)
                assert not result["ok"] and 1.8 <= result["wall_seconds"] < 3.5, result
            self.record(f"two-second absolute deadline: {mode}", result)
        for changes in [dict(run_mode=0o755), dict(socket_mode=0o666)]:
            with self.control(frame(status()), **changes) as home:
                assert not self.run("discover", home)["ok"]
        with self.control(frame(status())) as home:
            endpoint = home / "run/shell-control.sock"
            endpoint.rename(home / "run/actual.sock")
            endpoint.symlink_to("actual.sock")
            assert not self.run("discover", home)["ok"]
        self.record("unsafe directory/socket permissions and socket symlink rejected")
        for origin in ["http://user:pass@127.0.0.1:49152", "http://127.0.0.1:49152/path",
                       "http://127.0.0.1:49152/?secret=1", "http://127.0.0.1:49152/#fragment",
                       "http://sage.example:49152", "http://127.0.0.1", "file:///private/tmp/sage"]:
            with self.control(frame(status(ui_origin=origin))) as home:
                assert not self.run("discover", home)["ok"], origin
        self.record("control discovery rejects unsafe origins")

    @contextlib.contextmanager
    def http(self, behavior):
        fixture = HTTPFixture(behavior)
        thread = threading.Thread(target=fixture.server.serve_forever, daemon=True)
        thread.start()
        try:
            yield fixture
        finally:
            fixture.stop.set()
            fixture.server.shutdown()
            fixture.server.server_close()
            thread.join(timeout=3)
            assert not fixture.failures, fixture.failures

    def urlsession(self):
        for behavior in ["federation-off", "federation-on", "federation-unavailable", "federation-malformed"]:
            with self.http(behavior) as fixture:
                result = self.run("federation", fixture.origin)
                if behavior == "federation-malformed":
                    assert not result["ok"], result
                else:
                    assert result["ok"] and result["enabled"] == (behavior == "federation-on"), result
                    assert result["connections"] == 0, result
                if behavior == "federation-off":
                    assert len(fixture.requests) == 1 and fixture.requests[0]["path"].endswith("settings/federation"), fixture.requests
            self.record(f"real URLSession {behavior} uses explicit transport setting", result)
        with self.http("cookies") as fixture:
            result = self.run("cookies", fixture.origin)
            assert result["ok"] and result["login"] and result["after"], result
            assert not result["before"] and not result["other_session"] and not result["locked"], result
            assert len(fixture.requests) == 6, fixture.requests
        self.record("real URLSession private ephemeral cookies, login and lock", result)
        with self.http("unauthorized") as fixture:
            result = self.run("unauthorized", fixture.origin)
            assert result["ok"] and result["unauthorized_count"] == 1, result
        self.record("HTTP 401 invokes the native lock callback once")
        with self.http("healthy") as target, self.http("redirect") as fixture:
            fixture.redirect = target.origin + "/health"
            result = self.run("health", fixture.origin)
            assert not result["ok"] and not target.requests, (result, target.requests)
            fixture.redirect = fixture.origin + "/health"
            assert self.run("health", fixture.origin)["ok"]
        self.record("cross-origin redirect refused; exact-origin redirect accepted")
        for behavior, milliseconds in [("eof", 3800), ("empty", 7800), ("errors", 3800),
                                       ("cancel", 1100), ("event-401", 2200)]:
            with self.http(behavior) as fixture:
                result = self.run("events", fixture.origin, milliseconds, timeout=15)
                assert result["ok"], result
                count_after_cancel = len(fixture.requests)
                time.sleep(1.2)
                assert len(fixture.requests) == count_after_cancel, "stream reconnected after cancellation"
                times = [request["time"] for request in fixture.requests]
                gaps = [b - a for a, b in zip(times, times[1:])]
                if behavior == "eof":
                    assert len(times) >= 3 and result["events"].count("consensus") >= 3, result
                    assert "reconnecting" in result["states"] and all(gap >= 0.85 for gap in gaps), gaps
                elif behavior == "empty":
                    assert len(times) == 4 and gaps[0] >= 0.85 and gaps[1] >= 1.85 and gaps[2] >= 3.85, gaps
                elif behavior == "errors":
                    assert len(times) == 3 and result["events"] == ["consensus"], (result, gaps)
                    assert gaps[0] >= 0.85 and gaps[1] >= 1.85, gaps
                elif behavior == "cancel":
                    assert len(times) == 1 and "connected" in result["states"] and fixture.disconnected.wait(3), result
                else:
                    assert len(times) == 1 and result["unauthorized_count"] == 1 and "failure" in result, result
                self.record(f"real URLSession SSE {behavior} and cancellation", dict(transcript=result, gaps=gaps))

    def daemon(self, binary):
        # Reserve ports first; release only immediately before our owned child.
        reservations = [socket.socket() for _ in range(4)]
        for reservation in reservations:
            reservation.bind(("127.0.0.1", 0))
        ports = [reservation.getsockname()[1] for reservation in reservations]
        assert len(set(ports)) == 4 and not set(ports) & {8080, 8443, 26656, 26657}
        with tempfile.TemporaryDirectory(prefix="sage-pair-", dir="/private/tmp") as root:
            home = Path(root)
            (home / "config.yaml").write_text("federation:\n  enabled: false\nembedding:\n  provider: hash\nvoter:\n  enabled: true\n  poll_interval: 100ms\n")
            # Do not inherit live profile credentials, ports or test pause hooks.
            environment = {key: value for key, value in os.environ.items()
                           if not key.startswith(("SAGE_", "V119_")) and key not in ("REST_ADDR", "OLLAMA_URL", "OLLAMA_MODEL")}
            environment.update(SAGE_HOME=root, REST_ADDR=f"127.0.0.1:{ports[0]}",
                               SAGE_TLS_ADDR=f"127.0.0.1:{ports[1]}", SAGE_CMT_RPC_ADDR=f"tcp://127.0.0.1:{ports[2]}",
                               SAGE_CMT_P2P_ADDR=f"tcp://127.0.0.1:{ports[3]}", SAGE_NO_BROWSER="1")
            origin = f"http://127.0.0.1:{ports[0]}"
            log = self.evidence / "isolated-daemon.log"

            def start():
                return subprocess.Popen([str(binary), "serve"], env=environment, stdout=output, stderr=subprocess.STDOUT)

            def stop(child):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=5)
                        raise AssertionError("owned daemon required forced shutdown")
                assert child.returncode == 0, f"owned daemon exited {child.returncode}; see {log}"

            def ready(child, timeout):
                end = time.monotonic() + timeout
                last_version = None
                while time.monotonic() < end:
                    assert child.poll() is None, f"owned daemon died; see {log}"
                    try:
                        request = urllib.request.Request(origin + "/v1/dashboard/health",
                                                         headers={"Origin": origin, "Sec-Fetch-Site": "same-origin"})
                        with urllib.request.urlopen(request, timeout=2) as response:
                            health = json.load(response)
                        app_version = str(health.get("chain", {}).get("app_version"))
                        if app_version != last_version:
                            print(f"isolated daemon governed app_version={app_version}", flush=True)
                            last_version = app_version
                        if app_version == "28":
                            return health
                    except (OSError, urllib.error.URLError):
                        pass
                    time.sleep(1)
                raise AssertionError(f"isolated daemon did not activate app-v28 within {timeout}s; see {log}")

            def generation():
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                    conn.settimeout(2)
                    conn.connect(str(home / "run/shell-control.sock"))
                    conn.sendall(frame(dict(control_protocol=1, shell_protocol=1, operation="status")))
                    return receive_frame(conn)["instance_generation"]

            def create_task_event():
                marker = home / "native-probe-connected"
                reader = subprocess.Popen([self.probe, "events", origin, "10000", str(marker)],
                                          text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    end = time.monotonic() + 5
                    while not marker.exists():
                        assert reader.poll() is None and time.monotonic() < end, "native event stream did not connect"
                        time.sleep(0.05)
                    body = json.dumps(dict(content="Native transport fixture: verify local dashboard wire reads.",
                                           domain="general")).encode()
                    request = urllib.request.Request(origin + "/v1/dashboard/tasks", data=body, method="POST",
                                                     headers={"Origin": origin, "Sec-Fetch-Site": "same-origin",
                                                              "Content-Type": "application/json"})
                    with urllib.request.urlopen(request, timeout=10) as response:
                        assert response.status == 201
                        task = json.load(response)
                    stdout, stderr = reader.communicate(timeout=15)
                    assert reader.returncode == 0, f"native event reader died: {stderr[-3000:]}"
                    result = json.loads(stdout)
                    assert result["ok"] and "connected" in result["states"] and "task" in result["events"], result
                    self.record("real daemon canonical task event reaches URLSession and cancellation", result)
                    return task["memory_id"]
                finally:
                    if reader.poll() is None:
                        reader.kill()
                        reader.wait(timeout=5)

            with log.open("w") as output:
                for reservation in reservations:
                    reservation.close()
                child = start()
                try:
                    health = ready(child, 600)
                    result = self.run("discover", home)
                    assert result["ok"] and result["origin"].rstrip("/") == origin, result
                    result = self.run("overview", origin)
                    assert result["ok"] and result["version"] == "12.0.0-beta.1" and result["app_version"] == "28", result
                    assert result["validators"] == 1 and not result["federation_enabled"], result
                    self.record("production Swift discovery + typed dashboard reads against governed app-v28 beta daemon", result)
                    memory_id = create_task_event()
                    end = time.monotonic() + 15
                    while True:
                        populated = self.run("overview", origin)
                        assert populated["ok"], populated
                        if populated["memories"] > 0 and populated["graph_nodes"] > 0:
                            break
                        assert time.monotonic() < end, populated
                        time.sleep(0.2)
                    self.record("production native Search and Brain decode a canonical populated app-v28 memory", populated)
                    tags = self.run("memory-tags", origin, memory_id)
                    assert tags["ok"] and tags["before"] == [] and tags["replaced"] == ["native-fixture"], tags
                    assert tags["bulk_updated"] == 1 and set(tags["after"]) == {"native-fixture", "wire-qualified"}, tags
                    self.record("production native replace/bulk tag edits, canonical reread and related-memory endpoint", tags)
                    previous_generation = generation()
                finally:
                    stop(child)
                assert not self.run("discover", home)["ok"], "stopped daemon remained discoverable"
                child = start()
                try:
                    ready(child, 45)
                    current_generation = generation()
                    assert current_generation != previous_generation
                    assert self.run("discover", home)["ok"]
                    restarted = self.run("overview", origin)
                    assert restarted["ok"] and restarted["app_version"] == "28" and restarted["memories"] > 0, restarted
                    self.record("daemon restart preserves app-v28 and issues a fresh SSCP generation")
                finally:
                    stop(child)


class HTTPFixture:
    def __init__(self, behavior):
        self.behavior = behavior
        self.requests = []
        self.failures = []
        self.redirect = None
        self.stop = threading.Event()
        self.disconnected = threading.Event()
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def reply(self, code, body, headers=None):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                self.do_GET()

            def do_GET(self):
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(length) if length else b""
                    assert self.headers.get("Origin", "").rstrip("/") == fixture.origin, self.headers
                    assert self.headers.get("Sec-Fetch-Site") == "same-origin", self.headers
                    fixture.requests.append(dict(path=self.path, time=time.monotonic()))
                    if fixture.behavior.startswith("federation-"):
                        if self.path.endswith("settings/federation"):
                            self.reply(200, {} if fixture.behavior == "federation-malformed" else dict(enabled=fixture.behavior != "federation-off"))
                        else:
                            assert self.path.endswith("federation/connections"), self.path
                            if fixture.behavior == "federation-unavailable":
                                self.reply(501, dict(error="unavailable"))
                            else:
                                self.reply(200, dict(connections=[]))
                    elif fixture.behavior == "cookies":
                        if self.path.endswith("/login"):
                            assert json.loads(body)["passphrase"] == "fixture-passphrase"
                            self.reply(200, dict(ok=True), {"Set-Cookie": "sage_session=fixture; Path=/; HttpOnly; SameSite=Strict"})
                        elif self.path.endswith("/lock"):
                            self.reply(200, dict(ok=True), {"Set-Cookie": "sage_session=; Path=/; Max-Age=0"})
                        else:
                            self.reply(200, dict(auth_required=True, authenticated="sage_session=fixture" in self.headers.get("Cookie", "")))
                    elif fixture.behavior in ("unauthorized", "event-401"):
                        self.reply(401, dict(error="locked"))
                    elif fixture.behavior == "redirect" and self.path != "/health":
                        self.reply(302, {}, {"Location": fixture.redirect})
                    elif fixture.behavior in ("healthy", "redirect"):
                        self.reply(200, dict(sage="running", version="12.0.0-beta.1", encrypted=False,
                                             vault_locked=False, uptime="1s",
                                             chain=dict(block_time="2026-10-05T18:43:08.123456789Z", app_version="28")))
                    elif fixture.behavior == "errors" and len(fixture.requests) <= 2:
                        self.reply(503, dict(error="fixture unavailable"))
                    else:
                        self.send_response(200)
                        self.send_header("Content-Type", "text/event-stream")
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.close_connection = True
                        if fixture.behavior != "empty":
                            self.wfile.write(b": heartbeat\nevent: consensus\ndata: {\"height\":\ndata: 42}\n\n")
                            self.wfile.flush()
                        if fixture.behavior == "cancel":
                            while not fixture.stop.wait(0.1):
                                self.wfile.write(b": heartbeat\n\n")
                                self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    fixture.disconnected.set()
                except Exception as error:
                    fixture.failures.append(str(error))
                    self.close_connection = True

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"


def main():
    def terminate(_signal, _frame):
        raise SystemExit(143)  # Unwind owned daemon/profile cleanup on cancellation.
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    qualification = Qualification(args.probe.resolve(), args.evidence.resolve())
    result = dict(schema="sage.v12.native-transport.1", completed=False, skipped=0,
                  qualification_scope="Swift Foundation development transport; SSCP/1 + dashboard API schema 1",
                  daemon_fixture="v119testfixture: governed delay=3 blocks, proposer cooldown=1 block; not production timing",
                  production_native_session_bootstrap=False, app_session_restart_recovery=False,
                  physical_hid=False, voiceover=False, results=qualification.results,
                  probe_sha256=hashlib.sha256(args.probe.read_bytes()).hexdigest(),
                  daemon_sha256=hashlib.sha256(args.daemon.read_bytes()).hexdigest())
    try:
        qualification.versions()
        qualification.sockets()
        qualification.urlsession()
        qualification.daemon(args.daemon.resolve())
        result["completed"] = True
    except BaseException as error:
        result["failure"] = str(error)
        raise
    finally:
        (args.evidence / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
