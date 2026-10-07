#!/usr/bin/env python3
"""Qualify production AppSession against an isolated encrypted app-v28 daemon.

The release Swift probe uses real Foundation and AF_UNIX. Only the daemon's
existing v119testfixture governance timing differs from production. No live
profile, cookie store, key, process, listener or model service is reused.
"""
import argparse
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import queue
import re
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


EXPECTED_ASSERTIONS = 20
DISCONNECT_BOUND_SECONDS = 5
CONTENT = "[TASK] Native encrypted session fixture: preserve this disposable memory across daemon restart."


class Probe:
    def __init__(self, binary, home, environment, stderr):
        self.child = subprocess.Popen([str(binary), str(home)], env=environment,
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=stderr, text=True, bufsize=1)
        self.replies = queue.Queue()
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.child.stdout:
            self.replies.put(line)
        self.replies.put(None)

    def command(self, operation, **values):
        assert self.child.poll() is None, "native AppSession probe exited prematurely"
        self.child.stdin.write(json.dumps(dict(operation=operation, **values)) + "\n")
        self.child.stdin.flush()
        try:
            line = self.replies.get(timeout=25)
        except queue.Empty:
            raise AssertionError("native AppSession probe timed out") from None
        assert line is not None, "native AppSession probe closed its output prematurely"
        result = json.loads(line)
        assert result["ok"], "native AppSession command failed"
        return result

    def close(self):
        if self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
        self.child.stdin.close()
        self.reader.join(timeout=5)
        self.child.stdout.close()
        assert not self.reader.is_alive(), "native probe output reader did not stop"


class Qualification:
    def __init__(self, probe, daemon, evidence):
        self.probe = probe
        self.daemon = daemon
        self.evidence = evidence
        self.results = []

    def record(self, name, **detail):
        self.results.append(dict(name=name, passed=True, detail=detail))
        print(f"PASS {name}", flush=True)

    def run(self):
        with tempfile.TemporaryDirectory(prefix="sage-session-", dir="/private/tmp") as root:
            home = Path(root)
            home.chmod(0o700)
            (home / "config.yaml").write_text("federation:\n  enabled: false\nembedding:\n  provider: hash\nvoter:\n  enabled: true\n  poll_interval: 100ms\n")
            reservations = [socket.socket() for _ in range(4)]
            try:
                for reservation in reservations:
                    reservation.bind(("127.0.0.1", 0))
                ports = [reservation.getsockname()[1] for reservation in reservations]
                assert len(set(ports)) == 4 and not set(ports) & {8080, 8443, 26656, 26657}, "fixture ports are not isolated"
                self.record("fresh owned profile and four distinct non-production loopback ports")
                environment = {key: value for key, value in os.environ.items()
                               if not key.startswith(("SAGE_", "V119_")) and key not in
                               ("REST_ADDR", "OLLAMA_URL", "OLLAMA_MODEL", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                                "http_proxy", "https_proxy", "all_proxy")}
                environment.update(SAGE_HOME=root, REST_ADDR=f"127.0.0.1:{ports[0]}",
                                   SAGE_TLS_ADDR=f"127.0.0.1:{ports[1]}", SAGE_CMT_RPC_ADDR=f"tcp://127.0.0.1:{ports[2]}",
                                   SAGE_CMT_P2P_ADDR=f"tcp://127.0.0.1:{ports[3]}", SAGE_NO_BROWSER="1")
                origin = f"http://127.0.0.1:{ports[0]}"
                # Private memory-only cookie jar; neither cookies nor passphrase
                # are written to the evidence or passed on a process command line.
                cookie_jar = http.cookiejar.CookieJar()
                operator = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                                     urllib.request.HTTPCookieProcessor(cookie_jar))
                anonymous = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                passphrase = secrets.token_urlsafe(32)
                child = None
                native = None
                log = home / "daemon-private.log"
                with log.open("w") as output, (home / "probe-private.log").open("w") as errors:
                    def request(path, body=None, client=anonymous):
                        data = json.dumps(body).encode() if body is not None else None
                        req = urllib.request.Request(origin + path, data=data,
                                                     headers={"Origin": origin, "Sec-Fetch-Site": "same-origin",
                                                              "Content-Type": "application/json"})
                        try:
                            with client.open(req, timeout=10) as response:
                                return response.status, json.load(response)
                        except urllib.error.HTTPError as error:
                            # Publish only a bounded, redacted error diagnostic,
                            # never headers, successful responses or recovery keys.
                            try:
                                value = json.load(error)
                                diagnostic = str(value.get("code") or value.get("error") or "HTTP error")
                            except (ValueError, AttributeError):
                                diagnostic = "non-JSON HTTP error"
                            diagnostic = diagnostic.replace(passphrase, "[redacted]")
                            for cookie in cookie_jar:
                                diagnostic = diagnostic.replace(cookie.value, "[redacted]")
                            diagnostic = re.sub(r"[A-Za-z0-9_+/=-]{32,}", "[redacted]", diagnostic)[:300]
                            return error.code, dict(diagnostic=diagnostic)

                    def start():
                        return subprocess.Popen([str(self.daemon), "serve"], env=environment,
                                                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT)

                    def stop(process):
                        if process is None:
                            return
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=15)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=5)
                                raise AssertionError("owned daemon required forced shutdown") from None
                        assert process.returncode == 0, "owned daemon exited unsuccessfully"

                    def ready(process, encrypted=False):
                        end = time.monotonic() + (60 if encrypted else 600)
                        last_version = None
                        while time.monotonic() < end:
                            assert process.poll() is None, "owned daemon died before readiness"
                            try:
                                code, auth = request("/v1/dashboard/auth/check")
                                if code == 200 and auth["auth_required"] == encrypted:
                                    if encrypted:
                                        assert not auth["authenticated"], "anonymous readiness request unexpectedly authenticated"
                                        return
                                    code, health = request("/v1/dashboard/health")
                                    if code == 200:
                                        app_version = str(health.get("chain", {}).get("app_version"))
                                        if app_version != last_version:
                                            print(f"isolated daemon governed app_version={app_version}", flush=True)
                                            last_version = app_version
                                        if app_version == "28":
                                            return
                            except (OSError, urllib.error.URLError):
                                pass
                            time.sleep(0.2)
                        raise AssertionError("owned daemon did not reach expected governed readiness")

                    def reads(result):
                        return result.get("read_succeeded") and result.get("encrypted") and not result.get("vault_locked") and result.get("fixture_memory_found") and result.get("app_version") == "28"

                    def wait_snapshot(predicate, timeout):
                        end = time.monotonic() + timeout
                        result = {}
                        while time.monotonic() < end:
                            result = native.command("snapshot")
                            if predicate(result):
                                return result
                            time.sleep(0.05)
                        raise AssertionError(f"AppSession exceeded phase bound: phase={result.get('phase')}, has_api={result.get('has_api')}, ready_commands={result.get('ready_commands')}")

                    try:
                        for reservation in reservations:
                            reservation.close()
                        child = start()
                        ready(child)
                        self.record("isolated daemon reaches governed app-v28")
                        code, response = request("/v1/dashboard/settings/ledger/enable", {"passphrase": passphrase}, operator)
                        assert code == 200 and response["ok"] and response.get("recovery_key"), f"ledger enable failed: HTTP {code}"
                        del response  # Recovery material is never printed or persisted.
                        assert (home / "vault.key").is_file(), "ledger enable did not create disposable vault key"
                        self.record("production ledger endpoint enables encryption on disposable profile")
                        code, response = request("/v1/dashboard/tasks", {"content": CONTENT, "domain": "general"}, operator)
                        assert code == 201, f"encrypted fixture task creation failed: HTTP {code}, {response.get('diagnostic', 'unexpected response')}"
                        memory_id = response["memory_id"]
                        stop(child)
                        child = None
                        child = start()
                        ready(child, encrypted=True)
                        output.flush()
                        assert "Synaptic Ledger locked (writes blocked until unlock via CEREBRUM)" in log.read_text(), "encrypted restart did not report a locked vault"
                        code, health = request("/v1/dashboard/health")
                        assert code == 200 and health["encrypted"] and health["vault_locked"], "public health did not confirm an actually locked encrypted vault"
                        assert request("/v1/dashboard/memory/list")[0] == 401, "locked dashboard accepted an anonymous memory read"
                        self.record("encrypted daemon starts with locked vault and protected dashboard")
                        native = Probe(self.probe, home, environment, errors)
                        native.command("start")
                        initial = wait_snapshot(lambda item: item["phase"] == "locked" and item["has_api"], 8)
                        assert initial["phase"] == "locked" and not initial["ready_commands"] and initial["generation"], "initial native session did not fail closed"
                        self.record("production AppSession discovers locked encrypted daemon through AF_UNIX")
                        wrong = native.command("login", passphrase="incorrect-disposable-passphrase")
                        assert wrong["phase"] == "locked" and wrong["login_error"] and not wrong["ready_commands"], "wrong passphrase did not leave native session locked with an error"
                        assert not native.command("read")["read_succeeded"], "wrong passphrase allowed a protected read"
                        self.record("wrong passphrase cannot expose protected API reads or ready commands")
                        wait_snapshot(lambda item: item["phase"] == "locked" and item["has_api"], 8)
                        unlocked = native.command("login", passphrase=passphrase)
                        assert unlocked["phase"] == "ready" and unlocked["passphrase_empty"] and not unlocked["login_error"], "correct passphrase did not unlock native session and clear credentials"
                        deadline = time.monotonic() + 20
                        while not reads(native.command("read", memory_id=memory_id, content=CONTENT)):
                            assert time.monotonic() < deadline, "AppSession did not decrypt its governed fixture memory"
                            time.sleep(0.1)
                        self.record("correct native login opens actual vault and decodes encrypted fixture memory")
                        assert request("/v1/dashboard/memory/list")[0] == 401, "native cookie escaped its private Foundation session"
                        self.record("native session cookie remains private to its Foundation session")
                        assert request("/v1/dashboard/auth/login", {"passphrase": passphrase}, operator)[0] == 200, "independent operator could not authenticate"
                        native.command("route", route="search")
                        native.command("retain-client")
                        native.command("lock")
                        locked = wait_snapshot(lambda item: item["phase"] == "locked" and item["has_api"], 8)
                        assert locked["phase"] == "locked" and not locked["ready_commands"], "native lock left commands accessible"
                        assert not native.command("retained-read")["read_succeeded"], "native lock did not invalidate old client"
                        self.record("native lock revokes prior client and closes ready commands")
                        code, health = request("/v1/dashboard/health", client=operator)
                        assert code == 200 and health["encrypted"] and not health["vault_locked"], "session lock disrupted independently authenticated shared vault access"
                        assert request("/v1/dashboard/memory/list", client=operator)[0] == 200, "session lock invalidated an independent operator session"
                        self.record("session lock preserves independently authenticated shared vault access")
                        relogin = native.command("login", passphrase=passphrase)
                        assert relogin["phase"] == "ready" and relogin["route"] == "search", "relogin lost readiness or allowed route"
                        assert reads(native.command("read", memory_id=memory_id, content=CONTENT)), "relogin did not restore encrypted fixture read"
                        self.record("same-generation relogin restores allowed route and protected reads")
                        assert not native.command("retained-read")["read_succeeded"], "retired client inherited relogin credentials"
                        self.record("revoked client cannot inherit relogin credentials")
                        before = native.command("retain-client")
                        started = time.monotonic()
                        stop(child)
                        child = None
                        disconnected = wait_snapshot(lambda item: item["phase"] != "ready" and not item["has_api"], DISCONNECT_BOUND_SECONDS)
                        elapsed = time.monotonic() - started
                        assert elapsed < DISCONNECT_BOUND_SECONDS and not disconnected["ready_commands"], "daemon stop exceeded native disconnect bound"
                        self.record("daemon stop clears protected AppSession API within disclosed bound",
                                    elapsed_seconds=round(elapsed, 3), bound_seconds=DISCONNECT_BOUND_SECONDS)
                        assert not native.command("retained-read")["read_succeeded"], "stopped-generation client allowed a protected read"
                        self.record("stopped generation client cannot perform protected reads")
                        child = start()
                        ready(child, encrypted=True)
                        restarted = wait_snapshot(lambda item: item["phase"] == "locked" and item["generation"] != before["generation"], 8)
                        assert restarted["generation"] and restarted["route"] == "search" and restarted["epoch"] > before["epoch"], "restart lost generation fence or allowed route"
                        assert not restarted["ready_commands"], "restart exposed ready commands before fresh authentication"
                        self.record("fresh daemon generation automatically reconnects locked and preserves allowed route")
                        assert request("/v1/dashboard/memory/list", client=operator)[0] == 401, "restarted server accepted an old generation cookie"
                        self.record("old daemon cookie is rejected by restarted production server")
                        assert not native.command("read")["read_succeeded"], "restarted native session read protected data before fresh authentication"
                        self.record("restarted AppSession cannot read protected data before fresh authentication")
                        wait_snapshot(lambda item: item["phase"] == "locked" and item["has_api"], 8)
                        fresh = native.command("login", passphrase=passphrase)
                        assert fresh["phase"] == "ready" and fresh["route"] == "search" and fresh["passphrase_empty"], "fresh authentication did not restore readiness and clear credentials"
                        self.record("fresh native authentication restores readiness after daemon restart")
                        assert not native.command("retained-read")["read_succeeded"], "old generation client inherited fresh session authentication"
                        self.record("old generation client remains invalid after new session authentication")
                        assert reads(native.command("read", memory_id=memory_id, content=CONTENT)), "encrypted canonical fixture memory was not readable after restart"
                        self.record("encrypted canonical fixture memory survives restart and native reconnect")
                        assert native.command("finish")["finished"], "native probe did not acknowledge completion"
                        native.child.wait(timeout=5)
                        assert native.child.returncode == 0, "native AppSession probe failed on completion"
                        assert len(self.results) == EXPECTED_ASSERTIONS, "qualification did not execute every expected case"
                    finally:
                        try:
                            if native is not None:
                                native.close()
                        finally:
                            stop(child)
            finally:
                for reservation in reservations:
                    reservation.close()


def main():
    if not __debug__:
        raise RuntimeError("qualification requires Python assertions; remove optimization flags")
    def terminate(_signal, _frame):
        raise SystemExit(143)  # Unwind owned profile/process cleanup.
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    qualification = Qualification(args.probe.resolve(), args.daemon.resolve(), args.evidence.resolve())
    result = dict(schema="sage.v12.native-session.1", completed=False, skipped=0,
                  expected_assertions=EXPECTED_ASSERTIONS, results=qualification.results,
                  qualification_scope="release production AppSession, AF_UNIX and Foundation against isolated encrypted daemon",
                  daemon_fixture="v119testfixture: governed delay=3 blocks, proposer cooldown=1 block; not production timing",
                  disconnect_bound_seconds=DISCONNECT_BOUND_SECONDS,
                  physical_hid=False, voiceover=False, installed_app=False,
                  probe_sha256=hashlib.sha256(args.probe.read_bytes()).hexdigest(),
                  daemon_sha256=hashlib.sha256(args.daemon.read_bytes()).hexdigest())
    try:
        qualification.run()
        result["completed"] = True
    except BaseException as error:
        result["failure"] = str(error)
        raise
    finally:
        (args.evidence / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
