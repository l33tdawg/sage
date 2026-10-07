import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

spec = importlib.util.spec_from_file_location("wait_tx", Path(__file__).with_name("wait-comet-tx.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
HASH = "AB" * 32


def ok(**overrides):
    return {"result": {"hash": HASH, "height": "245", "tx_result": {"code": 0}, **overrides}}


def missing(data=None):
    return {"error": {"code": -32603, "message": "Internal error",
                      "data": data or f"tx ({HASH}) not found"}}


class Response(io.BytesIO):
    status = 200


class TxReadinessTests(unittest.TestCase):
    def invoke(self, replies, timeout=30):
        def request(url, timeout):
            self.assertEqual(url, f"http://127.0.0.1:37757/tx?hash=0x{HASH}&prove=false")
            self.assertGreater(timeout, 0)
            reply = replies.pop(0)
            if isinstance(reply, Exception):
                raise reply
            return Response(json.dumps(reply).encode())
        with patch.object(helper, "urlopen", side_effect=request) as fetch:
            result = helper.wait_tx(37757, HASH.lower(), timeout)
            return result, fetch.call_count

    def test_immediate_success(self):
        self.assertEqual(self.invoke([ok()]), (245, 1))

    def test_exact_http500_absence_then_success(self):
        error = HTTPError("local", 500, "Internal error", {}, io.BytesIO(json.dumps(missing()).encode()))
        self.assertEqual(self.invoke([error, ok()]), (245, 2))

    def test_json_rpc_absence_then_success(self):
        self.assertEqual(self.invoke([missing(), ok()]), (245, 2))

    def test_timeout_retains_diagnostic(self):
        with self.assertRaisesRegex(TimeoutError, "tx .* not found"):
            self.invoke([missing()], timeout=0.01)

    def test_nonretryable_errors(self):
        cases = [missing("transaction indexing is disabled"), missing("tx (OTHER) not found"),
                 ok(hash="CD" * 32), ok(height="0"), ok(height="-1"), ok(height=True),
                 ok(tx_result={"code": 1}), ok(tx_result={}), [],
                 HTTPError("local", 500, "Internal error", {}, io.BytesIO(b"upstream failure")),
                 HTTPError("local", 503, "Unavailable", {}, io.BytesIO(json.dumps(missing()).encode())),
                 URLError("connection refused")]
        for response in cases:
            with self.subTest(response=response):
                with self.assertRaises((ValueError, URLError)):
                    self.invoke([response])

    def test_invalid_hash_no_request(self):
        with patch.object(helper, "urlopen") as fetch:
            with self.assertRaises(ValueError):
                helper.wait_tx(37757, "BAD")
            fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
