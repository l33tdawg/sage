"""Synthetic actor-bound workflow tests; MockTransport never uses the network."""

from contextlib import asynccontextmanager
import hashlib
import inspect
import json
import math
import struct

import httpx
from nacl.signing import VerifyKey
import pytest
from pydantic import ValidationError

from sage_sdk.async_client import AsyncSageClient
from sage_sdk.client import SageClient
from sage_sdk.exceptions import SageAPIError, SageNotFoundError
from sage_sdk.models import WorkflowJournalPage, WorkflowJournalRecord


BASE = "http://127.0.0.1:8080"
IDENTIFIER = "e837d957-c686-4e53-894f-c050e84a3ec8"
PATH = "/v1/workflows/" + IDENTIFIER


def record(actor="a" * 64):
    return dict(schema="sage.workflow-journal.v1", agent_id=actor, record_id=IDENTIFIER,
                revision=1, kind="mesh_outbound", payload={"text": "synthetic"}, trust="untrusted_auxiliary")


@asynccontextmanager
async def client_with_transport(identity, asynchronous, handler):
    if asynchronous:
        client = AsyncSageClient(BASE, identity, trust_env=False)
        await client._client.aclose()
        client._client = httpx.AsyncClient(base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False)
    else:
        client = SageClient(BASE, identity, trust_env=False)
        client._client.close()
        client._client = httpx.Client(base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False)
    try:
        yield client
    finally:
        if asynchronous:
            await client.close()
        else:
            client.__exit__(None, None, None)


async def invoke(method, *args, **kwargs):
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


def verify(request, identity):
    assert request.headers["X-Agent-ID"] == identity.agent_id
    canonical = request.method.encode() + b" " + request.url.raw_path + b"\n" + request.content
    signed = hashlib.sha256(canonical).digest() + struct.pack(">q", int(request.headers["X-Timestamp"]))
    nonce = bytes.fromhex(request.headers["X-Nonce"])
    assert len(nonce) == 8
    VerifyKey(bytes.fromhex(identity.agent_id)).verify(signed + nonce, bytes.fromhex(request.headers["X-Signature"]))


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_get_put_exact_signed_contract_and_fresh_nonce(agent_identity, asynchronous):
    calls = []
    def handler(request):
        calls.append(request)
        verify(request, agent_identity)
        assert str(request.url) == BASE + PATH
        if request.method == "GET":
            assert request.content == b""
        else:
            assert request.method == "PUT"
            assert json.loads(request.content) == dict(kind="mesh_outbound", expected_revision=0, payload={"text": "synthetic"})
        return httpx.Response(200, json=record(agent_identity.agent_id))
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        received = await invoke(client.workflow_get, IDENTIFIER)
        written = await invoke(client.workflow_put, IDENTIFIER, "mesh_outbound", 0, {"text": "synthetic"})
    assert received.model_dump() == written.model_dump() == record(agent_identity.agent_id)
    assert json.loads(received.model_dump_json()) == record(agent_identity.agent_id)
    assert len(calls) == 2
    assert calls[0].headers["X-Nonce"] != calls[1].headers["X-Nonce"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("status", [404, 409, 429, 503])
@pytest.mark.parametrize("method", ["workflow_get", "workflow_put"])
async def test_errors_propagate_without_retries_or_empty_state(agent_identity, asynchronous, status, method):
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"detail": "Workflow journal unavailable"})
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        with pytest.raises(SageAPIError) as caught:
            arguments = (IDENTIFIER,) if method == "workflow_get" else (IDENTIFIER, "mesh_inbound", 0, {})
            await invoke(getattr(client, method), *arguments)
    assert caught.value.status_code == status
    assert isinstance(caught.value, SageNotFoundError) is (status == 404)
    assert len(calls) == 1


@pytest.mark.parametrize("missing", list(record()))
def test_response_requires_every_field(missing):
    value = record()
    del value[missing]
    with pytest.raises(ValidationError):
        WorkflowJournalRecord.model_validate(value)


@pytest.mark.parametrize("field,value", [
    ("revision", True), ("revision", 1.0), ("revision", "1"), ("revision", None),
    ("revision", 0), ("revision", 9007199254740992), ("kind", "canonical_memory"),
    ("trust", "trusted"), ("schema", "sage.workflow-journal.v2"), ("agent_id", "A" * 64),
    ("agent_id", b"a" * 64), ("record_id", IDENTIFIER.upper()),
    ("record_id", "00000000-0000-0000-0000-000000000000"), ("extra", True),
    ("payload", {1: "bad-key"}), ("payload", {"number": math.nan}), ("payload", (1, 2)),
])
def test_response_rejects_invalid_fields(field, value):
    with pytest.raises(ValidationError):
        WorkflowJournalRecord.model_validate({**record(), field: value})


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bad_request_values_never_reach_transport(agent_identity, asynchronous):
    def handler(request):
        pytest.fail("invalid request reached transport")
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        for identifier in (None, True, "../secret", IDENTIFIER.upper(), "0" * 32):
            with pytest.raises((ValueError, TypeError)):
                await invoke(client.workflow_get, identifier)
        for revision in (None, True, 1.0, "0", -1, 9007199254740991):
            with pytest.raises((ValueError, TypeError)):
                await invoke(client.workflow_put, IDENTIFIER, "mesh_outbound", revision, {})
        for payload in ({1: "not-json-key"}, {"value": math.inf}, {"value": b"bytes"}, {"text": "x" * 16384}):
            with pytest.raises((ValueError, TypeError)):
                await invoke(client.workflow_put, IDENTIFIER, "mesh_outbound", 0, payload)
        with pytest.raises(ValueError):
            await invoke(client.workflow_put, IDENTIFIER, "memory", 0, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("variant", ["actor", "uuid", "duplicate", "unknown", "oversize", "nan"])
async def test_untrusted_or_malformed_response_rejected(agent_identity, asynchronous, variant):
    value = record(agent_identity.agent_id)
    if variant == "actor":
        value["agent_id"] = "c" * 64
    elif variant == "uuid":
        value["record_id"] = "9726bde5-5f3d-49f0-b420-33a16875b59c"
    elif variant == "unknown":
        value["accepted"] = True
    elif variant == "nan":
        value["payload"] = {"value": math.nan}
    raw = json.dumps(value).encode()
    if variant == "duplicate":
        raw = raw[:-1] + b',"revision":1}'
    elif variant == "oversize":
        raw = b" " * 131073
    async with client_with_transport(agent_identity, asynchronous, lambda request: httpx.Response(200, content=raw)) as client:
        with pytest.raises(ValueError):
            await invoke(client.workflow_get, IDENTIFIER)


def test_payload_and_revision_boundaries_and_schema_alias():
    value = record()
    value["revision"] = 9007199254740991
    value["payload"] = "x" * 16382
    assert WorkflowJournalRecord.model_validate(value).model_dump() == value
    value["payload"] += "x"
    with pytest.raises(ValidationError):
        WorkflowJournalRecord.model_validate(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_list_signed_query_and_exact_page_contract(agent_identity, asynchronous):
    after = "12dce032-a342-4bcc-912d-3884d1f0df09"
    expected = dict(schema="sage.workflow-journal.v1", items=[record(agent_identity.agent_id)], next_after=IDENTIFIER, has_more=True)
    calls = []
    def handler(request):
        calls.append(request)
        verify(request, agent_identity)
        assert request.url.raw_path == ("/v1/workflows?after=" + after + "&limit=1").encode()
        assert request.content == b""
        return httpx.Response(200, json=expected)
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        result = await invoke(client.workflow_list, after=after, limit=1)
    assert result.model_dump() == expected
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_list_default_query_and_invalid_arguments(agent_identity, asynchronous):
    calls = []
    def handler(request):
        calls.append(request)
        verify(request, agent_identity)
        assert request.url.raw_path == b"/v1/workflows?limit=20"
        return httpx.Response(200, json=dict(schema="sage.workflow-journal.v1", items=[], next_after=None, has_more=False))
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        assert (await invoke(client.workflow_list)).items == []
        for limit in (0, 51, True, "20", 20.0, None):
            with pytest.raises(ValueError):
                await invoke(client.workflow_list, limit=limit)
        for after in ("", "../secret", True, IDENTIFIER.upper()):
            with pytest.raises(ValueError):
                await invoke(client.workflow_list, after=after)
    assert len(calls) == 1


@pytest.mark.parametrize("change", [
    {"has_more": 1}, {"has_more": "false"}, {"next_after": IDENTIFIER}, {"has_more": True},
    {"items": [record(), record()]}, {"extra": True}, {"items": [record()] * 51},
])
def test_list_strict_model_rejects_malformed_page(change):
    value = dict(schema="sage.workflow-journal.v1", items=[], next_after=None, has_more=False)
    with pytest.raises(ValidationError):
        WorkflowJournalPage.model_validate({**value, **change})


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("variant", ["actor", "cursor", "404", "503"])
async def test_list_actor_cursor_and_error_boundaries(agent_identity, asynchronous, variant):
    calls = []
    def handler(request):
        calls.append(request)
        if variant in ("404", "503"):
            return httpx.Response(int(variant), json={"detail": "Unavailable"})
        item = record("c" * 64 if variant == "actor" else agent_identity.agent_id)
        return httpx.Response(200, json=dict(schema="sage.workflow-journal.v1", items=[item], next_after=None, has_more=False))
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        with pytest.raises((ValueError, SageAPIError)) as caught:
            await invoke(client.workflow_list, after=IDENTIFIER if variant == "cursor" else None)
    if variant in ("404", "503"):
        assert caught.value.status_code == int(variant)
    assert len(calls) == 1
    value = record()
    value["schema_"] = value.pop("schema")
    with pytest.raises(ValidationError):
        WorkflowJournalRecord.model_validate(value)
