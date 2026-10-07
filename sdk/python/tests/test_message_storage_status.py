"""Storage metadata contract tests using synthetic identities and HTTP mocks."""

import hashlib
import json
import struct

from nacl.signing import VerifyKey
import pytest
from pydantic import ValidationError
import respx

from sage_sdk.async_client import AsyncSageClient
from sage_sdk.client import SageClient
from sage_sdk.exceptions import SageNotFoundError
from sage_sdk.models import MessageStorageStatus


BASE_URL = "http://localhost:8080"
PATH = "/v1/messages/storage"


def storage_status(agent_id="a" * 64):
    return {
        "schema": "sage.message-storage.v1",
        "instance_id": "b" * 64,
        "agent_id": agent_id,
        "encryption_expected": True,
        "vault_active": True,
        "vault_generation": "18446744073709551615",
        "stable": True,
        "canonical_send_idempotency": True,
        "encrypted_send_admission": True,
    }


def verify_signed_request(request, agent_id):
    assert request.method == "GET"
    assert str(request.url) == BASE_URL + PATH
    assert request.content == b""
    assert request.headers["X-Agent-ID"] == agent_id
    nonce = bytes.fromhex(request.headers["X-Nonce"])
    assert len(nonce) == 8
    canonical = hashlib.sha256(b"GET " + PATH.encode() + b"\n").digest()
    signed = canonical + struct.pack(">q", int(request.headers["X-Timestamp"])) + nonce
    VerifyKey(bytes.fromhex(agent_id)).verify(signed, bytes.fromhex(request.headers["X-Signature"]))


@respx.mock
def test_sync_signed_get_and_fresh_nonce(agent_identity):
    payload = storage_status(agent_identity.agent_id)
    route = respx.get(BASE_URL + PATH).respond(200, json=payload)
    client = SageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        for _ in range(2):
            result = client.message_storage_status()
            assert type(result) is MessageStorageStatus
            assert result.model_dump() == payload
    finally:
        client.__exit__(None, None, None)
    assert route.call_count == 2
    for call in route.calls:
        verify_signed_request(call.request, agent_identity.agent_id)
    assert route.calls[0].request.headers["X-Nonce"] != route.calls[1].request.headers["X-Nonce"]


@pytest.mark.asyncio
@respx.mock
async def test_async_signed_get_and_fresh_nonce(agent_identity):
    payload = storage_status(agent_identity.agent_id)
    route = respx.get(BASE_URL + PATH).respond(200, json=payload)
    client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        for _ in range(2):
            result = await client.message_storage_status()
            assert type(result) is MessageStorageStatus
            assert result.model_dump() == payload
    finally:
        await client.close()
    assert route.call_count == 2
    for call in route.calls:
        verify_signed_request(call.request, agent_identity.agent_id)
    assert route.calls[0].request.headers["X-Nonce"] != route.calls[1].request.headers["X-Nonce"]


@pytest.mark.parametrize("generation", ["0", "1", "18446744073709551615"])
def test_uint64_boundary_strings_preserved(generation):
    result = MessageStorageStatus.model_validate({**storage_status(), "vault_generation": generation})
    assert result.vault_generation == generation


@pytest.mark.parametrize("field", list(storage_status()))
def test_every_field_required(field):
    payload = storage_status()
    del payload[field]
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate(payload)


@pytest.mark.parametrize("field", ["encryption_expected", "vault_active", "stable", "canonical_send_idempotency", "encrypted_send_admission"])
@pytest.mark.parametrize("value", [None, 0, 1, "true", "false", [], {}])
def test_boolean_coercion_forbidden(field, value):
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate({**storage_status(), field: value})


@pytest.mark.parametrize("field", ["instance_id", "agent_id"])
@pytest.mark.parametrize("value", [None, 123, b"a" * 64, "a" * 63, "a" * 65, "A" * 64, "g" * 64, "a" * 64 + "\n"])
def test_identity_strings_strict_lowercase_hex(field, value):
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate({**storage_status(), field: value})


@pytest.mark.parametrize("value", [None, 0, 1, True, 1.0, b"1", "", "00", "01", "+1", "-1",
                                   "1.0", "1e3", " 1", "1 ", "1\n", "١", "18446744073709551616", "9" * 1000])
def test_generation_rejects_noncanonical_or_overflow(value):
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate({**storage_status(), "vault_generation": value})


@pytest.mark.parametrize("value", [None, "sage.message-storage.v2", b"sage.message-storage.v1", True])
def test_schema_is_exact(value):
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate({**storage_status(), "schema": value})


def test_unknown_fields_and_aliases_rejected():
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate({**storage_status(), "ready": True})
    payload = storage_status()
    payload["vaultGeneration"] = payload.pop("vault_generation")
    with pytest.raises(ValidationError):
        MessageStorageStatus.model_validate(payload)


def test_false_flags_preserved_without_activation_inference():
    payload = storage_status()
    for field in ("encryption_expected", "vault_active", "stable", "canonical_send_idempotency", "encrypted_send_admission"):
        payload[field] = False
    assert MessageStorageStatus.model_validate(payload).model_dump() == payload


@respx.mock
def test_sync_404_raises_without_retry_or_fabricated_status(agent_identity):
    route = respx.get(BASE_URL + PATH).respond(404, json={"detail": "unavailable"})
    client = SageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        with pytest.raises(SageNotFoundError):
            client.message_storage_status()
    finally:
        client.__exit__(None, None, None)
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_async_404_raises_without_retry_or_fabricated_status(agent_identity):
    route = respx.get(BASE_URL + PATH).respond(404, json={"detail": "unavailable"})
    client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        with pytest.raises(SageNotFoundError):
            await client.message_storage_status()
    finally:
        await client.close()
    assert route.call_count == 1


@respx.mock
def test_sync_invalid_success_response_raises(agent_identity):
    route = respx.get(BASE_URL + PATH).respond(200, json={**storage_status(), "stable": 1})
    client = SageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        with pytest.raises(ValidationError):
            client.message_storage_status()
    finally:
        client.__exit__(None, None, None)
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_async_invalid_success_response_raises(agent_identity):
    route = respx.get(BASE_URL + PATH).respond(200, json={**storage_status(), "vault_generation": 1})
    client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    try:
        with pytest.raises(ValidationError):
            await client.message_storage_status()
    finally:
        await client.close()
    assert route.call_count == 1


@pytest.mark.parametrize("options,expected", [({}, True), ({"trust_env": True}, True), ({"trust_env": False}, False)])
def test_sync_trust_env_reaches_httpx_client(agent_identity, options, expected):
    with SageClient(BASE_URL, agent_identity, **options) as client:
        assert client._client.trust_env is expected


@pytest.mark.asyncio
@pytest.mark.parametrize("options,expected", [({}, True), ({"trust_env": True}, True), ({"trust_env": False}, False)])
async def test_async_trust_env_reaches_httpx_client(agent_identity, options, expected):
    async with AsyncSageClient(BASE_URL, agent_identity, **options) as client:
        assert client._client.trust_env is expected


@pytest.mark.parametrize("client_type", [SageClient, AsyncSageClient])
@pytest.mark.parametrize("value", [None, 0, 1, "false", [], {}])
def test_trust_env_rejects_non_boolean_before_client_creation(agent_identity, client_type, value):
    with pytest.raises(TypeError, match="^trust_env must be a bool$"):
        client_type(BASE_URL, agent_identity, trust_env=value)


@pytest.mark.parametrize("client_type", [SageClient, AsyncSageClient])
def test_trust_env_is_keyword_only(agent_identity, client_type):
    with pytest.raises(TypeError):
        client_type(BASE_URL, agent_identity, 30.0, None, False)


def assert_send_body_and_signature(request, identity, required):
    expected = {"to_agent": "b" * 64, "payload": "synthetic payload", "idempotency_key": "synthetic-request"}
    if required:
        expected["require_encrypted_storage"] = True
    assert json.loads(request.content) == expected
    canonical = hashlib.sha256(b"POST /v1/messages\n" + request.content).digest()
    signed = canonical + struct.pack(">q", int(request.headers["X-Timestamp"])) + bytes.fromhex(request.headers["X-Nonce"])
    VerifyKey(bytes.fromhex(identity.agent_id)).verify(signed, bytes.fromhex(request.headers["X-Signature"]))


@pytest.mark.parametrize("options", [{}, {"require_encrypted_storage": False}, {"require_encrypted_storage": True}])
@respx.mock
def test_sync_encrypted_send_flag_signed_and_default_omitted(agent_identity, options):
    route = respx.post(BASE_URL + "/v1/messages").respond(200, json={
        "message_id": "synthetic-message", "status": "pending", "expires_at": "2026-09-07T00:00:00Z"})
    with SageClient(BASE_URL, agent_identity) as client:
        client.message_send("b" * 64, "synthetic payload", "synthetic-request", **options)
    assert route.call_count == 1
    assert_send_body_and_signature(route.calls[0].request, agent_identity, options.get("require_encrypted_storage", False))


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{}, {"require_encrypted_storage": False}, {"require_encrypted_storage": True}])
@respx.mock
async def test_async_encrypted_send_flag_signed_and_default_omitted(agent_identity, options):
    route = respx.post(BASE_URL + "/v1/messages").respond(200, json={
        "message_id": "synthetic-message", "status": "pending", "expires_at": "2026-09-07T00:00:00Z"})
    async with AsyncSageClient(BASE_URL, agent_identity) as client:
        await client.message_send("b" * 64, "synthetic payload", "synthetic-request", **options)
    assert route.call_count == 1
    assert_send_body_and_signature(route.calls[0].request, agent_identity, options.get("require_encrypted_storage", False))


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
@respx.mock
def test_sync_encrypted_send_flag_rejects_nonbool_without_http(agent_identity, value):
    with SageClient(BASE_URL, agent_identity) as client:
        with pytest.raises(TypeError, match="^require_encrypted_storage must be a bool$"):
            client.message_send("b" * 64, "synthetic payload", "synthetic-request", require_encrypted_storage=value)
    assert len(respx.calls) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
@respx.mock
async def test_async_encrypted_send_flag_rejects_nonbool_without_http(agent_identity, value):
    async with AsyncSageClient(BASE_URL, agent_identity) as client:
        with pytest.raises(TypeError, match="^require_encrypted_storage must be a bool$"):
            await client.message_send("b" * 64, "synthetic payload", "synthetic-request", require_encrypted_storage=value)
    assert len(respx.calls) == 0


def assert_pipe_request(request, identity, options):
    expected = {"payload": "synthetic payload", "to_agent": "b" * 64,
                "source_chain_id": "chain-a", "destination_chain_id": "chain-b"}
    if options.get("idempotency_key") is not None:
        expected["idempotency_key"] = options["idempotency_key"]
    assert request.method == "POST"
    assert str(request.url) == BASE_URL + "/v1/pipe/send"
    assert json.loads(request.content) == expected
    canonical = hashlib.sha256(b"POST /v1/pipe/send\n" + request.content).digest()
    signed = canonical + struct.pack(">q", int(request.headers["X-Timestamp"])) + bytes.fromhex(request.headers["X-Nonce"])
    VerifyKey(bytes.fromhex(identity.agent_id)).verify(signed, bytes.fromhex(request.headers["X-Signature"]))


@pytest.mark.parametrize("options", [{}, {"idempotency_key": None}, {"idempotency_key": "request-1"},
                                     {"idempotency_key": "é" * 128}])
@respx.mock
def test_sync_pipe_idempotency_key_signed_or_legacy_omitted(agent_identity, options):
    route = respx.post(BASE_URL + "/v1/pipe/send").respond(200, json={
        "pipe_id": "synthetic-pipe", "status": "pending", "expires_at": "2026-09-07T00:00:00Z"})
    with SageClient(BASE_URL, agent_identity) as client:
        result = client.pipe_send("synthetic payload", to_agent="b" * 64, source_chain_id="chain-a",
                                  destination_chain_id="chain-b", **options)
    assert result.pipe_id == "synthetic-pipe"
    assert route.call_count == 1
    assert_pipe_request(route.calls[0].request, agent_identity, options)


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{}, {"idempotency_key": None}, {"idempotency_key": "request-1"},
                                     {"idempotency_key": "é" * 128}])
@respx.mock
async def test_async_pipe_idempotency_key_signed_or_legacy_omitted(agent_identity, options):
    route = respx.post(BASE_URL + "/v1/pipe/send").respond(200, json={
        "pipe_id": "synthetic-pipe", "status": "pending", "expires_at": "2026-09-07T00:00:00Z"})
    async with AsyncSageClient(BASE_URL, agent_identity) as client:
        result = await client.pipe_send("synthetic payload", to_agent="b" * 64, source_chain_id="chain-a",
                                        destination_chain_id="chain-b", **options)
    assert result.pipe_id == "synthetic-pipe"
    assert route.call_count == 1
    assert_pipe_request(route.calls[0].request, agent_identity, options)


@pytest.mark.parametrize("value,error", [(True, TypeError), (1, TypeError), (b"key", TypeError),
    ([], TypeError), ("", ValueError), (" \t\n", ValueError), ("x" * 257, ValueError), ("é" * 129, ValueError)])
@respx.mock
def test_sync_pipe_invalid_key_never_sends(agent_identity, value, error):
    with SageClient(BASE_URL, agent_identity) as client:
        with pytest.raises(error):
            client.pipe_send("synthetic payload", to_agent="b" * 64, idempotency_key=value)
    assert len(respx.calls) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("value,error", [(True, TypeError), (1, TypeError), (b"key", TypeError),
    ([], TypeError), ("", ValueError), (" \t\n", ValueError), ("x" * 257, ValueError), ("é" * 129, ValueError)])
@respx.mock
async def test_async_pipe_invalid_key_never_sends(agent_identity, value, error):
    async with AsyncSageClient(BASE_URL, agent_identity) as client:
        with pytest.raises(error):
            await client.pipe_send("synthetic payload", to_agent="b" * 64, idempotency_key=value)
    assert len(respx.calls) == 0
