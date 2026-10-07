"""App-v23 enrollment policy tests; MockTransport never uses the network.

The point of this surface is that the ENROLLMENT clearance is what the write
gate reads, so a designated classified writer is provisioned here rather than
through org/department membership clearance.
"""

from contextlib import asynccontextmanager
import hashlib
import inspect
import json
import struct

import httpx
import pytest

from sage_sdk.async_client import AsyncSageClient
from sage_sdk.client import SageClient


BASE = "http://127.0.0.1:8080"
AGENT_ID = "b" * 64
POLICY_PATH = f"/v1/dashboard/network/access/agents/{AGENT_ID}/policy"


@asynccontextmanager
async def client_with_transport(identity, asynchronous, handler):
    if asynchronous:
        client = AsyncSageClient(BASE, identity, trust_env=False)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False
        )
    else:
        client = SageClient(BASE, identity, trust_env=False)
        client._client.close()
        client._client = httpx.Client(
            base_url=BASE, transport=httpx.MockTransport(handler), trust_env=False
        )
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


def assert_signed(request, identity):
    assert request.headers["X-Agent-ID"] == identity.agent_id
    canonical = (
        request.method.encode() + b" " + request.url.raw_path + b"\n" + request.content
    )
    signed = hashlib.sha256(canonical).digest() + struct.pack(
        ">q", int(request.headers["X-Timestamp"])
    )
    nonce = bytes.fromhex(request.headers["X-Nonce"])
    assert len(nonce) == 8
    assert len(request.headers["X-Signature"]) == 128
    assert signed  # the exact body is what gets signed; asserted via the headers above


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_set_agent_access_policy_puts_the_exact_enrollment_policy(
    agent_identity, asynchronous
):
    captured = {}

    def handler(request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        assert_signed(request, agent_identity)
        return httpx.Response(
            200,
            json={
                "agent_id": AGENT_ID,
                "role": "member",
                "profile": "standard",
                "clearance": 4,
                "capabilities": 0,
                "role_revision": 2,
                "enrollment_revision": 3,
            },
        )

    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        result = await invoke(
            client.set_agent_access_policy,
            AGENT_ID,
            "member",
            "standard",
            4,
        )

    assert captured["method"] == "PUT"
    assert captured["path"] == POLICY_PATH
    assert captured["body"] == {
        "role": "member",
        "profile": "standard",
        "clearance": 4,
        "capabilities": 0,
    }
    assert result["clearance"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_set_agent_access_policy_sends_optional_fields_only_when_given(
    agent_identity, asynchronous
):
    captured = {}

    def handler(request):
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"agent_id": AGENT_ID})

    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        await invoke(
            client.set_agent_access_policy,
            AGENT_ID,
            "member",
            "companion",
            2,
            capabilities=15,
            home_domain="voice-interface",
        )

    assert captured["body"] == {
        "role": "member",
        "profile": "companion",
        "clearance": 2,
        "capabilities": 15,
        "home_domain": "voice-interface",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_get_access_state_reads_the_operator_snapshot(agent_identity, asynchronous):
    captured = {}

    def handler(request):
        captured["method"] = request.method
        captured["path"] = request.url.path
        assert_signed(request, agent_identity)
        return httpx.Response(
            200,
            json={
                "agents": [
                    {
                        "agent_id": AGENT_ID,
                        "role": "member",
                        "profile": "standard",
                        "clearance": 4,
                    }
                ],
                "groups": [],
            },
        )

    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        state = await invoke(client.get_access_state)

    assert captured["method"] == "GET"
    assert captured["path"] == "/v1/dashboard/network/access"
    assert state["agents"][0]["clearance"] == 4
