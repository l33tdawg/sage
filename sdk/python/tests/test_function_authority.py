"""The SDK must sign the complete chain-bound functional authority contract."""

import json

import httpx
import pytest
import respx

from sage_sdk.async_client import AsyncSageClient
from sage_sdk.client import SageClient


BASE_URL = "http://localhost:8080"
ACTION = {
    "action": "assign",
    "function": "example-producer",
    "domain": "example-records",
    "scope": "example-chain-scope",
    "subject": "a" * 64,
    "activation_height": 0,
    "expected_revision": 3,
    "expires_at": 1900000000,
}


def assert_signed_contract(route):
    request = route.calls.last.request
    assert json.loads(request.content) == ACTION
    assert request.headers["X-Agent-ID"]
    assert request.headers["X-Signature"]


@respx.mock
def test_function_authority_signs_complete_chain_bound_contract(agent_identity):
    route = respx.post(f"{BASE_URL}/v1/access/function-authority").mock(
        return_value=httpx.Response(200, json={"status": "committed"})
    )
    with SageClient(base_url=BASE_URL, identity=agent_identity) as client:
        assert client.set_function_authority(**ACTION) == {"status": "committed"}
    assert_signed_contract(route)


@pytest.mark.asyncio
@respx.mock
async def test_async_function_authority_signs_complete_chain_bound_contract(agent_identity):
    route = respx.post(f"{BASE_URL}/v1/access/function-authority").mock(
        return_value=httpx.Response(200, json={"status": "committed"})
    )
    async with AsyncSageClient(base_url=BASE_URL, identity=agent_identity) as client:
        assert await client.set_function_authority(**ACTION) == {"status": "committed"}
    assert_signed_contract(route)
