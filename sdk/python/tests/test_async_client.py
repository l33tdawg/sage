import pytest
import pytest_asyncio
import httpx
import respx

BASE_URL = "http://localhost:8080"


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as respx_mock:
        yield respx_mock


@pytest_asyncio.fixture
async def async_client(agent_identity):
    from sage_sdk.async_client import AsyncSageClient
    client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_propose_memory(async_client, mock_api, sample_submit_response):
    mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    result = await async_client.propose(
        content="Test memory",
        memory_type="fact",
        domain_tag="crypto",
        confidence=0.8,
    )
    assert result.memory_id == sample_submit_response["memory_id"]


@pytest.mark.asyncio
async def test_query_memories(async_client, mock_api, sample_query_response):
    mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    result = await async_client.query(embedding=[0.1] * 768, domain_tag="crypto")
    assert len(result.results) == 1


@pytest.mark.asyncio
async def test_concurrent_queries(async_client, mock_api, sample_query_response):
    import asyncio
    mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    results = await asyncio.gather(
        async_client.query(embedding=[0.1] * 768),
        async_client.query(embedding=[0.2] * 768),
        async_client.query(embedding=[0.3] * 768),
    )
    assert len(results) == 3


@pytest.mark.asyncio
async def test_forget_with_reason(async_client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/forget").mock(
        return_value=httpx.Response(200, json={"message": "Memory forgotten.", "tx_hash": "FORGETHASH"})
    )
    result = await async_client.forget(memory_id, reason="duplicate")
    assert result["tx_hash"] == "FORGETHASH"
    assert route.calls.last.request.read() == b'{"reason":"duplicate"}'


@pytest.mark.asyncio
async def test_forget_without_reason(async_client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/forget").mock(
        return_value=httpx.Response(200, json={"message": "Memory forgotten.", "tx_hash": "FORGETHASH2"})
    )
    result = await async_client.forget(memory_id)
    assert result["tx_hash"] == "FORGETHASH2"
    assert route.calls.last.request.read() == b'{}'


@pytest.mark.asyncio
async def test_reinstate(async_client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/reinstate").mock(
        return_value=httpx.Response(200, json={"message": "Memory reinstated.", "tx_hash": "RESTOREHASH", "status": "committed"})
    )
    result = await async_client.reinstate(memory_id, reason="false alarm")
    assert result["status"] == "committed"
    assert route.calls.last.request.read() == b'{"reason":"false alarm"}'


@pytest.mark.asyncio
async def test_scope_read_surface(async_client, mock_api):
    record = {
        "scope_id": "scope-a",
        "revision": 2,
        "revision_hash": "ab" * 32,
        "state": "active",
        "controller_validator_id": "validator-a",
        "created_height": 10,
        "updated_height": 20,
        "domains": [{"name": "research", "subtree": False}],
        "members": [{
            "validator_id": "validator-a",
            "assigned_weight": 7,
            "joined_revision": 1,
            "active": True,
        }],
    }
    mock_api.get("/v1/scopes").mock(
        return_value=httpx.Response(200, json={"scopes": [record], "count": 1})
    )
    mock_api.get("/v1/scopes/scope-a").mock(
        return_value=httpx.Response(200, json=record)
    )
    # The client must keep a valid scope ID within one URL path segment.
    escaped = mock_api.get("/v1/scopes/scope%20a").mock(
        return_value=httpx.Response(200, json={**record, "scope_id": "scope a"})
    )

    listed = await async_client.list_scopes()
    assert listed.scopes[0].domains[0].name == "research"
    assert (await async_client.get_scope("scope-a")).state == "active"
    assert (await async_client.get_scope("scope a")).scope_id == "scope a"
    assert escaped.called


@pytest.mark.asyncio
async def test_governance_propose_scope_uses_guided_template(async_client, mock_api):
    import json

    route = mock_api.post("/v1/governance/propose").mock(
        return_value=httpx.Response(200, json={
            "proposal_id": "proposal-1", "tx_hash": "tx-1", "status": "voting",
        })
    )
    result = await async_client.governance_propose_scope(
        scope={
            "scope_id": "scope-a",
            "revision": 1,
            "state": "active",
            "controller_validator_id": "validator-a",
            "domains": ["research"],
            "members": [{"validator_id": "validator-a", "assigned_weight": 1}],
        },
        reason="form research quorum",
    )
    body = json.loads(route.calls.last.request.read())
    assert result.tx_hash == "tx-1"
    assert body["target_id"] == "scope-a"
    assert body["scope"]["domains"] == ["research"]
    assert "payload" not in body
