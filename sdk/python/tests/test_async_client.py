import pytest
import pytest_asyncio
import httpx
import respx

BASE_URL = "http://localhost:8080"


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as respx_mock:
        respx_mock.get(
            "/v1/governance/context", name="governance_context"
        ).mock(
            return_value=httpx.Response(200, json={
                "validator_id": "validator-a",
                "governance_domain": "sage.governance",
                "app_v20_active": True,
            })
        )
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
        "drain": {
            "pending_ballot_count": 1,
            "pending_memory_ids": ["memory-a"],
            "blocking_validator_ids": ["validator-a"],
        },
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
    assert listed.scopes[0].drain.blocking_validator_ids == ["validator-a"]
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
    assert body["validator_id"] == "validator-a"
    assert body["governance_domain"] == "sage.governance"
    assert body["scope"]["domains"] == ["research"]
    assert "payload" not in body


@pytest.mark.asyncio
async def test_governance_vote_and_cancel_include_fetched_context(async_client, mock_api):
    import json

    vote = mock_api.post("/v1/governance/vote").mock(
        return_value=httpx.Response(200, json={"tx_hash": "vote-tx", "status": "accepted"})
    )
    cancel = mock_api.post("/v1/governance/cancel").mock(
        return_value=httpx.Response(200, json={"tx_hash": "cancel-tx", "status": "cancelled"})
    )

    await async_client.governance_vote("proposal-1", "accept")
    await async_client.governance_cancel("proposal-1")

    for route in (vote, cancel):
        body = json.loads(route.calls.last.request.read())
        assert body["validator_id"] == "validator-a"
        assert body["governance_domain"] == "sage.governance"
    assert mock_api["governance_context"].call_count == 2


@pytest.mark.asyncio
async def test_governance_context_404_preserves_pre_v20_body(async_client, mock_api):
    import json

    mock_api["governance_context"].mock(
        return_value=httpx.Response(404, text="404 page not found")
    )
    route = mock_api.post("/v1/governance/cancel").mock(
        return_value=httpx.Response(200, json={"tx_hash": "cancel-tx", "status": "cancelled"})
    )

    await async_client.governance_cancel("proposal-1")
    body = json.loads(route.calls.last.request.read())
    assert "validator_id" not in body
    assert "governance_domain" not in body


@pytest.mark.asyncio
async def test_inactive_governance_context_preserves_pre_v20_body(async_client, mock_api):
    import json

    mock_api["governance_context"].mock(
        return_value=httpx.Response(200, json={
            "validator_id": "validator-a",
            "governance_domain": "",
            "app_v20_active": False,
        })
    )
    route = mock_api.post("/v1/governance/vote").mock(
        return_value=httpx.Response(200, json={"tx_hash": "vote-tx", "status": "accepted"})
    )

    await async_client.governance_vote("proposal-1", "accept")
    body = json.loads(route.calls.last.request.read())
    assert "validator_id" not in body
    assert "governance_domain" not in body


@pytest.mark.asyncio
async def test_federated_pipe_resolve_send_and_result_binding(async_client, mock_api):
    import json

    agent_id = "cd" * 32
    mock_api.post("/v1/pipe/resolve").mock(
        return_value=httpx.Response(200, json={
            "to_agent": agent_id,
            "to_provider": "",
            "source_chain_id": "local-sage",
            "destination_chain_id": "amy-sage",
        })
    )
    send = mock_api.post("/v1/pipe/send").mock(
        return_value=httpx.Response(201, json={
            "pipe_id": "sent-async", "status": "pending",
            "expires_at": "2026-07-19T00:00:00Z", "destination_chain_id": "amy-sage",
        })
    )
    target = await async_client.pipe_resolve("#amy/cdcdcdcd")
    sent = await async_client.pipe_send(
        "review this",
        to_agent=target.to_agent,
        source_chain_id=target.source_chain_id,
        destination_chain_id=target.destination_chain_id,
    )
    send_body = json.loads(send.calls.last.request.read())
    assert send_body["to_agent"] == agent_id
    assert send_body["source_chain_id"] == "local-sage"
    assert send_body["destination_chain_id"] == "amy-sage"
    assert sent.destination_chain_id == "amy-sage"

    mock_api.get("/v1/pipe/incoming-async").mock(
        return_value=httpx.Response(200, json={
            "pipe_id": "incoming-async", "status": "claimed",
            "source_chain_id": "amy-sage", "source_pipe_id": "remote-event-async",
            "reply_source_chain_id": "local-sage",
        })
    )
    result_route = mock_api.put("/v1/pipe/incoming-async/result").mock(
        return_value=httpx.Response(200, json={"status": "completed", "journal_id": "", "journaled": False})
    )
    completed = await async_client.pipe_result("incoming-async", "done")
    result_body = json.loads(result_route.calls.last.request.read())
    assert result_body == {
        "result": "done",
        "source_pipe_id": "remote-event-async",
        "source_chain_id": "local-sage",
    }
    assert completed.journaled is False

    mock_api.get("/v1/pipe/updates").mock(
        return_value=httpx.Response(200, json={
            "items": [{
                "event_id": "failed-async", "pipe_id": "incoming-async", "event_kind": "result",
                "remote_chain_id": "amy-sage", "target_agent_id": agent_id,
                "state": "failed", "attempts": 4, "last_error": "peer unavailable",
            }],
            "count": 1,
        })
    )
    updates = await async_client.pipe_updates()
    assert updates.items[0].event_kind == "result"
    assert updates.items[0].last_error == "peer unavailable"


@pytest.mark.asyncio
async def test_empty_pipe_collections_tolerate_legacy_null(async_client, mock_api):
    mock_api.get("/v1/pipe/inbox").mock(
        return_value=httpx.Response(200, json={"items": None, "count": 0})
    )
    mock_api.get("/v1/pipe/results").mock(
        return_value=httpx.Response(200, json={"items": None, "count": 0})
    )
    mock_api.get("/v1/pipe/updates").mock(
        return_value=httpx.Response(200, json={"items": None, "count": 0})
    )
    assert (await async_client.pipe_inbox()).items == []
    assert (await async_client.pipe_results()).items == []
    assert (await async_client.pipe_updates()).items == []
