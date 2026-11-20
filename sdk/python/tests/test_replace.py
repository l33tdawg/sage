import json

import httpx
import pytest
import pytest_asyncio
import respx

BASE_URL = "http://localhost:8080"
OLD = "11111111-1111-4111-8111-111111111111"
NEW = "22222222-2222-4222-8222-222222222222"


def record(memory_id, status, domain="maze", content_hash="ab" * 32, classification=2):
    return {
        "memory_id": memory_id,
        "submitting_agent": "a" * 64,
        "content": "lesson",
        "content_hash": content_hash,
        "memory_type": "observation",
        "domain_tag": domain,
        "confidence_score": 0.8,
        "status": status,
        "classification": classification,
        "created_at": "2026-11-20T10:00:00Z",
    }


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as m:
        m.get("/v1/governance/context").mock(
            return_value=httpx.Response(200, json={
                "validator_id": "validator-a",
                "governance_domain": "sage.governance",
                "app_v20_active": True,
            })
        )
        yield m


@pytest.fixture
def client(agent_identity, monkeypatch):
    import sage_sdk.client as mod
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    return mod.SageClient(base_url=BASE_URL, identity=agent_identity)


def wire(mock_api, new_statuses, challenge_response=None, old_status="committed"):
    mock_api.get(f"/v1/memory/{OLD}").mock(return_value=httpx.Response(200, json=record(OLD, old_status)))
    mock_api.get(f"/v1/memory/{NEW}").mock(
        side_effect=[httpx.Response(200, json=record(NEW, s)) for s in new_statuses]
    )
    submit = mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json={"memory_id": NEW, "tx_hash": "TX", "status": "committed"})
    )
    challenge = mock_api.post(f"/v1/memory/{OLD}/challenge").mock(
        return_value=challenge_response or httpx.Response(200, json={"tx_hash": "CH", "status": "deprecated"})
    )
    return submit, challenge


def test_replace_commits_first_then_challenges(client, mock_api):
    submit, challenge = wire(mock_api, ["proposed", "committed"])
    out = client.replace(OLD, "better lesson", 0.9)

    assert out["correction_status"] == "completed"
    assert out["memory_id"] == NEW and out["replaces_memory_id"] == OLD
    assert out["old_memory_status"] == "deprecated"
    body = json.loads(submit.calls.last.request.read())
    assert body["domain_tag"] == "maze"
    assert body["memory_type"] == "observation"
    assert body["parent_hash"] == "ab" * 32
    assert body["classification"] == 2
    assert json.loads(challenge.calls.last.request.read())["reason"] == f"replaced by corrected memory {NEW}"


def test_replace_never_challenges_before_commit(client, mock_api):
    _, challenge = wire(mock_api, ["proposed"] * 10)
    out = client.replace(OLD, "better lesson", 0.9)

    assert out["correction_status"] == "replacement_pending"
    assert out["old_memory_status"] == "unchanged"
    assert not challenge.called


def test_replace_reports_retained_old_when_challenge_fails(client, mock_api):
    wire(mock_api, ["committed"], challenge_response=httpx.Response(403, json={"title": "Forbidden", "detail": "no"}))
    out = client.replace(OLD, "better lesson", 0.9)

    assert out["correction_status"] == "replacement_committed_old_retained"
    assert out["replacement_status"] == "committed"
    assert out["old_memory_status"] == "unchanged"


def test_replace_refuses_dead_or_cross_domain_memory(client, mock_api):
    submit, _ = wire(mock_api, ["committed"], old_status="deprecated")
    with pytest.raises(ValueError, match="still be live"):
        client.replace(OLD, "x", 0.9)
    assert not submit.called

    mock_api.get(f"/v1/memory/{OLD}").mock(return_value=httpx.Response(200, json=record(OLD, "committed")))
    with pytest.raises(ValueError, match="original domain"):
        client.replace(OLD, "x", 0.9, domain_tag="elsewhere")
    assert not submit.called


def test_replace_indeterminate_submit_leaves_old(client, mock_api):
    mock_api.get(f"/v1/memory/{OLD}").mock(return_value=httpx.Response(200, json=record(OLD, "committed")))
    mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(202, json={"tx_hash": "TX", "status": "indeterminate", "retryable": False})
    )
    challenge = mock_api.post(f"/v1/memory/{OLD}/challenge").mock(return_value=httpx.Response(200, json={}))
    out = client.replace(OLD, "x", 0.9)
    assert out["correction_status"] == "replacement_pending"
    assert out["memory_id"] is None
    assert not challenge.called


def test_query_and_hybrid_send_not_tags(client, mock_api):
    empty = {"results": [], "total_count": 0}
    q = mock_api.post("/v1/memory/query").mock(return_value=httpx.Response(200, json=empty))
    h = mock_api.post("/v1/memory/hybrid").mock(return_value=httpx.Response(200, json=empty))
    client.query(embedding=[0.1], tags=["insight"], not_tags=["gen:1"])
    client.hybrid(query="x", embedding=[0.1], not_tags=["gen:1"])
    assert json.loads(q.calls.last.request.read())["not_tags"] == ["gen:1"]
    assert json.loads(h.calls.last.request.read())["not_tags"] == ["gen:1"]
    client.query(embedding=[0.1])
    assert "not_tags" not in json.loads(q.calls.last.request.read())


@pytest_asyncio.fixture
async def async_client(agent_identity, monkeypatch):
    import sage_sdk.async_client as mod

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(mod.asyncio, "sleep", no_sleep)
    c = mod.AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    yield c
    await c.close()


@pytest.mark.asyncio
async def test_async_replace_commits_first_then_challenges(async_client, mock_api):
    _, challenge = wire(mock_api, ["proposed", "committed"])
    out = await async_client.replace(OLD, "better lesson", 0.9)
    assert out["correction_status"] == "completed"
    assert challenge.called


@pytest.mark.asyncio
async def test_async_replace_never_challenges_before_commit(async_client, mock_api):
    _, challenge = wire(mock_api, ["proposed"] * 10)
    out = await async_client.replace(OLD, "better lesson", 0.9)
    assert out["correction_status"] == "replacement_pending"
    assert not challenge.called
