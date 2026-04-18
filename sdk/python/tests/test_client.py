import pytest
import httpx
import respx

BASE_URL = "http://localhost:8080"


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as respx_mock:
        yield respx_mock


@pytest.fixture
def client(agent_identity):
    from sage_sdk.client import SageClient
    return SageClient(base_url=BASE_URL, identity=agent_identity)


def test_propose_memory(client, mock_api, sample_submit_response):
    mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    result = client.propose(
        content="Test memory",
        memory_type="fact",
        domain_tag="crypto",
        confidence=0.8,
    )
    assert result.memory_id == sample_submit_response["memory_id"]


def test_query_memories(client, mock_api, sample_query_response):
    mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    result = client.query(embedding=[0.1] * 768, domain_tag="crypto")
    assert len(result.results) == 1


def test_get_memory(client, mock_api, sample_memory):
    memory_id = sample_memory["memory_id"]
    mock_api.get(f"/v1/memory/{memory_id}").mock(
        return_value=httpx.Response(200, json=sample_memory)
    )
    result = client.get_memory(memory_id)
    assert result.memory_id == memory_id


def test_vote(client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    mock_api.post(f"/v1/memory/{memory_id}/vote").mock(
        return_value=httpx.Response(200, json={"message": "vote recorded", "vote_id": "1"})
    )
    result = client.vote(memory_id, "accept", rationale="Verified")
    assert result["message"] == "vote recorded"


def test_forget_with_reason(client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/forget").mock(
        return_value=httpx.Response(200, json={"message": "Memory forgotten.", "tx_hash": "FORGETHASH"})
    )
    result = client.forget(memory_id, reason="duplicate")
    assert result["tx_hash"] == "FORGETHASH"
    assert route.calls.last.request.read() == b'{"reason":"duplicate"}'


def test_forget_without_reason(client, mock_api):
    # Caller can omit reason; server substitutes a default. SDK sends empty body.
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/forget").mock(
        return_value=httpx.Response(200, json={"message": "Memory forgotten.", "tx_hash": "FORGETHASH2"})
    )
    result = client.forget(memory_id)
    assert result["tx_hash"] == "FORGETHASH2"
    assert route.calls.last.request.read() == b'{}'


def test_error_handling(client, mock_api, sample_error_response):
    from sage_sdk.exceptions import SageNotFoundError
    mock_api.get("/v1/memory/nonexistent").mock(
        return_value=httpx.Response(404, json=sample_error_response)
    )
    with pytest.raises(SageNotFoundError):
        client.get_memory("nonexistent")


def test_context_manager(agent_identity, mock_api):
    from sage_sdk.client import SageClient
    mock_api.get("/health").mock(
        return_value=httpx.Response(200, json={"status": "healthy"})
    )
    with SageClient(base_url=BASE_URL, identity=agent_identity) as client:
        pass  # Just verify context manager works
