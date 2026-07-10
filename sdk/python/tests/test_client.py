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


def test_propose_memory_with_tags(client, mock_api, sample_submit_response):
    import json
    route = mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    client.propose(
        content="Tagged memory",
        memory_type="fact",
        domain_tag="crypto",
        confidence=0.8,
        tags=["project-x", "follow-up"],
    )
    body = json.loads(route.calls.last.request.read())
    assert body["tags"] == ["project-x", "follow-up"]


def test_propose_memory_without_tags_omits_field(client, mock_api, sample_submit_response):
    import json
    route = mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    client.propose(
        content="Plain memory",
        memory_type="fact",
        domain_tag="crypto",
        confidence=0.8,
    )
    body = json.loads(route.calls.last.request.read())
    # exclude_none + optional None default → field must not appear on the wire.
    assert "tags" not in body


def test_propose_memory_with_classification(client, mock_api, sample_submit_response):
    import json
    route = mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    client.propose(
        content="Classified memory",
        memory_type="fact",
        domain_tag="audit",
        confidence=0.9,
        classification=3,  # SECRET
    )
    body = json.loads(route.calls.last.request.read())
    assert body["classification"] == 3


def test_propose_memory_without_classification_omits_field(client, mock_api, sample_submit_response):
    import json
    route = mock_api.post("/v1/memory/submit").mock(
        return_value=httpx.Response(201, json=sample_submit_response)
    )
    client.propose(
        content="Plain memory",
        memory_type="fact",
        domain_tag="crypto",
        confidence=0.8,
    )
    body = json.loads(route.calls.last.request.read())
    # Omitted classification must not appear on the wire — server defaults to
    # PUBLIC (0), not INTERNAL (the v6.8.6 server-side behavior).
    assert "classification" not in body


def test_query_memories(client, mock_api, sample_query_response):
    mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    result = client.query(embedding=[0.1] * 768, domain_tag="crypto")
    assert len(result.results) == 1


def test_query_memories_with_tags(client, mock_api, sample_query_response):
    import json
    route = mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    client.query(embedding=[0.1] * 768, domain_tag="crypto", tags=["alpha"])
    body = json.loads(route.calls.last.request.read())
    assert body["tags"] == ["alpha"]


def test_query_memories_without_tags_omits_field(client, mock_api, sample_query_response):
    import json
    route = mock_api.post("/v1/memory/query").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    client.query(embedding=[0.1] * 768, domain_tag="crypto")
    body = json.loads(route.calls.last.request.read())
    assert "tags" not in body


def test_hybrid_recall(client, mock_api, sample_query_response):
    mock_api.post("/v1/memory/hybrid").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    result = client.hybrid(query="how does X work", embedding=[0.1] * 768, domain_tag="crypto")
    assert len(result.results) == 1


def test_hybrid_with_expansions(client, mock_api, sample_query_response):
    import json
    route = mock_api.post("/v1/memory/hybrid").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    client.hybrid(
        query="how does X work",
        embedding=[0.1] * 768,
        domain_tag="crypto",
        top_k=5,
        expansions=[{"query": "X mechanism", "embedding": [0.2] * 768}],
    )
    body = json.loads(route.calls.last.request.read())
    assert body["query"] == "how does X work"
    assert body["top_k"] == 5
    assert body["domain_tag"] == "crypto"
    assert len(body["expansions"]) == 1
    assert body["expansions"][0]["query"] == "X mechanism"


def test_hybrid_omits_optional_fields(client, mock_api, sample_query_response):
    import json
    route = mock_api.post("/v1/memory/hybrid").mock(
        return_value=httpx.Response(200, json=sample_query_response)
    )
    client.hybrid(query="ping", embedding=[0.1] * 768)
    body = json.loads(route.calls.last.request.read())
    assert "expansions" not in body
    assert "tags" not in body
    assert "domain_tag" not in body


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


def test_reinstate_with_reason(client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/reinstate").mock(
        return_value=httpx.Response(200, json={"message": "Memory reinstated.", "tx_hash": "RESTOREHASH", "status": "committed"})
    )
    result = client.reinstate(memory_id, reason="challenge withdrawn")
    assert result["status"] == "committed"
    assert route.calls.last.request.read() == b'{"reason":"challenge withdrawn"}'


def test_reinstate_without_reason(client, mock_api):
    memory_id = "550e8400-e29b-41d4-a716-446655440000"
    route = mock_api.post(f"/v1/memory/{memory_id}/reinstate").mock(
        return_value=httpx.Response(200, json={"message": "Memory reinstated.", "tx_hash": "RESTOREHASH2", "status": "committed"})
    )
    result = client.reinstate(memory_id)
    assert result["tx_hash"] == "RESTOREHASH2"
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
