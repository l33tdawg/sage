import pytest
from datetime import datetime


def test_memory_record_validation(sample_memory):
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**sample_memory)
    assert record.memory_id == sample_memory["memory_id"]
    assert record.confidence_score == 0.85


def test_memory_record_invalid_type():
    from sage_sdk.models import MemoryRecord
    with pytest.raises(Exception):  # ValidationError
        MemoryRecord(
            memory_id="test",
            submitting_agent="agent1",
            content="test",
            content_hash="abc",
            memory_type="invalid_type",
            domain_tag="test",
            confidence_score=0.5,
            status="proposed",
            created_at=datetime.now(),
        )


def test_confidence_range():
    from sage_sdk.models import MemorySubmitRequest
    with pytest.raises(Exception):
        MemorySubmitRequest(
            content="test",
            memory_type="fact",
            domain_tag="test",
            confidence_score=1.5,  # Out of range
        )


def test_query_response(sample_query_response):
    from sage_sdk.models import MemoryQueryResponse
    response = MemoryQueryResponse(**sample_query_response)
    assert len(response.results) == 1
    assert response.total_count == 1


def test_submit_request_valid():
    from sage_sdk.models import MemorySubmitRequest
    req = MemorySubmitRequest(
        content="Test memory content",
        memory_type="fact",
        domain_tag="crypto",
        confidence_score=0.8,
    )
    assert req.content == "Test memory content"


def test_vote_request():
    from sage_sdk.models import VoteRequest
    vote = VoteRequest(decision="accept", rationale="Verified correct")
    assert vote.decision == "accept"


def test_agent_registration_parses_already_registered_response():
    # Guards the wire format for the /v1/agent/register idempotent path.
    # Earlier versions declared `registered_at: str` while the server sent
    # an int (block height), producing pydantic validation errors on every
    # re-register. The field is now `on_chain_height: int | None`.
    from sage_sdk.models import AgentRegistration

    reg = AgentRegistration.model_validate({
        "agent_id": "abc123",
        "name": "my-agent",
        "registered_name": "my-agent",
        "role": "member",
        "provider": "test",
        "status": "already_registered",
        "on_chain_height": 92,
    })
    assert reg.on_chain_height == 92
    assert reg.status == "already_registered"


def test_agent_registration_fresh_register_has_no_height():
    # Fresh-register path returns tx_hash and no on_chain_height (the block
    # hasn't committed yet). Must still parse cleanly.
    from sage_sdk.models import AgentRegistration

    reg = AgentRegistration.model_validate({
        "agent_id": "abc123",
        "name": "my-agent",
        "registered_name": "my-agent",
        "role": "member",
        "provider": "test",
        "status": "registered",
        "tx_hash": "DEADBEEF",
    })
    assert reg.on_chain_height is None
    assert reg.tx_hash == "DEADBEEF"
