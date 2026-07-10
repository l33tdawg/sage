import pytest
from datetime import datetime


def test_memory_record_validation(sample_memory):
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**sample_memory)
    assert record.memory_id == sample_memory["memory_id"]
    assert record.confidence_score == 0.85


def test_memory_record_parses_provider(sample_memory):
    # The server emits `provider` (the submitter's provenance tag, e.g.
    # "claude-code") on every memory record, and list_memories()/query()
    # already accept it as a filter. The model must read it back so a caller
    # who filters by provider can also see which provider a record carried.
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**{**sample_memory, "provider": "claude-code"})
    assert record.provider == "claude-code"


def test_memory_record_tolerates_missing_provider(sample_memory):
    # An older server (or a record submitted without a provider) omits the
    # field; the additive Optional defaults to None so the model still
    # validates (forward/back compat).
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**sample_memory)
    assert record.provider is None


def test_memory_record_parses_disputed(sample_memory):
    # An app-v17 two-phase-challenged memory stays live and recallable but the
    # server flags it disputed=true (with the confidence haircut already
    # applied). The model must read the marker back so a caller can see a
    # returned record is under dispute.
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**{**sample_memory, "disputed": True})
    assert record.disputed is True


def test_memory_record_tolerates_missing_disputed(sample_memory):
    # An older server (or a record that is not challenged) omits the field; the
    # additive Optional defaults to None so the model still validates
    # (forward/back compat).
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**sample_memory)
    assert record.disputed is None


def test_memory_record_parses_linked_memories(sample_memory):
    # The server emits `linked_memories` on the GET /v1/memory/{id} detail
    # response and link_memories() lets a caller write links. The model must
    # read them back so a caller who creates links can also see them.
    from sage_sdk.models import MemoryRecord
    # Real MemoryLink wire shape the detail endpoint emits (internal/memory/model.go).
    links = [{"source_id": "mem-1", "target_id": "mem-2", "link_type": "supports"}]
    record = MemoryRecord(**{**sample_memory, "linked_memories": links})
    assert record.linked_memories == links


def test_memory_record_tolerates_missing_linked_memories(sample_memory):
    # An older server (or a record with no links) omits the field; the additive
    # Optional defaults to None so the model still validates (forward/back compat).
    from sage_sdk.models import MemoryRecord
    record = MemoryRecord(**sample_memory)
    assert record.linked_memories is None


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


def test_gov_proposal_parses_created_at():
    # The server stamps governance_proposals.created_at (NOT NULL DEFAULT
    # RFC3339) and emits it as `created_at` on both the list
    # (ListGovProposals) and detail (GetGovProposal) responses. The model must
    # read it back so a caller listing proposals can see when each was raised.
    from sage_sdk.models import GovProposal
    proposal = GovProposal.model_validate({
        "proposal_id": "prop-1",
        "operation": "add_validator",
        "target_agent_id": "agent-1",
        "proposer_id": "agent-0",
        "status": "pending",
        "created_height": 100,
        "expiry_height": 200,
        "reason": "onboard validator",
        "created_at": "2026-06-12T08:42:00.000Z",
    })
    assert proposal.created_at == datetime(2026, 6, 12, 8, 42, tzinfo=proposal.created_at.tzinfo)


def test_gov_proposal_tolerates_missing_created_at():
    # An older server (or an omitempty-empty value) omits the field; the
    # additive Optional defaults to None so the model still validates.
    from sage_sdk.models import GovProposal
    proposal = GovProposal.model_validate({
        "proposal_id": "prop-1",
        "operation": "add_validator",
        "target_agent_id": "agent-1",
        "proposer_id": "agent-0",
        "status": "pending",
        "created_height": 100,
        "expiry_height": 200,
    })
    assert proposal.created_at is None


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


def test_agent_profile_parses_poe_signals():
    # v8.6: /v1/agent/me now surfaces the on-chain PoE factors (corr_count,
    # per-domain expertise, authoritative accuracy). The model must parse them.
    from sage_sdk.models import AgentProfile

    profile = AgentProfile.model_validate({
        "agent_id": "abc123",
        "display_name": "my-agent",
        "domains": ["pwn_heap"],
        "poe_weight": 0.25,
        "vote_count": 7,
        "accuracy": 0.6,
        "corr_count": 2,
        "domain_expertise": {"pwn_heap": 0.55},
        "on_chain_height": 1200,
    })
    assert profile.corr_count == 2
    assert profile.domain_expertise == {"pwn_heap": 0.55}
    assert profile.accuracy == 0.6


def test_agent_profile_tolerates_old_server_omitting_poe_signals():
    # An older server omits the new fields entirely; the additive Optional
    # fields default to None so the model still validates (forward/back compat).
    from sage_sdk.models import AgentProfile

    profile = AgentProfile.model_validate({
        "agent_id": "abc123",
        "poe_weight": 0.1,
        "vote_count": 0,
    })
    assert profile.corr_count is None
    assert profile.domain_expertise is None
    assert profile.accuracy is None
