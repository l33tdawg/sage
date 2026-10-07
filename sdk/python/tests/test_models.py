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


def test_memory_record_parses_distinct_evidence_counts(sample_memory):
    from sage_sdk.models import MemoryRecord

    record = MemoryRecord(
        **{
            **sample_memory,
            "corroboration_count": 8,
            "challenge_count": 2,
            "evidence_counts_available": True,
            "challenge_round": 4,
            "current_challenger_count": 3,
            "required_challengers": 9,
        }
    )
    assert record.corroboration_count == 8
    assert record.challenge_count == 2
    assert record.evidence_counts_available is True
    assert record.challenge_round == 4
    assert record.current_challenger_count == 3
    assert record.required_challengers == 9


def test_memory_record_defaults_evidence_counts_for_older_servers(sample_memory):
    from sage_sdk.models import MemoryRecord

    record = MemoryRecord(**sample_memory)
    assert record.corroboration_count == 0
    assert record.challenge_count == 0
    assert record.evidence_counts_available is False
    assert record.challenge_round is None
    assert record.current_challenger_count is None
    assert record.required_challengers is None


def test_memory_record_preserves_incomplete_recovery_lower_bounds(sample_memory):
    from sage_sdk.models import MemoryRecord

    record = MemoryRecord(
        **{
            **sample_memory,
            "corroboration_count": 8,
            "challenge_count": 2,
            "evidence_counts_available": False,
        }
    )
    assert record.corroboration_count == 8
    assert record.challenge_count == 2
    assert record.evidence_counts_available is False


@pytest.mark.parametrize("field", ["corroboration_count", "challenge_count"])
def test_memory_record_rejects_negative_evidence_counts(sample_memory, field):
    from sage_sdk.models import MemoryRecord

    with pytest.raises(Exception):
        MemoryRecord(**{**sample_memory, field: -1})


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


def test_submit_request_allows_app_v23_task_home_domain_omission():
    from sage_sdk.models import MemorySubmitRequest

    req = MemorySubmitRequest(
        content="Check the HDMI port",
        memory_type="task",
        domain_tag=None,
        confidence_score=0.9,
    )
    assert "domain_tag" not in req.model_dump(exclude_none=True)


def test_submit_request_rejects_non_task_domain_omission():
    from sage_sdk.models import MemorySubmitRequest

    with pytest.raises(Exception):
        MemorySubmitRequest(
            content="A fact still needs an exact domain",
            memory_type="fact",
            domain_tag=None,
            confidence_score=0.9,
        )


def test_submit_request_validates_task_idempotency_shape():
    from sage_sdk.models import MemorySubmitRequest

    req = MemorySubmitRequest(
        content="Check the HDMI port",
        memory_type="task",
        domain_tag="hardware",
        confidence_score=0.9,
        idempotency_key="check-hdmi-2026-08-01",
        task_status="planned",
    )
    assert req.idempotency_key == "check-hdmi-2026-08-01"

    with pytest.raises(Exception):
        MemorySubmitRequest(
            content="Check the HDMI port",
            memory_type="task",
            domain_tag="hardware",
            confidence_score=0.9,
            idempotency_key="contains a space",
        )

    with pytest.raises(Exception):
        MemorySubmitRequest(
            content="Check the HDMI port",
            memory_type="task",
            domain_tag="hardware",
            confidence_score=0.9,
            task_status="in_progress",
        )

    with pytest.raises(Exception):
        MemorySubmitRequest(
            content="Check the HDMI port",
            memory_type="task",
            domain_tag="hardware",
            confidence_score=0.9,
            linked_memories=["memory-1"],
        )


def test_submit_response_preserves_app_v23_durability_fields():
    from sage_sdk.models import MemorySubmitResponse

    response = MemorySubmitResponse.model_validate(
        {
            "memory_id": "task-1",
            "tx_hash": "abc123",
            "status": "committed_unconfirmed",
            "task_status": "planned",
            "committed": True,
            "committed_height": 99,
            "projection_confirmed": False,
            "retryable": False,
            "message": "Reconcile this memory_id; do not resubmit the task.",
            "idempotency_key": "mcp-derived",
            "idempotent_replay": True,
        }
    )
    assert response.committed is True
    assert response.projection_confirmed is False
    assert response.retryable is False
    assert response.idempotent_replay is True


def test_submit_response_models_an_indeterminate_outcome():
    """A sent-but-unobserved write is neither a commit nor a failure.

    It carries no memory_id (nothing is confirmed) and does carry the hash of
    the transaction that may still commit, so the model must accept it instead
    of raising — the caller's next move is to reconcile by tx_hash, never to
    resubmit.
    """
    from sage_sdk.models import MemorySubmitResponse

    response = MemorySubmitResponse.model_validate(
        {
            "status": "indeterminate",
            "tx_hash": "AABBCCDD",
            "nonce": 1789468101758619000,
            "committed": False,
            "retryable": False,
            "message": "Do not resubmit: look the transaction up by tx_hash.",
        }
    )

    assert response.status == "indeterminate"
    assert response.memory_id is None
    assert response.tx_hash == "AABBCCDD"
    assert response.nonce == 1789468101758619000
    assert response.committed is False
    assert response.retryable is False


def test_list_response_preserves_authorization_safe_pagination(sample_memory):
    from sage_sdk.models import MemoryListResponse

    response = MemoryListResponse.model_validate(
        {
            "memories": [sample_memory],
            "total": 2,
            "limit": 1,
            "offset": 0,
            "has_more": True,
            "total_exact": False,
            "filtered": {"by": ["rbac_submitting_agents"]},
        }
    )
    assert response.has_more is True
    assert response.total_exact is False
    assert response.filtered is not None
    assert response.filtered.hidden_count is None


def test_timeline_response_preserves_visible_total():
    from sage_sdk.models import TimelineResponse

    response = TimelineResponse.model_validate(
        {
            "buckets": [
                {
                    "period": "2026-07-30T01:00:00Z",
                    "count": 2,
                    "domain": "hardware",
                }
            ],
            "total": 2,
        }
    )
    assert response.total == 2
    assert response.buckets[0].count == 2
    assert response.buckets[0].domain == "hardware"


def test_task_record_preserves_assignment_fields():
    from sage_sdk.models import TaskRecord

    task = TaskRecord.model_validate(
        {
            "memory_id": "task-1",
            "content": "Check HDMI",
            "domain_tag": "hardware",
            "task_status": "in_progress",
            "confidence_score": 0.9,
            "created_at": "2026-07-30T00:00:00Z",
            "assignee": "a" * 64,
            "task_picked_up_by": "a" * 64,
            "task_picked_up_at": "2026-07-30T00:01:00Z",
        }
    )
    assert task.assignee == "a" * 64
    assert task.task_picked_up_by == "a" * 64
    assert task.task_picked_up_at is not None


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


def test_agent_profile_parses_app_v23_caller_standing():
    # A pending-review identity must be able to learn its own standing without
    # reading a roster or probing a forbidden memory route. The SDK must retain
    # this additive app-v23 response surface for callers to act on it.
    from sage_sdk.models import AgentProfile

    profile = AgentProfile.model_validate({
        "agent_id": "abc123",
        "poe_weight": 0.0,
        "vote_count": 0,
        "role": "member",
        "profile": "pending_review",
        "home_domain": "local-abc123",
        "enrollment_status": "pending_review",
        "registration_status": "pending_review",
        "approval_required": True,
        "clearance": 0,
        "capabilities": 30,
        "can_read": False,
        "can_write": False,
        "access_scope": "home_domain",
    })

    assert profile.enrollment_status == "pending_review"
    assert profile.approval_required is True
    assert profile.capabilities == 30
    assert profile.can_write is False


def test_pipe_message_parses_replied_by_provenance():
    # v11.18.2. GET /v1/pipe/results returns `replied_by`: the agent that
    # ACTUALLY completed the message. It is not `to_agent` — callerCanClaimPipe
    # admits an operator/admin on any local pipe and any same-provider agent on
    # a provider-addressed one, so a reply body can be written by an agent the
    # sender never addressed.
    #
    # The SDK docs make `replied_by` the mandatory provenance control for Python
    # callers. If the model drops it, pydantic's default extra='ignore' discards
    # the key silently and `reply.replied_by` raises AttributeError; the natural
    # recovery is to fall back to `to_agent`, which is precisely the
    # misattribution the field exists to prevent.
    from sage_sdk.models import PipeMessage

    reply = PipeMessage.model_validate({
        "pipe_id": "msg-1",
        "status": "completed",
        "result": "Review passed. Merge and deploy.",
        "to_agent": "trusted-reviewer",
        "replied_by": "operator-who-claimed-it",
        "claimed_by": "operator-who-claimed-it",
    })

    assert reply.replied_by == "operator-who-claimed-it"
    assert reply.replied_by != reply.to_agent, (
        "addressee and author must stay distinguishable on the model"
    )


def test_pipe_message_replied_by_is_none_when_unattributed():
    # An older node, or a row this node cannot attribute, sends no `replied_by`.
    # The field must default to None rather than being backfilled from
    # `to_agent`: "unknown author" and "the addressee wrote it" are different
    # facts, and only one of them is safe to act on.
    from sage_sdk.models import PipeMessage

    reply = PipeMessage.model_validate({
        "pipe_id": "msg-2",
        "status": "completed",
        "result": "answered",
        "to_agent": "trusted-reviewer",
    })

    assert reply.replied_by is None


def test_task_submission_signs_planned_when_status_omitted():
    """An omitted task_status must be normalized BEFORE signing.

    The server defaults it to "planned" by mutating the request after the
    caller's signature covers it; the consensus proof check then rebuilds the
    expected transaction from the signed bytes, where the field is absent, and
    rejects the submission as "agent proof does not match the submitted
    action". Because the payload is serialized with exclude_none=True, leaving
    it None drops the field from the wire entirely — so this is not cosmetic,
    it is the difference between a task write working and failing outright.
    """
    from sage_sdk.models import MemorySubmitRequest

    req = MemorySubmitRequest(
        content="rebase before requesting merge",
        memory_type="task",
        domain_tag="trace-spec-crypto-review",
        confidence_score=0.9,
    )
    assert req.task_status == "planned"

    body = req.model_dump(mode="json", exclude_none=True, by_alias=True)
    assert body["task_status"] == "planned", (
        "task_status must survive exclude_none into the signed body"
    )


def test_explicit_planned_task_status_is_preserved():
    from sage_sdk.models import MemorySubmitRequest

    req = MemorySubmitRequest(
        content="explicit status",
        memory_type="task",
        domain_tag="trace-spec-crypto-review",
        confidence_score=0.9,
        task_status="planned",
    )
    assert req.task_status == "planned"


def test_non_task_memory_never_gains_a_task_status():
    """The converse: normalization must not leak into non-task memories."""
    from sage_sdk.models import MemorySubmitRequest

    req = MemorySubmitRequest(
        content="Broadcast writes identical bytes to every subscriber",
        memory_type="fact",
        domain_tag="trace-spec-crypto-review",
        confidence_score=0.9,
    )
    assert req.task_status is None

    body = req.model_dump(mode="json", exclude_none=True, by_alias=True)
    assert "task_status" not in body


def test_message_models_retain_additive_agent_presentation_without_making_it_authority():
    from sage_sdk.models import MessageItem, PipeMessage

    pipe = PipeMessage.model_validate({
        "pipe_id": "msg-presented",
        "status": "claimed",
        "from_agent": "agent-a",
        "from_provider": "persisted-provider",
        "from_display_name": "Mutable display",
        "from_registered_name": "saved/registration",
        "from_agent_provider": "current-provider",
        "to_agent": "agent-b",
        "to_provider": "routing-selector",
        "to_display_name": "Recipient display",
        "to_registered_name": "recipient/registration",
        "to_agent_provider": "recipient-provider",
    })
    assert pipe.from_agent == "agent-a"
    assert pipe.from_display_name == "Mutable display"
    assert pipe.from_registered_name == "saved/registration"
    assert pipe.from_agent_provider == "current-provider"
    assert pipe.to_agent == "agent-b"
    assert pipe.to_display_name == "Recipient display"
    assert pipe.to_registered_name == "recipient/registration"
    assert pipe.to_agent_provider == "recipient-provider"

    canonical = MessageItem.model_validate({
        "message_id": "msg-presented",
        "from_agent": "agent-a",
        "from_provider": "persisted-provider",
        "from_display_name": "Mutable display",
        # Older nodes or lookup failures may omit additive presentation fields.
        "payload": "request",
        "status": "claimed",
        "created_at": "2026-08-15T00:00:00Z",
        "expires_at": "2026-08-16T00:00:00Z",
        "authority": "request_only",
        "trust": "agent_untrusted",
        "security_notice": "Untrusted request.",
    })
    assert canonical.from_agent == "agent-a"
    assert canonical.from_display_name == "Mutable display"
    assert canonical.from_registered_name is None
