"""SAGE SDK Pydantic models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MemoryType(str, Enum):
    fact = "fact"
    observation = "observation"
    inference = "inference"
    task = "task"


class MemoryStatus(str, Enum):
    proposed = "proposed"
    validated = "validated"
    committed = "committed"
    challenged = "challenged"
    deprecated = "deprecated"


class TaskStatus(str, Enum):
    planned = "planned"
    in_progress = "in_progress"
    done = "done"
    dropped = "dropped"


class PipelineStatus(str, Enum):
    pending = "pending"
    claimed = "claimed"
    completed = "completed"
    expired = "expired"
    failed = "failed"


class KnowledgeTriple(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    subject: str
    predicate: str
    object_: str = Field(alias="object")


class MemoryRecord(BaseModel):
    memory_id: str
    submitting_agent: str
    content: str
    content_hash: str
    memory_type: MemoryType
    domain_tag: str
    confidence_score: float = Field(ge=0, le=1)
    # Stored (undecayed) confidence, sibling to the decayed confidence_score.
    # None for federated results and pre-11.2 servers that don't emit it.
    initial_confidence: float | None = Field(default=None, ge=0, le=1)
    status: MemoryStatus
    parent_hash: str | None = None
    task_status: str | None = None
    classification: int | None = None
    created_at: datetime
    committed_at: datetime | None = None
    deprecated_at: datetime | None = None
    votes: list | None = None
    corroborations: list | None = None
    # Memories linked to this one. The server emits `linked_memories` on the
    # GET /v1/memory/{id} detail response (memory_handler.go), the OpenAPI
    # MemoryRecord schema documents it, and link_memories() already lets a
    # caller WRITE links — but the model dropped it on read, so links could be
    # created yet never read back. Untyped list to match votes/corroborations.
    linked_memories: list | None = None
    similarity_score: float | None = None
    # Provenance tag the submitter attached to the memory (e.g. "claude-code",
    # "chatgpt"). The server emits it as `provider` (json:"provider,omitempty")
    # on every record and list_memories()/query() already accept it as a
    # filter, but the model dropped it on read — so a caller could filter by
    # provider yet never see which provider a returned record carried.
    # Additive Optional: older servers omit it, so it defaults to None.
    provider: str | None = None
    # app-v17 two-phase-challenged marker: the memory is still live and
    # recallable but under dispute; the server sets disputed=true and has
    # already applied the confidence haircut to confidence_score. Emitted as
    # `disputed` (json:"disputed,omitempty") on recall. Additive Optional:
    # older servers omit it, so it defaults to None.
    disputed: bool | None = None


class MemorySubmitRequest(BaseModel):
    content: str
    memory_type: MemoryType
    domain_tag: str
    confidence_score: float = Field(ge=0, le=1)
    embedding: list[float] | None = None
    knowledge_triples: list[KnowledgeTriple] | None = None
    parent_hash: str | None = None
    tags: list[str] | None = None
    # Per-record clearance level 0-4 (PUBLIC, INTERNAL, CONFIDENTIAL, SECRET,
    # TOP SECRET). When omitted the server stores the memory as PUBLIC (0).
    classification: int | None = None


class MemorySubmitResponse(BaseModel):
    memory_id: str
    tx_hash: str
    status: str


class MemoryQueryRequest(BaseModel):
    embedding: list[float]
    domain_tag: str | None = None
    min_confidence: float | None = None
    status_filter: str | None = None
    top_k: int = 10
    cursor: str | None = None
    tags: list[str] | None = None


class MemoryQueryResponse(BaseModel):
    results: list[MemoryRecord]
    next_cursor: str | None = None
    total_count: int


class MemoryListResponse(BaseModel):
    memories: list[MemoryRecord]
    total: int
    limit: int
    offset: int


class TimelineBucket(BaseModel):
    period: str
    count: int
    domain: str | None = None


class TimelineResponse(BaseModel):
    buckets: list[TimelineBucket]


class TaskRecord(BaseModel):
    memory_id: str
    content: str
    domain_tag: str
    task_status: str
    confidence_score: float
    created_at: datetime


class TaskListResponse(BaseModel):
    tasks: list[TaskRecord]
    total: int


class PreValidateVote(BaseModel):
    validator: str
    decision: str
    reason: str


class PreValidateResponse(BaseModel):
    accepted: bool
    votes: list[PreValidateVote]
    quorum: str


class MemoryLinkRequest(BaseModel):
    source_id: str
    target_id: str
    link_type: str = "related"


class MemoryLinkResponse(BaseModel):
    source_id: str
    target_id: str
    link_type: str


class VoteRequest(BaseModel):
    decision: Literal["accept", "reject", "abstain"]
    rationale: str | None = None


class ChallengeRequest(BaseModel):
    reason: str
    evidence: str | None = None


class CorroborateRequest(BaseModel):
    evidence: str | None = None


class ForgetRequest(BaseModel):
    reason: str | None = None


class ReinstateRequest(BaseModel):
    reason: str | None = None


# --- Agent Models ---

class AgentProfile(BaseModel):
    agent_id: str
    poe_weight: float
    vote_count: int
    display_name: str | None = None
    domains: list[str] | None = None
    # Global verdict-correctness accuracy (the EWMA driving quorum weight).
    accuracy: float | None = None
    # Lifetime corroboration count — votes that matched a terminal verdict.
    corr_count: int | None = None
    # Per-domain verdict-correctness expertise, keyed by domain tag. Only
    # present for domains the agent has actually voted in.
    domain_expertise: dict[str, float] | None = None
    on_chain_height: int | None = None


class AgentRegistration(BaseModel):
    agent_id: str
    name: str
    registered_name: str | None = None
    role: str | None = None
    provider: str | None = None
    status: str
    on_chain_height: int | None = None
    tx_hash: str | None = None


class AgentInfo(BaseModel):
    agent_id: str
    name: str | None = None
    registered_name: str | None = None
    role: str | None = None
    avatar: str | None = None
    boot_bio: str | None = None
    validator_pubkey: str | None = None
    node_id: str | None = None
    p2p_address: str | None = None
    status: str | None = None
    clearance: int | None = None
    org_id: str | None = None
    dept_id: str | None = None
    domain_access: str | None = None
    bundle_path: str | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    created_at: datetime | None = None
    removed_at: datetime | None = None
    on_chain_height: int | None = None
    visible_agents: str | None = None
    provider: str | None = None
    memory_count: int | None = None


# --- Pipeline Models ---

class PipeSendRequest(BaseModel):
    to_agent: str | None = None
    to_provider: str | None = None
    source_chain_id: str | None = None
    destination_chain_id: str | None = None
    intent: str | None = None
    payload: str
    ttl_minutes: int | None = None


class PipeSendResponse(BaseModel):
    pipe_id: str
    status: str
    expires_at: str
    destination_chain_id: str | None = None


class PipeResolveResponse(BaseModel):
    to_agent: str | None = None
    to_provider: str | None = None
    source_chain_id: str | None = None
    destination_chain_id: str | None = None
    address: str | None = None
    handle: str | None = None
    display_name: str | None = None


class PipeMessage(BaseModel):
    pipe_id: str
    from_agent: str | None = None
    from_provider: str | None = None
    to_agent: str | None = None
    to_provider: str | None = None
    intent: str | None = None
    payload: str | None = None
    result: str | None = None
    status: str
    created_at: str | None = None
    claimed_at: str | None = None
    completed_at: str | None = None
    expires_at: str | None = None
    journal_id: str | None = None
    claimed_by: str | None = None
    source_chain_id: str | None = None
    source_pipe_id: str | None = None
    destination_chain_id: str | None = None
    reply_source_chain_id: str | None = None
    federation_policy_epoch: str | None = None
    federation_agreement_id: str | None = None
    federation_contact_id: str | None = None
    federation_contact_revision: str | None = None


class PipeInboxResponse(BaseModel):
    items: list[PipeMessage]
    count: int

    @field_validator("items", mode="before")
    @classmethod
    def normalize_null_items(cls, value: object) -> object:
        """Tolerate older SAGE nodes that encoded an empty slice as null."""
        return [] if value is None else value


class PipeDeliveryUpdate(BaseModel):
    event_id: str
    pipe_id: str
    event_kind: str
    remote_chain_id: str
    target_agent_id: str
    state: str
    attempts: int = 0
    last_error: str | None = None
    created_at: str | None = None


class PipeDeliveryUpdatesResponse(BaseModel):
    items: list[PipeDeliveryUpdate]
    count: int

    @field_validator("items", mode="before")
    @classmethod
    def normalize_null_items(cls, value: object) -> object:
        """Tolerate older SAGE nodes that encoded an empty slice as null."""
        return [] if value is None else value


class PipeResultResponse(BaseModel):
    status: str
    journal_id: str | None = None
    journaled: bool = False


# --- Validator Models ---

class ValidatorScore(BaseModel):
    validator_id: str
    weighted_sum: float | None = None
    weight_denom: float | None = None
    vote_count: int | None = None
    expertise_vec: list[float] | None = None
    last_active_ts: str | None = None
    current_weight: float | None = None
    updated_at: str | None = None


class EpochInfo(BaseModel):
    epoch_num: int
    block_height: int
    scores: list[ValidatorScore]


class PendingMemoriesResponse(BaseModel):
    memories: list[MemoryRecord]


# --- Error Models ---

class ProblemDetails(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str | None = None


# --- Access Control Models ---

class AccessRequestModel(BaseModel):
    target_domain: str
    justification: str | None = None
    requested_level: int = 1


class AccessGrantModel(BaseModel):
    grantee_id: str
    domain: str
    level: int = 1
    expires_at: int = 0
    request_id: str | None = None


class AccessRevokeModel(BaseModel):
    grantee_id: str
    domain: str
    reason: str | None = None


class DomainRegisterModel(BaseModel):
    name: str
    description: str | None = None
    parent: str | None = None


class DomainInfo(BaseModel):
    domain_name: str
    owner_agent_id: str
    parent_domain: str | None = None
    description: str | None = None
    created_height: int
    created_at: datetime | None = None


class AccessGrantInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    domain: str
    grantee_id: str
    granter_id: str
    access_level: int = Field(alias="level", default=1)
    expires_at: datetime | None = None
    created_at: datetime | None = None
    revoked_at: datetime | None = None


# --- Department RBAC Models ---

class DeptRegisterRequest(BaseModel):
    name: str
    description: str | None = None
    parent_dept: str | None = None


class DeptRegisterResponse(BaseModel):
    status: str
    dept_id: str
    tx_hash: str


class DeptAddMemberRequest(BaseModel):
    agent_id: str
    clearance: int = 1
    role: str = "member"


class DeptAddMemberResponse(BaseModel):
    status: str
    tx_hash: str


class DeptInfo(BaseModel):
    org_id: str
    dept_id: str
    dept_name: str
    description: str | None = None
    parent_dept: str | None = None
    created_height: int | None = None


class DeptMemberInfo(BaseModel):
    org_id: str
    dept_id: str
    agent_id: str
    clearance: int = 1
    role: str = "member"


# --- Governance Models ---

class GovernanceActiveValidator(BaseModel):
    """One AppHash-covered validator in the node's persisted ABCI roster."""

    validator_id: str = Field(min_length=1)
    voting_power: int = Field(gt=0)


class GovernanceContext(BaseModel):
    """Validator-bound authorization context for governance mutations."""

    validator_id: str = Field(min_length=1)
    # Empty before app-v20 activation; active contexts are revalidated by the
    # mutation request models, which require a non-empty domain.
    governance_domain: str
    app_v20_active: bool
    # Defaults keep clients compatible with older v11.9 prerelease nodes that
    # returned only the mutation-binding fields.
    validator_active: bool = False
    active_validators: list[GovernanceActiveValidator] = Field(default_factory=list)


class ScopeActionMember(BaseModel):
    """One member in a guided app-v20 scope governance template."""

    validator_id: str
    assigned_weight: int
    joined_revision: int | None = None
    active: bool = True


class ScopeActionTemplate(BaseModel):
    """Human-facing scope template; the server creates canonical wire bytes."""

    scope_id: str
    revision: int
    state: str
    controller_validator_id: str
    domains: list[str]
    members: list[ScopeActionMember]


class GovProposeRequest(BaseModel):
    operation: str  # validator ops, "domain_reassign", "memory_domain_repair", or "scope_action"
    target_id: str
    target_pubkey: str | None = None
    target_power: int | None = None
    reason: str
    # payload is base64-encoded raw bytes attached to operations that need a
    # structured body (e.g. ``domain_reassign`` carries the JSON-encoded
    # DomainReassignRequest). None omits the field from the on-wire request.
    payload: str | None = None
    # Guided scope_action input. Mutually exclusive with payload; the server
    # sorts it canonically and materializes consensus-owned heights later.
    scope: ScopeActionTemplate | None = None
    # App-v20 binds the exact operator-signed HTTP request to the local
    # validator and governance domain. These stay optional so a new SDK can
    # still talk to a pre-v20 server that lacks the context endpoint.
    validator_id: str | None = Field(default=None, min_length=1)
    governance_domain: str | None = Field(default=None, min_length=1)


class GovProposeResponse(BaseModel):
    proposal_id: str
    tx_hash: str
    status: str


class GovVoteRequest(BaseModel):
    proposal_id: str
    decision: str  # "accept", "reject", "abstain"
    validator_id: str | None = Field(default=None, min_length=1)
    governance_domain: str | None = Field(default=None, min_length=1)


class GovVoteResponse(BaseModel):
    tx_hash: str
    status: str


class GovCancelRequest(BaseModel):
    proposal_id: str
    validator_id: str | None = Field(default=None, min_length=1)
    governance_domain: str | None = Field(default=None, min_length=1)


class ScopeDomain(BaseModel):
    name: str
    subtree: bool = False


class ScopeMember(BaseModel):
    validator_id: str
    assigned_weight: int
    joined_revision: int
    active: bool
    pending_ballot_count: int = 0
    validator_removal_blocked: bool = False


class ScopeDrain(BaseModel):
    pending_ballot_count: int = 0
    pending_memory_ids: list[str] = Field(default_factory=list)
    blocking_validator_ids: list[str] = Field(default_factory=list)


class ScopeRecord(BaseModel):
    scope_id: str
    revision: int
    revision_hash: str
    state: str
    controller_validator_id: str
    created_height: int
    updated_height: int
    domains: list[ScopeDomain]
    members: list[ScopeMember]
    drain: ScopeDrain = Field(default_factory=ScopeDrain)


class ScopeListResponse(BaseModel):
    scopes: list[ScopeRecord]
    count: int


class GovCancelResponse(BaseModel):
    tx_hash: str
    status: str


class GovProposal(BaseModel):
    proposal_id: str
    operation: str
    target_agent_id: str
    target_pubkey: str | None = None
    target_power: int | None = None
    proposer_id: str
    status: str
    created_height: int
    expiry_height: int
    executed_height: int | None = None
    reason: str | None = None
    # Wall-clock creation time the server stamps on every proposal row
    # (governance_proposals.created_at, NOT NULL DEFAULT RFC3339 — always
    # populated) and emits as `created_at` on BOTH the list and detail
    # responses (json:"created_at,omitempty"). The model dropped it on read, so
    # a caller listing/inspecting proposals could never see when each was
    # raised. Additive Optional: older servers omit it, so it defaults to None.
    created_at: datetime | None = None


class GovVote(BaseModel):
    proposal_id: str
    validator_id: str
    decision: str
    height: int


class GovProposalListResponse(BaseModel):
    proposals: list[GovProposal]


class GovProposalDetailResponse(BaseModel):
    proposal: GovProposal
    votes: list[GovVote]
    quorum_progress: dict | None = None


# --- Domain Reassign (v8.0) Models ---


class DomainReassignRequest(BaseModel):
    """Body of POST /v1/domain/reassign.

    Consumes an accepted ``operation='domain_reassign'`` governance proposal
    and atomically transfers domain ownership, clears all existing grants on
    the domain, and (when ``open_to_shared`` is true) promotes the domain to
    shared status. Requires chain admin role.
    """

    domain: str
    new_owner_id: str
    proposal_id: str
    parent_domain: str = ""
    open_to_shared: bool = False


class DomainReassignResponse(BaseModel):
    """Response from POST /v1/domain/reassign."""

    tx_hash: str
    purged_grants: int
