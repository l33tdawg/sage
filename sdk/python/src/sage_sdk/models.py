"""SAGE SDK Pydantic models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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
    status: MemoryStatus
    parent_hash: str | None = None
    task_status: str | None = None
    classification: int | None = None
    created_at: datetime
    committed_at: datetime | None = None
    deprecated_at: datetime | None = None
    votes: list | None = None
    corroborations: list | None = None
    similarity_score: float | None = None


class MemorySubmitRequest(BaseModel):
    content: str
    memory_type: MemoryType
    domain_tag: str
    confidence_score: float = Field(ge=0, le=1)
    embedding: list[float] | None = None
    knowledge_triples: list[KnowledgeTriple] | None = None
    parent_hash: str | None = None
    tags: list[str] | None = None


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


# --- Agent Models ---

class AgentProfile(BaseModel):
    agent_id: str
    poe_weight: float
    vote_count: int


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
    intent: str | None = None
    payload: str
    ttl_minutes: int | None = None


class PipeSendResponse(BaseModel):
    pipe_id: str
    status: str
    expires_at: str


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


class PipeInboxResponse(BaseModel):
    items: list[PipeMessage]
    count: int


class PipeResultResponse(BaseModel):
    status: str
    journal_id: str | None = None


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

class GovProposeRequest(BaseModel):
    operation: str  # "add_validator", "remove_validator", "update_power"
    target_id: str
    target_pubkey: str | None = None
    target_power: int | None = None
    reason: str


class GovProposeResponse(BaseModel):
    proposal_id: str
    tx_hash: str
    status: str


class GovVoteRequest(BaseModel):
    proposal_id: str
    decision: str  # "accept", "reject", "abstain"


class GovVoteResponse(BaseModel):
    tx_hash: str
    status: str


class GovCancelRequest(BaseModel):
    proposal_id: str


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
