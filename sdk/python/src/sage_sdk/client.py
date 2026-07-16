"""SAGE synchronous client."""

from __future__ import annotations

from typing import Any, Literal

import httpx

from sage_sdk.auth import AgentIdentity
from sage_sdk.exceptions import SageAPIError, SageNotFoundError
from sage_sdk.models import (
    AgentInfo,
    AgentProfile,
    AgentRegistration,
    ChallengeRequest,
    CorroborateRequest,
    DomainReassignRequest,
    DomainReassignResponse,
    ForgetRequest,
    EpochInfo,
    GovCancelRequest,
    GovCancelResponse,
    GovProposeRequest,
    GovProposeResponse,
    GovProposalDetailResponse,
    GovProposalListResponse,
    GovVoteRequest,
    GovVoteResponse,
    GovernanceContext,
    KnowledgeTriple,
    MemoryLinkResponse,
    MemoryListResponse,
    MemoryQueryResponse,
    MemoryRecord,
    MemorySubmitRequest,
    MemorySubmitResponse,
    MemoryType,
    PendingMemoriesResponse,
    PipeInboxResponse,
    PipeMessage,
    PipeResultResponse,
    PipeSendResponse,
    PreValidateResponse,
    ReinstateRequest,
    ScopeActionTemplate,
    ScopeListResponse,
    ScopeRecord,
    TaskListResponse,
    TimelineResponse,
    VoteRequest,
)


# OrgIDs are derived server-side as ``hex(sha256(adminID + ":" + name +
# ":" + height)[:16])`` — a 32-char lowercase hex string. Anything else
# (e.g. "levelup", "Acme Corp") is treated as a human-readable name and
# routed through the by-name lookup. Conservative match: only treat
# unambiguous orgIDs as IDs to avoid false positives on hex-looking names.
_ORG_ID_HEX_LEN = 32


def _looks_like_org_id(identifier: str) -> bool:
    """Heuristic: treat 32-char lowercase hex strings as orgIDs."""
    if len(identifier) != _ORG_ID_HEX_LEN:
        return False
    return all(c in "0123456789abcdef" for c in identifier)


def _encode_gov_payload(payload: dict | bytes | None) -> str | None:
    """Serialize a governance proposal payload to base64.

    - ``dict``: JSON-encode (compact) then base64-encode.
    - ``bytes``: base64-encode the raw bytes directly.
    - ``None``: return ``None`` so the field is omitted from the request.

    Raises ``TypeError`` for any other shape.
    """
    if payload is None:
        return None
    import base64

    if isinstance(payload, dict):
        import json as json_mod

        raw = json_mod.dumps(payload, separators=(",", ":")).encode("utf-8")
    elif isinstance(payload, (bytes, bytearray)):
        raw = bytes(payload)
    else:
        raise TypeError(
            "governance_propose payload must be dict, bytes, or None; "
            f"got {type(payload).__name__}"
        )
    return base64.b64encode(raw).decode("ascii")


class SageClient:
    """Synchronous SAGE API client."""

    def __init__(
        self,
        base_url: str,
        identity: AgentIdentity,
        timeout: float = 30.0,
        ca_cert: str | bool | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._identity = identity
        if ca_cert is None:
            verify: bool | str = True
        else:
            verify = ca_cert
        self._client = httpx.Client(base_url=self._base_url, timeout=timeout, verify=verify)

    def _request(
        self,
        method: str,
        path: str,
        json: Any = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        body = None
        if json is not None:
            import json as json_mod

            body = json_mod.dumps(json, separators=(",", ":")).encode()

        # Include query params in the signing path so the signature matches
        # what the server verifies (method + path?query + body).
        sign_path = path
        if params:
            from urllib.parse import urlencode

            sign_path = path + "?" + urlencode(params, doseq=True)

        headers = self._identity.sign_request(method, sign_path, body)
        if body is not None:
            headers["Content-Type"] = "application/json"

        response = self._client.request(
            method, path, content=body, headers=headers, params=params
        )
        self._handle_response(response)
        return response

    def _handle_response(self, response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise SageAPIError.from_response(response)

    # --- Health ----------------------------------------------------------------

    def health(self) -> dict:
        """Check node health."""
        resp = self._client.get("/health")
        return resp.json()

    def ready(self) -> dict:
        """Check node readiness."""
        resp = self._client.get("/ready")
        return resp.json()

    # --- Memory ----------------------------------------------------------------

    def propose(
        self,
        content: str,
        memory_type: MemoryType | str,
        domain_tag: str,
        confidence: float,
        embedding: list[float] | None = None,
        knowledge_triples: list[KnowledgeTriple] | None = None,
        parent_hash: str | None = None,
        tags: list[str] | None = None,
        classification: int | None = None,
    ) -> MemorySubmitResponse:
        """Submit a new memory proposal.

        tags: optional user-defined labels queryable via the `tags` argument
        on :meth:`query`. Above app-v20 they are normalized into the signed
        transaction; scoped-domain tags are also AppHash-covered and restored
        with the canonical scoped projection. Ordinary-domain tags remain
        node-local serving metadata.

        classification: per-record clearance level 0-4 (PUBLIC, INTERNAL,
        CONFIDENTIAL, SECRET, TOP SECRET). When omitted the server stores
        the memory as PUBLIC (0); pass an explicit level to classify (e.g.
        3 for SECRET, 4 for TOP SECRET).
        """
        req = MemorySubmitRequest(
            content=content,
            memory_type=MemoryType(memory_type),
            domain_tag=domain_tag,
            confidence_score=confidence,
            embedding=embedding,
            knowledge_triples=knowledge_triples,
            parent_hash=parent_hash,
            tags=tags,
            classification=classification,
        )
        resp = self._request("POST", "/v1/memory/submit", json=req.model_dump(mode="json", exclude_none=True, by_alias=True))
        return MemorySubmitResponse.model_validate(resp.json())

    def query(
        self,
        embedding: list[float],
        domain_tag: str | None = None,
        min_confidence: float | None = None,
        top_k: int = 10,
        status_filter: str | None = None,
        cursor: str | None = None,
        tags: list[str] | None = None,
    ) -> MemoryQueryResponse:
        """Query memories by vector similarity.

        tags: when non-empty, restricts results to memories tagged with ANY
        of the listed values (OR semantics).
        """
        body: dict[str, Any] = {"embedding": embedding, "top_k": top_k}
        if domain_tag is not None:
            body["domain_tag"] = domain_tag
        if min_confidence is not None:
            body["min_confidence"] = min_confidence
        if status_filter is not None:
            body["status_filter"] = status_filter
        if cursor is not None:
            body["cursor"] = cursor
        if tags:
            body["tags"] = tags

        resp = self._request("POST", "/v1/memory/query", json=body)
        return MemoryQueryResponse.model_validate(resp.json())

    def hybrid(
        self,
        query: str,
        embedding: list[float],
        domain_tag: str | None = None,
        top_k: int = 10,
        status_filter: str | None = None,
        min_confidence: float | None = None,
        provider: str | None = None,
        tags: list[str] | None = None,
        expansions: list[dict[str, Any]] | None = None,
    ) -> MemoryQueryResponse:
        """Hybrid recall: fuse BM25/FTS5 keyword and vector cosine results via
        Reciprocal Rank Fusion in one round trip. Callers send both the text
        query and the precomputed embedding so SAGE runs both indexes server-side.

        expansions: optional list of {"query": str, "embedding": list[float]}
        paraphrase/entity/temporal variants. When provided, SAGE runs hybrid
        recall per variant and fuses across variants via RRF. The caller must
        produce embeddings with the same model that generated the primary.

        Server respects reranker env vars (SAGE_RERANK_ENABLED, SAGE_RERANK_URL)
        if configured; otherwise plain RRF over BM25 + vector.
        """
        body: dict[str, Any] = {
            "query": query,
            "embedding": embedding,
            "top_k": top_k,
        }
        if domain_tag is not None:
            body["domain_tag"] = domain_tag
        if status_filter is not None:
            body["status_filter"] = status_filter
        if min_confidence is not None:
            body["min_confidence"] = min_confidence
        if provider is not None:
            body["provider"] = provider
        if tags:
            body["tags"] = tags
        if expansions:
            body["expansions"] = expansions

        resp = self._request("POST", "/v1/memory/hybrid", json=body)
        return MemoryQueryResponse.model_validate(resp.json())

    def get_memory(self, memory_id: str) -> MemoryRecord:
        """Get a single memory by ID."""
        resp = self._request("GET", f"/v1/memory/{memory_id}")
        return MemoryRecord.model_validate(resp.json())

    def list_memories(
        self,
        limit: int = 50,
        offset: int = 0,
        domain: str | None = None,
        tag: str | None = None,
        provider: str | None = None,
        status: str | None = None,
        sort: str | None = None,
        agent: str | None = None,
    ) -> MemoryListResponse:
        """List memories with filtering and pagination."""
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if domain is not None:
            params["domain"] = domain
        if tag is not None:
            params["tag"] = tag
        if provider is not None:
            params["provider"] = provider
        if status is not None:
            params["status"] = status
        if sort is not None:
            params["sort"] = sort
        if agent is not None:
            params["agent"] = agent
        resp = self._request("GET", "/v1/memory/list", params=params)
        return MemoryListResponse.model_validate(resp.json())

    def timeline(
        self,
        domain: str | None = None,
        bucket: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> TimelineResponse:
        """Get memory timeline with time-bucketed counts."""
        params: dict[str, Any] = {}
        if domain is not None:
            params["domain"] = domain
        if bucket is not None:
            params["bucket"] = bucket
        if from_time is not None:
            params["from"] = from_time
        if to_time is not None:
            params["to"] = to_time
        resp = self._request("GET", "/v1/memory/timeline", params=params)
        return TimelineResponse.model_validate(resp.json())

    def link_memories(
        self,
        source_id: str,
        target_id: str,
        link_type: str = "related",
    ) -> MemoryLinkResponse:
        """Link two related memories."""
        body = {"source_id": source_id, "target_id": target_id, "link_type": link_type}
        resp = self._request("POST", "/v1/memory/link", json=body)
        return MemoryLinkResponse.model_validate(resp.json())

    def pre_validate(
        self,
        content: str,
        domain: str,
        memory_type: str = "observation",
        confidence: float = 0.8,
    ) -> PreValidateResponse:
        """Dry-run validator checks without submitting."""
        body = {"content": content, "domain": domain, "type": memory_type, "confidence": confidence}
        resp = self._request("POST", "/v1/memory/pre-validate", json=body)
        return PreValidateResponse.model_validate(resp.json())

    # --- Tasks -----------------------------------------------------------------

    def list_tasks(
        self,
        domain: str | None = None,
        provider: str | None = None,
    ) -> TaskListResponse:
        """Get open tasks."""
        params: dict[str, Any] = {}
        if domain is not None:
            params["domain"] = domain
        if provider is not None:
            params["provider"] = provider
        resp = self._request("GET", "/v1/memory/tasks", params=params)
        return TaskListResponse.model_validate(resp.json())

    def update_task_status(self, memory_id: str, task_status: str) -> dict:
        """Update a task memory's status (planned/in_progress/done/dropped)."""
        body = {"task_status": task_status}
        resp = self._request("PUT", f"/v1/memory/{memory_id}/task-status", json=body)
        return resp.json()

    # --- Voting ----------------------------------------------------------------

    def vote(
        self,
        memory_id: str,
        decision: Literal["accept", "reject", "abstain"],
        rationale: str | None = None,
    ) -> dict:
        """Cast a vote on a proposed memory."""
        req = VoteRequest(decision=decision, rationale=rationale)
        resp = self._request(
            "POST",
            f"/v1/memory/{memory_id}/vote",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    def challenge(
        self,
        memory_id: str,
        reason: str,
        evidence: str | None = None,
    ) -> dict:
        """Challenge a committed memory."""
        req = ChallengeRequest(reason=reason, evidence=evidence)
        resp = self._request(
            "POST",
            f"/v1/memory/{memory_id}/challenge",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    def corroborate(
        self,
        memory_id: str,
        evidence: str | None = None,
    ) -> dict:
        """Corroborate an existing memory."""
        req = CorroborateRequest(evidence=evidence)
        resp = self._request(
            "POST",
            f"/v1/memory/{memory_id}/corroborate",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    def forget(
        self,
        memory_id: str,
        reason: str | None = None,
    ) -> dict:
        """Challenge a memory through the user-facing forget endpoint.

        Thin wrapper over POST /v1/memory/{id}/forget. The server substitutes
        a default reason when none is supplied. On app-v17, a memory with
        multiple modify holders is first parked as challenged; a one-holder
        memory is deprecated immediately.
        """
        req = ForgetRequest(reason=reason)
        resp = self._request(
            "POST",
            f"/v1/memory/{memory_id}/forget",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    def reinstate(
        self,
        memory_id: str,
        reason: str | None = None,
    ) -> dict:
        """Reinstate a two-phase-challenged memory.

        Submits the app-v17 ``MemoryReinstate`` transaction and waits for
        consensus commit. The chain must have activated app-v17; the caller
        must be a current modify-verb holder or the original challenger
        withdrawing their own challenge.
        """
        req = ReinstateRequest(reason=reason)
        resp = self._request(
            "POST",
            f"/v1/memory/{memory_id}/reinstate",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    # --- Embeddings ------------------------------------------------------------

    def embed(self, text: str) -> list[float]:
        """Generate a vector embedding via the SAGE network's local Ollama.

        Agents can call this instead of running Ollama locally.
        All computation stays within the SAGE network — no cloud API calls.
        """
        resp = self._request("POST", "/v1/embed", json={"text": text})
        data = resp.json()
        return data["embedding"]

    # --- Agent -----------------------------------------------------------------

    def get_profile(self) -> AgentProfile:
        """Get the current agent's profile."""
        resp = self._request("GET", "/v1/agent/me")
        return AgentProfile.model_validate(resp.json())

    def register_agent(
        self,
        name: str,
        role: str = "member",
        boot_bio: str | None = None,
        provider: str | None = None,
        p2p_address: str | None = None,
    ) -> AgentRegistration:
        """Register the current agent on-chain."""
        body: dict[str, Any] = {"name": name, "role": role}
        if boot_bio is not None:
            body["boot_bio"] = boot_bio
        if provider is not None:
            body["provider"] = provider
        if p2p_address is not None:
            body["p2p_address"] = p2p_address
        resp = self._request("POST", "/v1/agent/register", json=body)
        return AgentRegistration.model_validate(resp.json())

    def update_agent(self, name: str | None = None, boot_bio: str | None = None) -> dict:
        """Update the current agent's profile."""
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if boot_bio is not None:
            body["boot_bio"] = boot_bio
        resp = self._request("PUT", "/v1/agent/update", json=body)
        return resp.json()

    def get_agent(self, agent_id: str) -> AgentInfo:
        """Get a registered agent by ID."""
        resp = self._request("GET", f"/v1/agent/{agent_id}")
        return AgentInfo.model_validate(resp.json())

    def set_agent_permission(
        self,
        agent_id: str,
        clearance: int | None = None,
        domain_access: str | None = None,
        visible_agents: str | None = None,
        org_id: str | None = None,
        dept_id: str | None = None,
    ) -> dict:
        """Update an agent's permissions (admin only)."""
        body: dict[str, Any] = {}
        if clearance is not None:
            body["clearance"] = clearance
        if domain_access is not None:
            body["domain_access"] = domain_access
        if visible_agents is not None:
            body["visible_agents"] = visible_agents
        if org_id is not None:
            body["org_id"] = org_id
        if dept_id is not None:
            body["dept_id"] = dept_id
        resp = self._request("PUT", f"/v1/agent/{agent_id}/permission", json=body)
        return resp.json()

    def list_agents(self) -> list[dict]:
        """List all registered agents (public info)."""
        resp = self._request("GET", "/v1/agents")
        return resp.json()

    # --- Validator -------------------------------------------------------------

    def get_pending(
        self,
        domain_tag: str | None = None,
        limit: int = 20,
    ) -> PendingMemoriesResponse:
        """Get memories pending validation."""
        params: dict[str, Any] = {"limit": limit}
        if domain_tag is not None:
            params["domain_tag"] = domain_tag
        resp = self._request("GET", "/v1/validator/pending", params=params)
        return PendingMemoriesResponse.model_validate(resp.json())

    def get_epoch(self) -> EpochInfo:
        """Get current epoch info and validator scores."""
        resp = self._request("GET", "/v1/validator/epoch")
        return EpochInfo.model_validate(resp.json())

    # --- Pipeline (Agent-to-Agent Messaging) -----------------------------------

    def pipe_send(
        self,
        payload: str,
        to_agent: str | None = None,
        to_provider: str | None = None,
        intent: str | None = None,
        ttl_minutes: int | None = None,
    ) -> PipeSendResponse:
        """Send a message through the agent pipeline."""
        body: dict[str, Any] = {"payload": payload}
        if to_agent is not None:
            body["to_agent"] = to_agent
        if to_provider is not None:
            body["to_provider"] = to_provider
        if intent is not None:
            body["intent"] = intent
        if ttl_minutes is not None:
            body["ttl_minutes"] = ttl_minutes
        resp = self._request("POST", "/v1/pipe/send", json=body)
        return PipeSendResponse.model_validate(resp.json())

    def pipe_inbox(self, limit: int = 5) -> PipeInboxResponse:
        """Get pending messages in the agent's inbox."""
        resp = self._request("GET", "/v1/pipe/inbox", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    def pipe_claim(self, pipe_id: str) -> dict:
        """Claim a pipeline message for processing."""
        resp = self._request("PUT", f"/v1/pipe/{pipe_id}/claim")
        return resp.json()

    def pipe_result(self, pipe_id: str, result: str) -> PipeResultResponse:
        """Submit a result for a claimed pipeline message."""
        resp = self._request("PUT", f"/v1/pipe/{pipe_id}/result", json={"result": result})
        return PipeResultResponse.model_validate(resp.json())

    def pipe_status(self, pipe_id: str) -> PipeMessage:
        """Get the status of a pipeline message."""
        resp = self._request("GET", f"/v1/pipe/{pipe_id}")
        return PipeMessage.model_validate(resp.json())

    def pipe_results(self, limit: int = 5) -> PipeInboxResponse:
        """List completed pipeline message results."""
        resp = self._request("GET", "/v1/pipe/results", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    # --- Access Control --------------------------------------------------------

    def request_access(self, domain: str, justification: str = "", level: int = 1) -> dict:
        """Request access to a domain."""
        body = {"target_domain": domain, "justification": justification, "requested_level": level}
        resp = self._request("POST", "/v1/access/request", json=body)
        return resp.json()

    def grant_access(
        self,
        grantee_id: str,
        domain: str,
        level: int = 1,
        expires_at: int = 0,
        request_id: str | None = None,
    ) -> dict:
        """Grant access to a domain (domain owner only)."""
        body: dict[str, Any] = {
            "grantee_id": grantee_id,
            "domain": domain,
            "level": level,
            "expires_at": expires_at,
        }
        if request_id:
            body["request_id"] = request_id
        resp = self._request("POST", "/v1/access/grant", json=body)
        return resp.json()

    def revoke_access(self, grantee_id: str, domain: str, reason: str = "") -> dict:
        """Revoke access to a domain (domain owner only)."""
        body = {"grantee_id": grantee_id, "domain": domain, "reason": reason}
        resp = self._request("POST", "/v1/access/revoke", json=body)
        return resp.json()

    def list_grants(self, agent_id: str | None = None) -> list[dict]:
        """List active access grants for an agent."""
        aid = agent_id or self._identity.agent_id
        resp = self._request("GET", f"/v1/access/grants/{aid}")
        return resp.json()

    def register_domain(self, name: str, description: str = "", parent: str = "") -> dict:
        """Register a new domain."""
        body: dict[str, Any] = {"name": name}
        if description:
            body["description"] = description
        if parent:
            body["parent"] = parent
        resp = self._request("POST", "/v1/domain/register", json=body)
        return resp.json()

    def get_domain(self, name: str) -> dict:
        """Get domain info."""
        resp = self._request("GET", f"/v1/domain/{name}")
        return resp.json()

    # --- Domain Reassign (v8.0) -----------------------------------------------

    def submit_domain_reassign(
        self,
        domain: str,
        new_owner_id: str,
        proposal_id: str,
        parent_domain: str = "",
        open_to_shared: bool = False,
    ) -> DomainReassignResponse:
        """Submit the on-chain TxTypeDomainReassign that consumes an accepted
        gov_propose of operation='domain_reassign' and atomically transfers
        domain ownership + clears existing grants + optionally promotes the
        domain to shared. Requires chain admin role.

        Returns the tx_hash plus the number of grant rows purged.
        """
        req = DomainReassignRequest(
            domain=domain,
            new_owner_id=new_owner_id,
            proposal_id=proposal_id,
            parent_domain=parent_domain,
            open_to_shared=open_to_shared,
        )
        resp = self._request(
            "POST",
            "/v1/domain/reassign",
            json=req.model_dump(),
        )
        return DomainReassignResponse.model_validate(resp.json())

    def reassign_domain(
        self,
        domain: str,
        new_owner_id: str,
        reason: str,
        parent_domain: str = "",
        open_to_shared: bool = False,
        poll_interval_s: float = 2.0,
        timeout_s: float = 120.0,
    ) -> DomainReassignResponse:
        """End-to-end domain reassign: propose with the right payload, poll
        until the proposal is executed (or rejected/expired/cancelled), then
        submit the DomainReassign tx referencing the accepted proposal_id.

        Raises :class:`SageAPIError` on proposal rejection / timeout / submit
        failure.
        """
        import time

        payload = {
            "domain": domain,
            "new_owner_id": new_owner_id,
            "parent_domain": parent_domain,
            "open_to_shared": open_to_shared,
        }
        propose_resp = self.governance_propose(
            operation="domain_reassign",
            target_id=domain,
            reason=reason,
            payload=payload,
        )
        proposal_id = propose_resp.proposal_id

        terminal_non_exec = {"rejected", "expired", "cancelled"}
        deadline = time.monotonic() + timeout_s
        while True:
            detail = self.governance_proposal_detail(proposal_id)
            status = (detail.proposal.status or "").lower()
            if status == "executed":
                break
            if status in terminal_non_exec:
                raise SageAPIError(
                    status_code=409,
                    detail=f"domain reassign proposal {proposal_id} ended as {status}",
                )
            if time.monotonic() >= deadline:
                raise SageAPIError(
                    status_code=408,
                    detail=(
                        f"timed out after {timeout_s:.0f}s waiting for "
                        f"domain reassign proposal {proposal_id} (last status={status})"
                    ),
                )
            time.sleep(poll_interval_s)

        return self.submit_domain_reassign(
            domain=domain,
            new_owner_id=new_owner_id,
            proposal_id=proposal_id,
            parent_domain=parent_domain,
            open_to_shared=open_to_shared,
        )

    # --- Department RBAC --------------------------------------------------------

    def register_dept(self, org_id: str, name: str, description: str = "", parent_dept: str = "") -> dict:
        """Register a new department within an organization."""
        body: dict[str, Any] = {"name": name}
        if description:
            body["description"] = description
        if parent_dept:
            body["parent_dept"] = parent_dept
        resp = self._request("POST", f"/v1/org/{org_id}/dept", json=body)
        return resp.json()

    def get_dept(self, org_id: str, dept_id: str) -> dict:
        """Get department info."""
        resp = self._request("GET", f"/v1/org/{org_id}/dept/{dept_id}")
        return resp.json()

    def list_depts(self, org_id: str) -> list[dict]:
        """List all departments in an organization."""
        resp = self._request("GET", f"/v1/org/{org_id}/depts")
        return resp.json()

    def add_dept_member(
        self,
        org_id: str,
        dept_id: str,
        agent_id: str,
        clearance: int = 1,
        role: str = "member",
    ) -> dict:
        """Add an agent to a department."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance, "role": role}
        resp = self._request("POST", f"/v1/org/{org_id}/dept/{dept_id}/member", json=body)
        return resp.json()

    def remove_dept_member(self, org_id: str, dept_id: str, agent_id: str) -> dict:
        """Remove an agent from a department."""
        resp = self._request("DELETE", f"/v1/org/{org_id}/dept/{dept_id}/member/{agent_id}")
        return resp.json()

    def list_dept_members(self, org_id: str, dept_id: str) -> list[dict]:
        """List all members of a department."""
        resp = self._request("GET", f"/v1/org/{org_id}/dept/{dept_id}/members")
        return resp.json()

    # --- Organization -----------------------------------------------------------

    def register_org(self, name: str, description: str = "") -> dict:
        """Register a new organization."""
        body: dict[str, Any] = {"name": name, "description": description}
        resp = self._request("POST", "/v1/org/register", json=body)
        return resp.json()

    def get_org(self, identifier: str) -> dict:
        """Get organization info by orgID or human-readable name.

        ``identifier`` is treated as an orgID when it looks like a 32-char
        lowercase hex string (matching the server's
        ``hex(sha256(...)[:16])`` derivation). Anything else is resolved
        against ``GET /v1/org/by-name/{name}`` and the single match is
        returned. Raises ``SageAPIError`` if no orgs match the name, and a
        ``ValueError`` if multiple do — the caller must then disambiguate
        by orgID via ``list_orgs_by_name``.
        """
        if _looks_like_org_id(identifier):
            resp = self._request("GET", f"/v1/org/{identifier}")
            return resp.json()
        orgs = self.list_orgs_by_name(identifier)
        if not orgs:
            raise SageAPIError(
                status_code=404,
                detail=f"no organization registered with name {identifier!r}",
            )
        if len(orgs) > 1:
            org_ids = ", ".join(o.get("org_id", "?") for o in orgs)
            raise ValueError(
                f"multiple organizations registered as {identifier!r}: "
                f"{org_ids}. Pass an orgID to disambiguate."
            )
        return orgs[0]

    def list_orgs_by_name(self, name: str) -> list[dict]:
        """List every organization registered with the given human-readable name.

        Names are not enforced unique on-chain, so this can return zero,
        one, or many entries. Each entry has keys ``org_id``, ``name``,
        ``admin_agent_id``, and ``description``.
        """
        resp = self._request("GET", f"/v1/org/by-name/{name}")
        body = resp.json()
        return list(body.get("orgs", []))

    def list_org_members(self, org_id: str) -> list[dict]:
        """List all members of an organization."""
        resp = self._request("GET", f"/v1/org/{org_id}/members")
        return resp.json()

    def add_org_member(
        self,
        org_id: str,
        agent_id: str,
        clearance: int = 1,
        role: str = "member",
    ) -> dict:
        """Add an agent to an organization."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance, "role": role}
        resp = self._request("POST", f"/v1/org/{org_id}/member", json=body)
        return resp.json()

    def remove_org_member(self, org_id: str, agent_id: str) -> dict:
        """Remove an agent from an organization."""
        resp = self._request("DELETE", f"/v1/org/{org_id}/member/{agent_id}")
        return resp.json()

    def set_org_clearance(self, org_id: str, agent_id: str, clearance: int) -> dict:
        """Update an agent's clearance level within an organization."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance}
        resp = self._request("POST", f"/v1/org/{org_id}/clearance", json=body)
        return resp.json()

    # --- Federation -------------------------------------------------------------

    def propose_federation(
        self,
        target_org_id: str,
        allowed_domains: list[str] | None = None,
        allowed_depts: list[str] | None = None,
        max_clearance: int = 2,
        expires_at: int = 0,
        requires_approval: bool = True,
    ) -> dict:
        """Propose a federation agreement with another organization."""
        body: dict[str, Any] = {
            "target_org_id": target_org_id,
            "allowed_domains": allowed_domains or [],
            "allowed_depts": allowed_depts or [],
            "max_clearance": max_clearance,
            "expires_at": expires_at,
            "requires_approval": requires_approval,
        }
        resp = self._request("POST", "/v1/federation/propose", json=body)
        return resp.json()

    def approve_federation(self, federation_id: str) -> dict:
        """Approve a pending federation agreement."""
        resp = self._request("POST", f"/v1/federation/{federation_id}/approve", json={})
        return resp.json()

    def revoke_federation(self, federation_id: str, reason: str = "") -> dict:
        """Revoke an active federation agreement."""
        body: dict[str, Any] = {}
        if reason:
            body["reason"] = reason
        resp = self._request("POST", f"/v1/federation/{federation_id}/revoke", json=body)
        return resp.json()

    def get_federation(self, federation_id: str) -> dict:
        """Get federation agreement info."""
        resp = self._request("GET", f"/v1/federation/{federation_id}")
        return resp.json()

    def list_federations(self, org_id: str) -> list[dict]:
        """List active federation agreements for an organization."""
        resp = self._request("GET", f"/v1/federation/active/{org_id}")
        return resp.json()

    # --- Governance ---------------------------------------------------------------

    def _governance_context(self) -> GovernanceContext | None:
        """Load the signed validator binding used by app-v20 governance.

        Pre-v20 servers either omit the route (404) or return an inactive
        context with an empty governance domain; both must receive their
        historical request body. Every other error remains fail-closed so a
        broken active context endpoint cannot silently remove authorization.
        """
        try:
            resp = self._request("GET", "/v1/governance/context")
        except SageNotFoundError:
            return None
        governance_context = GovernanceContext.model_validate(resp.json())
        return governance_context if governance_context.app_v20_active else None

    def governance_propose(
        self,
        operation: str,
        target_id: str,
        reason: str,
        target_pubkey: str | None = None,
        target_power: int | None = None,
        payload: dict | bytes | None = None,
        scope: ScopeActionTemplate | dict[str, Any] | None = None,
    ) -> GovProposeResponse:
        """Submit a governance proposal.

        Supports validator-set operations (``add_validator``,
        ``remove_validator``, ``update_power``) and access-control recovery
        (``domain_reassign``, v8.0+) plus canonical app-v20 scope decisions
        (``scope_action`` with a guided ``scope`` template).

        ``payload`` carries an optional structured body for operations that
        need one — e.g. ``domain_reassign`` expects a JSON-encoded
        :class:`DomainReassignRequest`. Accepted shapes:

          - ``dict``: JSON-encoded (compact), then base64-encoded onto the
            wire as ``payload``.
          - ``bytes``: base64-encoded directly.
          - ``None``: the ``payload`` field is omitted from the request.

        ``scope`` is the preferred app-v20 surface. The server converts it to
        canonical ScopeRecordV1 bytes; it is mutually exclusive with
        ``payload``.
        """
        if payload is not None and scope is not None:
            raise ValueError("payload and scope are mutually exclusive")
        governance_context = self._governance_context()
        req = GovProposeRequest(
            operation=operation,
            target_id=target_id,
            target_pubkey=target_pubkey,
            target_power=target_power,
            reason=reason,
            payload=_encode_gov_payload(payload),
            scope=scope,
            validator_id=(
                governance_context.validator_id if governance_context is not None else None
            ),
            governance_domain=(
                governance_context.governance_domain
                if governance_context is not None
                else None
            ),
        )
        resp = self._request("POST", "/v1/governance/propose", json=req.model_dump(exclude_none=True))
        return GovProposeResponse.model_validate(resp.json())

    def governance_propose_scope(
        self,
        scope: ScopeActionTemplate | dict[str, Any],
        reason: str,
    ) -> GovProposeResponse:
        """Submit a guided canonical app-v20 scope_action proposal."""
        template = ScopeActionTemplate.model_validate(scope)
        return self.governance_propose(
            operation="scope_action",
            target_id=template.scope_id,
            reason=reason,
            scope=template,
        )

    def governance_vote(self, proposal_id: str, decision: str) -> GovVoteResponse:
        """Vote on an active governance proposal."""
        governance_context = self._governance_context()
        req = GovVoteRequest(
            proposal_id=proposal_id,
            decision=decision,
            validator_id=(
                governance_context.validator_id if governance_context is not None else None
            ),
            governance_domain=(
                governance_context.governance_domain
                if governance_context is not None
                else None
            ),
        )
        resp = self._request(
            "POST", "/v1/governance/vote", json=req.model_dump(exclude_none=True)
        )
        return GovVoteResponse.model_validate(resp.json())

    def governance_cancel(self, proposal_id: str) -> GovCancelResponse:
        """Cancel a governance proposal (proposer only)."""
        governance_context = self._governance_context()
        req = GovCancelRequest(
            proposal_id=proposal_id,
            validator_id=(
                governance_context.validator_id if governance_context is not None else None
            ),
            governance_domain=(
                governance_context.governance_domain
                if governance_context is not None
                else None
            ),
        )
        resp = self._request(
            "POST", "/v1/governance/cancel", json=req.model_dump(exclude_none=True)
        )
        return GovCancelResponse.model_validate(resp.json())

    def list_scopes(self) -> ScopeListResponse:
        """List canonical v11.9 quorum scopes (operator/admin only)."""
        resp = self._request("GET", "/v1/scopes")
        return ScopeListResponse.model_validate(resp.json())

    def get_scope(self, scope_id: str) -> ScopeRecord:
        """Read one canonical v11.9 quorum scope (operator/admin only)."""
        from urllib.parse import quote

        resp = self._request("GET", f"/v1/scopes/{quote(scope_id, safe='')}")
        return ScopeRecord.model_validate(resp.json())

    def governance_proposals(self, status: str | None = None) -> GovProposalListResponse:
        """List governance proposals, optionally filtered by status."""
        params = {"status": status} if status else None
        resp = self._request("GET", "/v1/dashboard/governance/proposals", params=params)
        return GovProposalListResponse.model_validate(resp.json())

    def governance_proposal_detail(self, proposal_id: str) -> GovProposalDetailResponse:
        """Get detailed info about a governance proposal including votes and quorum."""
        resp = self._request("GET", f"/v1/dashboard/governance/proposals/{proposal_id}")
        return GovProposalDetailResponse.model_validate(resp.json())

    def __enter__(self) -> SageClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.close()
