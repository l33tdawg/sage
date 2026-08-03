"""SAGE asynchronous client."""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import quote

import httpx

from sage_sdk.auth import AgentIdentity
from sage_sdk.client import (
    _encode_gov_payload,
    _httpx_verify,
    _looks_like_org_id,
    _receipt_batch_proof_item,
)
from sage_sdk.exceptions import SageAPIError, SageNotFoundError
from sage_sdk.models import (
    AgentDirectoryResponse,
    AgentDomainAccessSample,
    AgentInfo,
    AgentLookupResponse,
    OwnedDomainPage,
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
    MessageActionResponse,
    MessageReceiveResponse,
    MessageSendResponse,
    MessageStatusResponse,
    PendingMemoriesResponse,
    PipeDeliveryUpdatesResponse,
    PipeInboxResponse,
    PipeMessage,
    PipeResolveResponse,
    PipeResultResponse,
    PipeSendResponse,
    PreValidateResponse,
    ReinstateRequest,
    ScopeActionTemplate,
    ScopeListResponse,
    ScopeRecord,
    TaskListResponse,
    TaskStatus,
    TimelineResponse,
    VoteRequest,
)


class AsyncSageClient:
    """Asynchronous SAGE API client."""

    def __init__(
        self,
        base_url: str,
        identity: AgentIdentity,
        timeout: float = 30.0,
        ca_cert: str | bool | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._identity = identity
        verify = _httpx_verify(ca_cert)
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout, verify=verify)

    async def _request(
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

        response = await self._client.request(
            method, path, content=body, headers=headers, params=params
        )
        self._handle_response(response)
        return response

    def _handle_response(self, response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise SageAPIError.from_response(response)

    # --- Health ----------------------------------------------------------------

    async def health(self) -> dict:
        """Check node health."""
        resp = await self._client.get("/health")
        return resp.json()

    async def ready(self) -> dict:
        """Check node readiness."""
        resp = await self._client.get("/ready")
        return resp.json()

    # --- Memory ----------------------------------------------------------------

    async def propose(
        self,
        content: str,
        memory_type: MemoryType | str,
        domain_tag: str | None,
        confidence: float,
        embedding: list[float] | None = None,
        knowledge_triples: list[KnowledgeTriple] | None = None,
        parent_hash: str | None = None,
        tags: list[str] | None = None,
        classification: int | None = None,
        provider: str | None = None,
        task_status: TaskStatus | str | None = None,
        linked_memories: list[str] | None = None,
        idempotency_key: str | None = None,
    ) -> MemorySubmitResponse:
        """Submit a memory transaction and wait for its chain commit.

        tags: optional user-defined labels queryable via the `tags` argument
        on :meth:`query`. Above app-v20 they are normalized into the signed
        transaction; scoped-domain tags are also AppHash-covered and restored
        with the canonical scoped projection. Ordinary-domain tags remain
        node-local serving metadata.

        classification: per-record clearance level 0-4 (PUBLIC, INTERNAL,
        CONFIDENTIAL, SECRET, TOP SECRET). When omitted the server stores
        the memory as PUBLIC (0); pass an explicit level to classify (e.g.
        3 for SECRET, 4 for TOP SECRET).

        App-v23 tasks may pass ``domain_tag=None`` to use the caller's committed
        owned home domain. The node derives a permanent semantic idempotency key
        when one is omitted. A 200 response is an existing replay, 201 is a new
        commit, and 202 has ``committed=True`` but
        ``projection_confirmed=False``: reconcile that exact ``memory_id`` and
        do not resubmit it.
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
            provider=provider,
            task_status=task_status,
            linked_memories=linked_memories,
            idempotency_key=idempotency_key,
        )
        resp = await self._request(
            "POST",
            "/v1/memory/submit",
            json=req.model_dump(
                mode="json", exclude_none=True, by_alias=True
            ),
        )
        return MemorySubmitResponse.model_validate(resp.json())

    async def query(
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

        resp = await self._request("POST", "/v1/memory/query", json=body)
        return MemoryQueryResponse.model_validate(resp.json())

    async def hybrid(
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
        Reciprocal Rank Fusion in one round trip. See SageClient.hybrid for
        full semantics, including the 8-expansion limit, shared app-v23
        authorization budget, and fail-closed governed errors. The async
        variant has the same signature and shape."""
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

        resp = await self._request("POST", "/v1/memory/hybrid", json=body)
        return MemoryQueryResponse.model_validate(resp.json())

    async def get_memory(self, memory_id: str) -> MemoryRecord:
        """Get a single memory by ID."""
        resp = await self._request("GET", f"/v1/memory/{memory_id}")
        return MemoryRecord.model_validate(resp.json())

    async def list_memories(
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
        resp = await self._request("GET", "/v1/memory/list", params=params)
        return MemoryListResponse.model_validate(resp.json())

    async def timeline(
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
        resp = await self._request("GET", "/v1/memory/timeline", params=params)
        return TimelineResponse.model_validate(resp.json())

    async def link_memories(
        self,
        source_id: str,
        target_id: str,
        link_type: str = "related",
    ) -> MemoryLinkResponse:
        """Link two related memories."""
        body = {"source_id": source_id, "target_id": target_id, "link_type": link_type}
        resp = await self._request("POST", "/v1/memory/link", json=body)
        return MemoryLinkResponse.model_validate(resp.json())

    async def pre_validate(
        self,
        content: str,
        domain: str,
        memory_type: str = "observation",
        confidence: float = 0.8,
    ) -> PreValidateResponse:
        """Dry-run validator checks without submitting."""
        body = {"content": content, "domain": domain, "type": memory_type, "confidence": confidence}
        resp = await self._request("POST", "/v1/memory/pre-validate", json=body)
        return PreValidateResponse.model_validate(resp.json())

    # --- Tasks -----------------------------------------------------------------

    async def list_tasks(
        self,
        domain: str | None = None,
        provider: str | None = None,
    ) -> TaskListResponse:
        """Get open tasks exactly assigned and currently readable by this agent."""
        params: dict[str, Any] = {}
        if domain is not None:
            params["domain"] = domain
        if provider is not None:
            params["provider"] = provider
        resp = await self._request("GET", "/v1/memory/tasks", params=params)
        return TaskListResponse.model_validate(resp.json())

    async def update_task_status(self, memory_id: str, task_status: str) -> dict:
        """Set an assigned task to in_progress, done, or dropped.

        Agents cannot self-claim unassigned work, re-plan, or reopen terminal
        work; those are local CEREBRUM operator actions.
        """
        body = {"task_status": task_status}
        resp = await self._request("PUT", f"/v1/memory/{memory_id}/task-status", json=body)
        return resp.json()

    # --- Voting ----------------------------------------------------------------

    async def vote(
        self,
        memory_id: str,
        decision: Literal["accept", "reject", "abstain"],
        rationale: str | None = None,
    ) -> dict:
        """Authorize this node's local validator to vote (operator-only)."""
        req = VoteRequest(decision=decision, rationale=rationale)
        resp = await self._request(
            "POST",
            f"/v1/memory/{memory_id}/vote",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    async def challenge(
        self,
        memory_id: str,
        reason: str,
        evidence: str | None = None,
    ) -> dict:
        """Challenge a committed memory."""
        req = ChallengeRequest(reason=reason, evidence=evidence)
        resp = await self._request(
            "POST",
            f"/v1/memory/{memory_id}/challenge",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    async def corroborate(
        self,
        memory_id: str,
        evidence: str | None = None,
    ) -> dict:
        """Corroborate an existing memory."""
        req = CorroborateRequest(evidence=evidence)
        resp = await self._request(
            "POST",
            f"/v1/memory/{memory_id}/corroborate",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    async def forget(
        self,
        memory_id: str,
        reason: str | None = None,
    ) -> dict:
        """Challenge a memory through the user-facing forget endpoint.

        Thin wrapper over POST /v1/memory/{id}/forget. The server substitutes
        a default reason when none is supplied. Under app-v17, multiple modify
        holders park the first challenge while a one-holder memory deprecates
        immediately. Post-app-v21, k=0 remains immediate; k>0 parks until k+1
        distinct challengers accrue, even on a one-holder domain.
        """
        req = ForgetRequest(reason=reason)
        resp = await self._request(
            "POST",
            f"/v1/memory/{memory_id}/forget",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    async def reinstate(
        self,
        memory_id: str,
        reason: str | None = None,
    ) -> dict:
        """Reinstate a two-phase-challenged memory after consensus commit."""
        req = ReinstateRequest(reason=reason)
        resp = await self._request(
            "POST",
            f"/v1/memory/{memory_id}/reinstate",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()

    # --- Embeddings ------------------------------------------------------------

    async def embed(self, text: str) -> list[float]:
        """Generate a vector embedding via the SAGE network's local Ollama.

        Agents can call this instead of running Ollama locally.
        All computation stays within the SAGE network — no cloud API calls.
        """
        resp = await self._request("POST", "/v1/embed", json={"text": text})
        data = resp.json()
        return data["embedding"]

    # --- Agent -----------------------------------------------------------------

    async def get_profile(self) -> AgentProfile:
        """Get the current agent's profile."""
        resp = await self._request("GET", "/v1/agent/me")
        return AgentProfile.model_validate(resp.json())

    async def register_agent(
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
        resp = await self._request("POST", "/v1/agent/register", json=body)
        return AgentRegistration.model_validate(resp.json())

    async def update_agent(self, name: str | None = None, boot_bio: str | None = None) -> dict:
        """Partially update the current agent's profile.

        A ``None`` argument is omitted and its current canonical value is
        preserved by the server.
        """
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if boot_bio is not None:
            body["boot_bio"] = boot_bio
        resp = await self._request("PUT", "/v1/agent/update", json=body)
        return resp.json()

    async def get_agent(self, agent_id: str) -> AgentInfo:
        """Get a registered agent by ID."""
        resp = await self._request("GET", f"/v1/agent/{agent_id}")
        return AgentInfo.model_validate(resp.json())

    async def list_agents(self) -> dict:
        """List active ordinary agents visible to this signed caller."""
        resp = await self._request("GET", "/v1/agents")
        return resp.json()

    async def agent_directory(self) -> AgentDirectoryResponse:
        """List minimal active local recipients visible to this caller.

        This is discovery metadata, not online, delivery, or read evidence.
        """
        resp = await self._request("GET", "/v1/agents/directory")
        return AgentDirectoryResponse.model_validate(resp.json())

    async def lookup_agents(self, name: str, limit: int = 20) -> AgentLookupResponse:
        """Resolve a bounded local recipient name without loading the roster."""
        resp = await self._request(
            "GET", "/v1/agents/lookup", params={"name": name, "limit": limit}
        )
        return AgentLookupResponse.model_validate(resp.json())

    async def owned_domains(
        self, cursor: str | None = None, limit: int = 50
    ) -> OwnedDomainPage:
        """List one bounded page of domains currently owned by this agent."""
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        resp = await self._request("GET", "/v1/agent/me/domains/owned", params=params)
        return OwnedDomainPage.model_validate(resp.json())

    async def domain_access_sample(self) -> AgentDomainAccessSample:
        """Return bounded readable/writable/owned samples for choosing a scope."""
        resp = await self._request("GET", "/v1/agent/me/domains")
        return AgentDomainAccessSample.model_validate(resp.json())

    # --- Validator -------------------------------------------------------------

    async def get_pending(
        self,
        domain_tag: str | None = None,
        limit: int = 20,
    ) -> PendingMemoriesResponse:
        """Get memories pending validation."""
        params: dict[str, Any] = {"limit": limit}
        if domain_tag is not None:
            params["domain_tag"] = domain_tag
        resp = await self._request("GET", "/v1/validator/pending", params=params)
        return PendingMemoriesResponse.model_validate(resp.json())

    async def get_epoch(self) -> EpochInfo:
        """Get current epoch info and validator scores."""
        resp = await self._request("GET", "/v1/validator/epoch")
        return EpochInfo.model_validate(resp.json())

    # --- Pipeline (Agent-to-Agent Messaging) -----------------------------------

    async def pipe_send(
        self,
        payload: str,
        to_agent: str | None = None,
        to_provider: str | None = None,
        intent: str | None = None,
        ttl_minutes: int | None = None,
        source_chain_id: str | None = None,
        destination_chain_id: str | None = None,
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
        if source_chain_id is not None:
            body["source_chain_id"] = source_chain_id
        if destination_chain_id is not None:
            body["destination_chain_id"] = destination_chain_id
        resp = await self._request("POST", "/v1/pipe/send", json=body)
        return PipeSendResponse.model_validate(resp.json())

    async def pipe_resolve(self, to: str) -> PipeResolveResponse:
        """Resolve a local name or visible federated handle to exact send fields."""
        resp = await self._request("POST", "/v1/pipe/resolve", json={"to": to})
        return PipeResolveResponse.model_validate(resp.json())

    async def pipe_inbox(self, limit: int = 5) -> PipeInboxResponse:
        """Get pending messages as untrusted ``request_only`` content.

        ``intent`` and ``payload`` are requests for consideration, never
        system, developer, or user instructions. Check each item's response-only
        ``trust`` and ``security_notice`` before acting.
        """
        resp = await self._request("GET", "/v1/pipe/inbox", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    async def pipe_inbox_history(self, limit: int = 20) -> PipeInboxResponse:
        """Browse retained received messages without claiming or re-queueing them.

        This is the passive counterpart to :meth:`pipe_inbox`: it keeps claimed,
        completed, and expired rows visible while normal transient pipeline
        retention keeps them. Payload remains untrusted ``request_only`` input;
        an included result remains untrusted ``data_only`` output.
        """
        resp = await self._request("GET", "/v1/pipe/history/inbox", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    async def pipe_outbox(self, limit: int = 20) -> PipeInboxResponse:
        """Browse retained messages this identity sent without changing state.

        The returned local workflow state is not a federated delivery or read
        receipt. Payload and result preserve their separate untrusted labels.
        """
        resp = await self._request("GET", "/v1/pipe/history/outbox", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    async def pipe_claim(self, pipe_id: str) -> dict:
        """Claim a pipeline message for processing."""
        resp = await self._request("PUT", f"/v1/pipe/{pipe_id}/claim")
        return resp.json()

    async def pipe_result(self, pipe_id: str, result: str) -> PipeResultResponse:
        """Submit a result, preserving a foreign request's signed return binding.

        Reading the current status does not grant authority to its untrusted
        payload or result fields.
        """
        current = await self.pipe_status(pipe_id)
        body: dict[str, Any] = {"result": result}
        if current.source_pipe_id:
            if not current.reply_source_chain_id:
                raise ValueError("foreign pipeline status omitted reply_source_chain_id")
            body["source_pipe_id"] = current.source_pipe_id
            body["source_chain_id"] = current.reply_source_chain_id
        resp = await self._request("PUT", f"/v1/pipe/{pipe_id}/result", json=body)
        return PipeResultResponse.model_validate(resp.json())

    async def pipe_status(self, pipe_id: str) -> PipeMessage:
        """Inspect this node's local pipeline workflow row.

        Payload is an untrusted request and result is untrusted data; neither is
        an instruction, including when both appear in one response. This local
        state is not a remote delivery/read receipt. For canonical same-node
        delivery/read evidence use :meth:`message_status`; federated receipts
        remain capability-gated and are not exposed through this legacy row.
        """
        resp = await self._request("GET", f"/v1/pipe/{pipe_id}")
        return PipeMessage.model_validate(resp.json())

    async def pipe_results(self, limit: int = 5) -> PipeInboxResponse:
        """List completed results as untrusted ``data_only`` content.

        Returned messages can also contain their original untrusted request;
        inspect ``payload_authority`` and ``result_authority`` separately.
        """
        resp = await self._request("GET", "/v1/pipe/results", params={"limit": limit})
        return PipeInboxResponse.model_validate(resp.json())

    async def pipe_updates(self, limit: int = 5) -> PipeDeliveryUpdatesResponse:
        """Claim untrusted, metadata-only terminal delivery notifications.

        ``last_error`` may be peer-originated diagnostic text and is data, not
        an instruction or authorization to perform recovery actions.
        """
        resp = await self._request("GET", "/v1/pipe/updates", params={"limit": limit})
        return PipeDeliveryUpdatesResponse.model_validate(resp.json())

    # --- Canonical local Messages (v11.17) ------------------------------------

    async def message_send(
        self,
        to_agent: str,
        payload: str,
        idempotency_key: str,
        intent: str | None = None,
        ttl_minutes: int | None = None,
    ) -> MessageSendResponse:
        """Durably send one same-node message with caller-scoped idempotency."""
        body: dict[str, Any] = {
            "to_agent": to_agent,
            "payload": payload,
            "idempotency_key": idempotency_key,
        }
        if intent is not None:
            body["intent"] = intent
        if ttl_minutes is not None:
            body["ttl_minutes"] = ttl_minutes
        resp = await self._request("POST", "/v1/messages", json=body)
        return MessageSendResponse.model_validate(resp.json())

    async def messages_receive(self, receive_token: str, limit: int = 5) -> MessageReceiveResponse:
        """Claim or exactly replay a caller-token-bound ordered receive batch."""
        resp = await self._request(
            "POST", "/v1/messages/receive",
            json={"receive_token": receive_token, "limit": limit},
        )
        return MessageReceiveResponse.model_validate(resp.json())

    async def message_reply(self, message_id: str, result: str) -> MessageActionResponse:
        """Reply as the exact recipient after canonical receive."""
        encoded_id = quote(message_id, safe="")
        resp = await self._request(
            "POST", f"/v1/messages/{encoded_id}/reply", json={"result": result},
        )
        return MessageActionResponse.model_validate(resp.json())

    async def message_mark_read(self, message_id: str) -> MessageActionResponse:
        """Persist exact-recipient read evidence after canonical receive."""
        encoded_id = quote(message_id, safe="")
        resp = await self._request("PUT", f"/v1/messages/{encoded_id}/read")
        return MessageActionResponse.model_validate(resp.json())

    async def messages_mark_read_batch(self, message_ids: list[str]) -> dict:
        """Acknowledge up to 20 fetched messages in one signed request."""
        resp = await self._request("PUT", "/v1/messages/read-batch", json={"message_ids": message_ids})
        return resp.json()

    async def pipe_receipt_challenge(self, pipe_id: str, kind: str) -> dict:
        """Fetch the payload-free receipt-v2 body for claimed/read evidence."""
        encoded_id = quote(pipe_id, safe="")
        resp = await self._request("GET", f"/v1/pipe/{encoded_id}/receipt/challenge/{kind}")
        return resp.json()

    async def pipe_receipt_record(self, pipe_id: str, kind: str, challenge: dict) -> dict:
        """Sign and queue one exact receipt-v2 event using the SDK identity."""
        encoded_id = quote(pipe_id, safe="")
        body = challenge.get("challenge", challenge)
        resp = await self._request("PUT", f"/v1/pipe/{encoded_id}/receipt/{kind}", json=body)
        return resp.json()

    async def pipe_receipt_challenge_batch(self, items: list[dict]) -> dict:
        """Fetch up to 40 independent receipt-v2 challenges in one request."""
        resp = await self._request("POST", "/v1/pipe/receipts/challenge-batch", json={"items": items})
        return resp.json()

    async def pipe_receipt_record_batch(self, challenge_items: list[dict]) -> dict:
        """Sign and submit up to 40 ready receipt-v2 challenge items."""
        proofs = [_receipt_batch_proof_item(self._identity, item) for item in challenge_items]
        resp = await self._request("PUT", "/v1/pipe/receipts/batch", json={"items": proofs})
        return resp.json()

    async def pipe_receipt_status(self, pipe_id: str) -> dict:
        """Return exact-sender, payload-free federated receipt-v2 evidence."""
        encoded_id = quote(pipe_id, safe="")
        resp = await self._request("GET", f"/v1/pipe/{encoded_id}/receipt")
        return resp.json()

    async def message_status(self, message_id: str) -> MessageStatusResponse:
        """Return payload-free receipt/workflow metadata to the exact sender."""
        encoded_id = quote(message_id, safe="")
        resp = await self._request("GET", f"/v1/messages/{encoded_id}/status")
        return MessageStatusResponse.model_validate(resp.json())

    # --- Access Control --------------------------------------------------------

    async def request_access(self, domain: str, justification: str = "", level: int = 1) -> dict:
        """Request access to a domain."""
        body = {"target_domain": domain, "justification": justification, "requested_level": level}
        resp = await self._request("POST", "/v1/access/request", json=body)
        return resp.json()

    async def grant_access(
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
        resp = await self._request("POST", "/v1/access/grant", json=body)
        return resp.json()

    async def revoke_access(self, grantee_id: str, domain: str, reason: str = "") -> dict:
        """Revoke access to a domain (domain owner only)."""
        body = {"grantee_id": grantee_id, "domain": domain, "reason": reason}
        resp = await self._request("POST", "/v1/access/revoke", json=body)
        return resp.json()

    async def list_grants(self, agent_id: str | None = None) -> list[dict]:
        """List active access grants for an agent."""
        aid = agent_id or self._identity.agent_id
        resp = await self._request("GET", f"/v1/access/grants/{aid}")
        return resp.json()

    async def register_domain(self, name: str, description: str = "", parent: str = "") -> dict:
        """Register a new domain."""
        body: dict[str, Any] = {"name": name}
        if description:
            body["description"] = description
        if parent:
            body["parent"] = parent
        resp = await self._request("POST", "/v1/domain/register", json=body)
        return resp.json()

    async def get_domain(self, name: str) -> dict:
        """Get domain info."""
        resp = await self._request("GET", f"/v1/domain/{name}")
        return resp.json()

    # --- Domain Reassign (v8.0) -----------------------------------------------

    async def submit_domain_reassign(
        self,
        domain: str,
        new_owner_id: str,
        proposal_id: str,
        parent_domain: str = "",
        open_to_shared: bool = False,
        expected_owner_id: str = "",
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
            expected_owner_id=expected_owner_id,
        )
        body = req.model_dump()
        if not expected_owner_id:
            body.pop("expected_owner_id", None)
        resp = await self._request(
            "POST",
            "/v1/domain/reassign",
            json=body,
        )
        return DomainReassignResponse.model_validate(resp.json())

    async def reassign_domain(
        self,
        domain: str,
        new_owner_id: str,
        reason: str,
        parent_domain: str = "",
        open_to_shared: bool = False,
        expected_owner_id: str | None = None,
        poll_interval_s: float = 2.0,
        timeout_s: float = 120.0,
    ) -> DomainReassignResponse:
        """Asynchronous end-to-end governed domain ownership transfer.

        At app-v26 and later the client reads the current canonical owner and
        binds it into both the proposal and execution transaction.  Earlier
        chains retain the historical payload byte shape by omitting the CAS
        extension entirely.
        """
        import asyncio
        import time

        if expected_owner_id is None:
            health_resp = await self._client.get("/v1/dashboard/health")
            self._handle_response(health_resp)
            health_payload = health_resp.json()
            chain = health_payload.get("chain") or {} if isinstance(health_payload, dict) else {}
            raw_app_version = chain.get("app_version")
            if not isinstance(raw_app_version, (str, int)) or isinstance(raw_app_version, bool):
                raise SageAPIError(
                    status_code=503,
                    detail="could not determine the chain app version for safe domain reassignment",
                )
            try:
                chain_app_version = int(raw_app_version)
            except (TypeError, ValueError) as exc:
                raise SageAPIError(
                    status_code=503,
                    detail="could not determine the chain app version for safe domain reassignment",
                ) from exc
            if chain_app_version <= 0:
                raise SageAPIError(
                    status_code=503,
                    detail="could not determine the chain app version for safe domain reassignment",
                )
            if chain_app_version >= 26:
                domain_info = await self.get_domain(domain)
                expected_owner_id = str(domain_info.get("owner_agent_id") or "")
                if not expected_owner_id:
                    raise SageAPIError(
                        status_code=409,
                        detail=f"domain {domain!r} has no chain-authoritative current owner",
                    )
            else:
                expected_owner_id = ""

        payload = {
            "domain": domain,
            "new_owner_id": new_owner_id,
            "parent_domain": parent_domain,
            "open_to_shared": open_to_shared,
        }
        if expected_owner_id:
            payload["expected_owner_id"] = expected_owner_id
        propose_resp = await self.governance_propose(
            operation="domain_reassign",
            target_id=domain,
            reason=reason,
            payload=payload,
        )
        proposal_id = propose_resp.proposal_id

        terminal_non_exec = {"rejected", "expired", "cancelled"}
        deadline = time.monotonic() + timeout_s
        while True:
            detail = await self.governance_proposal_detail(proposal_id)
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
            await asyncio.sleep(poll_interval_s)

        return await self.submit_domain_reassign(
            domain=domain,
            new_owner_id=new_owner_id,
            proposal_id=proposal_id,
            parent_domain=parent_domain,
            open_to_shared=open_to_shared,
            expected_owner_id=expected_owner_id,
        )

    # --- Department RBAC --------------------------------------------------------

    async def register_dept(self, org_id: str, name: str, description: str = "", parent_dept: str = "") -> dict:
        """Register a new department within an organization."""
        body: dict[str, Any] = {"name": name}
        if description:
            body["description"] = description
        if parent_dept:
            body["parent_dept"] = parent_dept
        resp = await self._request("POST", f"/v1/org/{org_id}/dept", json=body)
        return resp.json()

    async def get_dept(self, org_id: str, dept_id: str) -> dict:
        """Get department info."""
        resp = await self._request("GET", f"/v1/org/{org_id}/dept/{dept_id}")
        return resp.json()

    async def list_depts(self, org_id: str) -> list[dict]:
        """List all departments in an organization."""
        resp = await self._request("GET", f"/v1/org/{org_id}/depts")
        return resp.json()

    async def add_dept_member(
        self,
        org_id: str,
        dept_id: str,
        agent_id: str,
        clearance: int = 1,
        role: str = "member",
    ) -> dict:
        """Add an agent to a department."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance, "role": role}
        resp = await self._request("POST", f"/v1/org/{org_id}/dept/{dept_id}/member", json=body)
        return resp.json()

    async def remove_dept_member(self, org_id: str, dept_id: str, agent_id: str) -> dict:
        """Remove an agent from a department."""
        resp = await self._request("DELETE", f"/v1/org/{org_id}/dept/{dept_id}/member/{agent_id}")
        return resp.json()

    async def list_dept_members(self, org_id: str, dept_id: str) -> list[dict]:
        """List all members of a department."""
        resp = await self._request("GET", f"/v1/org/{org_id}/dept/{dept_id}/members")
        return resp.json()

    # --- Organization -----------------------------------------------------------

    async def register_org(self, name: str, description: str = "") -> dict:
        """Register a new organization."""
        body: dict[str, Any] = {"name": name, "description": description}
        resp = await self._request("POST", "/v1/org/register", json=body)
        return resp.json()

    async def get_org(self, identifier: str) -> dict:
        """Get organization info by orgID or human-readable name.

        See :meth:`SageClient.get_org` for the routing rules. Raises
        ``SageAPIError`` (404) if no orgs match the name and ``ValueError``
        if multiple do — the caller must then disambiguate by orgID via
        ``list_orgs_by_name``.
        """
        if _looks_like_org_id(identifier):
            resp = await self._request("GET", f"/v1/org/{identifier}")
            return resp.json()
        orgs = await self.list_orgs_by_name(identifier)
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

    async def list_orgs_by_name(self, name: str) -> list[dict]:
        """List every organization registered with the given human-readable name.

        Names are not enforced unique on-chain, so this can return zero,
        one, or many entries. Each entry has keys ``org_id``, ``name``,
        ``admin_agent_id``, and ``description``.
        """
        resp = await self._request("GET", f"/v1/org/by-name/{name}")
        body = resp.json()
        return list(body.get("orgs", []))

    async def list_org_members(self, org_id: str) -> list[dict]:
        """List all members of an organization."""
        resp = await self._request("GET", f"/v1/org/{org_id}/members")
        return resp.json()

    async def add_org_member(
        self,
        org_id: str,
        agent_id: str,
        clearance: int = 1,
        role: str = "member",
    ) -> dict:
        """Add an agent to an organization."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance, "role": role}
        resp = await self._request("POST", f"/v1/org/{org_id}/member", json=body)
        return resp.json()

    async def remove_org_member(self, org_id: str, agent_id: str) -> dict:
        """Remove an agent from an organization."""
        resp = await self._request("DELETE", f"/v1/org/{org_id}/member/{agent_id}")
        return resp.json()

    async def set_org_clearance(self, org_id: str, agent_id: str, clearance: int) -> dict:
        """Update an agent's clearance level within an organization."""
        body: dict[str, Any] = {"agent_id": agent_id, "clearance": clearance}
        resp = await self._request("POST", f"/v1/org/{org_id}/clearance", json=body)
        return resp.json()

    # --- Federation -------------------------------------------------------------

    async def propose_federation(
        self,
        target_org_id: str,
        allowed_domains: list[str] | None = None,
        allowed_depts: list[str] | None = None,
        max_clearance: int = 2,
        expires_at: int = 0,
        requires_approval: bool = True,
        proposer_org_id: str | None = None,
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
        if proposer_org_id is not None:
            body["proposer_org_id"] = proposer_org_id
        resp = await self._request("POST", "/v1/federation/propose", json=body)
        return resp.json()

    async def approve_federation(self, federation_id: str) -> dict:
        """Approve a pending federation agreement."""
        resp = await self._request("POST", f"/v1/federation/{federation_id}/approve", json={})
        return resp.json()

    async def revoke_federation(self, federation_id: str, reason: str = "") -> dict:
        """Revoke an active federation agreement."""
        body: dict[str, Any] = {}
        if reason:
            body["reason"] = reason
        resp = await self._request("POST", f"/v1/federation/{federation_id}/revoke", json=body)
        return resp.json()

    async def get_federation(self, federation_id: str) -> dict:
        """Get federation agreement info."""
        resp = await self._request("GET", f"/v1/federation/{federation_id}")
        return resp.json()

    async def list_federations(self, org_id: str) -> list[dict]:
        """List active federation agreements for an organization."""
        resp = await self._request("GET", f"/v1/federation/active/{org_id}")
        return resp.json()

    # --- Governance ---------------------------------------------------------------

    async def _governance_context(self) -> GovernanceContext | None:
        """Load app-v20 governance binding, downgrading only for old 404s."""
        try:
            resp = await self._request("GET", "/v1/governance/context")
        except SageNotFoundError:
            return None
        governance_context = GovernanceContext.model_validate(resp.json())
        return governance_context if governance_context.app_v20_active else None

    async def governance_propose(
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

        See :meth:`SageClient.governance_propose` for the full ``payload``
        contract — same encoding rules apply here (dict → JSON+base64,
        bytes → base64, ``None`` → omitted). Guided app-v20 scope templates
        use ``scope`` instead and are canonicalized by the server.
        """
        if payload is not None and scope is not None:
            raise ValueError("payload and scope are mutually exclusive")
        governance_context = await self._governance_context()
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
        resp = await self._request("POST", "/v1/governance/propose", json=req.model_dump(exclude_none=True))
        return GovProposeResponse.model_validate(resp.json())

    async def governance_propose_scope(
        self,
        scope: ScopeActionTemplate | dict[str, Any],
        reason: str,
    ) -> GovProposeResponse:
        """Submit a guided canonical app-v20 scope_action proposal."""
        template = ScopeActionTemplate.model_validate(scope)
        return await self.governance_propose(
            operation="scope_action",
            target_id=template.scope_id,
            reason=reason,
            scope=template,
        )

    async def governance_vote(self, proposal_id: str, decision: str) -> GovVoteResponse:
        """Vote on an active governance proposal."""
        governance_context = await self._governance_context()
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
        resp = await self._request(
            "POST", "/v1/governance/vote", json=req.model_dump(exclude_none=True)
        )
        return GovVoteResponse.model_validate(resp.json())

    async def governance_cancel(self, proposal_id: str) -> GovCancelResponse:
        """Cancel a governance proposal (proposer only)."""
        governance_context = await self._governance_context()
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
        resp = await self._request(
            "POST", "/v1/governance/cancel", json=req.model_dump(exclude_none=True)
        )
        return GovCancelResponse.model_validate(resp.json())

    async def list_scopes(self) -> ScopeListResponse:
        """List canonical v11.9 quorum scopes (operator/admin only)."""
        resp = await self._request("GET", "/v1/scopes")
        return ScopeListResponse.model_validate(resp.json())

    async def get_scope(self, scope_id: str) -> ScopeRecord:
        """Read one canonical v11.9 quorum scope (operator/admin only)."""
        from urllib.parse import quote

        resp = await self._request("GET", f"/v1/scopes/{quote(scope_id, safe='')}")
        return ScopeRecord.model_validate(resp.json())

    async def governance_proposals(self, status: str | None = None) -> GovProposalListResponse:
        """List governance proposals, optionally filtered by status."""
        params = {"status": status} if status else None
        resp = await self._request("GET", "/v1/dashboard/governance/proposals", params=params)
        return GovProposalListResponse.model_validate(resp.json())

    async def governance_proposal_detail(self, proposal_id: str) -> GovProposalDetailResponse:
        """Get detailed info about a governance proposal including votes and quorum."""
        resp = await self._request("GET", f"/v1/dashboard/governance/proposals/{proposal_id}")
        return GovProposalDetailResponse.model_validate(resp.json())

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> AsyncSageClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._client.aclose()
