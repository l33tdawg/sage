"""Tests for the v8.0 domain-reassign SDK surface.

Covers:
  - submit_domain_reassign (sync + async): request body shape, response parse.
  - governance_propose payload kwarg: dict / bytes / None encoding.
  - reassign_domain: happy path, rejection path, timeout path.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
import respx

from sage_sdk.exceptions import SageAPIError
from sage_sdk.models import DomainReassignResponse


BASE_URL = "http://localhost:8080"

DOMAIN = "acme.engineering"
NEW_OWNER = "b" * 64
PROPOSAL_ID = "c" * 64
TX_HASH_HEX = "deadbeef" * 8


# --- Fixtures ------------------------------------------------------------------


@pytest.fixture
def client(agent_identity):
    from sage_sdk.client import SageClient

    return SageClient(base_url=BASE_URL, identity=agent_identity)


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE_URL, assert_all_called=False) as respx_mock:
        respx_mock.get("/v1/governance/context").mock(
            return_value=httpx.Response(200, json={
                "validator_id": "validator-a",
                "governance_domain": "sage.governance",
                "app_v20_active": True,
            })
        )
        yield respx_mock


# --- submit_domain_reassign: sync ---------------------------------------------


def test_submit_domain_reassign_posts_expected_body(client, mock_api):
    route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(
            200,
            json={"tx_hash": TX_HASH_HEX, "purged_grants": 7},
        )
    )
    resp = client.submit_domain_reassign(
        domain=DOMAIN,
        new_owner_id=NEW_OWNER,
        proposal_id=PROPOSAL_ID,
        parent_domain="acme",
        open_to_shared=True,
    )

    assert isinstance(resp, DomainReassignResponse)
    assert resp.tx_hash == TX_HASH_HEX
    assert resp.purged_grants == 7

    # Inspect what the SDK actually sent over the wire.
    assert route.called
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert sent == {
        "domain": DOMAIN,
        "new_owner_id": NEW_OWNER,
        "proposal_id": PROPOSAL_ID,
        "parent_domain": "acme",
        "open_to_shared": True,
    }


def test_submit_domain_reassign_defaults_parent_and_shared(client, mock_api):
    route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(
            200, json={"tx_hash": TX_HASH_HEX, "purged_grants": 0}
        )
    )
    client.submit_domain_reassign(
        domain=DOMAIN, new_owner_id=NEW_OWNER, proposal_id=PROPOSAL_ID
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert sent["parent_domain"] == ""
    assert sent["open_to_shared"] is False


def test_submit_domain_reassign_propagates_errors(client, mock_api):
    mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(
            403,
            json={
                "type": "about:blank",
                "title": "Forbidden",
                "status": 403,
                "detail": "shared domain not ownable",
            },
        )
    )
    with pytest.raises(Exception) as exc:  # noqa: PT011 -- SageAuthError subclass
        client.submit_domain_reassign(
            domain=DOMAIN, new_owner_id=NEW_OWNER, proposal_id=PROPOSAL_ID
        )
    assert "shared domain not ownable" in str(exc.value)


# --- governance_propose: payload encoding -------------------------------------


def _gov_propose_response() -> httpx.Response:
    return httpx.Response(
        201,
        json={
            "proposal_id": PROPOSAL_ID,
            "tx_hash": TX_HASH_HEX,
            "status": "voting",
        },
    )


def test_governance_propose_payload_dict_is_json_then_base64(client, mock_api):
    route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    payload = {
        "domain": DOMAIN,
        "new_owner_id": NEW_OWNER,
        "parent_domain": "",
        "open_to_shared": False,
    }
    client.governance_propose(
        operation="domain_reassign",
        target_id=DOMAIN,
        reason="recovery",
        payload=payload,
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert "payload" in sent
    raw = base64.b64decode(sent["payload"])
    assert json.loads(raw.decode("utf-8")) == payload
    # Confirm compact JSON encoding (no spaces).
    assert b" " not in raw


def test_governance_propose_payload_bytes_is_base64_directly(client, mock_api):
    route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    blob = b"\x00\x01\x02raw-bytes\xff"
    client.governance_propose(
        operation="domain_reassign",
        target_id=DOMAIN,
        reason="recovery",
        payload=blob,
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert base64.b64decode(sent["payload"]) == blob


def test_governance_propose_payload_none_is_omitted(client, mock_api):
    route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    client.governance_propose(
        operation="add_validator",
        target_id="some-validator",
        reason="onboard",
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert "payload" not in sent


def test_governance_propose_payload_invalid_type_raises(client, mock_api):
    mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    with pytest.raises(TypeError):
        client.governance_propose(
            operation="domain_reassign",
            target_id=DOMAIN,
            reason="recovery",
            payload=12345,  # type: ignore[arg-type]
        )


# --- reassign_domain: end-to-end orchestration --------------------------------


def _proposal_detail(status: str) -> dict:
    return {
        "proposal": {
            "proposal_id": PROPOSAL_ID,
            "operation": "domain_reassign",
            "target_agent_id": DOMAIN,
            "proposer_id": "a" * 64,
            "status": status,
            "created_height": 10,
            "expiry_height": 100,
            "executed_height": 20 if status == "executed" else None,
            "reason": "recovery",
        },
        "votes": [],
        "quorum_progress": None,
    }


def test_reassign_domain_happy_path(client, mock_api, monkeypatch):
    # No real sleeping in tests.
    monkeypatch.setattr("time.sleep", lambda _s: None)

    propose_route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    # First poll: still voting. Second poll: executed.
    detail_route = mock_api.get(
        f"/v1/dashboard/governance/proposals/{PROPOSAL_ID}"
    ).mock(
        side_effect=[
            httpx.Response(200, json=_proposal_detail("voting")),
            httpx.Response(200, json=_proposal_detail("executed")),
        ]
    )
    reassign_route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(
            200, json={"tx_hash": TX_HASH_HEX, "purged_grants": 3}
        )
    )

    result = client.reassign_domain(
        domain=DOMAIN,
        new_owner_id=NEW_OWNER,
        reason="recovery",
        open_to_shared=True,
        poll_interval_s=0.01,
        timeout_s=5.0,
    )
    assert result.tx_hash == TX_HASH_HEX
    assert result.purged_grants == 3

    # Verify the propose body carried the right payload.
    propose_body = json.loads(propose_route.calls.last.request.content.decode("utf-8"))
    assert propose_body["operation"] == "domain_reassign"
    assert propose_body["target_id"] == DOMAIN
    decoded_payload = json.loads(
        base64.b64decode(propose_body["payload"]).decode("utf-8")
    )
    assert decoded_payload == {
        "domain": DOMAIN,
        "new_owner_id": NEW_OWNER,
        "parent_domain": "",
        "open_to_shared": True,
    }

    # Verify we polled until executed (2 calls), then submitted with the proposal_id.
    assert detail_route.call_count == 2
    submit_body = json.loads(reassign_route.calls.last.request.content.decode("utf-8"))
    assert submit_body["proposal_id"] == PROPOSAL_ID
    assert submit_body["open_to_shared"] is True


@pytest.mark.parametrize("terminal_status", ["rejected", "expired", "cancelled"])
def test_reassign_domain_non_executed_terminal_raises(
    client, mock_api, monkeypatch, terminal_status
):
    monkeypatch.setattr("time.sleep", lambda _s: None)

    mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    mock_api.get(
        f"/v1/dashboard/governance/proposals/{PROPOSAL_ID}"
    ).mock(return_value=httpx.Response(200, json=_proposal_detail(terminal_status)))
    # Make sure submit_domain_reassign never gets called.
    submit_route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(500, text="must not be called")
    )

    with pytest.raises(SageAPIError) as exc:
        client.reassign_domain(
            domain=DOMAIN,
            new_owner_id=NEW_OWNER,
            reason="recovery",
            poll_interval_s=0.01,
            timeout_s=5.0,
        )
    assert terminal_status in str(exc.value)
    assert not submit_route.called


def test_reassign_domain_timeout_raises(client, mock_api, monkeypatch):
    # Fake clock: monotonic advances 5s per call, sleep is a no-op.
    fake_now = {"t": 0.0}

    def fake_monotonic() -> float:
        fake_now["t"] += 5.0
        return fake_now["t"]

    monkeypatch.setattr("time.monotonic", fake_monotonic)
    monkeypatch.setattr("time.sleep", lambda _s: None)

    mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    # Always voting — never terminal.
    detail_route = mock_api.get(
        f"/v1/dashboard/governance/proposals/{PROPOSAL_ID}"
    ).mock(return_value=httpx.Response(200, json=_proposal_detail("voting")))
    submit_route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(500, text="must not be called")
    )

    with pytest.raises(SageAPIError) as exc:
        client.reassign_domain(
            domain=DOMAIN,
            new_owner_id=NEW_OWNER,
            reason="recovery",
            poll_interval_s=0.01,
            timeout_s=10.0,
        )
    msg = str(exc.value)
    assert "timed out" in msg
    assert "voting" in msg
    assert detail_route.called
    assert not submit_route.called


# --- Async parity --------------------------------------------------------------


import pytest_asyncio


@pytest_asyncio.fixture
async def async_client(agent_identity):
    from sage_sdk.async_client import AsyncSageClient

    client = AsyncSageClient(base_url=BASE_URL, identity=agent_identity)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_async_submit_domain_reassign(async_client, mock_api):
    route = mock_api.post("/v1/domain/reassign").mock(
        return_value=httpx.Response(
            200, json={"tx_hash": TX_HASH_HEX, "purged_grants": 2}
        )
    )
    resp = await async_client.submit_domain_reassign(
        domain=DOMAIN,
        new_owner_id=NEW_OWNER,
        proposal_id=PROPOSAL_ID,
        open_to_shared=False,
    )
    assert resp.tx_hash == TX_HASH_HEX
    assert resp.purged_grants == 2
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert sent["domain"] == DOMAIN
    assert sent["proposal_id"] == PROPOSAL_ID


@pytest.mark.asyncio
async def test_async_governance_propose_payload_dict(async_client, mock_api):
    route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    payload = {"domain": DOMAIN, "new_owner_id": NEW_OWNER, "open_to_shared": True}
    await async_client.governance_propose(
        operation="domain_reassign",
        target_id=DOMAIN,
        reason="recovery",
        payload=payload,
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert json.loads(base64.b64decode(sent["payload"]).decode("utf-8")) == payload


@pytest.mark.asyncio
async def test_async_governance_propose_payload_none_omits(async_client, mock_api):
    route = mock_api.post("/v1/governance/propose").mock(
        return_value=_gov_propose_response()
    )
    await async_client.governance_propose(
        operation="add_validator", target_id="v1", reason="onboard"
    )
    sent = json.loads(route.calls.last.request.content.decode("utf-8"))
    assert "payload" not in sent
