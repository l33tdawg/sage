"""Synthetic guarded workflow requests; no network or runtime activation."""

import json

import httpx
import pytest
from pydantic import ValidationError

from sage_sdk.exceptions import SageAPIError
from sage_sdk.models import WorkflowJournalGuard, WorkflowJournalRecord
from .test_workflow_journal import client_with_transport, invoke, verify, record, IDENTIFIER, PATH


CONTROL = "9726bde5-5f3d-49f0-b420-33a16875b59c"
MAXIMUM = 9007199254740991


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("typed", [False, True])
async def test_guard_exact_signed_body(agent_identity, asynchronous, typed):
    condition = dict(record_id=CONTROL, expected_revision=MAXIMUM)
    guard = WorkflowJournalGuard.model_validate(condition) if typed else condition
    calls = []
    def handler(request):
        calls.append(request)
        verify(request, agent_identity)
        assert request.method == "PUT" and request.url.path == PATH
        assert json.loads(request.content) == dict(kind="conversation_session", expected_revision=0, payload={"text":"synthetic"}, guard=condition)
        return httpx.Response(200,json={**record(agent_identity.agent_id),"kind":"conversation_session"})
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        for _ in range(2):
            result = await invoke(client.workflow_put,IDENTIFIER,"conversation_session",0,{"text":"synthetic"},guard=guard)
            assert result.kind == "conversation_session"
    assert len(calls)==2
    assert calls[0].headers["X-Nonce"] != calls[1].headers["X-Nonce"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("kind", ["mesh_outbound","mesh_inbound","public_proposal","conversation_control","conversation_session"])
async def test_omitted_guard_preserves_three_fields(agent_identity,asynchronous,kind):
    def handler(request):
        verify(request,agent_identity)
        assert json.loads(request.content)==dict(kind=kind,expected_revision=0,payload={})
        return httpx.Response(200,json={**record(agent_identity.agent_id),"kind":kind})
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        assert (await invoke(client.workflow_put,IDENTIFIER,kind,0,{})).kind==kind


INVALID = [None,True,1,"guard",[],{},
           {"record_id":CONTROL},{"expected_revision":1},
           {"record_id":CONTROL,"expected_revision":1,"actor":"a"*64},
           *[{"record_id":CONTROL,"expected_revision":revision} for revision in (None,True,1.0,"1",0,-1,MAXIMUM+1)],
           *[{"record_id":identifier,"expected_revision":1} for identifier in (None,1,CONTROL.upper(),"../control","00000000-0000-0000-0000-000000000000")]]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("guard", INVALID)
async def test_invalid_guard_never_reaches_transport(agent_identity,asynchronous,guard):
    def handler(request):
        pytest.fail("invalid guard reached transport")
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        with pytest.raises(ValueError):
            await invoke(client.workflow_put,IDENTIFIER,"conversation_session",0,{},guard=guard)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_self_and_wrong_target_kind_rejected(agent_identity,asynchronous):
    def handler(request):
        pytest.fail("invalid guard target reached transport")
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        with pytest.raises(ValueError):
            await invoke(client.workflow_put,IDENTIFIER,"conversation_session",0,{},guard=dict(record_id=IDENTIFIER,expected_revision=1))
        for kind in ("conversation_control","mesh_outbound","mesh_inbound","public_proposal"):
            with pytest.raises(ValueError):
                await invoke(client.workflow_put,IDENTIFIER,kind,0,{},guard=dict(record_id=CONTROL,expected_revision=1))


@pytest.mark.parametrize("revision", [1,MAXIMUM])
def test_guard_model_boundary(revision):
    guard = WorkflowJournalGuard(record_id=CONTROL,expected_revision=revision)
    assert guard.model_dump()==dict(record_id=CONTROL,expected_revision=revision)


@pytest.mark.parametrize("kind", ["conversation_control","conversation_session"])
def test_response_accepts_new_kinds_without_guard_field(kind):
    value = {**record(),"kind":kind}
    assert WorkflowJournalRecord.model_validate(value).model_dump()==value
    with pytest.raises(ValidationError):
        WorkflowJournalRecord.model_validate({**value,"guard":dict(record_id=CONTROL,expected_revision=1)})


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("status", [409,503])
async def test_guard_failure_no_retry_or_unguarded_fallback(agent_identity,asynchronous,status):
    calls=[]
    def handler(request):
        calls.append(request)
        verify(request,agent_identity)
        assert "guard" in json.loads(request.content)
        return httpx.Response(status,json={"detail":"Workflow journal unavailable"})
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        with pytest.raises(SageAPIError) as caught:
            await invoke(client.workflow_put,IDENTIFIER,"conversation_session",0,{},guard=dict(record_id=CONTROL,expected_revision=1))
    assert caught.value.status_code==status
    assert len(calls)==1
