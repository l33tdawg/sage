"""Synthetic signed binary requests; no network, photos, or device keys."""

import hashlib
import json

import httpx
import pytest
from pydantic import ValidationError

from sage_sdk.client import SageClient
from sage_sdk.async_client import AsyncSageClient
from sage_sdk.exceptions import SageAPIError, SageNotFoundError
from sage_sdk.models import PrivateMediaMetadata
from .test_workflow_journal import client_with_transport, invoke, verify, IDENTIFIER, BASE


PATH = "/v1/private-media/" + IDENTIFIER
JPEG = bytes([255,216,255,192,0,11,8,2,208,5,0,1,1,17,0,255,218,0,8,1,1,0,0,63,0]) + b"synthetic-canary\xff\xd9"
MAXIMUM = 2097152


def metadata(actor="a" * 64, jpeg=JPEG):
    return dict(schema="sage.private-media.v1", agent_id=actor, object_id=IDENTIFIER,
                revision=1, length=len(jpeg), digest=hashlib.sha256(jpeg).hexdigest())


class Stream(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.reads = 0
        self.closed = False

    def __iter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def __aiter__(self):
        for chunk in self:
            yield chunk

    def close(self):
        self.closed = True

    async def aclose(self):
        self.close()


def bound_headers(body, content_type, actor="a"*64):
    return {"content-type":content_type,"x-sage-private-media-schema":"sage.private-media.v1",
            "cache-control":"no-store","x-content-type-options":"nosniff",
            "content-length":str(len(body)),"x-sage-agent-id":actor,"x-sage-media-id":IDENTIFIER,
            "x-sage-media-sha256":hashlib.sha256(body).hexdigest()}


def response(body, content_type, status=200, headers=None, actor="a"*64):
    return httpx.Response(status, headers={**bound_headers(body,content_type,actor), **(headers or {})}, stream=Stream([body]))


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_signed_original_roundtrip_fresh_nonce(agent_identity, asynchronous):
    calls = []
    def handler(request):
        calls.append(request)
        verify(request, agent_identity)
        assert str(request.url) == BASE + PATH
        assert request.headers["accept-encoding"] == "identity"
        if request.method == "PUT":
            assert request.content == JPEG
            assert request.headers["content-type"] == "image/jpeg"
            return response(json.dumps(metadata(agent_identity.agent_id)).encode(), "application/json")
        assert request.content == b""
        return response(JPEG, "image/jpeg", actor=agent_identity.agent_id)
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        written = await invoke(client.private_media_put, IDENTIFIER, JPEG)
        assert written.model_dump() == metadata(agent_identity.agent_id)
        assert json.loads(written.model_dump_json()) == written.model_dump()
        assert await invoke(client.private_media_get, IDENTIFIER) == JPEG
    assert len({request.headers["x-nonce"] for request in calls}) == 2


@pytest.mark.parametrize("field,value", [
    ("schema", "other"), ("schema", None), ("agent_id", "A"*64), ("agent_id", 1),
    ("object_id", IDENTIFIER.upper()), ("object_id", "00000000-0000-0000-0000-000000000000"),
    ("revision", True), ("revision", 1.0), ("revision", "1"), ("revision", 2),
    ("length", True), ("length", 1.0), ("length", "3"), ("length", 0), ("length", MAXIMUM+1),
    ("digest", "A"*64), ("digest", None), ("extra", "forbidden"),
])
def test_metadata_strict(field, value):
    with pytest.raises(ValidationError):
        PrivateMediaMetadata.model_validate({**metadata(), field:value})


@pytest.mark.parametrize("missing", list(metadata()))
def test_metadata_requires_every_field(missing):
    record = metadata()
    del record[missing]
    with pytest.raises(ValidationError):
        PrivateMediaMetadata.model_validate(record)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_inputs_reject_before_transport(agent_identity, asynchronous):
    def handler(request):
        pytest.fail("invalid input reached transport")
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        for identifier in (None, 12, "../secret", IDENTIFIER.upper(), "0"*36):
            for method, args in ((client.private_media_get, (identifier,)), (client.private_media_put, (identifier, JPEG))):
                with pytest.raises(ValueError):
                    await invoke(method, *args)
        for body in (None, "synthetic-canary", bytearray(JPEG), b"", b"\xff\xd8\xff\xd9", b"x"*(MAXIMUM+1), JPEG[:9]+b"\xff\xff"+JPEG[11:]):
            with pytest.raises(ValueError) as error:
                await invoke(client.private_media_put, IDENTIFIER, body)
            assert "synthetic-canary" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("status", [301,401,403,404,409,413,415,429,503])
async def test_status_is_preserved_without_error_body_read(agent_identity, asynchronous, status):
    stream = Stream([b"synthetic-canary" * 100000])
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers={"location":"http://evil.invalid/","x-sage-private-media-schema":"sage.private-media.v1"}, stream=stream)
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        with pytest.raises(SageAPIError) as error:
            await invoke(client.private_media_get, IDENTIFIER)
    assert error.value.status_code == status
    assert isinstance(error.value, SageNotFoundError) == (status == 404)
    assert "synthetic-canary" not in str(error.value)
    assert stream.reads == 0 and stream.closed and len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("headers", [
    {"content-type":"text/html"}, {"content-type":"image/jpeg; charset=utf-8"},
    {"content-type":"image/jpeg", "content-encoding":"gzip"},
    {"content-type":"image/jpeg", "content-length":str(MAXIMUM+1)},
    {"content-type":"image/jpeg", "content-length":"-1"},
    {"content-type":"image/jpeg", "content-length":"2, 2"},
    {},
])
async def test_header_rejection_before_read(agent_identity, asynchronous, headers):
    stream = Stream([JPEG])
    async with client_with_transport(agent_identity, asynchronous, lambda request: httpx.Response(200, headers=headers, stream=stream)) as client:
        with pytest.raises(ValueError):
            await invoke(client.private_media_get, IDENTIFIER)
    assert stream.reads == 0 and stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_stream_limit_and_maximum_original(agent_identity, asynchronous):
    maximum = JPEG[:-2] + b"x" * (MAXIMUM-len(JPEG)) + JPEG[-2:]
    async with client_with_transport(agent_identity, asynchronous, lambda request: response(maximum,"image/jpeg",actor=agent_identity.agent_id)) as client:
        assert await invoke(client.private_media_get,IDENTIFIER) == maximum
    for method, args, limit, content_type in (
        ("private_media_get", (IDENTIFIER,), MAXIMUM, "image/jpeg"),
        ("private_media_put", (IDENTIFIER,JPEG), 4096, "application/json"),
    ):
        stream = Stream([b"x"*limit,b"x",b"must-not-read"])
        headers=bound_headers(b"x"*limit,content_type,agent_identity.agent_id)
        async with client_with_transport(agent_identity, asynchronous, lambda request: httpx.Response(200,headers=headers,stream=stream)) as client:
            with pytest.raises(ValueError):
                await invoke(getattr(client,method),*args)
        assert stream.reads == 2 and stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_metadata_binding_and_redacted_parse_failures(agent_identity, asynchronous):
    good = metadata(agent_identity.agent_id)
    bad = [json.dumps({**good,field:value}).encode() for field,value in (
        ("agent_id","b"*64),("object_id","d837d957-c686-4e53-894f-c050e84a3ec8"),
        ("length",1),("digest","f"*64),("extra","synthetic-canary"))]
    bad += [b'{"schema":"synthetic-canary","schema":"duplicate"}',b"synthetic-canary"]
    for body in bad:
        async with client_with_transport(agent_identity, asynchronous, lambda request: response(body,"application/json")) as client:
            with pytest.raises(ValueError) as error:
                await invoke(client.private_media_put,IDENTIFIER,JPEG)
            assert "synthetic-canary" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_transport_error_and_short_body(agent_identity, asynchronous):
    def handler(request):
        raise httpx.ReadTimeout("synthetic-canary",request=request)
    async with client_with_transport(agent_identity, asynchronous, handler) as client:
        with pytest.raises(SageAPIError) as error:
            await invoke(client.private_media_get,IDENTIFIER)
        assert "synthetic-canary" not in str(error.value)
    async with client_with_transport(agent_identity, asynchronous, lambda request: response(JPEG,"image/jpeg",headers={"content-length":str(len(JPEG)+1)},actor=agent_identity.agent_id)) as client:
        with pytest.raises(ValueError):
            await invoke(client.private_media_get,IDENTIFIER)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_proxy_optout(agent_identity, asynchronous, monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9999")
    client = (AsyncSageClient if asynchronous else SageClient)(BASE,agent_identity,trust_env=False)
    assert client._client.trust_env is False
    assert client._client._mounts == {}
    if asynchronous:
        await client.close()
    else:
        client.__exit__(None,None,None)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("header,value", [
    ("x-sage-private-media-schema",None),("x-sage-private-media-schema","other"),
    ("x-sage-agent-id","b"*64),("x-sage-media-id","d837d957-c686-4e53-894f-c050e84a3ec8"),
    ("x-sage-media-sha256","f"*64),("x-sage-media-sha256","A"*64),
    ("content-length",None),("cache-control","public"),("x-content-type-options",None),
])
async def test_get_response_binding(agent_identity,asynchronous,header,value):
    headers=bound_headers(JPEG,"image/jpeg",agent_identity.agent_id)
    if value is None:
        del headers[header]
    else:
        headers[header]=value
    async with client_with_transport(agent_identity,asynchronous,lambda request:httpx.Response(200,headers=headers,stream=Stream([JPEG]))) as client:
        with pytest.raises(ValueError):
            await invoke(client.private_media_get,IDENTIFIER)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_unknown_route_404_is_unavailable(agent_identity,asynchronous):
    stream=Stream([b"not found"])
    async with client_with_transport(agent_identity,asynchronous,lambda request:httpx.Response(404,stream=stream)) as client:
        with pytest.raises(SageAPIError) as error:
            await invoke(client.private_media_get,IDENTIFIER)
    assert not isinstance(error.value,SageNotFoundError)
    assert error.value.status_code==503
    assert stream.reads==0 and stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_prebuffered_mock_transport(agent_identity,asynchronous):
    def handler(request):
        body = JPEG if request.method == "GET" else json.dumps(metadata(agent_identity.agent_id)).encode()
        content_type = "image/jpeg" if request.method == "GET" else "application/json"
        return httpx.Response(200,content=body,headers=bound_headers(body,content_type,agent_identity.agent_id))
    async with client_with_transport(agent_identity,asynchronous,handler) as client:
        assert await invoke(client.private_media_get,IDENTIFIER)==JPEG
        assert (await invoke(client.private_media_put,IDENTIFIER,JPEG)).length==len(JPEG)
