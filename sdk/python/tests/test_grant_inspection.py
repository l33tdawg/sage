import copy
import httpx
import pytest
import respx

from sage_sdk import AgentIdentity, SageClient, AsyncSageClient
from sage_sdk.grant_inspection import DirectGrantsSnapshot
from sage_sdk.exceptions import SageAPIError

BASE = "http://localhost:8080"
TARGET = "b" * 64


def snapshot(identity):
    return dict(schema="sage.direct-grants-snapshot.v1", caller_id=identity.agent_id,
                target_agent_id=TARGET, chain_domain="c" * 64, height=42,
                app_hash="d" * 64, root_principal_id="e" * 64, root_generation=2,
                evaluated_at=100, coverage="direct-grants-for-target", complete=True,
                grants=[dict(domain="test", grantee_id=TARGET, granter_id="e" * 64,
                             access_level=1, expires_at=0)])


def test_sync_inspection_signed_read_and_true_empty():
    identity = AgentIdentity.generate()
    with respx.mock(base_url=BASE) as mock:
        data = snapshot(identity); data["grants"] = []
        route = mock.get('/v1/access/grants-snapshot/' + TARGET).respond(200, json=data)
        with SageClient(base_url=BASE, identity=identity) as client:
            result = client.inspect_direct_grants(TARGET)
        assert result.complete is True and result.grants == []
        assert route.calls.last.request.headers['X-Agent-ID'] == identity.agent_id
        assert route.calls.last.request.method == 'GET'


@pytest.mark.asyncio
async def test_async_inspection_signed_read():
    identity = AgentIdentity.generate()
    with respx.mock(base_url=BASE) as mock:
        route = mock.get('/v1/access/grants-snapshot/' + TARGET).respond(200, json=snapshot(identity))
        async with AsyncSageClient(base_url=BASE, identity=identity) as client:
            result = await client.inspect_direct_grants(TARGET)
        assert result.grants[0].access_level == 1
        assert route.calls.last.request.headers['X-Agent-ID'] == identity.agent_id


@pytest.mark.parametrize('change', [dict(complete=False), dict(complete=1), dict(height=0), dict(height=True),
    dict(app_hash='bad'), dict(root_generation=0), dict(coverage='global'), dict(extra=True),
    dict(grants=None), dict(grants=[dict(domain='test', grantee_id='a' * 64,
        granter_id='e' * 64, access_level=1, expires_at=0)])])
def test_invalid_snapshots_never_become_empty(change):
    data = snapshot(AgentIdentity.generate()); data.update(change)
    with pytest.raises(ValueError):
        DirectGrantsSnapshot.model_validate(data)


def test_expired_or_duplicate_rows_rejected():
    data = snapshot(AgentIdentity.generate())
    data['grants'][0]['expires_at'] = 100
    with pytest.raises(ValueError): DirectGrantsSnapshot.model_validate(data)
    data['grants'][0]['expires_at'] = 0
    data['grants'].append(copy.deepcopy(data['grants'][0]))
    with pytest.raises(ValueError): DirectGrantsSnapshot.model_validate(data)


@pytest.mark.parametrize('status', [403, 503])
def test_denied_unavailable_never_fallback_to_old_list(status):
    identity = AgentIdentity.generate()
    with respx.mock(base_url=BASE) as mock:
        mock.get('/v1/access/grants-snapshot/' + TARGET).respond(status, json={'detail':'unavailable'})
        with SageClient(base_url=BASE, identity=identity) as client:
            with pytest.raises(SageAPIError) as error: client.inspect_direct_grants(TARGET)
            assert error.value.status_code == status
        assert len(mock.calls) == 1


def test_foreign_receipt_rejected():
    identity = AgentIdentity.generate()
    with respx.mock(base_url=BASE) as mock:
        mock.get('/v1/access/grants-snapshot/' + TARGET).respond(200, json=snapshot(AgentIdentity.generate()))
        with SageClient(base_url=BASE, identity=identity) as client:
            with pytest.raises(ValueError): client.inspect_direct_grants(TARGET)


def test_invalid_target_cannot_change_path():
    with SageClient(base_url=BASE, identity=AgentIdentity.generate()) as client:
        with pytest.raises(ValueError): client.inspect_direct_grants('../other')


class OversizeSyncStream(httpx.SyncByteStream):
    def __init__(self): self.reads = 0; self.closed = False
    def __iter__(self):
        for _ in range(100):
            self.reads += 1
            yield b'x' * 65536
    def close(self): self.closed = True


class OversizeAsyncStream(httpx.AsyncByteStream):
    def __init__(self): self.reads = 0; self.closed = False
    async def __aiter__(self):
        for _ in range(100):
            self.reads += 1
            yield b'x' * 65536
    async def aclose(self): self.closed = True


def test_sync_chunked_overflow_stops_early_and_closes_stream():
    stream = OversizeSyncStream()
    with SageClient(base_url=BASE, identity=AgentIdentity.generate()) as client:
        client._client.close()
        client._client = httpx.Client(base_url=BASE, transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=stream)))
        with pytest.raises(ValueError, match='response budget'):
            client.inspect_direct_grants(TARGET)
    assert stream.closed
    assert stream.reads == 17  # one bounded chunk beyond the ceiling; 83 never consumed


@pytest.mark.asyncio
async def test_async_chunked_overflow_stops_early_and_closes_stream():
    stream = OversizeAsyncStream()
    async with AsyncSageClient(base_url=BASE, identity=AgentIdentity.generate()) as client:
        await client._client.aclose()
        client._client = httpx.AsyncClient(base_url=BASE, transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=stream)))
        with pytest.raises(ValueError, match='response budget'):
            await client.inspect_direct_grants(TARGET)
    assert stream.closed
    assert stream.reads == 17


def test_declared_oversize_never_reads_body():
    stream = OversizeSyncStream()
    with SageClient(base_url=BASE, identity=AgentIdentity.generate()) as client:
        client._client.close()
        client._client = httpx.Client(base_url=BASE, transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={'content-length': '1048577'}, stream=stream)))
        with pytest.raises(ValueError, match='response budget'):
            client.inspect_direct_grants(TARGET)
    assert stream.closed and stream.reads == 0


def test_gzip_body_is_bounded_as_decompressed_json_without_double_decoding():
    import gzip
    import json
    identity = AgentIdentity.generate()
    body = gzip.compress(json.dumps(snapshot(identity)).encode())
    with SageClient(base_url=BASE, identity=identity) as client:
        client._client.close()
        client._client = httpx.Client(base_url=BASE, transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={'content-encoding': 'gzip'},
                stream=httpx.ByteStream(body))))
        result = client.inspect_direct_grants(TARGET)
    assert result.complete is True
