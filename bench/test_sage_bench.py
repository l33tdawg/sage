"""Offline signed fake-server regressions; never connect to a SAGE node/model."""
import contextlib
import importlib.util
import io
import json
import os
import struct
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import httpx
from nacl.signing import SigningKey

from sage_bench import BenchmarkError, ExpansionSource, SageBenchmark, pinned_revision, sha256


class FakeSage:
    def __init__(self):
        self.key = SigningKey.generate()
        self.agent_id = self.key.verify_key.encode().hex()
        self.calls, self.nonces, self.seeds, self.domains = [], set(), {}, {}
        self.profile_overrides, self.embed_overrides, self.receipt_overrides = {}, {}, {}
        self.detail_overrides, self.result_overrides, self.health_overrides = {}, {}, {}
        self.submit_status = 201
        self.proposed_reads = 0
        self.fail_submit_transport = False

    def handle(self, request):
        nonce = bytes.fromhex(request.headers['X-Nonce'])
        assert len(nonce) == 8 and nonce not in self.nonces
        self.nonces.add(nonce)
        message = bytes.fromhex(sha256(f'{request.method} {request.url.path}\n'.encode() + request.content))
        message += struct.pack('>q', int(request.headers['X-Timestamp'])) + nonce
        self.key.verify_key.verify(message, bytes.fromhex(request.headers['X-Signature']))
        assert request.headers['X-Agent-ID'] == self.agent_id
        body = json.loads(request.content) if request.content else None
        path = request.url.path
        self.calls.append((request.method, path, body))
        if path == '/v1/agent/me':
            data = dict(agent_id=self.agent_id, role='member', enrollment_status='active', registration_status='active', approval_required=False, can_read=True, can_write=True, clearance=2, home_domain='fixture')
            data.update(self.profile_overrides)
        elif path == '/v1/embed/info':
            data = dict(semantic=True, ready=True, submit_embedding_authoritative=True, provider='ollama', dimension=3)
        elif path == '/v1/embed':
            data = dict(embedding=[0.25, 0.5, 0.75], dimension=3, model='fixture-model', embedding_provider='ollama:fixture-model:3')
            data.update(self.embed_overrides)
        elif path == '/v1/dashboard/health':
            data = dict(version='fixture-v11', boot_id='fixture-boot', embedder=dict(provider='ollama', model='fixture-model', dimension=3, reranker=dict(enabled=False, model='fixture-reranker')), memory_gate=dict(enabled=False))
            data.update(self.health_overrides)
        elif path == '/v1/domain/register':
            self.domains[body['name']] = dict(domain_name=body['name'], owner_agent_id=self.agent_id, created_height=9)
            data = dict(status='registered', tx_hash='a' * 64)
        elif path.startswith('/v1/domain/'):
            data = self.domains[path.rsplit('/', 1)[1]]
        elif path == '/v1/memory/submit':
            if self.fail_submit_transport:
                raise httpx.ReadTimeout('fixture submit outcome unknown')
            assert 'embedding' not in body
            memory_id = str(uuid.uuid4())
            self.seeds[memory_id] = dict(memory_id=memory_id, submitting_agent=self.agent_id, content_hash=sha256(body['content'].encode()), status='committed', **body)
            data = dict(memory_id=memory_id, tx_hash='b' * 64, committed=True, committed_height=10, status='proposed', embedding_provider='ollama:fixture-model:3', embedding_queued=False)
            data.update(self.receipt_overrides)
            return httpx.Response(self.submit_status, json=data)
        elif path.startswith('/v1/memory/') and path != '/v1/memory/hybrid':
            data = dict(self.seeds[path.rsplit('/', 1)[1]])
            if self.proposed_reads:
                self.proposed_reads -= 1
                data['status'] = 'proposed'
            data.update(self.detail_overrides)
        elif path == '/v1/memory/hybrid':
            assert body['embedding'] == [0.25, 0.5, 0.75]
            assert body['embedding_provider'] == 'ollama:fixture-model:3'
            assert body['status_filter'] == 'committed'
            assert all(v['embedding'] == [0.25, 0.5, 0.75] for v in body.get('expansions', []))
            data = {'results': [dict(row, **self.result_overrides) for row in self.seeds.values() if row['domain_tag'] == body['domain_tag']]}
        else:
            raise AssertionError(f'Unexpected request: {path}')
        return httpx.Response(200 if request.method == 'GET' or path != '/v1/domain/register' else 201, json=data)

    def benchmark(self, **kwargs):
        return SageBenchmark(httpx.Client(base_url='https://fixture.invalid', transport=httpx.MockTransport(self.handle)), self.key, {'server_build_ref': 'fixture-sha256:' + 'c' * 64}, False, **kwargs)


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fake = FakeSage()
        self.sage = self.fake.benchmark(commit_timeout=0.05)
        self.addCleanup(self.sage.client.close)

    def seeded(self):
        self.sage.preflight()
        domain = self.sage.new_domain('lme', 'question-one')
        self.sage.seed(domain, '[longmemeval-sid:one]\nuser: fixture evidence')
        return domain

    def test_signed_nonce_seed_receipt_and_governed_commit_precede_query(self):
        self.fake.proposed_reads = 1
        with patch('sage_bench.time.sleep'):
            domain = self.seeded()
        results = self.sage.hybrid(domain, 'fixture question', 10, ['variant one', 'variant two'])
        self.assertEqual(len(results), 1)
        self.assertEqual(len(self.sage.seed_receipts[domain]), 1)
        self.assertEqual(self.sage.seed_receipts[domain][0]['lifecycle_verified'], 'committed')
        embeds = [body['text'] for _, path, body in self.fake.calls if path == '/v1/embed']
        self.assertEqual(embeds, ['SAGE retrieval benchmark embedding-space probe', 'fixture question', 'variant one', 'variant two'])
        self.assertEqual(len(self.fake.calls), len(self.fake.nonces))
        self.assertEqual(sum(path.startswith('/v1/memory/') and path != '/v1/memory/submit' for _, path, _ in self.fake.calls), 3)

    def test_inactive_unapproved_root_or_admin_never_embed_or_seed(self):
        for override in ({'role': 'admin'}, {'agent_id': 'root'}, {'enrollment_status': 'pending_review'}, {'registration_status': 'inactive'}, {'approval_required': True}, {'clearance': 0}, {'can_write': False}):
            with self.subTest(override=override):
                fake = FakeSage()
                fake.profile_overrides = override
                sage = fake.benchmark()
                with self.assertRaises(BenchmarkError):
                    sage.preflight()
                sage.client.close()
                self.assertEqual([path for _, path, _ in fake.calls], ['/v1/agent/me'])

    def test_bad_vectors_and_space_drift_abort_before_hybrid(self):
        for override in ({'dimension': 4}, {'embedding': [0, 0, 0]}, {'embedding': [True, 1, 2]}, {'embedding': [float('nan'), 1, 2]}, {'embedding_provider': 'hash', 'model': 'hash'}, {'embedding_provider': 'different:3'}, {'model': 'different-model'}):
            with self.subTest(override=override):
                fake = FakeSage()
                sage = fake.benchmark()
                sage.preflight()
                fake.embed_overrides = override
                with self.assertRaises((BenchmarkError, ValueError)):
                    sage.embed('question')
                sage.client.close()
                self.assertNotIn('/v1/memory/hybrid', [path for _, path, _ in fake.calls])

    def test_uncommitted_queued_wrong_space_or_ambiguous_receipt_not_scored_or_retried(self):
        for status, override in ((201, {'committed': False}), (201, {'embedding_queued': True}), (201, {'embedding_provider': 'foreign-space'}), (201, {'tx_hash': 'bad'}), (201, {'committed_height': 0}), (201, {'memory_id': 'bad'}), (202, {'status': 'indeterminate'})):
            with self.subTest(status=status, override=override):
                fake = FakeSage()
                sage = fake.benchmark()
                sage.preflight()
                domain = sage.new_domain('lme', 'fixture')
                fake.submit_status, fake.receipt_overrides = status, override
                with self.assertRaises(BenchmarkError):
                    sage.seed(domain, 'evidence')
                self.assertEqual(sage.seed_receipts[domain], [])
                self.assertEqual(sage.write_outcomes[-1]["http_status"], status)
                self.assertIn("tx_hash", sage.write_outcomes[-1]["receipt"])
                self.assertEqual(sum(path == '/v1/memory/submit' for _, path, _ in fake.calls), 1)
                self.assertNotIn('/v1/memory/hybrid', [path for _, path, _ in fake.calls])
                sage.client.close()

    def test_transport_failure_is_retained_as_unknown_and_not_retried(self):
        self.sage.preflight()
        domain = self.sage.new_domain("lme", "fixture")
        self.fake.fail_submit_transport = True
        with self.assertRaises(httpx.ReadTimeout):
            self.sage.seed(domain, "fixture evidence")
        self.assertEqual(self.sage.write_outcomes[-1]["outcome"], "unknown_do_not_resubmit")
        self.assertEqual(sum(path == "/v1/memory/submit" for _, path, _ in self.fake.calls), 1)
        self.assertEqual(self.sage.seed_receipts[domain], [])

    def test_commit_deadline_and_exact_envelope_failure_do_not_score(self):
        for override in ({'status': 'proposed'}, {'status': 'deprecated'}, {'content': 'tampered'}, {'content_hash': '0' * 64}, {'classification': 0}, {'submitting_agent': 'someone-else'}, {'domain_tag': 'foreign'}):
            with self.subTest(override=override):
                fake = FakeSage()
                sage = fake.benchmark(commit_timeout=0.001)
                sage.preflight()
                domain = sage.new_domain('lme', 'fixture')
                fake.detail_overrides = override
                with self.assertRaises(BenchmarkError):
                    sage.seed(domain, 'evidence')
                self.assertEqual(sage.seed_receipts[domain], [])
                sage.client.close()

    def test_wrong_arm_boot_change_or_escaped_result_fails_closed(self):
        domain = self.seeded()
        self.fake.health_overrides = {'boot_id': 'replacement-process'}
        with self.assertRaisesRegex(BenchmarkError, 'changed'):
            self.sage.hybrid(domain, 'question', 10, [])
        self.fake.health_overrides = {'embedder': {'reranker': {'enabled': True}}}
        with self.assertRaisesRegex(BenchmarkError, 'reranker setting'):
            self.sage.hybrid(domain, 'question', 10, [])
        self.fake.health_overrides = {}
        for override in ({'domain_tag': 'foreign'}, {'status': 'proposed'}, {'memory_id': str(uuid.uuid4())}, {'content': 'altered evidence'}):
            self.fake.result_overrides = override
            with self.assertRaises(BenchmarkError):
                self.sage.hybrid(domain, 'question', 10, [])

    def test_each_run_domain_is_fresh_and_dataset_revision_must_be_immutable(self):
        self.sage.preflight()
        one = self.sage.new_domain('lme', 'same-question')
        other = self.fake.benchmark()
        other.preflight()
        two = other.new_domain('lme', 'same-question')
        other.client.close()
        self.assertNotEqual(one, two)
        with patch.dict(os.environ, {'FIXTURE_REVISION': 'main'}):
            with self.assertRaises(BenchmarkError):
                pinned_revision('FIXTURE_REVISION')

    def test_cached_expansion_replay_needs_no_key_and_incomplete_arm_is_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'variants.json'
            path.write_text(json.dumps({'model': 'fixture-chat', 'temperature': 0.4, 'max_tokens': 200, 'variants': {sha256(b'question'): ['one', 'two', 'three']}}))
            with patch.dict(os.environ, {}, clear=True):
                source = ExpansionSource(3, str(path), 'fixture-chat')
                self.assertEqual(source.variants('question'), ['one', 'two', 'three'])
                self.assertEqual(source.metadata()['cache_sha256'], sha256(path.read_bytes()))
                with self.assertRaises(BenchmarkError):
                    source.variants('uncached question')
                with self.assertRaises(BenchmarkError):
                    ExpansionSource(2, str(path), 'fixture-chat').variants('question')
                self.assertEqual(ExpansionSource(0, None, 'fixture-chat').variants('question'), [])

    def test_pinned_dataset_fetch_receipt_cache_and_tampering(self):
        from fetch_locomo import fetch
        from sage_bench import dataset_provenance
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "locomo.json"
            payload = b'[{"sample_id":"fixture"}]'
            with patch("fetch_locomo.urllib.request.urlopen", return_value=io.BytesIO(payload)) as download:
                receipt = fetch(path, "a" * 40)
                self.assertIn("/" + "a" * 40 + "/", download.call_args.args[0])
            with patch("fetch_locomo.urllib.request.urlopen") as download:
                self.assertEqual(fetch(path, "a" * 40), receipt)
                download.assert_not_called()
                with self.assertRaises(ValueError):
                    fetch(path, "main")
                with self.assertRaises(ValueError):
                    fetch(path, "b" * 40)
            provenance = dataset_provenance(json.loads(payload), str(path), "locomo", None)
            self.assertEqual(provenance["upstream_revision"], "a" * 40)
            path.write_bytes(b"[]")
            with self.assertRaises(ValueError):
                fetch(path, "a" * 40)
            with self.assertRaises(BenchmarkError):
                dataset_provenance([], str(path), "locomo", None)

    def test_locomo_missing_evidence_never_queries_but_empty_evidence_scores_zero(self):
        spec = importlib.util.spec_from_file_location("locomo_evidence_runner", Path(__file__).parent / "locomo" / "run.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.sage.preflight()
        haystack = [{"turn_id": "D1:1", "speaker": "user", "text": "fixture evidence"}]
        seed = module.seed_conversation(self.sage, "fixture-conversation", haystack)
        self.assertEqual(seed["seeded_turn_ids"], ["D1:1"])
        question = {"question_id": "fixture-question", "conversation_id": "fixture-conversation", "question": "fixture question", "evidence_turn_ids": ["D1:missing"]}
        before = len(self.fake.calls)
        with self.assertRaisesRegex(BenchmarkError, "missing/unseeded evidence"):
            module.query_question(self.sage, ExpansionSource(0, None, "fixture"), question, seed["domain"], 10, set(seed["seeded_turn_ids"]))
        self.assertEqual(len(self.fake.calls), before)
        question["evidence_turn_ids"] = []
        row = module.query_question(self.sage, ExpansionSource(0, None, "fixture"), question, seed["domain"], 10, set(seed["seeded_turn_ids"]))
        self.assertEqual((row["r5"], row["r10"], row["rr"]), (0.0, 0.0, 0.0))

    def test_both_cli_runners_work_without_openai_key_and_write_observed_provenance(self):
        fixtures = {
            'longmemeval': [{'question_id': 'fixture-q', 'question_type': 'single-session-user', 'question': 'fixture question', 'answer_session_ids': ['one'], 'haystack_session_ids': ['one'], 'haystack_sessions': [[{'role': 'user', 'content': 'fixture evidence'}]]}],
            'locomo': [{'sample_id': 'fixture-conversation', 'conversation': {'session_1': [{'dia_id': 'D1:1', 'speaker': 'user', 'text': 'fixture evidence'}]}, 'qa': [{'question': 'fixture question', 'evidence': ['D1:1'], 'category': 1}]}],
        }
        real_client = httpx.Client
        for name, fixture in fixtures.items():
            with self.subTest(runner=name), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                dataset, key, manifest, output = [directory / v for v in ('data.json', 'key', 'server.json', 'output.json')]
                dataset.write_text(json.dumps(fixture))
                fake = FakeSage()
                key.write_bytes(fake.key.encode())
                manifest.write_text(json.dumps({'server_build_ref': 'fixture-immutable-build'}))
                spec = importlib.util.spec_from_file_location(name + '_runner', Path(__file__).parent / name / 'run.py')
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                argv = ['run.py', '--sage-url', 'https://fixture.invalid', '--identity', str(key), '--server-manifest', str(manifest), '--expect-reranker', 'off', '--limit', '1', '--out', str(output)]
                def client(*args, **kwargs):
                    kwargs['transport'] = httpx.MockTransport(fake.handle)
                    return real_client(*args, **kwargs)
                with patch.dict(os.environ, {name.upper() + '_DATA_PATH': str(dataset)}, clear=True), patch.object(sys, 'argv', argv), patch('sage_bench.httpx.Client', side_effect=client), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(module.main(), 0)
                payload = json.loads(output.read_text())
                self.assertEqual(payload['summary']['overall']['r10'], 1.0)
                self.assertEqual(payload['server_observed']['version'], 'fixture-v11')
                self.assertNotIn('rerank_enabled_env', payload)
                self.assertEqual(payload['dataset']['source_file_sha256'], sha256(dataset.read_bytes()))
                self.assertEqual(payload['expansion']['n'], 0)
                self.assertEqual(len(payload['seed_receipts']), 1)
                self.assertEqual(len(payload['harness_git_sha']), 40)
                # Repeat with a first-write ambiguous result and a second item.
                # No subsequent domain/write may occur after the unsafe outcome.
                if name == 'longmemeval':
                    second = dict(fixture[0], question_id='second-question')
                else:
                    second = dict(fixture[0], sample_id='second-conversation')
                dataset.write_text(json.dumps(fixture + [second]))
                fake = FakeSage()
                fake.submit_status = 202
                fake.receipt_overrides = {'status': 'indeterminate', 'committed': False}
                key.write_bytes(fake.key.encode())
                failure_output = directory / 'failure.json'
                argv = [value if value != str(output) else str(failure_output) for value in argv]
                argv[argv.index('--limit') + 1] = '2'
                with patch.dict(os.environ, {name.upper() + '_DATA_PATH': str(dataset)}, clear=True), patch.object(sys, 'argv', argv), patch('sage_bench.httpx.Client', side_effect=client), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(module.main(), 1)
                failure = json.loads(failure_output.read_text())
                self.assertFalse(failure['complete'])
                self.assertEqual(failure['n_planned'], 2)
                self.assertEqual(failure['n_total'], 1)
                self.assertNotIn('r5', failure['per_question'][0])
                self.assertEqual(sum(path == '/v1/domain/register' for _, path, _ in fake.calls), 1)
                self.assertEqual(sum(path == '/v1/memory/submit' for _, path, _ in fake.calls), 1)
                self.assertEqual(failure['write_outcomes'][-1]['http_status'], 202)
                if name == 'locomo':
                    fixture[0]['qa'][0]['evidence'] = ['D1:missing']
                    dataset.write_text(json.dumps(fixture))
                    fake = FakeSage()
                    key.write_bytes(fake.key.encode())
                    with patch.dict(os.environ, {name.upper() + '_DATA_PATH': str(dataset)}, clear=True), patch.object(sys, 'argv', argv), patch('sage_bench.httpx.Client', side_effect=client), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(module.main(), 1)
                    failure = json.loads(failure_output.read_text())
                    self.assertFalse(failure['complete'])
                    self.assertIn('missing/unseeded evidence', failure['per_question'][0]['error'])
                    self.assertFalse(failure['seed_receipts'])
                    self.assertNotIn('/v1/domain/register', [path for _, path, _ in fake.calls])
                    self.assertNotIn('/v1/memory/submit', [path for _, path, _ in fake.calls])




if __name__ == '__main__':
    unittest.main()
