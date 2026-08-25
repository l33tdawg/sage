import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const root = new URL('../', import.meta.url);
const read = (path) => readFileSync(new URL(path, root), 'utf8');
const server = JSON.parse(read('server.json'));
const version = server.version;

test('release-facing version metadata stays aligned', () => {
  assert.match(
    version,
    /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$/,
  );
  assert.deepEqual(
    server.packages.filter(({ registryType }) => registryType === 'oci'),
    [{
      registryType: 'oci',
      identifier: `ghcr.io/l33tdawg/sage:${version}`,
      transport: { type: 'stdio' },
    }],
  );

  const exact = [
    ['sdk/python/pyproject.toml', `version = "${version}"`],
    ['sdk/python/src/sage_sdk/__init__.py', `__version__ = "${version}"`],
    ['api/openapi.yaml', `version: ${version}`],
    ['desktop/sage-shell/Cargo.toml', `version = "${version}"`],
    ['desktop/sage-shell/Cargo.lock', `name = "sage-shell"\nversion = "${version}"`],
    ['desktop/sage-shell/tauri.conf.json', `"version": "${version}"`],
    ['desktop/sage-shell/src/control.rs', 'matches!(minor, Some(10..=19))'],
    ['scripts/stage-native-shell-daemon.sh', 'through v11.19.x semver'],
    ['web/static/js/app.js', `const SAGE_VERSION = 'v${version}';`],
    ['README.md', `## What's New in v${version}`],
    ['README.md', '## What\'s New in v11.18.10'],
    ['README.md', 'App-v23 replaced capability-bit administration'],
    ['README.md', 'App-v24 closes the canonical terminal-hash lifecycle defect'],
    ['README.md', `SDK ${version}.`],
    ['README.md', `ghcr.io/l33tdawg/sage:${version}`],
    ['README.md', `Pin a specific version with \`ghcr.io/l33tdawg/sage:${version}\`.`],
    ['sdk/python/README.md', `SAGE v${version} SDK`],
    ['docs/reference/INDEX.md', `reconciled for SAGE v${version}`],
    ['docs/reference/INDEX.md', `reconciled through v${version}`],
    ['docs/reference/environment-variables.md', `Reconciled through SAGE v${version}`],
    ['docs/reference/federation-and-brain-api.md', `Verified against SAGE v${version} code`],
    ['docs/reference/mcp-tools.md', `internal/mcp for SAGE v${version}`],
    ['docs/reference/python-sdk.md', `Version:** ${version}`],
    ['docs/reference/rest-api.md', `Reconciled through SAGE v${version}`],
    ['docs/reference/concepts/rbac-orgs-federation.md', `reconciled through SAGE v${version}`],
    ['docs/reference/app-v23-access-control-design.md', `SAGE v${version}`],
    ['docs/reference/upgrade-lineage-repair.md', `SAGE v${version}`],
    ['docs/ADMIN_BOOTSTRAP.md', `Reconciled through SAGE v${version}/app-v27`],
    ['docs/GETTING_STARTED.md', 'From Source (Go 1.25.13+)'],
    ['docs/GETTING_STARTED.md', `# sage-gui v${version}`],
    ['docs/GETTING_STARTED.md', `SAGE v${version} advertises 33 MCP tools`],
    ['docs/ARCHITECTURE.md', 'Go 1.25.13+ ABCI application'],
    ['docs/ARCHITECTURE.md', '| Go | 1.25.13+ |'],
    ['docs/reference/concepts/signer-nonce-fence.md', `Status: v${version}.`],
    ['docs/reference/concepts/signer-nonce-fence.md', `not in v${version}`],
    ['deploy/federation-acceptance/README.md', `# v${version} federation Docker acceptance`],
    ['docs/FEDERATION.md', `Verified against SAGE v${version} federation behavior`],
    ['docs/reference/concepts/message-reply-lifecycle.md', `SAGE v${version} code`],
    ['docs/UPGRADING.md', `| v${version} | app-v27:`],
    ['docs/ROADMAP.md', `## v${version} release`],
    ['docs/UPGRADING.md', 'The recovery commands in this guide require SAGE v11.18.0 or later.'],
    ['docs/UPGRADING.md', '`backup --full`, `restore --from`,'],
    ['docs/UPGRADING.md', '`upgrade lineage status|doctor|verify`'],
    ['deploy/federation-acceptance/Dockerfile.node', `ARG VERSION=v${version}-acceptance`],
    ['deploy/federation-acceptance/docker-compose.yml', 'name: sage-v111900-federation'],
    ['deploy/federation-acceptance/docker-compose.yml', `VERSION: v${version}-acceptance`],
    ['docs/reference/app-v23-access-control-design.md', '## App-v24 readiness and memory-write barrier'],
    ['docs/reference/mcp-tools.md', 'A level-2 grant is never a remedy for a hard'],
    ['docs/reference/mcp-tools.md', 'never be substituted for this caller-scoped projection'],
    ['internal/abci/app.go', 'const maxSupportedAppVersion uint64 = 27'],
    [
      'docs/ROADMAP.md',
      version.includes('-')
        ? `v${version} is the current release candidate`
        : `v${version} is the current release`,
    ],
  ];
  for (const [path, marker] of exact) {
    assert.ok(read(path).includes(marker), `${path} is missing current version marker: ${marker}`);
  }
});

test('upgrade release table stays in ascending semantic-version order', () => {
  const releases = [...read('docs/UPGRADING.md').matchAll(/^\| v(\d+)\.(\d+)\.(\d+) \|/gm)]
    .map((match) => match.slice(1).map(Number));
  assert.ok(releases.length > 1, 'docs/UPGRADING.md is missing the release table');
  for (let index = 1; index < releases.length; index += 1) {
    const previous = releases[index - 1];
    const current = releases[index];
    assert.ok(
      current[0] > previous[0]
        || (current[0] === previous[0] && current[1] > previous[1])
        || (current[0] === previous[0] && current[1] === previous[1] && current[2] > previous[2]),
      `docs/UPGRADING.md release table is out of order: v${previous.join('.')} before v${current.join('.')}`,
    );
  }
});

test('historical v11.18.4 Go floor stays 1.25.12', () => {
  const section = (path, start, end) => {
    const body = read(path);
    const from = body.indexOf(start);
    assert.notEqual(from, -1, `${path} is missing ${start}`);
    const to = body.indexOf(end, from + start.length);
    assert.notEqual(to, -1, `${path} is missing ${end}`);
    return body.slice(from, to);
  };
  for (const [path, start, end] of [
    ['README.md', "## What's New in v11.18.4", "## What's New in v11.18.3"],
    ['docs/ROADMAP.md', '## v11.18.4 patch', '## v11.18.3 patch'],
  ]) {
    const historical = section(path, start, end);
    assert.match(historical, /Go floor to 1\.25\.12|modules require Go 1\.25\.12/);
    assert.match(historical, /container builders (?:to |uses )?1\.25\.12|every Go container builder uses 1\.25\.12/);
    assert.doesNotMatch(historical, /1\.25\.13/);
  }
});

test('v11.18 user, recovery, federation, and SDK guides stay aligned', () => {
  const architecture = read('docs/ARCHITECTURE.md');
  const roadmap = read('docs/ROADMAP.md');
  const gettingStarted = read('docs/GETTING_STARTED.md');
  const sdkReadme = read('sdk/python/README.md');

  assert.match(architecture, /Default TTL:\*\* none; unread\/unclaimed Messages remain durable until handled/);
  assert.doesNotMatch(architecture, /Default TTL:\*\* 60 minutes/);
  assert.match(architecture, /sage_message_send/);
  assert.match(read('docs/reference/mcp-tools.md'), /message_inbox_unread/);
  assert.match(read('docs/reference/mcp-tools.md'), /call `sage_inbox`/);
  assert.match(read('internal/mcp/tools.go'), /Canonical Messages remain durable and queryable/);
  assert.doesNotMatch(read('internal/mcp/tools.go'), /History is retained only for the normal transient message window/);
  assert.match(roadmap, /v11\.18\.0 completion ledger/);
  assert.match(roadmap, /v11\.18\.1 patch/);
  assert.match(roadmap, /does not introduce app-v27/);
  assert.match(read('README.md'), /`reply_event_id`/);
  assert.match(read('README.md'), /\*\*Agents\*\*,\s+\*\*Groups\*\*, and \*\*Federation\*\* tabs/);
  assert.match(read('docs/FEDERATION.md'), /up to five minutes/);
  assert.match(read('docs/FEDERATION.md'), /15 minutes/);
  assert.match(read('docs/UPGRADING.md'), /sage-gui upgrade lineage verify --json --manifest repair\.json/);
  assert.match(roadmap, /helper outside the replaceable bundle/);
  assert.match(gettingStarted, /advertises 33 MCP tools/);
  assert.match(gettingStarted, /Deprecated `sage_pipe\*`\s+compatibility/);
  assert.match(sdkReadme, /Compatibility pipeline/);
});

test('v11.17 app-v23 through app-v26 public contract markers stay release-visible', () => {
  const openapi = read('api/openapi.yaml');
  for (const marker of [
    'committed_unconfirmed',
    'projection_confirmed:',
    'idempotent_replay:',
    'DomainWriteDeniedProblem:',
    'has_more:',
    'total_exact:',
    'federated-pipeline-receipts-v2',
    '/v1/messages/{message_id}/status:',
    'MessageStatusResponse:',
  ]) {
    assert.ok(openapi.includes(marker), `OpenAPI is missing app-v23 contract marker: ${marker}`);
  }

  const readme = read('README.md');
  const environment = read('docs/reference/environment-variables.md');
  assert.match(readme, /Mynah has no released legacy population/);
  assert.match(readme, /waiting_for_app_v24/);
  assert.doesNotMatch(readme, /Existing installations stranded by app-v22/);
  assert.doesNotMatch(environment, /appv23_vendored_repair\.go/);
  assert.match(environment, /not an upgraded-node repair selector/);
});

test('legacy pipe send documents canonical exact-local admission and replay', () => {
  const openapi = read('api/openapi.yaml');
  const schemaStart = openapi.indexOf('    PipeSendRequest:');
  const schemaEnd = openapi.indexOf('    PipeResultRequest:', schemaStart);
  const routeStart = openapi.indexOf('  /v1/pipe/send:');
  const routeEnd = openapi.indexOf('  /v1/pipe/inbox:', routeStart);
  assert.notEqual(schemaStart, -1);
  assert.notEqual(schemaEnd, -1);
  assert.notEqual(routeStart, -1);
  assert.notEqual(routeEnd, -1);

  const schema = openapi.slice(schemaStart, schemaEnd);
  const route = openapi.slice(routeStart, routeEnd);
  assert.match(schema, /ttl_minutes:\n\s+type: integer\n\s+minimum: 0\n\s+maximum: 1440/);
  assert.match(schema, /idempotency_key:\n\s+type: string\n\s+minLength: 1\n\s+maxLength: 256/);
  assert.match(schema, /PipeSendResponse:[\s\S]*required: \[pipe_id, status, expires_at, destination_chain_id, idempotent_replay\]/);
  assert.match(schema, /pipe_id:\n\s+type: string\n\s+pattern: '\^msg-'/);
  assert.match(route, /"200":[\s\S]*PipeSendResponse/);
  assert.match(route, /"201":[\s\S]*PipeSendResponse/);
  assert.match(route, /"409":[\s\S]*Conflict/);
  assert.match(route, /"501":[\s\S]*NotImplemented/);

  const rest = read('docs/reference/rest-api.md');
  const wake = read('docs/reference/concepts/message-wake-bus.md');
  assert.match(rest, /HTTP 201 fresh; HTTP 200 exact keyed replay/);
  assert.match(rest, /HTTP 501 before insertion/);
  assert.match(wake, /Provider-only and federated rows have no exact local recipient/);
});

test('hybrid expansion authorization contract stays release-visible', () => {
  const section = (path, start, end) => {
    const body = read(path);
    const from = body.indexOf(start);
    assert.notEqual(from, -1, `${path} is missing contract section start: ${start}`);
    const to = body.indexOf(end, from + start.length);
    assert.notEqual(to, -1, `${path} is missing contract section end: ${end}`);
    return body.slice(from, to);
  };
  const hybridSchema = section(
    'api/openapi.yaml',
    '    MemoryHybridRequest:',
    '    FilterInfo:',
  );
  const hybridRoute = section(
    'api/openapi.yaml',
    '  /v1/memory/hybrid:',
    '  /v1/memory/list:',
  );

  assert.match(hybridSchema, /expansions:\n\s+type: array\n\s+maxItems: 8/);
  assert.match(hybridSchema, /one 8,192-candidate live\s+authorization budget is shared/);
  assert.match(hybridRoute, /never returns a 200\s+response containing partial fused results/);

  for (const [path, contract] of [
    [
      'docs/reference/rest-api.md',
      section('docs/reference/rest-api.md', '### `POST /v1/memory/hybrid`', '### `GET /v1/memory/{memory_id}`'),
    ],
    [
      'docs/reference/python-sdk.md',
      section('docs/reference/python-sdk.md', '#### `hybrid()`', '#### `get_memory()`'),
    ],
    [
      'docs/reference/mcp-tools.md',
      section('docs/reference/mcp-tools.md', '### sage_turn', '### sage_reflect'),
    ],
    [
      'sdk/python/README.md',
      section(
        'sdk/python/README.md',
        '# Hybrid BM25/vector recall with optional query expansions',
        '# Get a single memory',
      ),
    ],
  ]) {
    assert.match(contract, /at most (?:eight|8)/i, `${path} is missing the expansion cap`);
    assert.match(contract, /8,192/, `${path} is missing the aggregate authorization budget`);
    assert.match(contract, /partial/i, `${path} is missing fail-closed partial-result semantics`);
  }
});
