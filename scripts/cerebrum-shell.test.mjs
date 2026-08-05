import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');
const apiSource = await readFile(new URL('../web/static/js/api.js', import.meta.url), 'utf8');
const cssSource = await readFile(new URL('../web/static/css/sage.css', import.meta.url), 'utf8');
const indexSource = await readFile(new URL('../web/static/index.html', import.meta.url), 'utf8');
const mriSource = await readFile(new URL('../web/static/js/mri-brain.js', import.meta.url), 'utf8');
const mriPageSource = await readFile(new URL('../web/static/mri.html', import.meta.url), 'utf8');
const federationRouteSource = await readFile(new URL('../web/static/js/federation-route-state.js', import.meta.url), 'utf8');
const traySource = await readFile(new URL('../cmd/sage-tray/main.swift', import.meta.url), 'utf8');
const { MRI_LAYOUT, mriBrainstemBias, mriDepthForAge, mriVerticalPosition } = await import('../web/static/js/mri-layout.js');

function inlineScriptBodies(source) {
    const lower = source.toLowerCase();
    const bodies = [];
    let cursor = 0;
    while (cursor < source.length) {
        const openStart = lower.indexOf('<script', cursor);
        if (openStart === -1) break;
        const openEnd = lower.indexOf('>', openStart + '<script'.length);
        if (openEnd === -1) break;
        const closeStart = lower.indexOf('</script', openEnd + 1);
        if (closeStart === -1) break;
        const closeEnd = lower.indexOf('>', closeStart + '</script'.length);
        if (closeEnd === -1) break;
        bodies.push(source.slice(openEnd + 1, closeStart));
        cursor = closeEnd + 1;
    }
    return bodies;
}

const fedPipeHelperSource = appSource.slice(
    appSource.indexOf('function normalizeFedPipeContactGrant('),
    appSource.indexOf('function fedFriendlyLocalAgentLabel(')
);
const { normalizeFedPipeContactGrant, mergeFedPipeContactGrant } = new Function(
    `${fedPipeHelperSource}; return { normalizeFedPipeContactGrant, mergeFedPipeContactGrant };`
)();

test('CEREBRUM has a dependency-free bootstrap shell and sanitized failure state', () => {
    const inlineScripts = inlineScriptBodies(indexSource)
        .filter((source) => source.trim() !== '');
    assert.ok(inlineScripts.length >= 3, 'theme, bootstrap, and HTM inline scripts must be present');
    for (const source of inlineScripts) {
        assert.doesNotThrow(() => new Function(source), 'inline bootstrap script must parse');
    }
    assert.match(indexSource, /data-sage-bootstrap-state="pending"/);
    assert.match(indexSource, /Opening your SAGE brain/);
    assert.match(indexSource, /ui-bootstrap\/module-load/);
    assert.match(indexSource, /Diagnostic code: " \+ pendingCode/);
    assert.match(indexSource, /pendingCode = diagnosticCodes\[code\] \|\| diagnosticCodes\.unknown/);
    assert.doesNotMatch(
        indexSource,
        /diagnostic\.textContent\s*=.*(?:detail|event|error|message)/,
        'raw exceptions, rejected values, and messages must never reach the visible diagnostic',
    );
    assert.match(indexSource, /textContent = "CEREBRUM could not start"/);
    assert.match(indexSource, /window\.setTimeout\(\(\) => fail\("timeout"\), 10000\)/);
    assert.ok(
        indexSource.indexOf('window.__sageBootstrap') <
            indexSource.indexOf('/ui/js/vendor/preact.umd.js'),
        'the bootstrap guard must run before any vendor or application dependency',
    );
    assert.match(indexSource, /const shell = document\.getElementById\("sage-bootstrap-shell"\)/);
    assert.match(indexSource, /if \(shell\) shell\.remove\(\)/,
        'successful render must remove the dependency-free shell instead of covering the app forever');
    assert.match(appSource, /window\.__sageBootstrap\?\.ready\?\.\(\)/);
    assert.match(appSource, /window\.__sageBootstrap\?\.fail\?\.\('render', error\)/);
    assert.match(apiSource, /if \(!res\.ok\) throw new Error\('memory statistics are temporarily unavailable'\)/,
        'a projection 503 must reject instead of becoming a plausible zero-memory payload');
    assert.match(appSource, /CEREBRUM refused to display an unverified or incomplete projection instead of pretending the brain is empty/);
    assert.match(appSource, /\.catch\(\(\) => setAuthState\('error'\)\)/,
        'authentication preflight failures must not be treated as an unencrypted ready session');
    assert.doesNotMatch(appSource, /if auth check fails, assume no auth/);
});

test('inline script inspection follows case-insensitive HTML end-tag parsing', () => {
    assert.deepEqual(
        inlineScriptBodies('<SCRIPT>one()</script ><script type="module">two()</SCRIPT data-x>'),
        ['one()', 'two()'],
    );
});

test('task status mutations reject HTTP failures before optimistic board state can settle', () => {
    const updateTaskStatusSource = apiSource.slice(
        apiSource.indexOf('export async function updateTaskStatus('),
        apiSource.indexOf('export async function reorderTasks('),
    );
    assert.match(updateTaskStatusSource, /const payload = await res\.json\(\)\.catch\(\(\) => \(\{\}\)\)/);
    assert.match(updateTaskStatusSource, /if \(!res\.ok\) throw new Error\(payload\.error/,
        'bulk clear uses Promise.all, so the API helper must reject instead of fulfilling with an error body');
});

test('task board always leaves its loading state after success or failure', () => {
    const tasksPage = appSource.slice(
        appSource.indexOf('function TasksPage('),
        appSource.indexOf('function Combobox('),
    );
    const loadTasks = tasksPage.slice(
        tasksPage.indexOf('async function loadTasks()'),
        tasksPage.indexOf('const pause ='),
    );
    assert.match(loadTasks, /finally\s*\{\s*setLoading\(false\);\s*\}/,
        'a successful return must not strand the task board in its loading state');
    assert.ok(loadTasks.indexOf('finally') > loadTasks.indexOf('catch'),
        'the same cleanup must cover both fulfilled and rejected task requests');
});

test('static JavaScript gate rejects the v11.15 nested-template failure under module grammar', () => {
    const checker = fileURLToPath(new URL('./check-static-js.mjs', import.meta.url));
    const fixture = fileURLToPath(
        new URL('./fixtures/cerebrum-malformed-module.txt', import.meta.url),
    );
    const result = spawnSync(process.execPath, [checker, fixture], {
        encoding: 'utf8',
    });

    assert.notEqual(result.status, 0, 'malformed browser module must fail the static gate');
    assert.match(
        `${result.stdout}\n${result.stderr}`,
        /SyntaxError|ES module syntax check failed/,
    );
});

test('global health bar surfaces projection unavailability instead of disappearing', () => {
    const healthBar = appSource.slice(
        appSource.indexOf('function HealthBar()'),
        appSource.indexOf('// ============================================================================', appSource.indexOf('function HealthBar()')),
    );
    assert.match(healthBar, /setHealthUnavailable\(true\)/);
    assert.match(healthBar, /health-bar health-bar-unavailable/);
    assert.match(healthBar, /role="alert"/);
    assert.match(healthBar, /Your memories are unchanged\. CEREBRUM could not verify the local memory projection/);
    assert.doesNotMatch(healthBar, /if \(!health\) return null/);
});

test('CEREBRUM labels a partial canonical projection and never calls it an empty brain', () => {
    const inventory = appSource.slice(
        appSource.indexOf('function BrainDomainInventory('),
        appSource.indexOf('// MriView'),
    );
    const mri = appSource.slice(
        appSource.indexOf('function MriView('),
        appSource.indexOf('// Global tooltips state'),
    );
    const healthBar = appSource.slice(
        appSource.indexOf('function HealthBar()'),
        appSource.indexOf('// ============================================================================', appSource.indexOf('function HealthBar()')),
    );

    assert.match(inventory, /projection\?\.partial === true/);
    assert.match(inventory, /Available memories shown/);
    assert.match(inventory, /Your stored data was not changed/);
    assert.match(mri, /No memories can be safely shown yet/);
    assert.match(mri, /noLocalMemories && !partialProjection/);
    assert.match(healthBar, /health\.memory_projection\?\.partial === true/);
    assert.match(healthBar, /health\.memory_projection\?\.verified_only !== false/);
    assert.match(healthBar, /verified memories/);
    assert.match(healthBar, /available memories/);
    assert.match(healthBar, /Canonically verified memories currently visible/);
    assert.match(healthBar, /Readable historical memories currently visible/);
    assert.doesNotMatch(healthBar, /Available view/,
        'the health bar must not duplicate the automatic repair notice in the domain panel');
});

test('preserved historical records have one explicit repair-resolution flow', () => {
    const resolution = appSource.slice(
        appSource.indexOf('function MemoryAdoptionResolutionModal('),
        appSource.indexOf('function BrainDomainInventory('),
    );
    const inventory = appSource.slice(
        appSource.indexOf('function BrainDomainInventory('),
        appSource.indexOf('// MriView'),
    );
    const retryAPI = apiSource.slice(
        apiSource.indexOf('export async function retryMemoryAdoption('),
        apiSource.indexOf('export async function deprecateMemoryAdoption('),
    );
    const deprecateAPI = apiSource.slice(
        apiSource.indexOf('export async function deprecateMemoryAdoption('),
        apiSource.indexOf('export async function fetchHealth('),
    );

    assert.match(inventory, /adoptionProgress\.state === 'recovery'/);
    assert.match(inventory, /brain-projection-resolve/);
    assert.match(inventory, />Resolve<\/button>/);
    assert.match(inventory, /repairExplicitlyResolved = adoptionProgress &&[\s\S]*adoptionProgress\.state === 'complete' &&[\s\S]*Number\(adoptionProgress\.remaining \|\| 0\) === 0 &&[\s\S]*Number\(adoptionProgress\.recovery \|\| 0\) === 0/,
        'only explicit terminal zero-count progress may clear the entire repair warning');
    assert.match(inventory, /showAutomaticRepairNotice = !repairExplicitlyResolved &&[\s\S]*\(repairActive \|\| repairFinishedWithResidue \|\| projectionNotice\)/,
        'terminal progress must suppress even a stale generic projection notice');
    assert.match(inventory, /fetchMemoryAdoptionProgress\(\)/);
    assert.match(inventory, /setAdoptionProgress\(adoption\)/,
        'a reload must restore the durable complete/zero progress proof before rendering the notice');

    assert.match(resolution, /useModalDialog\(requestClose\)/);
    assert.match(resolution, /role="dialog" aria-modal="true"/);
    assert.match(resolution, /aria-labelledby="memory-repair-resolution-title"/);
    assert.match(resolution, /role="alert"/);
    assert.match(resolution, /role="status"/);
    assert.match(resolution, /Retrying repair…/);
    assert.match(resolution, /Deprecating records…/);
    assert.match(resolution, /Recovery Decision \(local\)|local recovery decision/);
    assert.match(cssSource, /\.cerebrum-recovery-assignment\s*>\s*div\s*\{[^}]*grid-column:\s*1\s*\/\s*-1/s,
        'recovery authority guidance must span the panel instead of being squeezed beside selectors');
    assert.match(cssSource, /\.cerebrum-repair-choice\.danger\s*>\s*div:first-child\s*\{[^}]*grid-column:\s*1\s*\/\s*-1/s,
        'deprecation explanation must span the full card above confirmation controls');
    assert.match(resolution, /const deprecationCount = selectedIDs\.length \|\| recoveryCount/);
    assert.match(resolution, /DEPRECATE \$\{deprecationCount\}/);
    assert.match(resolution, /<strong>\$\{requiredConfirmation\}<\/strong>/);
    assert.match(resolution, /confirmation !== requiredConfirmation/);
    assert.match(resolution, /const returned = result\?\.progress \|\| result/);
    const immediateProgress = resolution.indexOf("if (next && typeof onProgress === 'function') onProgress(next)");
    const reconciliationFetch = resolution.indexOf('const refreshed = await fetchMemoryAdoptionProgress()');
    assert.ok(immediateProgress !== -1 && reconciliationFetch !== -1 && immediateProgress < reconciliationFetch,
        'successful deprecation must publish returned complete/zero progress before the refresh request');
    assert.match(resolution, /if \(refreshed\) \{[\s\S]*next = refreshed;[\s\S]*onProgress\(next\)/,
        'the follow-up GET must reconcile progress without reviving a stale local state');
    assert.match(resolution, /const result = await deprecateMemoryAdoption\([\s\S]*await refreshProgress\(result\)/);
    assert.match(resolution, /CEREBRUM refreshed it; review the current count and try again/,
        'a genuine inventory race must refresh the operator\'s snapshot instead of leaving a stale modal');
    assert.match(resolution, /remain preserved for audit/);
    assert.match(resolution, /not been deleted or rewritten/);
    assert.match(resolution, /fetchMemoryAdoptionInventory\(\{ after, limit: 50 \}\)/,
        'the review list must use bounded keyset pagination');
    assert.match(resolution, /item\.content_preview/);
    assert.match(resolution, /item\.assignable \? 'assignable' : 'deprecate only'/);
    assert.match(resolution, /assignMemoryAdoption\(\{[\s\S]*memoryIDs: selectedIDs,[\s\S]*targetAgentID/);
    assert.match(resolution, /memoryIDs: selectedIDs/,
        'deprecation must support the exact checked subset');
    assert.match(resolution, /Historical authorship and content remain unchanged/);

    assert.match(retryAPI, /adoption-retry/);
    assert.match(retryAPI, /projection_revision: projectionRevision/);
    assert.match(retryAPI, /expected_count: expectedCount/);
    assert.doesNotMatch(retryAPI, /confirmation/,
        'retry is revision/count guarded but must not invent a typed confirmation contract');
    assert.match(deprecateAPI, /adoption-deprecate/);
    assert.match(deprecateAPI, /projection_revision: projectionRevision/);
    assert.match(deprecateAPI, /expected_count: expectedCount/);
    assert.match(deprecateAPI, /confirmation/);
    assert.match(deprecateAPI, /memory_ids: memoryIDs/);
});

test('Search page renders projection failures as errors, never empty search results', () => {
    const searchPage = appSource.slice(
        appSource.indexOf('function SearchPage()'),
        appSource.indexOf('function SynapticLedger()'),
    );
    assert.match(searchPage, /const data = await fetchMemories\(params\)/);
    assert.match(searchPage, /setError\(err && err\.message \? err\.message : 'Failed to load memories'\)/);
    assert.match(searchPage, /results\.length === 0 && !loading && !error/);
});

test('Access Controls is a first-class sidebar route', () => {
    assert.match(appSource, /hash === '\/access'\) setPage\('access'\)/);
    assert.match(appSource, /navigate\('access'\)/);
    assert.match(appSource, /page === 'access'.*NetworkPage/s);
    assert.match(appSource, /accessMode \? 'access' : 'overview'/);
    const access = appSource.slice(
        appSource.indexOf('function AppV23AccessControl()'),
        appSource.indexOf('function NetworkPage('),
    );
    assert.match(access, /appV23NormalizeAccessState\(await fetchAppV23Access\(\)\)/);
    assert.ok(
        access.indexOf('appV23NormalizeAccessState(await fetchAppV23Access())') <
            access.indexOf('setState(next)'),
        'nullable historical collections must be normalized before the first full render',
    );
});

test('CEREBRUM Root is separate from agents and uses a two-stage one-time handover', () => {
    const access = appSource.slice(
        appSource.indexOf('function AppV23AccessControl()'),
        appSource.indexOf('function NetworkPage('),
    );
    assert.match(access, /Root is this node’s owner, not an agent or an agent role/);
    assert.match(access, /It stays out of agent lists and groups/);
    assert.match(access, /does not promote an agent, move or copy domains or memories, or rewrite authorship/);
    assert.match(access, /Historical memories and their original authors remain readable and unchanged/);
    assert.match(access, /replacement immediately controls the same Root-owned domains/);
    assert.match(access, /retires the current credential from CEREBRUM Root authority/);
    assert.match(access, /federation connections keep this node’s separate stable transport identity and peer pins/);
    assert.match(access, /Root handover does not rotate it/);
    assert.match(access, /<details class="v23-root-diagnostics">/,
        'credential identifiers belong in a collapsed diagnostic instead of the normal control hierarchy');
    assert.match(access, /useModalDialog\(\(\) =>/);
    assert.match(access, /ref=\$\{rootHandoverDialogRef\}/);
    assert.match(access, /role="alertdialog" aria-modal="true" tabIndex="-1"/);
    assert.match(access, /step \$\{rootHandoverStage\} of 2/);
    assert.match(access, /I understand — continue/);
    assert.match(access, /APPV23_ROOT_HANDOVER_PHRASE/);
    assert.match(access, /Commit irreversible handover/);
    assert.match(access, /result\.recovery_bundle/);
    assert.doesNotMatch(access, /downloadBundle\(.*root/i);
    assert.match(apiSource, /expected_generation: Number\(expectedGeneration\)/);
    assert.match(apiSource, /\/v1\/dashboard\/network\/access\/root\/handover/);
    assert.doesNotMatch(apiSource, /access\/root\/bundle/);
    assert.ok(
        access.indexOf('class="v23-root-card"') > access.indexOf('class="v23-groups-section"'),
        'rare Root handover must render below everyday agent and group controls',
    );
    assert.ok(
        access.indexOf('class="v23-root-card"') < access.indexOf('class="v23-linked-readers"'),
        'Root handover should remain discoverable before the separate federation compartment',
    );
});

test('app-v23 access UI uses named policy choices and visibly separates local groups from federated readers', () => {
    const access = appSource.slice(
        appSource.indexOf('function AppV23AccessControl()'),
        appSource.indexOf('function NetworkPage('),
    );
    assert.doesNotMatch(access, /type="checkbox"/,
        'raw capability bits must never return as an editable checkbox wall');
    assert.match(access, /role="group" aria-labelledby="v23-role-label"/);
    assert.match(access, /role="group" aria-labelledby="v23-profile-label"/);
    assert.match(access, /class="btn btn-primary" disabled=\$\{saveDisabled\}/);
    assert.match(access, /selected\.needs_approval && !selected\.enrollment_active/,
        'Reject registration must be rendered only for a pending review, never an active agent');
    assert.match(access, /removeAgent\(selected\.agent_id, false\)/,
        'pending rejection must not bypass the server memory-resolution guard with force');
    assert.match(access, /If the same key registers again, it will return as a fresh review request/,
        'the operator must be told that rejection preserves immutable registration history');
    assert.match(access, /<em>\$\{member\?\.role \|\| 'member'\}\$\{member\?\.needs_reauthorization/,
        'group membership must expose the role while the explicit tier determines group data authority');
    assert.match(access, /Consensus confirmation still pending/);
    assert.match(access, /blocked duplicate authority changes in this view/);
    assert.match(apiSource, /error\.code = 'access_control_transport_uncertain'/,
        'a mutation transport failure must be treated as possibly committed, never as permission to submit a duplicate');
    assert.match(access, /error\?\.code === 'access_control_transport_uncertain'/,
        'the Access Controls UI must enter confirmation-pending state after an uncertain mutation transport');
    assert.match(access, /group\.group_id\}:\$\{group\.revision/,
        'a committed revision must remount uncontrolled group-name inputs');
    assert.match(access, /Every agent keeps full control of its own domain tree/);
    assert.match(access, /Remote agents never join local groups/);
    assert.match(access, /separate read-only link — never local membership/);
    assert.match(access, /Federation messaging is controlled separately/);
    assert.match(access, /addAdvertisedAgent\(contact\.agent_id/);
    assert.match(access, /fetchAppV23LinkedReaderIdentities\(\)/);
    assert.match(access, /'existing_link'/,
        'durable linked-reader identities must remain browsable when live advertisement is blank');
    assert.match(access, /Known exact address/);
    assert.match(access, /64-character-agent-id@chain/);
    assert.match(access, /checkAppV23LinkedReaderEligibility\(chain, id\)/);
    assert.match(access, /peer must verify it live before it appears here, and Attach\/Rebind repeats the check/);
    assert.match(access, /source: 'exact_address'/);
    assert.doesNotMatch(access, /add(?:AdvertisedAgent)?\(connection\.peer_agent_id/,
        'the JOIN transport/operator key may be Root and must never become a linked-reader target');
    assert.match(access, /Add local agent menu/,
        'drag and drop must keep an accessible non-pointer alternative');
    assert.match(access, /appV23DirectLocalGroupPlan\(state\?\.groups \|\| \[\], source, target\)/,
        'dropping one approved local agent onto another must create a real narrow Access Group');
    assert.match(access, /Drop here to remove this agent from this Access Group/,
        'dragging a member back out must revoke that group relationship');
    assert.match(access, /if \(!mutationReady \|\| groupBusy \|\| mutationLockRef\.current\)/,
        'drag/drop must not bypass broker readiness, pending confirmation, or the synchronous mutation lock');
    assert.match(access, /v23-agent-pill local" draggable=\$\{mutationReady && !groupBusy\}/,
        'the local drag tray must visibly disable while authority mutation is unsafe');
    assert.match(access, /v23-agent-pill federated" draggable=\$\{mutationReady && !linkedBusy\}/,
        'the federated drag tray must obey the same mutation-readiness gate as its explicit Attach control');
    assert.match(access, /linked:\$\{operation\}:\$\{remote\?\.remote_chain_id/,
        'linked-reader drag and explicit controls must share the synchronous mutation lock');
    assert.match(access, /created on-chain at Read\. Each owner keeps full control/,
        'new direct groups must communicate the app-v26 read baseline without implying role-based widening');
    assert.match(appSource, /Memory ownership & recovery/,
        'the agent review card must expose the affected memory inventory where the operator is making the decision');
    assert.match(appSource, /fetchMemories\(\{[\s\S]*agent: agent\.agent_id/,
        'agent recovery must load only the selected agent’s records');
    assert.match(appSource, /reassignDomainOwnership\(\{[\s\S]*source_agent_id: ''/,
        'the recovery transfer must reuse the real on-chain flow and resolve current ownership server-side instead of guessing from authorship');
    assert.match(appSource, /deleteMemory\(item\.memory_id\)/,
        'the recovery card must permit an operator to deprecate an exact checked subset');
    assert.match(appSource, /Every memory in each listed domain moves under the new owner’s current control, including records not selected here/,
        'the selected-record entry point must disclose that RBAC transfer is whole-domain, not a false per-record ownership rewrite');
    assert.match(appSource, /transferred before the next governance operation failed/,
        'multi-domain recovery must report partial success instead of implying rollback');
    assert.match(appSource, /deprecated before the next operation failed/,
        'multi-record deprecation must report partial success instead of implying rollback');
    assert.match(appSource, /Grant access again or point ownership/,
        'historical recovery must make both authority choices discoverable without conflating them with principal repair');
    assert.match(appSource, /Transfer whole domain ownership/,
        'Root recovery must expose a real governed transfer action, not only descriptive copy');
    assert.match(appSource, /source_agent_id: reviewed\.authority_owner_id/,
        'the recovery transfer must bind the exact owner the operator reviewed so stale authority fails closed');
    assert.match(appSource, /const ownedDomain = reviewed\?\.authority_owned_domain \|\| authorityDomain/,
        'whole-domain recovery must transfer the actual owning ancestor rather than a non-owning descendant label');
    assert.match(appSource, /#\/network\?agent=\$\{encodeURIComponent\(authorityTargetAgentID\)\}&domain=\$\{encodeURIComponent\(authorityDomain\)\}&recovery=1/,
        'the exact recovery choice must deep-link to the existing governed domain matrix');
    assert.match(appSource, /setExpandedTab\('recovery-domain-access'\)/,
        'the recovery deep-link must open a reachable exact-domain authority surface');
    assert.match(appSource, /initialFilter=\$\{recoveryDomain\}/,
        'the recovered domain must be the only initially visible matrix row');
    assert.match(appSource, /principal repair did not grant authority/i,
        'the matrix must not imply that an operational-principal association granted access');
    assert.match(appSource, /Ownership handover at block \$\{handover\.transferred_at\}/,
        'Root recovery must expose the append-only owner handover chain');
    assert.match(appSource, /domain created at block \$\{handover\.domain_created_at\}/,
        'owner handovers must not relabel the domain creation height');
    assert.match(access, /v23-access-legend/);
    assert.match(access, /v23-agent-choice local/);
    assert.match(access, /v23-agent-pill federated/);
    assert.match(cssSource, /\.v23-agent-choice\.local \{ border-left:/,
        'same-node identity needs a solid non-color cue');
    assert.match(cssSource, /\.v23-agent-pill\.federated[\s\S]*border-style: dashed;/,
        'federated identity needs a different non-color cue as well as a different accent');
    assert.match(cssSource, /\.v23-agent-choice\[draggable="true"\]/,
        'the real local-agent cards must advertise the direct grouping gesture');
    assert.match(access, /selected\.needs_reauthorization/);
    assert.match(access, /Root handover suspended this delegated Admin/);
    assert.match(access, /Admin suspended/);
    assert.match(access, /suspended after Root handover/);
    assert.match(access, /Reauthorize Admin/);
    assert.match(access, /class="v23-agent-name-editor"/);
    assert.match(access, /updateAppV26AgentDisplayName\(selected\.agent_id, nextName\)/);
    assert.match(access, /Registered as \$\{selected\.registered_name/);
    assert.match(access, /registered identity, agent ID, and boot purpose stay immutable/);
    assert.match(access, /result\.status === 'unchanged'/,
        'a no-op rename must not be presented as a committed transaction');
    assert.match(apiSource, /\/v1\/dashboard\/network\/access\/agents\/\$\{encodeURIComponent\(id\)\}\/name/);
    assert.match(cssSource, /\.v23-agent-name-editor \{/);
});

test('Agents page cannot expose browser-only drag grouping as access policy', () => {
    const network = appSource.slice(
        appSource.indexOf('function NetworkPage('),
        appSource.indexOf('function FederationPage('),
    );
    assert.match(network, /Browser-only layout groups are retired\. They grant nothing/);
    assert.match(network, /const grouped = \[\]/,
        'retired browser drafts must not shape the Agents list');
    assert.match(network, /draggable=\$\{false\}/,
        'the Agents page must not offer a local-only drag gesture');
    assert.match(network, /window\.location\.hash = '\/access'/,
        'the Agents page must route policy changes to consensus Access Controls');
});

test('agent removal leaves the confirmation usable if the server rejects it', () => {
    assert.match(appSource, /if \(res\.redeploy_required !== false\)/,
        'the client must respect app-v23 agent removal without a redeploy');
    assert.match(appSource, /const completed = await onConfirm\(\);\s*if \(!completed\) setRemoving\(false\);/,
        'a failed removal must release the confirmation instead of spinning indefinitely');
    assert.match(appSource, /Confirming its removal and preserving its historical memory attribution/,
        'the progress copy must not promise a redeployment that app-v23 removal does not perform');
});

test('MCP setup copy describes the app-v23 restricted identity instead of obsolete Root inheritance', () => {
    assert.doesNotMatch(appSource, /bearer (?:runs as|is) (?:an? )?(?:the )?local node operator/i);
    assert.doesNotMatch(appSource, /node-operator access|administrator credential for this SAGE node/i);
    assert.match(appSource, /separate restricted Member identity/);
    assert.match(appSource, /never inherits the approving Root or Admin key/);
});

test('historical Root authors are labelled without becoming selectable graph agents', () => {
    const brain = appSource.slice(appSource.indexOf('function BrainView('), appSource.indexOf('function MemoryDetail('));
    const memoryDetail = appSource.slice(appSource.indexOf('function MemoryDetail('), appSource.indexOf('function SearchPage('));
    assert.match(brain, /nodes\.filter\(n => !n\.agent_is_root\)/);
    assert.match(memoryDetail, /<strong>\$\{m\.agent_label\}<\/strong>/);
    assert.match(memoryDetail, /Immutable submitting-agent credential ID/);
    assert.match(memoryDetail, /m\.agent \|\| m\.submitting_agent \|\| 'unknown'/);
});

test('Access Controls exposes the exact signer identity behind every grant', () => {
    const networkPage = appSource.slice(appSource.indexOf('function NetworkPage('), appSource.indexOf('function AddAgentWizard('));
    assert.match(networkPage, /class="agent-id-short"[^>]*>ID \$\{String\(agent\.agent_id/);
    assert.match(networkPage, /These permissions apply only to signer/);
    assert.match(networkPage, /client must report this same Agent ID/);
    assert.match(networkPage, /sage_status/);
    assert.match(networkPage, /sage_inception/);
});

test('app-v26 domain transfer UI never invents a deferred owner self-grant', () => {
    assert.match(
        appSource,
        /res\.owner_access === 'legacy_self_grant' && !!res\.grant_deferred/,
        'only the dormant legacy transfer path may surface the historical self-grant warning',
    );
    assert.doesNotMatch(
        appSource,
        /if \(res\.grant_deferred\) \{/,
        'app-v26 owner-derived authority must not be treated as a deferred target-key grant',
    );
});

test('first-run onboarding offers a real create-or-join decision', () => {
    const onboarding = appSource.slice(appSource.indexOf('function OnboardingWizard('), appSource.indexOf('// PipelineView'));
    const guestJoin = appSource.slice(appSource.indexOf('function NetworkJoinGuestPanel('), appSource.indexOf('function RemoveConfirmModal('));

    assert.match(onboarding, /Start my own SAGE/);
    assert.match(onboarding, /Join an existing SAGE network/);
    assert.match(onboarding, /setShowJoinNetwork\(true\)/,
        'first-run join must open the existing authenticated join ceremony');
    assert.match(onboarding, /return html`<\$\{NetworkJoinGuestPanel\}/,
        'the join ceremony must replace the setup dialog instead of nesting two modal dialogs');
    assert.match(onboarding, /if \(showEmbedSetup\)[\s\S]*return html`<\$\{EmbeddingsSetupModal\}/);
    assert.match(onboarding, /if \(showRerankSetup\)[\s\S]*return html`<\$\{RerankerSetupModal\}/);
    assert.match(onboarding, /if \(showConnect\)[\s\S]*return html`<\$\{ConnectToolModal\}/);
    assert.match(onboarding, /if \(showChatGPT\)[\s\S]*return html`<\$\{ChatGPTTunnelWizard\}/,
        'every setup child must replace onboarding so modal trees and tab order never stack');
    assert.match(onboarding, /Nothing is shared unless you choose who can access it/,
        'creating a SAGE must default to private in plain language');
    assert.match(onboarding, /Joining here means this computer becomes part of the same SAGE network/,
        'same-chain join must not be confused with file-sharing-style federation');
    assert.match(onboarding, /useState\('private'\)/,
        'the first-run sharing decision must fail closed to private');
    assert.match(onboarding, /Keep this SAGE private/);
    assert.match(onboarding, /Share with another SAGE/);
    assert.match(onboarding, /Pairing alone shares nothing/,
        'trust must remain visibly separate from access');
    assert.match(onboarding, /Sharing works like a shared folder/,
        'the RBAC choice should use a familiar file-sharing mental model');
    assert.match(onboarding, /remove access later without deleting the trusted connection/,
        'onboarding must preserve the trust-versus-sharing lifecycle boundary');
    assert.match(onboarding, /role="group" aria-label="Choose whether to share with another SAGE"/);
    assert.match(onboarding, /aria-pressed=\$\{privacyChoice === 'private'\}/);
    assert.match(onboarding, /finish\(\); if \(onNavigate\) onNavigate\('federation'\)/,
        'sharing setup must reuse Federation rather than create a second permission path');
    assert.match(onboarding, /Keep private & continue/);
    assert.match(onboarding, /fetchLedgerStatus\(\)/,
        'onboarding recovery status must come from the node, not local browser state');
    assert.match(onboarding, /return html`<\$\{SynapticLedgerModal\}/,
        'recovery setup must reuse the real ledger flow with only one modal exposed');
    assert.match(onboarding, /Encryption is on — back up the recovery key/);
    assert.match(onboarding, /SAGE cannot recover your encrypted memories/);
    assert.match(onboarding, /Anyone with it can reset the passphrase and unlock your memories/);
    assert.match(onboarding, /Set up encryption & recovery/);
    assert.match(onboarding, /Turn on <strong>smart search<\/strong>/,
        'first-run copy must explain the benefit before implementation names');
    assert.match(onboarding, /const titles = \['How do you want to start\?', 'Smart search'/,
        'the first-run heading must use the same plain-language name');
    assert.match(onboarding, /Smart search is <strong>on<\/strong> — your AI tools can find memories by meaning/,
        'enabled smart search must not fall back to implementation jargon');
    assert.doesNotMatch(onboarding, /Wire an AI tool to this node|agent identit/,
        'first-run copy must not require node or agent-identity jargon');
    assert.match(onboarding, /Finish setup/,
        'choosing sharing must still leave an obvious way to finish without sharing now');
    assert.match(onboarding, /role="dialog" aria-modal="true" aria-labelledby="onboarding-title"/);
    assert.match(onboarding, /ref=\$\{dialogRef\} tabIndex="-1"/);
    assert.match(onboarding, /aria-label="Close setup"/);
    assert.match(guestJoin, /Your stored memories are kept/,
        'the destructive-action warning must say what remains safe');
    assert.match(guestJoin, /export a backup from <em>Settings → Maintenance<\/em> first/,
        'a lived-in node must get a concrete safety step before replacing network history');
    assert.match(guestJoin, /<label for="network-join-code">Pairing code from the host<\/label>/,
        'the visible pairing-code label must name the text box for assistive technology');
    assert.match(guestJoin, /<textarea id="network-join-code"/);
    assert.match(cssSource, /\.onboarding-choice:focus-visible/,
        'the choice buttons need an obvious keyboard focus indicator');
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.onboarding-choices\s*\{\s*grid-template-columns:\s*1fr/,
        'create-or-join choices must remain readable on a narrow window');
});

test('onboarding setup dialogs own and restore keyboard focus', () => {
    const modalHook = appSource.slice(appSource.indexOf('function useModalDialog('), appSource.indexOf('// MriView'));
    const ledger = appSource.slice(appSource.indexOf('function SynapticLedgerModal('), appSource.indexOf('function SoftwareUpdate('));
    const embeddings = appSource.slice(appSource.indexOf('function EmbeddingsSetupModal('), appSource.indexOf('function RestartNodeButton('));
    const reranker = appSource.slice(appSource.indexOf('function RerankerSetupModal('), appSource.indexOf('function FederationSettingRow('));
    const chatGPT = appSource.slice(appSource.indexOf('function ChatGPTTunnelWizard('), appSource.indexOf('function ChatGPTCopyField('));
    const connect = appSource.slice(appSource.indexOf('function ConnectToolModal('), appSource.indexOf('function NetworkJoinHostPanel('));
    const join = appSource.slice(appSource.indexOf('function NetworkJoinGuestPanel('), appSource.indexOf('function RemoveConfirmModal('));

    assert.match(modalHook, /requestAnimationFrame\(\(\) => dialog\.focus\(\)\)/,
        'a modal must receive focus when it opens');
    assert.match(modalHook, /event\.key !== 'Tab'/);
    assert.match(modalHook, /event\.key === 'Escape'/);
    assert.match(modalHook, /dialog\.contains\(document\.activeElement\)/,
        'a setup modal must not steal keyboard events from an alertdialog above it');
    assert.match(modalHook, /child\.inert = true/,
        'non-modal app branches must leave the accessibility tree');
    assert.match(modalHook, /while \(branch && branch !== app\)/);
    assert.match(modalHook, /for \(const child of parent\.children\)/,
        'nested modals must inert underlying controls in their own page branch too');
    assert.match(modalHook, /origin && origin\.isConnected/,
        'focus must return to a surviving opener when the modal closes');
    for (const source of [ledger, embeddings, reranker, chatGPT, connect, join]) {
        assert.match(source, /useModalDialog\(/);
        assert.match(source, /role="dialog" aria-modal="true"/);
        assert.match(source, /ref=\$\{dialogRef\} tabIndex="-1"/);
    }
    assert.match(join, /aria-labelledby="join-network-title"/);
    assert.match(join, /aria-label="Close network join setup"/);
});

test('recovery-key backup confirmation is durable and explicit', () => {
    const ledger = appSource.slice(appSource.indexOf('function SynapticLedger('), appSource.indexOf('function SynapticLedgerModal('));

    assert.match(apiSource, /settings\/ledger\/recovery-key\/confirm/);
    assert.match(apiSource, /export async function confirmRecoveryKeyBackup\(\)/);
    assert.doesNotMatch(apiSource.match(/export async function confirmRecoveryKeyBackup\(\)[\s\S]*?\n\}/)?.[0] || '', /recovery_key|passphrase/,
        'backup acknowledgement must never retransmit key material or a passphrase');
    assert.match(ledger, /await confirmRecoveryKeyBackup\(\)/);
    assert.match(ledger, /recovery_backup_confirmed: true/);
    assert.match(ledger, /I've stored it safely/,
        'starting a download alone must not falsely mark the key as stored');
    assert.match(ledger, /Not now — return to Security/);
    assert.match(ledger, /leaveRecoveryBackupForLater/,
        'a failed acknowledgement must not trap the user on the recovery-key screen');
    assert.match(ledger, /Backup confirmed/);
    assert.match(ledger, /Backup needed/);
    assert.match(ledger, /Back up recovery key/);
    assert.match(ledger, /aria-label="Passphrase"/);
    assert.match(ledger, /aria-label="Confirm passphrase"/,
        'recovery fields need meaningful screen-reader names, not placeholder-only names');
    assert.match(ledger, /<summary style="cursor:pointer;">Technical details<\/summary>/,
        'crypto names and filesystem paths belong behind an optional disclosure');
    assert.doesNotMatch(ledger, /<span class="label">Vault<\/span>/,
        'the normal security view must not lead with an internal vault path');
    assert.match(appSource, /aria-labelledby="ledger-setup-title"/);
    assert.match(appSource, /aria-label="Close encryption and recovery setup"/);
});

test('recovery-key transition restores keyboard and screen-reader context', () => {
    const ledger = appSource.slice(appSource.indexOf('function SynapticLedger()'), appSource.indexOf('function SynapticLedgerModal('));
    assert.match(ledger, /const recoveryHeadingRef = useRef\(null\)/);
    assert.match(ledger, /requestAnimationFrame\(\(\) => recoveryHeadingRef\.current\?\.focus\(\)\)/,
        'focus must move when the focused enable/change button is replaced by the recovery screen');
    assert.match(ledger, /<h3 ref=\$\{recoveryHeadingRef\} tabIndex="-1"/);
    assert.match(ledger, /<div class="warning-banner" role="alert"/,
        'the one-time recovery warning must be announced when it appears');
});

test('search and maintenance controls keep useful screen-reader names', () => {
    const search = appSource.slice(appSource.indexOf('function SearchPage('), appSource.indexOf('// Combobox'));
    const cleanup = appSource.slice(appSource.indexOf('function CleanupSettings('), appSource.indexOf('function UnreadableMemoriesPanel('));
    const settings = appSource.slice(appSource.indexOf('function SettingsPage('), appSource.indexOf('function GuidePage('));

    for (const label of [
        'Filter memories by domain',
        'Filter by memory lifecycle status',
        'Filter memories by tag',
        'Filter memories by agent',
        'Filter by when the memory was created',
        'Created on or after',
        'Created on or before',
        'Sort memories',
    ]) {
        assert.match(search, new RegExp(`aria-label="${label}"`));
    }
    assert.match(search, /aria-label=\$\{`Select memory from \$\{m\.domain_tag\} for bulk actions`\}/);
    for (const label of [
        'Enable automatic memory cleanup',
        'Observation lifetime in days',
        'Session context lifetime in days',
        'Stale confidence threshold',
        'Cleanup interval in hours',
    ]) {
        assert.match(cleanup, new RegExp(`aria-label="${label}"`));
    }
    assert.match(settings, /aria-label="Enable contextual tooltips"/);
    assert.match(appSource, /aria-label="Open SAGE at login"/);
});

test('destructive memory actions use the consistent explanatory dialog', () => {
    const detail = appSource.slice(appSource.indexOf('function MemoryDetail('), appSource.indexOf('function SearchPage('));
    const cleanup = appSource.slice(appSource.indexOf('function CleanupSettings('), appSource.indexOf('function UnreadableMemoriesPanel('));
    const restart = appSource.slice(appSource.indexOf('function RestartNodeButton('), appSource.indexOf('function RerankerSetupModal('));
    const settings = appSource.slice(appSource.indexOf('function SettingsPage('), appSource.indexOf('function GuidePage('));

    assert.match(detail, /await showConfirmation\(message/);
    assert.match(detail, /stops appearing in normal recall and search, but remains in the on-chain audit history/);
    assert.match(detail, /Other memories, domains, and trusted connections are unchanged/,
        'forgetting one memory must say what remains safe');
    assert.match(detail, /Could not forget memory/,
        'a failed destructive action must keep an actionable error');
    assert.doesNotMatch(detail, /Click again to confirm|Confirm Delete/,
        'memory deletion must not use an undiscoverable click-twice interaction');

    assert.match(cleanup, /Clean Synaptic Ledger\?/);
    assert.match(cleanup, /Use Preview first if you want to see the exact count/,
        'cleanup must provide a concrete next step before mutation');
    assert.match(cleanup, /remain in the audit history/);
    assert.match(cleanup, /Memories outside the current rules stay active/,
        'cleanup must say what remains safe');
    assert.doesNotMatch(cleanup, /Click again to confirm|confirmCleanup/,
        'cleanup must use the shared accessible dialog instead of a five-second click-twice latch');
    assert.match(restart, /await showConfirmation\(/);
    assert.match(restart, /memories, settings, trusted connections, sharing groups, and access rules are not deleted/,
        'restart must say what remains safe and that the encrypted ledger re-locks');
    assert.doesNotMatch(restart, /Click again to confirm|arming|armRef/);
    assert.match(settings, /Turn off semantic memory\?/);
    assert.match(settings, /Your memories remain stored/,
        'switching recall mode must explain that stored memories remain safe');
    assert.doesNotMatch(settings, /Click again to switch|confirmHashEmbedding/);
    assert.doesNotMatch(detail, /setConfirming/,
        'the shared-dialog conversion must not leave a stale state setter that crashes memory detail');
});

test('memory export is honest about plaintext and fresh-machine scope', () => {
    const settings = appSource.slice(appSource.indexOf('function SettingsPage('), appSource.indexOf('function GuidePage('));
    const importPage = appSource.slice(appSource.indexOf('function ImportPage('), appSource.indexOf('function NetworkPage('));

    assert.match(settings, /await showConfirmation\(/,
        'privacy-affecting export must use the shared accessible dialog');
    assert.match(settings, /Synaptic Ledger is encrypted on this computer, but the downloaded file will not be encrypted/,
        'encrypted-at-rest users must be warned that export is plaintext');
    assert.match(settings, /does not include trusted connections, sharing groups, access rules, AI-tool credentials, settings, or full chain history/,
        'the export must not be described as a complete node backup');
    assert.match(settings, /Download unencrypted backup/);
    assert.match(settings, /Memories you chose to forget are not included/,
        'backup copy must make the forgotten-memory boundary explicit');
    assert.doesNotMatch(settings, /window\.open\('\/v1\/dashboard\/export'/,
        'the old one-click export path must not bypass confirmation');
    assert.match(settings, /You can import it on a new SAGE to recreate your memories/);
    assert.match(importPage, /Trusted connections, sharing groups, access rules, credentials, settings, and chain history are not restored/,
        'the restore surface must repeat the memory-only backup boundary');
    assert.match(importPage, /Memories you chose to forget are not restored/,
        'older backups must not silently resurrect forgotten memories');
    assert.match(importPage, /setError\(res\.message \|\| res\.error\)/,
        'import errors must show the friendly server explanation instead of a machine code');
});

test('governance wizard builds structured canonical quorum scopes', () => {
    const networkPage = appSource.slice(appSource.indexOf('function NetworkPage('), appSource.indexOf('function AddAgentWizard('));
    assert.match(networkPage, /<option value="scope_action">Form or Revise Quorum Scope<\/option>/);
    assert.match(networkPage, /proposal\.scope = \{/);
    assert.match(networkPage, /controller_validator_id: govScopeController/);
    assert.match(networkPage, /govScopeDomains\.split\(\/\[\\n,\]\//);
    assert.match(networkPage, /assigned_weight: parseInt\(member\.weight, 10\)/);
    assert.match(networkPage, /joined_revision: parseInt\(member\.joinedRevision, 10\)/);
    assert.match(networkPage, /govScopeControllerMember/,
        'the selected controller must also be an active selected roster member');
    assert.match(networkPage, /govScopeValidatorOptions = govScopeValidators/,
        'scope authority must come from the live CometBFT validator set, not ordinary agent rows');
    assert.match(networkPage, /Number\.isSafeInteger\(weight\)/,
        'the browser must not round canonical uint64 weights before submission');
    assert.match(networkPage, /above two-thirds of this pinned integer weight/);
    const scopeSubmitBranch = networkPage.slice(
        networkPage.indexOf("if (govNewOp === 'scope_action')"),
        networkPage.indexOf('const res = await submitGovProposal(proposal);')
    );
    assert.doesNotMatch(scopeSubmitBranch, /btoa\(|proposal\.payload/,
        'the dashboard must submit structured scope JSON, not recreate the binary codec');
});

test('app-v24 memory repair stays Root-planned and requires an explicit vote', () => {
    const networkPage = appSource.slice(appSource.indexOf('function NetworkPage('), appSource.indexOf('function AddAgentWizard('));
    assert.match(networkPage, /fetchMemoryReanchorPlan\(\)/,
        'the browser must request a locally attested plan rather than inventorying memory rows itself');
    assert.match(networkPage, /operation: memoryRepairPlan\.operation/);
    assert.match(networkPage, /payload: memoryRepairPlan\.payload/);
    assert.match(networkPage, /No memory content, author, domain, status, or chain history will be rewritten/);
    assert.match(networkPage, /It does not execute automatically/);
    assert.match(networkPage, /cast this validator’s explicit vote/);
    assert.doesNotMatch(networkPage, /submitGovVote\(result\.proposal_id/,
        'repair proposal creation must never cast a synthetic browser vote');
});

test('chain activity exposes committed RBAC changes and remains usable while empty', () => {
    const activity = appSource.slice(appSource.indexOf('function ChainActivityLog('), appSource.indexOf('// ============================================================================\n// Health Status Bar'));
    assert.match(activity, /sse\.on\('access'/);
    assert.match(activity, /Permissions Updated/);
    assert.match(activity, /Access Granted/);
    assert.match(activity, /Access Revoked/);
    assert.match(activity, /height: \$\{logHeight\}px/);
    assert.match(activity, /↓ Expand/);
    assert.match(activity, /aria-label="Resize Chain Activity"/);
});

test('chain health recognizes app-v26 as the current consensus protocol', () => {
    assert.match(appSource, /const appVerTone = appVer === '26'/);
    assert.match(appSource, /Green when current \(26\)\./);
    assert.doesNotMatch(appSource, /appVer === '25'/);
    assert.doesNotMatch(appSource, /appVer === '22'/);
    assert.doesNotMatch(appSource, /appVer === '15'/);
});

test('task board scrolls as one page instead of trapping wheel input in columns', () => {
    const tasksPage = cssSource.match(/\.tasks-page\s*\{([^}]*)\}/)?.[1] || '';
    const cards = cssSource.match(/\.kanban-cards\s*\{([^}]*)\}/)?.[1] || '';
    assert.match(tasksPage, /overflow-y:\s*auto/);
    assert.doesNotMatch(tasksPage, /overflow:\s*hidden/);
    assert.match(cards, /overflow-y:\s*visible/);
});

test('federation owns a viewport-bounded vertical scroll container', () => {
    const federationPage = cssSource.match(/\.fed-page\s*\{([^}]*)\}/)?.[1] || '';
    assert.match(federationPage, /flex:\s*1/);
    assert.match(federationPage, /min-height:\s*0/);
    assert.match(federationPage, /overflow-y:\s*auto/);
    const scanVideo = cssSource.match(/\.fed-scan-video\s*\{([^}]*)\}/)?.[1] || '';
    assert.match(scanVideo, /max-height:\s*min\(46vh,\s*340px\)/,
        'a live camera must remain bounded on short laptop viewports');
    assert.match(cssSource, /@media \(max-width:\s*820px\)[\s\S]*\.fed-exchange\s*\{\s*grid-template-columns:\s*1fr/,
        'the two-way scan cards must stack into the page scroll container on narrow screens');
    assert.match(cssSource, /@media \(max-height:\s*760px\)[\s\S]*\.fed-exchange-card \.fed-qr-canvas/,
        'short laptop viewports must compact the initial QR instead of opening with it clipped');
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.health-bar[\s\S]*white-space:\s*nowrap/,
        'mobile health metadata must stay on one horizontally scrollable line');
});

test('federation endpoint discovery uses the node listener as the signed source of truth', () => {
    const hook = appSource.slice(appSource.indexOf('function useLanEndpoint()'), appSource.indexOf('// FedEndpointPicker'));
    const guestWizard = appSource.slice(appSource.indexOf('function GuestJoinWizard('), appSource.indexOf('// HostJoinWizard'));
    assert.match(hook, /useState\(''\)/,
        'the browser must not guess the historical 8444 port before asking the node');
    assert.match(hook, /const authoritative = r\.suggested_endpoint \|\| ''/,
        'only the server-validated listener suggestion may drive the advertised JOIN endpoint');
    assert.doesNotMatch(hook, /location\.hostname/,
        'the browser must not fabricate an actionable host after authoritative discovery returns empty');
    assert.match(hook, /if \(!authoritative\) setEndpointFailed\(true\)/,
        'an empty authoritative suggestion must stay a visible recovery state');
    assert.match(hook, /setEndpointFailed\(true\)/,
        'endpoint discovery failures must remain explicit instead of being swallowed');
    assert.match(guestWizard, /busy=\$\{busy \|\| !endpointReady \|\| endpointInvalid\}/,
        'a partial endpoint must stay blocked while an empty endpoint may scan an Internet/P2P code');
    assert.match(guestWizard, /!!String\(endpoint \|\| ''\)\.trim\(\) && !isFederationEndpointFormatValid\(endpoint\)/,
        'an empty endpoint is reserved for backend-verified P2P enrollment, never accepted as a partial LAN endpoint');
    assert.match(guestWizard, /open=\$\{endpointFailed \|\| endpointMissing \|\| endpointInvalid/,
        'endpoint failure must open the manual recovery controls');
    assert.match(hook, /if \(isFederationEndpointFormatValid\(normalized\)\) setEndpointFailed\(false\)/,
        'a complete manual recovery address should clear stale discovery clutter without accepting partial input');
    const validatorSource = appSource.match(/function isFederationEndpointFormatValid\(ep\) \{[\s\S]*?\n\}/)?.[0];
    assert.ok(validatorSource, 'endpoint format validator must exist');
    const validate = Function(`${validatorSource}; return isFederationEndpointFormatValid;`)();
    assert.equal(validate('h'), false);
    assert.equal(validate('https://192.168.1.20:18444'), true);
    assert.equal(validate(' https://192.168.1.20:18444 '), false);
    assert.equal(validate('https://192.168.1.20:443'), true);
    assert.equal(validate('https://[fd00::20]:18444'), true);
    assert.equal(validate('https://[::1]:443'), true);
    assert.equal(validate('https://192.168.1.20:'), false);
    assert.equal(validate('https://[::1]:'), false);
    assert.equal(validate('https://192.168.1.20'), false);
    assert.equal(validate('https://[::1]'), false);
    assert.equal(validate('https:192.168.1.20:18444'), false);
    assert.equal(validate('https://fd00::20:18444'), false);
    assert.equal(validate('https://192.168.1.20:0'), false);
    assert.equal(validate('https://192.168.1.20:65536'), false);
    assert.equal(validate('https://192.168.1.20:nope'), false);
    assert.equal(validate('http://192.168.1.20:18444'), false);
    assert.equal(validate('https://192.168.1.20:18444/path'), false);
    assert.equal(validate('https://192.168.1.20:18444?x=1'), false);
    const hostWizard = appSource.slice(appSource.indexOf('function HostJoinWizard('), appSource.indexOf('function fedCatalogMap('));
    assert.match(hostWizard, /fedJoinRoutes\(\)/,
        'the host must ask the node to prepare routes instead of guessing from the browser');
    assert.match(hostWizard, /fedHostCreate\(endpoint, 'auto'\)/,
        'one product intent must let the node choose Direct or Secure relay');
    assert.match(hostWizard, /Advanced Direct address/,
        'a manual endpoint remains recovery detail rather than a required topology choice');
    assert.match(guestWizard, /if \(confirmInFlight\.current\) return;[\s\S]*confirmInFlight\.current = true/,
        'same-tick final-confirm clicks must collapse into one dashboard request');
    assert.match(guestWizard, /catch \(e\) \{[\s\S]*confirmInFlight\.current = false;[\s\S]*fail\(e\);/,
        'a failed final confirmation must release the latch for one explicit retry');
    assert.doesNotMatch(guestWizard, /federation\.listen_addr<\/code>[^<]*in Settings/,
        'guest recovery copy must not send operators to a nonexistent listen-address setting');
    assert.doesNotMatch(appSource, /https:\/\/192\.168\.1\.(?:10|20):8444/,
        'manual recovery hints must not reintroduce the historical default-port defect');
});

test('federation ceremony presents one clear two-way scan flow without dropping the safety check', () => {
    const hostWizard = appSource.slice(appSource.indexOf('function HostJoinWizard('), appSource.indexOf('function fedCatalogMap('));
    assert.match(hostWizard, /\['showqr', 'waiting', 'compare', 'readback'\]\.includes\(step\)/,
        'the visible host QR must keep polling so an expired backend join window cannot look open');
    assert.match(hostWizard, /fed-wizard-wide/);
    assert.match(hostWizard, /class="fed-exchange"/);
    assert.match(hostWizard, /They scan this SAGE/);
    assert.match(hostWizard, /Scan their SAGE back/);
    assert.ok(hostWizard.indexOf('They scan this SAGE') < hostWizard.indexOf('Scan their SAGE back'));
    assert.match(appSource, /Scan each other[\s\S]*Confirm colleague[\s\S]*Connected/,
        'operators should see three human stages rather than protocol internals');
    assert.match(hostWizard, /expectedCode=\$\{view\.code_g\}/,
        'the pin-bound anti-relay safety code remains deliberate and unskippable');
    assert.match(appSource, /expectedCode=\$\{codes\.code_h\}/,
        'both operators still independently confirm the connection');
    const guestWizard = appSource.slice(appSource.indexOf('function GuestJoinWizard('), appSource.indexOf('// HostJoinWizard'));
    assert.match(guestWizard, /useState\('scan'\)/,
        'the normal guest flow must open directly on the camera scan');
    assert.doesNotMatch(appSource, /function FedChannelGate/,
        'remote/fallback help must be progressive disclosure, not a three-choice preflight');
    assert.match(guestWizard, /Advanced: use a specific Direct address/);
    assert.match(appSource, /Why check a number after scanning\?/,
        'the remaining anti-relay fingerprint must explain why it exists');
    assert.match(guestWizard, /They confirm you[\s\S]*You confirm them/,
        'the guest should see one mutual check with one responsibility per person');
    assert.match(hostWizard, /You confirm them[\s\S]*They confirm you/,
        'the host should see the same mutual check from the opposite perspective');
    assert.match(hostWizard, /step === 'waiting' && v\.guest_chain\) setStep\('compare'\)/,
        'the host should move directly from the reciprocal scan to the one real trust decision');
    assert.doesNotMatch(hostWizard, /step === 'review'/,
        'a redundant pre-confirmation screen must not make the host approve the same colleague twice');
    assert.match(hostWizard, /trustOnly=\$\{true\}/);
    assert.match(appSource, /This establishes trust only; no domains are shared yet/,
        'the single host confirmation screen must preserve the trust-versus-permissions boundary');
    assert.match(apiSource, /fedGuestAbort/);
    assert.match(guestWizard, /await fedGuestAbort\(scan\.session_id\)/,
        'a guest-side Stop must notify the waiting peer');
});

test('federation uses one automatic route flow and explains every actionable route state', () => {
    const hostWizard = appSource.slice(appSource.indexOf('function HostJoinWizard('), appSource.indexOf('function fedCatalogMap('));

    assert.match(apiSource, /fedJoinRoutes\(\).*\/v1\/dashboard\/federation\/join\/routes/);
    assert.match(apiSource, /fedHostCreate\(endpoint, transport = 'auto'\)/);
    assert.match(appSource, /from '\.\/federation-route-state\.js'/);
    assert.match(hostWizard, /<h2>Connect another SAGE<\/h2>/);
    assert.match(hostWizard, /checks Direct and Secure relay routes and chooses the best one automatically/);
    assert.match(hostWizard, /phase: 'prepared'/);
    assert.doesNotMatch(hostWizard, /Same Wi|local network<\/button>|Across the internet|setRouteMode/,
        'operators must not choose topology that SAGE can negotiate itself');
    assert.match(hostWizard, /legacy_compatible: true/);
    assert.match(appSource, /Older SAGE versions remain compatible through the Direct route/);
    assert.match(appSource, /normalized\.phase === 'prepared' \? 'Prepared' : 'Ready'/);

    for (const [state, label] of [
        ['locked', 'SAGE locked'],
        ['offline', 'Offline'],
        ['old_peer', 'Older SAGE'],
        ['route_failure', 'Route failed'],
        ['trust_failure', 'Trust check failed'],
        ['security_blocked', 'Security blocked'],
        ['disabled', 'Federation off'],
    ]) {
        assert.match(federationRouteSource, new RegExp(`state === '${state}'[\\s\\S]*label: '${label}'`),
            `${state} must remain distinguishable instead of collapsing to unreachable`);
    }
    assert.match(federationRouteSource, /label: 'Direct'/);
    assert.match(federationRouteSource, /label: 'Secure relay'/);
    assert.match(federationRouteSource, /if \(value\.failure_state\)/);
    assert.ok(
        federationRouteSource.indexOf('if (value.failure_state)') < federationRouteSource.indexOf("if (value.route && typeof value.route === 'object')"),
        'typed failures must win over historical route diagnostics',
    );
    assert.match(appSource, /federationConnectionAction/);
    assert.match(cssSource, /\.fed-route-diagnostic\.danger/);
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.fed-roles\s*\{\s*grid-template-columns:\s*1fr/,
        'the one flow and its recovery entry points must remain readable on mobile');
});

test('federation separates trust from directional per-domain RBAC', () => {
    const panel = appSource.slice(appSource.indexOf('function FedPermissionsPanel('), appSource.indexOf('// FederationWarmup'));
    const remoteSection = panel.slice(panel.indexOf('fed-perm-section fed-perm-remote'));

    assert.match(apiSource, /\/v1\/dashboard\/federation\/shareable-domains/);
    assert.match(apiSource, /connections\/\$\{encodeURIComponent\(chainId\)\}\/permissions/);
    assert.match(apiSource, /\{ permissions \}/,
        'permission updates must replace a complete directional permission snapshot');
    assert.match(panel, /local_permissions/);
    assert.match(panel, /remote_permissions/);
    assert.match(panel, /remote_known/);
    assert.match(panel, /setSaved\(null\); setDraft\(null\)/,
        'a failed snapshot read must fail closed instead of making an empty replacement editable');
    assert.match(panel, /\.\.\.Object\.keys\(catalog\).*\.\.\.Object\.keys\(saved\).*\.\.\.Object\.keys\(draft\)/s,
        'catalog rows must retain stale grants so they remain revocable');
    assert.match(appSource, /query\.endsWith\('\*'\).*name\.startsWith/s,
        'a filter such as tii* must use prefix matching');
    assert.match(panel, /visibleRows\.forEach/,
        'bulk changes must apply only to the filtered rows');
    assert.match(panel, /visibleSharedRows = visibleRows\.filter\(row => fedPermissionIsEnabled\(saved\[row\.domain\]\)\)/,
        'already-shared rows must be grouped from the persisted permission snapshot');
    assert.match(panel, /visibleUnsharedRows = visibleRows\.filter\(row => !fedPermissionIsEnabled\(saved\[row\.domain\]\)\)/,
        'unshared rows must be kept in a separate group');
    assert.match(appSource, /const fedPermissionRenderBatch = 75;/,
        'agreement detail tables must have a strict initial render bound');
    assert.match(panel, /renderedSharedRows = visibleSharedRows\.slice\(0, localRowLimit\)/,
        'existing grants must lead the bounded local window');
    assert.match(panel, /renderedUnsharedRows = visibleUnsharedRows\.slice\(0, remainingLocalSlots\)/,
        'unshared rows may only consume the remaining bounded window');
    assert.match(panel, /orderedRemoteRows = \[[\s\S]*subscribeDraft\.includes\(domain\)[\s\S]*!subscribeDraft\.includes\(domain\)/,
        'retained remote subscriptions must lead the bounded remote window');
    assert.match(panel, /renderedRemoteRows = orderedRemoteRows\.slice\(0, remoteRowLimit\)/,
        'the remote permission table must also remain render-bounded');
    assert.match(panel, /setLocalRowLimit\(limit => limit \+ fedPermissionRenderBatch\)/);
    assert.match(panel, /setRemoteRowLimit\(limit => limit \+ fedPermissionRenderBatch\)/);
    assert.match(panel, /role="tablist" aria-label=\$\{`Domain sharing direction for \$\{peerName\}`\}/,
        'directional sharing must use an explicit accessible two-tab switch');
    assert.match(panel, /directionView === 'outgoing' && \(showOutgoing/,
        'the large local matrix must only be mounted on the outgoing tab');
    assert.match(panel, /directionView === 'incoming' && html`<section class="fed-perm-section fed-perm-remote">/,
        'the remote matrix must only be mounted on the incoming tab');
    assert.match(panel, /This SAGE ← \$\{peerName\}/);
    assert.match(panel, /visibleRows\.forEach/,
        'render windowing must not narrow filtered bulk-action semantics');
    assert.match(appSource, /function fedCatalogMapsEqual\(a, b\)/);
    assert.match(panel, /setCatalog\(current => current !== null && fedCatalogMapsEqual\(current, nextCatalog\) \? current : nextCatalog\)/,
        'unchanged catalog polls must preserve object identity and skip table reconciliation');
    assert.match(panel, /onInput=\$\{e => \{ setLocalRowLimit\(fedPermissionRenderBatch\); setFilter\(e\.target\.value\); \}\}/,
        'changing the filter must reset the bound before rendering a broader result set');
    assert.match(panel, /setRemote\(current => fedPermissionMapsEqual\(current, nextRemote\) \? current : nextRemote\)/,
        'unchanged remote permission polls must not reconcile the bounded table');
    assert.ok(panel.indexOf('Already shared with') < panel.indexOf('Not shared'),
        'already-shared domains must be rendered before unshared domains');
    assert.match(panel, /renderedSharedRows\.map\(renderLocalPermissionRow\)/);
    assert.match(panel, /renderedUnsharedRows\.map\(renderLocalPermissionRow\)/);
    assert.match(cssSource, /\.fed-perm-rowgroup \+ \.fed-perm-rowgroup/);
    assert.match(cssSource, /\.fed-perm-group-head\.shared > span/,
        'the already-shared group must be visually distinct');
    assert.match(panel, /if \(field === 'write'\) return/,
        'the reserved Write control must not mutate the local policy');
    assert.match(panel, /field === 'copy'\) nextPermission\.read = true/,
        'copy grants imply read');
    assert.match(panel, /field === 'read' && !enabling[\s\S]*nextPermission\.write = false;[\s\S]*nextPermission\.copy = false;/,
        'clearing read must clear dependent copy and any stale reserved Write bit');
    assert.match(panel, /Write unavailable/);
    assert.match(panel, /disabled=\$\{field === 'write'/,
        'the reserved per-domain Write checkbox must stay disabled');
    assert.match(appSource, /write: false, copy: !!p\.copy/,
        'serialized snapshots must fail closed even if stale UI state carries a Write bit');
    assert.match(remoteSection, /Copy offered/);
    assert.match(remoteSection, /Save here/);
    assert.match(panel, /fedSyncStatus\(chain\)/,
        'copy provenance must include the delivery state already exposed by the API');
    assert.match(panel, /Memories saved here are local copies sourced from/);
    assert.match(panel, /copies already retained on this SAGE remain until you explicitly delete those local memories/,
        'revocation must never imply that retained copies are remotely erased');
    assert.match(remoteSection, /toggleSubscription\(domain, permission\.copy\)/,
        'the recipient independently opts into copies only after the source offers Copy');
    assert.match(apiSource, /subscribe_domains:\s*subscribeDomains/);
    assert.match(appSource, /const trustOnlyGrant = \{ max_clearance: 4, allowed_domains: \[\]/,
        'new pairing ceremonies must establish trust with an empty legacy domain scope');
    assert.doesNotMatch(appSource, /function FedShareForm/,
        'mutable domain permissions must not be part of the trust ceremony');
    assert.doesNotMatch(panel, /Add a topic|syncRole|Managed by the host|host-managed/);
    assert.match(appSource, /<\$\{FedPermissionsPanel\} conn=\$\{c\}/);
    assert.doesNotMatch(appSource, /FedSyncPanel/);
});

test('direct federation controls are symmetric after pairing', () => {
    const panel = appSource.slice(appSource.indexOf('function FedPermissionsPanel('), appSource.indexOf('// FederationWarmup'));
    assert.match(panel, /const showOutgoing = roleKnown/);
    assert.match(panel, /\$\{roleKnown && html`<section class="fed-perm-section fed-agent-section">/,
        'agent contact controls must not disappear merely because this SAGE scanned the first code');
    assert.match(panel, /The setup role does not control ongoing access/);
    assert.match(panel, /<h4>This SAGE → \$\{peerName\}<\/h4>/);
    assert.match(panel, /<h4>\$\{peerName\} → this SAGE<\/h4>/);
    assert.doesNotMatch(panel, /\$\{localIsHost && html`<section class="fed-perm-section fed-agent-section">/);
    assert.doesNotMatch(panel, /Optionally share domains back to host|You are the guest/);
    assert.match(panel, /memories already copied onto either SAGE survive until their local owner explicitly deletes them/);
});

test('federation agent contacts stay administrative and default-off', () => {
    const panel = appSource.slice(appSource.indexOf('function FedPermissionsPanel('), appSource.indexOf('// FederationWarmup'));
    const page = appSource.slice(appSource.indexOf('function FederationPage('), appSource.indexOf('// PAGE_LABELS'));

    assert.match(apiSource, /connections\/\$\{encodeURIComponent\(chainId\)\}\/pipe-contacts/);
    assert.match(apiSource, /agent_id: agentId, contact_id: contactId, accepting: !!accepting/,
        'a toggle must carry the exact agent and contact revision instead of a display handle');
    assert.match(panel, /Agent work requests/);
    assert.match(panel, /It is not a chat/,
        'CEREBRUM must remain the administrative surface, not become a second inbox');
    assert.match(panel, /New contacts start off/);
    assert.match(panel, /role="switch"/);
    assert.match(panel, /contact\.contact_id/,
        'acceptance mutations must use the opaque contact identity');
    assert.match(panel, /fetchAgents\(\)/,
        'the local agent picker must use CEREBRUM’s friendly agent directory');
    assert.match(panel, /<select id=\$\{`fed-agent-picker-/,
        'operators must choose a local agent from a name-friendly dropdown');
    assert.match(panel, /Choose an agent by name…/);
    assert.doesNotMatch(panel, /64-character agent ID|exact agent ID/,
        'raw agent hashes must not be required by the federation UI');
    assert.match(panel, /fedPipeContactsGet\(chain, false, agentID\)/,
        'the friendly selection must still resolve through the exact authorization identity');
	assert.match(panel, /fedPermissionsSet\(chain, fedPermissionSnapshot\(nextPermissions\)\)/,
		'auto-sharing an agent-owned domain must send the permissions array required by the API');
	assert.doesNotMatch(panel, /fedPermissionsSet\(chain, nextPermissions\)/,
		'the normalized permission map must never be sent as the wire-level permissions array');
    assert.match(appSource, /function mergeFedPipeContactGrant\(base, targeted = \[\]\)/);
    assert.match(appSource, /grant\.agreement_id \|\| grant\.agreementId/,
        'normalized grants must retain their agreement binding');
    assert.match(appSource, /exact\.contacts\.find\(candidate => candidate\.agent_id === agentID\)/,
        'targeted responses may extend the bounded sample only for their requested identity');
    assert.match(panel, /pinnedLocalAgentIDsRef/);
    assert.match(panel, /\.\.\.pinnedAgentIDs\.map\(agentID => fedPipeContactsGet\(chain, false, agentID\)\)/,
        'selected out-of-sample contacts must be revalidated by exact identity');
    assert.match(panel, /const merged = mergeFedPipeContactGrant\(result\.local_contacts, targeted\)[\s\S]*setLocalPipeContacts\(merged\)/,
        'each background poll must rebuild from the authoritative bounded sample');
    assert.match(appSource, /const maxPinnedFedPipeContacts = 4;/,
        'exact-agent background revalidation must remain strictly bounded');
    assert.match(apiSource, /if \(!res\.ok\) throw new Error\(data\?\.error/,
        'agent-directory HTTP failures must reject instead of masquerading as an empty directory');
    assert.match(panel, /fetchAgents\(\)\.then\([\s\S]*setLocalAgentDirectory/,
        'manual Refresh must retry the friendly agent directory');
    assert.match(panel, /fed-agent-lookup-status" role="status" aria-live="polite"/,
        'exact-agent access checks must be visibly announced');
    assert.match(panel, /added\. Work requests are Off until you enable them/,
        'successful exact-agent checks must announce both insertion and safe default-off consent');
    assert.match(panel, /pinnedLocalAgentGenerationRef/,
        'older polls must not overwrite an agent selected while they were in flight');
    assert.match(panel, /pinnedGeneration === pinnedLocalAgentGenerationRef\.current/,
        'poll and manual refresh commits must be generation-bound');
    assert.match(panel, /const lookupGeneration = pinnedLocalAgentGenerationRef\.current \+ 1;[\s\S]*fedPipeContactsGet\(chain, false, agentID\);[\s\S]*if \(lookupGeneration !== pinnedLocalAgentGenerationRef\.current\)/,
        'exact-agent lookup responses must not publish after a newer contact projection wins');
    assert.match(panel, /const mutationGeneration = pinnedLocalAgentGenerationRef\.current \+ 1;[\s\S]*fedPipeContactSet[\s\S]*if \(mutationGeneration !== pinnedLocalAgentGenerationRef\.current\) return;/,
        'contact mutation responses must not publish after a newer projection wins');
    assert.match(panel, /pipeContactMutationRef\.current = true;[\s\S]*finally \{[\s\S]*pipeContactMutationRef\.current = false;/,
        'background projections must be excluded throughout a contact mutation');
    assert.match(panel, /const contactGeneration = pinnedLocalAgentGenerationRef\.current;[\s\S]*fedPipeContactsGet\(chain, false\);[\s\S]*if \(contactGeneration === pinnedLocalAgentGenerationRef\.current\)/,
        'the post-policy-save refresh must not overwrite a newer contact mutation');
    assert.doesNotMatch(panel, /\}\s*setLocalPipeContactsKnown\(true\);\s*if \(result\.remote_known/,
        'a stale poll must not mark a newer failed contact projection as known');
    assert.match(panel, /check access<\/option>/,
        'directory options must disclose that shared-domain eligibility is not known until selection');
    assert.match(panel, /Up to four newly selected agents outside the recent list/,
        'the bounded exact-revalidation behavior must be explained beside the picker');
    assert.match(panel, /contact\.address \|\| contact\.handle/,
        'the primary copy action must prefer the exact single-peer route');
    assert.match(panel, /Copy address/);
    assert.match(panel, /aria-label=\$\{`Copy address for \$\{contact\./,
        'remote contact copy controls must identify the contact they copy');
    assert.match(panel, /Update and reset/,
        'domain replacement must warn before clearing enabled work-request consent');
	assert.match(panel, /setSyncSaveErr\(String\(e\.message \|\| e\)\)/,
		'remote copy-save failures must use feedback local to their Save controls');
	assert.match(panel, /Array\.isArray\(response\.warnings\)/,
		'local permission saves must consume partial-success cleanup warnings');
	assert.match(panel, /warnings\.length > 0[\s\S]*setErr\([\s\S]*showToast\([\s\S]*'warning'[\s\S]*else[\s\S]*showToast\([\s\S]*'success'/,
		'cleanup warnings must stay beside the local Save controls and suppress the green success toast');
	assert.match(panel, /copy_alignment_pending === true/,
		'permission loads must surface a durable Copy/RBAC alignment retry');
	assert.match(panel, /disabled=\$\{\(!dirty && !alignmentPending\) \|\| busy \|\| !!pipeContactBusy \|\| pipeContactLookupBusy\}[\s\S]*Retry copy alignment/,
		'a pure alignment retry must remain actionable after reload');
	assert.match(panel, /response\.policy_replaced !== false/,
		'a pure alignment retry must not reset unchanged agent acceptance switches');
    assert.match(panel, /\$\{syncErr && html`<div class="fed-err fed-perm-error" role="alert">/,
        'remote refresh feedback must be visible and announced in its section');
    assert.match(panel, /\$\{syncSaveErr && html`<div class="fed-err fed-perm-error" role="alert">/,
        'remote copy-save feedback must remain visible beside the bottom Save controls');
    assert.match(panel, /if \(dirty && !localPipeContactsKnown/,
        'an unavailable contact snapshot must conservatively warn before a domain save');
    assert.match(page, /Live Read, Copy, and agent work requests stop immediately/,
        'pause feedback must cover every suspended federation capability');
    assert.doesNotMatch(panel, /Send message|Compose message|Message body/,
        'the federation panel must not grow a human chat composer');
});

test('federation contact projection drops stale exact contacts and keeps only revalidated extensions', () => {
    const contact = (agentID, contactID) => ({ agent_id: agentID, contact_id: contactID });
    const grant = (contacts, agreement = 'agreement-a') => ({
        agreement_id: agreement,
        contacts,
    });

    const normalized = normalizeFedPipeContactGrant(grant([contact('base', 'base-1')]));
    assert.equal(normalizeFedPipeContactGrant(normalized).agreementId, 'agreement-a',
        'normalization must be idempotent for the agreement binding');

    const selected = mergeFedPipeContactGrant(
        grant([contact('base', 'base-1')]),
        [{ agentID: 'selected', grant: grant([contact('base', 'base-1'), contact('selected', 'selected-1')]) }]
    );
    assert.deepEqual(selected.contacts.map(item => item.agent_id).sort(), ['base', 'selected']);

    const selectedRevoked = mergeFedPipeContactGrant(
        grant([contact('base', 'base-1'), contact('selected', 'stale-selected')]),
        [{ agentID: 'selected', grant: grant([contact('base', 'base-1')]) }]
    );
    assert.deepEqual(selectedRevoked.contacts.map(item => item.agent_id), ['base'],
        'a targeted response that no longer contains the selected agent must remove it');

    const authoritativeEmpty = mergeFedPipeContactGrant(grant([]), []);
    assert.deepEqual(authoritativeEmpty.contacts, [],
        'an authoritative empty bounded poll must remain empty');

    const changedAgreement = mergeFedPipeContactGrant(
        grant([contact('base', 'base-2')], 'agreement-b'),
        [{ agentID: 'selected', grant: grant([contact('selected', 'stale')], 'agreement-a') }]
    );
    assert.deepEqual(changedAgreement.contacts.map(item => item.agent_id), ['base'],
        'an exact lookup from a prior agreement must not extend the current projection');
});

test('access-control bulk actions stay scoped and saves expose consensus progress', () => {
    const matrix = appSource.slice(appSource.indexOf('function DomainAccessMatrix('), appSource.indexOf('// --- Activity Tab ---'));
    const network = appSource.slice(appSource.indexOf('function NetworkPage('), appSource.indexOf('// PAGE_LABELS'));

    assert.match(matrix, /filtered\.forEach\(d =>/,
        'filtered bulk controls must change only the rows the operator can see');
    assert.doesNotMatch(matrix, /allDomains\.forEach\(d =>/,
        'a filtered bulk action must never silently change every domain');
    assert.match(matrix, /Read visible/);
    assert.match(matrix, /Write visible/);
    assert.match(matrix, /Modify visible/);
    assert.match(matrix, /Revoke visible/);
    assert.match(matrix, /field === 'modify' && upd\.modify[\s\S]*upd\.read = true;[\s\S]*upd\.write = true;/,
        'Modify must be a monotonic level-3 tier that includes Read and Write');
    assert.match(matrix, /field === 'write' && !upd\.write\) upd\.modify = false;/,
        'removing Write must also remove Modify');
    assert.match(matrix, /field === 'read' && !upd\.read[\s\S]*upd\.write = false;[\s\S]*upd\.modify = false;/,
        'removing Read must also remove higher permission tiers');
    assert.match(matrix, /isReservedSharedDomain\(d\)/,
        'bulk Modify must skip reserved shared domains that cannot carry level-3 grants');
    assert.match(matrix, /sharedModifyUnavailable[\s\S]*Shared domains support read\/write but cannot carry a level-3 Modify grant/,
        'reserved shared-domain rows must explain why Modify is unavailable');
    assert.match(matrix, /denySharedWrite && isReservedSharedDomain\(domain\)/,
        'the consensus shared-write deny bit must disable stale per-domain write projections');
    assert.match(network, /sharedWriteDenied = \(editCapabilities & 2\) !== 0 && isReservedSharedDomain\(domain\)/,
        'saving the companion preset must clear stale shared-domain write and modify selections');
    assert.match(network, /if \(accessSavingRef\.current\) return;/,
        'repeated clicks must not start duplicate on-chain save requests');
    assert.match(network, /accessSavingRef\.current = true;[\s\S]*finally \{[\s\S]*accessSavingRef\.current = false;/,
        'the duplicate-submit guard must cover the full asynchronous save');
    assert.match(network, /Applying access on-chain…/);
    assert.match(network, /disabled=\$\{!accessDirty \|\| accessSaving\}/);
    assert.match(network, /\$\{accessSaving \? 'Saving…' : 'Save'\}/);
    assert.match(matrix, /function DomainAccessMatrix\(\{ domains, domainAccess, onChange, disabled, busy = false, allowModify = true, denySharedWrite = false, initialFilter = '' \}\)/);
    assert.doesNotMatch(matrix, /Add new domain tag|handleAddDomain|setNewDomain|onAddDomain/,
        'operators attribute access to agent-created domains instead of creating domain tags');
    assert.match(matrix, /No domains yet\. Domains appear here as agents submit memories\./,
        'the empty state must explain that domain creation belongs to agents');
    assert.match(matrix, /placeholder="Filter domains\.\.\."/,
        'operators must retain the existing-domain finder');
    assert.match(matrix, /const allDomains = \[\.\.\.new Set\(\[\.\.\.domains, \.\.\.Object\.keys\(domainAccess\)\]\)\]\.sort\(\)/,
        'persisted grants must remain visible for attribution even while inventory refreshes');
    assert.match(matrix, />assigned<\/span>/,
        'a grant absent from the current inventory must be attributed without calling it a new domain');
    assert.match(matrix, /disabled=\$\{busy\}/,
        'access controls must be frozen while their on-chain snapshot is being saved');
    assert.match(network, /if \(accessSavingRef\.current\) \{[\s\S]*Wait for Save to finish before switching agents/,
        'operators must not switch agents while an earlier agent save is still in flight');
    assert.match(network, /class="agent-tab[^"]*" disabled=\$\{accessSaving\}/,
        'tabs must not discard or replace access state during an in-flight save');
    assert.match(network, /busy=\$\{accessSaving\}/);
    assert.match(network, /setAccessDirty\(failed\.length > 0\)/,
        'partial on-chain failures must leave Save enabled for reconciliation');
    assert.match(network, /On-chain access needs attention/,
        'a stored dashboard snapshot must not be mislabeled as merely unsaved when consensus reconciliation failed');
    assert.match(network, /f\.level === 3 \? 'Read \+ write \+ modify'/,
        'level-3 admin confirmation must describe Modify rather than removal');
    assert.doesNotMatch(network, /disabled=\$\{editRole === 'admin'\}/,
        'an admin still needs explicit level-3 grants, so its access matrix must remain editable');
    const addAgent = appSource.slice(appSource.indexOf('function AddAgentWizard('), appSource.indexOf('// PAGE_LABELS'));
    const createPayload = addAgent.slice(
        addAgent.indexOf('const res = await createAgent({'),
        addAgent.indexOf('});', addAgent.indexOf('const res = await createAgent({')) + 3
    );
    assert.doesNotMatch(addAgent, /<\$\{DomainAccessMatrix\}/,
        'agent creation must not present a second, non-atomic domain-policy editor');
    assert.doesNotMatch(createPayload, /\b(role|clearance_level|domain_access|security_profile|capability_mask)\s*:/,
        'registration must carry identity metadata only, never unenforced access intent');
    assert.match(addAgent, /restricted pending Member/);
    assert.match(addAgent, /open Access Controls to approve the agent's role, named security profile, clearance, and owned home domain in one consensus transaction/,
        'agent creation must direct the operator to the atomic post-registration policy approval');
});

test('federation keeps temporary pause separate from permanent revocation and makes peer revocation recoverable', () => {
    const page = appSource.slice(appSource.indexOf('function FederationPage('), appSource.indexOf('// PAGE_LABELS'));
    const panel = appSource.slice(appSource.indexOf('function FedPermissionsPanel('), appSource.indexOf('// FederationWarmup'));
    assert.match(apiSource, /connections\/\$\{encodeURIComponent\(chainId\)\}\/pause/);
    assert.match(appSource, /return paused \? 'Resume sharing' : 'Pause sharing'/);
    assert.match(page, /aria-label=\$\{`\$\{actionLabel\} with \$\{c\.peer_name/,
        'connection controls must identify the SAGE they affect');
    assert.match(page, /pairing preserved/);
    assert.match(panel, /Revoke trust permanently/);
    assert.match(page, /Previous connections \(\$\{visiblePastConns\.length\}\)/);
    assert.match(page, /if \(!dismissed && lastRemoteRevokeKey\.current !== key\) setShowPast\(true\)/,
        'a newly received peer revocation must expose its audit record instead of hiding the recovery path');
    assert.match(page, /Pair again with \$\{remoteNotice\.peer_name \|\| 'this SAGE'\}/,
        'the removal notice must make clear that recovery is a new pairing');
    assert.match(page, /beginGuestRejoin\(remoteNotice\)/,
        'the recovery action must preserve the former host identity');
    assert.match(page, /Pair again…/,
        'the past connection row must make clear that recovery is a new pairing');
    assert.match(page, /Hide from this list/,
        'past records must be removable from the everyday view without erasing security state');
    assert.doesNotMatch(page, /Host a new guest/,
        'a guest whose host revoked trust must not be offered the misleading inverse role as recovery');
    const guestWizard = appSource.slice(appSource.indexOf('function GuestJoinWizard('), appSource.indexOf('// HostJoinWizard'));
    assert.match(guestWizard, /function GuestJoinWizard\(\{ onExit, recoveryPeer \}\)/);
    assert.match(guestWizard, /Ask <strong>\$\{recoveryPeer\}<\/strong> to create a new connection code/,
        'the reconnect screen must explain that the former host must issue a fresh ceremony code');
    assert.match(guestWizard, /They will need to approve the reconnection before anything can be shared/,
        'the UI must explain both the safety boundary and the next human action');
    assert.match(panel, /remotePaused/);
    assert.match(panel, /paused sharing/);
    assert.match(page, /lastGoodConns/);
    assert.doesNotMatch(page, /setConns\(\[\]\)/,
        'one failed poll must not unmount live rows and unsaved permission drafts');
    assert.match(page, /setConns\(next\); setLocalChain[\s\S]*const probes = await Promise\.all/,
        'trusted relationships must paint from local state before background peer probes finish');
    assert.match(page, /route: connection\.route,[\s\S]*cached: !!connection\.route/,
        'the first paint must reuse manager route diagnostics when available');
    assert.match(panel, /fedPermissionsGet\(chain, false\)/,
        'opening a connection must load local permissions without waiting for the peer');
    assert.match(panel, /fedPipeContactsGet\(chain, false\)/,
        'opening a connection must load local agent controls without waiting for the peer');
    assert.match(page, /ended this connection/);
    assert.match(page, /sage-fed-revoke-dismissed/,
        'peer revocation must have a persistent, dismissible explanation outside collapsed history');
    assert.match(page, /ended_at/);
    assert.match(page, /const reconnect = async \(conn\)/,
        'an intact but unreachable relationship must offer a retry path');
    assert.match(page, /Scan a connection code/);
    assert.match(page, /Create a connection code/);
});

test('Sharing & Sync groups expose health and guarded operator controls', () => {
    const panel = appSource.slice(appSource.indexOf('function SharingSyncGroupsPanel('), appSource.indexOf('// FederationPage'));
    assert.match(apiSource, /export function fedGroups\(\)/);
    assert.match(panel, /function SharingSyncGroupsPanel\(\{ connections = \[\], reachability = \{\} \}\)/,
        'groups must reuse the parent connection cache instead of issuing duplicate peer probes');
    assert.doesNotMatch(panel, /fedPeerStatus\(/,
        'group rendering must not add another synchronous reachability probe per peer');
    assert.match(apiSource, /export function fedGroupsRefresh\(\)/);
    assert.match(panel, /await fedGroupsRefresh\(\);[\s\S]*await loadGroups\(\)/,
        'Refresh must finish a prompted journal convergence pass before reloading the local group projection');
    assert.match(panel, /Refreshing…/,
        'the explicit peer refresh needs visible progress instead of looking like an ordinary local reload');
    assert.match(panel, /aria-busy=\$\{busy === 'groups:refresh'\}/,
        'assistive technology must receive the same refresh progress state');
    assert.match(apiSource, /export function fedGroupCreate\(name\)/,
        'a group must be explicitly created instead of being implied by a connection');
    assert.match(apiSource, /groups\/\$\{encodeURIComponent\(groupId\)\}\/domains/);
    assert.match(apiSource, /entry_type: 'member_remove', payload: \{ member_chain: memberChain \}/);
    assert.match(panel, /Sharing & Sync/);
    assert.match(panel, /Members and catch-up/);
    assert.match(panel, /member\.peer_delivery\.backlog/);
    assert.match(panel, /last_delivered_at/,
        'last successful sync must come from a real delivered outbox transition');
    assert.match(panel, /<table class="fed-group-table">/);
    assert.match(panel, /<th scope="col">Health<\/th>/);
    assert.match(panel, /showConfirmation\('Remove '/);
    assert.match(panel, /showConfirmation\('Stop sharing/);
    assert.match(panel, /SAGE verifies that connection automatically before adding it to this group/,
        'adding an already-trusted SAGE must not imply a hidden manual acceptance step');
    assert.doesNotMatch(panel, /remote operator must still accept|waiting for invited SAGEs to accept/,
        'the synchronous trusted-connection ceremony must not be presented as a second person-facing step');
    assert.match(apiSource, /export function fedGroupRename\(groupId, name\)/);
    assert.match(apiSource, /export function fedGroupDissolve\(groupId\)/,
        'owners need an explicit shared-space lifecycle action');
    assert.match(panel, /Group name/,
        'controllers must be able to give the group a friendly replicated label');
    assert.match(panel, /const groupName = group => String\(group\.display_name \|\| ''\)\.trim\(\) \|\| 'Sharing group';/,
        'legacy nameless groups need a neutral fallback instead of an alarming unnamed label');
    assert.doesNotMatch(panel, /Unnamed sharing group/,
        'new and legacy groups must not render an unresolved-name placeholder');
    assert.match(panel, /class="fed-group-rename"/,
        'rename controls need their own structured panel rather than flowing into group facts');
    assert.match(panel, /Delete sharing group/,
        'an owner must be able to end a shared space, not merely remove people one by one');
    assert.match(panel, /This does not end any trusted connection/,
        'dissolving group RBAC must never be presented as revoking the pairwise connection');
    assert.match(panel, /Their direct trusted connection and direct sharing choices stay unchanged/,
        'member removal must explicitly preserve the separate pairwise relationship');
    assert.match(panel, /Direct trusted connections and direct sharing choices stay unchanged/,
        'removing a group topic must explicitly preserve direct relationship choices');
    assert.match(panel, /group\.lifecycle_state === 'dissolving'/,
        'partial dissolution must remain visible and retryable instead of hiding live access behind a failed request');
    assert.match(panel, /Group sharing is already stopped\. Retry to finish/,
        'a partial failure must explain the fail-closed state in plain language');
    assert.match(panel, /connection needs attention/,
        'a missing trust link must not be misrepresented as the other SAGE being offline');
    assert.doesNotMatch(panel, /<code>\$\{groupId\}<\/code>/,
        'group implementation IDs must never be shown to operators');
    assert.doesNotMatch(panel, /\$\{group\.controller_chain_id\}/,
        'controller protocol identities must never be shown to operators');
    assert.doesNotMatch(panel, /<br \/><code>\$\{member\.chain_id\}<\/code>/,
        'raw chain IDs must not displace the friendly member names in the roster');
    assert.match(panel, /Share existing domains/,
        'sharing must select existing controlled domains instead of accepting fragile free text');
    assert.match(panel, /Add a member/);
    assert.match(panel, /Create a sharing group/);
    assert.match(panel, /Add trusted SAGEs/,
        'group membership must be chosen from existing trusted SAGEs');
    assert.match(panel, /newGroupMembers\.length < 2/,
        'a 1:1 relationship must not be accidentally turned into a group');
    const createGroup = panel.slice(panel.indexOf('const createGroup = async event => {'), panel.indexOf('const connectionFor'));
    assert.match(createGroup, /const members = connections\.filter[\s\S]*if \(members\.length < 2\)[\s\S]*selected trusted connections is no longer active[\s\S]*fedGroupCreate\(name\)/,
        'a connection revoked after selection must be rejected before an undersized group is created');
    assert.match(createGroup, /for \(const connection of members\) \{[\s\S]*await fedGroupMemberInvite/,
        'multi-member setup must serialize signed invite ceremonies so each uses the latest roster revision');
    assert.match(createGroup, /for \(let attempt = 0; attempt < 3; attempt\+\+\)[\s\S]*setTimeout\(resolve, 300 \* \(attempt \+ 1\)\)/,
        'one-click group creation must absorb brief idempotent bootstrap races instead of requiring a manual retry');
    assert.doesNotMatch(createGroup, /Promise\.allSettled/,
        'parallel group invites race against the same signed roster head');
    assert.match(createGroup, /outcome = \{ status: 'rejected', reason \}/,
        'a failed invite must not prevent later selected SAGEs from being attempted');
    assert.match(createGroup, /await loadGroups\(\);[\s\S]*if \(finalError\) setError\(finalError\)/,
        'a final partial-setup error must survive the local projection reload');
    assert.match(appSource, /direct 1:1 relationship/,
        'the direct relationship and group models must be visibly separate');
    assert.match(appSource, /The setup role does not control ongoing access/,
        'the ceremony role must not hide ordinary directional controls after trust is established');
    assert.match(appSource, /each SAGE can independently share domains/,
        'symmetric control means independent explicit grants, never an implied bilateral grant');
    assert.match(panel, /Select a trusted SAGE/,
        'adding a member must choose an established trust connection, not copy a key by hand');
    assert.match(panel, /online · syncing/,
        'member reachability and catch-up state must be distinguishable');
    assert.match(panel, /const saved = await mutate[\s\S]*if \(saved\) patchDraft/,
        'failed mutations must preserve the operator draft for correction and retry');
    assert.match(appSource, /<\$\{SharingSyncGroupsPanel\} connections=\$\{liveConns\} reachability=\$\{connectionReachability\} \/>/);
});

test('federation ceremony controls expose accessible dialog, focus, and table semantics', () => {
    assert.match(appSource, /aria-label=\$\{rendered \? 'Enlarge connection QR code' : null\}/);
    assert.match(appSource, /role=\$\{rendered \? 'button' : null\} tabindex=\$\{rendered \? '0' : '-1'\}/,
        'a failed QR render must not leave a dead keyboard button');
    assert.match(appSource, /role="dialog" aria-modal="true"/);
    assert.match(appSource, /triggerRef\.current\.focus\(\)/);
    assert.match(appSource, /for="fed-safety-code"/);
    assert.match(appSource, /role="alert">That doesn't match/);
    assert.match(appSource, /querySelector\('\.fed-step h2, \.fed-step h3, \.fed-compare h3'\)/);
    assert.match(appSource, /role="table" aria-label=/);
    assert.match(appSource, /class="fed-perm-cell" role="cell"/);
    assert.match(appSource, /fed-qr-manual-code/,
        'clipboard or QR failure must expose selectable plaintext instead of a dead-end instruction');
    assert.match(appSource, /label for="fed-endpoint-picker"/);
    assert.match(appSource, /select id="fed-endpoint-picker"/);
    assert.match(appSource, /class="fed-paste"[^>]*aria-label="Paste a federation connection code"/s,
        'the manual scan fallback must have a stable accessible name');
    assert.match(appSource, /aria-controls=\$\{`fed-connection-/);
    assert.match(appSource, /label for="fed-network-name"/);
    assert.match(appSource, /input id="fed-network-name"/);
    assert.match(appSource, /class="btn fed-back" disabled=\$\{busy\}/,
        'ceremony navigation must not race an in-flight submission');
    assert.match(appSource, /class="fed-err" role="alert">Couldn't load connections: \$\{err\}/,
        'connection-load errors must remain visible and announced');
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.fed-perm-grid-copy-choice/);
});

test('mobile header preserves controls in encrypted and large-text modes', () => {
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.lock-btn\s*\{[^}]*margin-right:\s*0/s);
    assert.match(cssSource, /@media \(max-width:\s*560px\)[\s\S]*\.text-size-toggle\s*\{[^}]*flex:\s*none/s);
    assert.match(cssSource, /@media \(max-width:\s*480px\)[\s\S]*\.sage-version\s*\{\s*display:\s*none/s,
        'the release badge must yield space before encrypted and accessibility controls clip');
    assert.match(appSource, /role="group" aria-label="Text size"/);
    assert.match(appSource, /class="lock-btn" title="Lock CEREBRUM" aria-label="Lock CEREBRUM"/,
        'the icon-only vault control must keep a stable accessible name after rerenders');
    for (const size of ['Small', 'Medium', 'Large']) {
        assert.match(appSource, new RegExp(`aria-label="${size} text" aria-pressed=`));
    }
});

test('task cards expand fully and planned task edits preserve consensus history', () => {
    const tasksPage = appSource.slice(appSource.indexOf('function TasksPage('), appSource.indexOf('function PipelineView('));
    assert.match(tasksPage, /expandedTasks\.has\(task\.memory_id\) \? 'Collapse' : 'Expand'/);
    assert.match(tasksPage, /task\.task_status === 'planned'/);
    assert.match(tasksPage, /const created = await createTask\(content, task\.domain_tag \|\| 'general'\)/);
    assert.match(tasksPage, /await updateTaskStatus\(task\.memory_id, 'dropped'\)/);
    assert.ok(tasksPage.indexOf('const created = await createTask') < tasksPage.indexOf("await updateTaskStatus(task.memory_id, 'dropped')"),
        'the replacement must commit before the original planned task is retired');
    assert.match(cssSource, /\.kanban-card\.expanded \.kanban-card-content\s*\{[^}]*display:\s*block/s);
    assert.match(cssSource, /white-space:\s*pre-wrap/);
});

test('clearing a task column reconciles delayed consensus instead of restoring stale cards', () => {
    const tasksPage = appSource.slice(appSource.indexOf('function TasksPage('), appSource.indexOf('function PipelineView('));
    const clearColumn = tasksPage.slice(tasksPage.indexOf('async function clearColumn('), tasksPage.indexOf('async function handleAddTask('));
    assert.match(tasksPage, /const settlingClears = useRef\(new Set\(\)\)/);
    assert.match(clearColumn, /await Promise\.allSettled\(ids\.map\(id => deleteMemory\(id\)\)\)/,
        'one delayed task must not fail a whole column clear');
    assert.match(tasksPage, /async function reconcileClearedTasks\(ids\)/);
    assert.match(clearColumn, /SAGE is still confirming/,
        'an indeterminate consensus response must be treated as a pending reconciliation');
    assert.match(clearColumn, /challenge_opened/,
        'a multi-holder challenge must be explained instead of being called cleared');
    assert.doesNotMatch(clearColumn, /setTasks\(previousTasks\)/,
        'a pre-commit React snapshot must never resurrect cards after an uncertain commit');
});

test('settings does not force a full-page render every 100ms', () => {
    assert.doesNotMatch(appSource, /setInterval\(\(\) => setTick\([^\n]+, 100\)/);
    assert.match(appSource, /function ChainCountdown\(\{ blockTime \}\)/);
    assert.match(appSource, /document\.hidden/);
});

test('settings pauses background polling and avoids duplicate full-store scans', () => {
    const settings = appSource.match(/function SettingsPage\([\s\S]+?\n}\n\n\/\/ ={20,}/)?.[0] || '';
    assert.match(settings, /interval = setInterval\(poll, 30000\)/);
    assert.match(settings, /document\.addEventListener\('visibilitychange', sync\)/);
    assert.doesNotMatch(settings, /setInterval\(poll, 3000\)/);
    assert.doesNotMatch(settings, /fetchStats\(\)/);
    assert.match(settings, /if \(settingsTab !== 'overview' \|\| document\.hidden\)/);
});

test('unconfirmed RBAC remains retryable and clearly says it is not active', () => {
    assert.match(appSource, /Access is not active yet\./);
    assert.match(appSource, /setAccessDirty\(true\);\s*setAccessSaved\(false\);\s*return;/);
    assert.doesNotMatch(appSource, /Access was saved locally, but the network has not confirmed it yet/);
});

test('embedding cutover happens before migration and controls expose accessible state', () => {
    const runAll = appSource.slice(appSource.indexOf('const runAll = async () => {'), appSource.indexOf('const pct = prog'));
    assert.ok(runAll.indexOf('await enableToReady()') < runAll.indexOf('await reembedToDone()'),
        'Ollama must become authoritative before the background migration starts');
    assert.match(appSource, /role="group" aria-label="Embedding provider"/);
    assert.match(appSource, /aria-pressed=\$\{embedderStatus\.provider === 'ollama'\}/);
    assert.match(appSource, /role="status" aria-live="polite"/);
    assert.match(appSource, /Reranker \$\{rerankerOn \? 'on' : 'off'\}/);
});

test('contextual help flips below clipping-container top edges', () => {
    const helpTip = appSource.slice(appSource.indexOf('function HelpTip('), appSource.indexOf('// SmartTooltipLayer'));
    assert.match(helpTip, /popupRef/);
    assert.match(helpTip, /getComputedStyle\(node\)/);
    assert.match(helpTip, /auto\|scroll\|hidden\|clip/);
    assert.match(helpTip, /popup\.getBoundingClientRect\(\)\.top < visibleTop \+ 8/);
    assert.match(helpTip, /setBelow\(true\)/);
});

test('macOS tray focuses an existing CEREBRUM tab before opening a new one', () => {
    const launch = traySource.slice(
        traySource.indexOf('func applicationDidFinishLaunching'),
        traySource.indexOf('// Clicking dock icon'),
    );
    const reopen = traySource.match(/func applicationShouldHandleReopen[\s\S]+?\n    \}/)?.[0] || '';
    const open = traySource.match(/private func openDashboardOnce\(\)[\s\S]+?\n    \}/)?.[0] || '';
    assert.match(launch, /self\.openDashboard\(\)/,
        'an app restart must reuse a browser tab left alive by the previous app process');
    assert.doesNotMatch(launch, /NSWorkspace\.shared\.open/);
    assert.match(reopen, /openDashboard\(\)/);
    assert.match(open, /focusExistingDashboardTab\(\)/);
    assert.match(open, /hasActiveDashboard\(\), activateDefaultBrowser\(\)/,
        'Firefox needs SSE presence plus default-browser activation because it has no tab AppleScript API');
    assert.ok(open.indexOf('focusExistingDashboardTab()') < open.indexOf('NSWorkspace.shared.open'));
    assert.ok(open.indexOf('hasActiveDashboard()') < open.indexOf('NSWorkspace.shared.open'));
    assert.match(traySource, /finished\.wait\(timeout: \.now\(\) \+ 5\)/,
        'browser automation must be time-bounded so dock clicks cannot freeze the app');
});

test('MRI uses one dense shared memory sample limit', () => {
    assert.match(mriSource, /export const DEFAULT_MRI_NODE_LIMIT = 1500/);
    assert.match(mriSource, /limit=\$\{DEFAULT_MRI_NODE_LIMIT\}/);
    assert.match(mriPageSource, /String\(DEFAULT_MRI_NODE_LIMIT\)/);
    assert.match(mriSource,
        /\.nn'\)\.textContent = fmtN\(d\.total && d\.total > d\.nodes\.length \? d\.total : d\.nodes\.length\)/,
        'the HUD memory count must prefer the server total over the rendered sample size');
    assert.match(mriSource, /showing \$\{d\.nodes\.length\} of \$\{fmtN\(d\.total\)\} · representative sample/,
        'a sampled graph must label the rendered node count separately from total memories');
    const mriView = appSource.slice(appSource.indexOf('function MriView('), appSource.indexOf('// Global tooltips state'));
    assert.doesNotMatch(mriView, /limit=500/);
});

test('MRI coalesces live memory bursts and never overlaps graph reloads', () => {
    assert.match(mriSource, /let graphLoadInFlight = false/);
    assert.match(mriSource, /if \(graphLoadInFlight\) \{[\s\S]*graphReloadPending = true/);
    assert.match(mriSource, /setTimeout\(load, 3000\)/);
    assert.doesNotMatch(mriSource, /setTimeout\(load, 450\)/);
});

test('MRI distinguishes domains stored here from domains shared by other SAGE nodes', () => {
    const mriView = appSource.slice(appSource.indexOf('function BrainDomainInventory('), appSource.indexOf('// Global tooltips state'));
    assert.match(appSource, /import \{ buildBrainDomainInventory \} from '\.\/domain-inventory\.js'/);
    assert.match(mriView, /Local domains/);
    assert.match(mriView, /Stored on this SAGE, including copies retained here\./);
    assert.match(mriView, /includes copies from \$\{copyLabel\}/);
    assert.match(mriView, /Shared domains/);
    assert.match(mriView, /Available from other SAGE nodes\./);
    assert.match(mriView, /from \$\{sourceLabel\}/);
    assert.match(mriView, /Copy offered/);
    assert.match(mriView, /Saved here/);
    assert.match(mriView, /Last known/);
    assert.match(mriView, /Manage sharing →/);
    assert.match(mriView, /aria-label="Local and shared domains"/);
    assert.match(mriView, /Filter local domains…/);
    assert.match(mriView, /How to read/);
    assert.match(mriView, /Show whole brain/);
    assert.match(mriView, /onSelectDomain\(active \? '' : domain\.domain\)/,
        'the consolidated source list must retain the old domain drill-down');
    assert.match(mriView, /showDomainLegend: false/,
        'CEREBRUM must not render a redundant right-hand domain legend');
    assert.match(mriSource, /const showDomainLegend = opts\.showDomainLegend !== false/);
    assert.match(mriSource, /sage:mri-domain-select/);
    assert.match(cssSource, /\.brain-domain-inventory/);
    assert.match(cssSource, /@media \(max-width:\s*760px\)[\s\S]*\.brain-domain-inventory/);
});

test('MRI domain-source panel can be moved, resized, persisted, and reset', () => {
    const inventoryView = appSource.slice(
        appSource.indexOf('function BrainDomainInventory('),
        appSource.indexOf('// MriView'),
    );
    assert.match(inventoryView, /sage-brain-domain-panel-layout/);
    assert.match(inventoryView, /ResizeObserver/);
    assert.match(inventoryView, /data-domain-drag-handle/);
    assert.match(inventoryView, /pointermove/);
    assert.match(inventoryView, /sage:domain-panel-reset/);
    assert.match(cssSource, /\.brain-domain-inventory[\s\S]*resize:\s*both/);
    assert.match(cssSource, /\.brain-domain-inventory\.dragging/);
});

test('MRI spreads long-lived memory histories through the brain volume', () => {
    assert.equal(MRI_LAYOUT.ageWindowDays, 365,
        'older memories should not all collapse onto a 90-day inner shell');
    const ageBands = [0, 90 / 365, 180 / 365, 270 / 365, 1]
        .map(age => mriDepthForAge(age, 0.5));
    for (let i = 1; i < ageBands.length; i++) {
        assert.ok(ageBands[i - 1] > ageBands[i], 'depth must move strictly inward with age');
    }
    assert.ok(mriDepthForAge(0, 1) <= 0.89, 'fresh memories must stay inside the cortex');
    assert.ok(mriDepthForAge(1, 0) >= 0.20, 'old memories retain enough separation around the core');
    assert.ok(Math.abs(mriBrainstemBias(0)) === 0);
    assert.ok(mriBrainstemBias(1) < mriBrainstemBias(0),
        'older memories should settle toward the lower inner brainstem');
    assert.match(mriSource, /mriDepthForAge\(age,hsh\(n\.id,3\)\)/);
    assert.equal(mriVerticalPosition(0.89, 1, 0), MRI_LAYOUT.halfExtentY * 0.89,
        'the upper cortex should retain the full vertical spread');
    const lowestNodeCenter = MRI_LAYOUT.lowerCraniumY + MRI_LAYOUT.nodeClearance;
    assert.ok(lowestNodeCenter >= -93,
        'the bundled anatomical mesh needs node centres at or above -93 for lower-cranium clearance');
    assert.ok(MRI_LAYOUT.nodeClearance >= 10,
        'the cranium envelope must reserve space for node spheres, not only their centres');
    for (let age = 0; age <= 1; age += 0.01) {
        for (const jitter of [0, 0.5, 1]) {
            const depth = mriDepthForAge(age, jitter);
            assert.ok(mriVerticalPosition(depth, -1, age) >= lowestNodeCenter,
                'lower memories must retain sphere clearance inside the cranium');
        }
    }
    assert.ok(MRI_LAYOUT.lowerHalfExtentY < MRI_LAYOUT.halfExtentY,
        'the lower anatomical envelope must be shallower than the upper cortex');
    assert.match(mriSource, /mriVerticalPosition\(depth,Math\.sin\(el\),age\)/);
});

test('guide describes token efficiency without promising lower usage', () => {
    const useCases = appSource.slice(
        appSource.indexOf("key: 'use-cases'"),
        appSource.indexOf("key: 'network'"),
    );
    assert.match(useCases, /Spend context where it matters/);
    assert.match(useCases, /does not promise that every session uses fewer tokens/);
    assert.match(useCases, /relevant pieces when they are needed/);
    assert.match(useCases, /instead of repeated explanations/);
});

test('CEREBRUM recommends canonical Messages instead of deprecated pipeline tools', () => {
    assert.doesNotMatch(appSource, /sage_pipe/);
    assert.match(appSource, /sends a message through SAGE/);
});
