import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');
const apiSource = await readFile(new URL('../web/static/js/api.js', import.meta.url), 'utf8');
const cssSource = await readFile(new URL('../web/static/css/sage.css', import.meta.url), 'utf8');
const mriSource = await readFile(new URL('../web/static/js/mri-brain.js', import.meta.url), 'utf8');
const mriPageSource = await readFile(new URL('../web/static/mri.html', import.meta.url), 'utf8');
const traySource = await readFile(new URL('../cmd/sage-tray/main.swift', import.meta.url), 'utf8');
const { MRI_LAYOUT, mriBrainstemBias, mriDepthForAge, mriVerticalPosition } = await import('../web/static/js/mri-layout.js');

test('Access Controls is a first-class sidebar route', () => {
    assert.match(appSource, /hash === '\/access'\) setPage\('access'\)/);
    assert.match(appSource, /navigate\('access'\)/);
    assert.match(appSource, /page === 'access'.*NetworkPage/s);
    assert.match(appSource, /accessMode \? 'access' : 'overview'/);
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
    assert.doesNotMatch(networkPage, /btoa\(|payload.*scope_action/s,
        'the dashboard must submit structured scope JSON, not recreate the binary codec');
});

test('chain health recognizes app-v20 as the current consensus protocol', () => {
    assert.match(appSource, /const appVerTone = appVer === '20'/);
    assert.match(appSource, /Green when current \(20\)\./);
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
    assert.match(hostWizard, /mode === 'lan' && !isFederationEndpointFormatValid\(endpoint\)/,
        'host creation must reject a partially typed LAN address before calling the server');
    assert.match(hostWizard, /!isFederationEndpointFormatValid\(endpoint\) \|\| isLoopbackEndpoint\(endpoint\)/,
        'host auto-create must wait for a complete non-loopback endpoint');
    assert.match(hostWizard, /disabled=\$\{busy \|\| !endpointReady \|\| endpointMissing \|\| endpointInvalid\}/,
        'host manual create must stay disabled for a partial endpoint');
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
    assert.match(guestWizard, /Connecting remotely or need to change the network address\?/);
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
	assert.match(panel, /disabled=\$\{\(!dirty && !alignmentPending\) \|\| busy\}[\s\S]*Retry copy alignment/,
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

test('federation keeps temporary pause separate from permanent revocation and hides past clutter', () => {
    const page = appSource.slice(appSource.indexOf('function FederationPage('), appSource.indexOf('// PAGE_LABELS'));
    const panel = appSource.slice(appSource.indexOf('function FedPermissionsPanel('), appSource.indexOf('// FederationWarmup'));
    assert.match(apiSource, /connections\/\$\{encodeURIComponent\(chainId\)\}\/pause/);
    assert.match(page, /Resume sharing/);
    assert.match(page, /aria-label=\$\{`\$\{c\.sharing_paused \? 'Resume' : 'Pause'\} sharing with/,
        'pause controls must identify the connection they affect');
    assert.match(page, /pairing preserved/);
    assert.match(panel, /Revoke trust permanently/);
    assert.match(page, /Past connections \(\$\{pastConns\.length\}\)/);
    assert.match(page, /showPast && html/,
        'immutable past rows must remain collapsed until the operator asks for history');
    assert.match(panel, /remotePaused/);
    assert.match(panel, /paused sharing/);
    assert.match(page, /lastGoodConns/);
    assert.doesNotMatch(page, /setConns\(\[\]\)/,
        'one failed poll must not unmount live rows and unsaved permission drafts');
    assert.match(page, /ended this connection/);
    assert.match(page, /sage-fed-revoke-dismissed/,
        'peer revocation must have a persistent, dismissible explanation outside collapsed history');
    assert.match(page, /ended_at/);
});

test('Sharing & Sync groups expose health and guarded operator controls', () => {
    const panel = appSource.slice(appSource.indexOf('function SharingSyncGroupsPanel('), appSource.indexOf('// FederationPage'));
    assert.match(apiSource, /export function fedGroups\(\)/);
    assert.match(apiSource, /groups\/\$\{encodeURIComponent\(groupId\)\}\/domains/);
    assert.match(apiSource, /entry_type: 'member_remove', payload: \{ member_chain: memberChain \}/);
    assert.match(panel, /Sharing & Sync groups/);
    assert.match(panel, /Members and catch-up/);
    assert.match(panel, /member\.peer_delivery\.backlog/);
    assert.match(panel, /last_delivered_at/,
        'last successful sync must come from a real delivered outbox transition');
    assert.match(panel, /<table class="fed-group-table">/);
    assert.match(panel, /<th scope="col">Health<\/th>/);
    assert.match(panel, /showConfirmation\('Remove '/);
    assert.match(panel, /showConfirmation\('Stop sharing/);
    assert.match(panel, /remote operator must cryptographically co-sign/);
    assert.match(apiSource, /export function fedGroupRename\(groupId, name\)/);
    assert.match(panel, /Group name/,
        'controllers must be able to give the group a friendly replicated label');
    assert.match(panel, /Share existing domains/,
        'sharing must select existing controlled domains instead of accepting fragile free text');
    assert.match(panel, /Add a member/);
    assert.match(panel, /Select a trusted SAGE/,
        'adding a member must choose an established trust connection, not copy a key by hand');
    assert.match(panel, /online · syncing/,
        'member reachability and catch-up state must be distinguishable');
    assert.match(panel, /const saved = await mutate[\s\S]*if \(saved\) patchDraft/,
        'failed mutations must preserve the operator draft for correction and retry');
    assert.match(appSource, /<\$\{SharingSyncGroupsPanel\} \/>/);
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
    assert.match(mriSource, /export const DEFAULT_MRI_NODE_LIMIT = 2500/);
    assert.match(mriSource, /limit=\$\{DEFAULT_MRI_NODE_LIMIT\}/);
    assert.match(mriPageSource, /String\(DEFAULT_MRI_NODE_LIMIT\)/);
    const mriView = appSource.slice(appSource.indexOf('function MriView('), appSource.indexOf('// Global tooltips state'));
    assert.doesNotMatch(mriView, /limit=500/);
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
