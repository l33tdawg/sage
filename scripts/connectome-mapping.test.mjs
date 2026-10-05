import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { createGraphLoadCoordinator, mapConnectome, agentConnections, diffConnectomeActivity, createConnectomeActivityTracker, createConnectomeReloadIntent } from '../web/static/js/connectome-map.js';

const mriSource = await readFile(new URL('../web/static/js/mri-brain.js', import.meta.url), 'utf8');
const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');
const cssSource = await readFile(new URL('../web/static/css/sage.css', import.meta.url), 'utf8');
const mriPageSource = await readFile(new URL('../web/static/mri.html', import.meta.url), 'utf8');

function bracedBlock(source, marker, from = 0) {
  const start = source.indexOf(marker, from);
  assert.notEqual(start, -1, `${marker} not found`);
  const open = source.indexOf('{', start + marker.length);
  assert.notEqual(open, -1, `${marker} block did not open`);
  let depth = 0;
  for (let i = open; i < source.length; i++) {
    if (source[i] === '{') depth++;
    else if (source[i] === '}' && --depth === 0) {
      return { body: source.slice(open + 1, i), start, end: i + 1 };
    }
  }
  assert.fail(`${marker} block did not close`);
}

function cssDeclarations(source, exactSelector) {
  const executable = source.replace(/\/\*[\s\S]*?\*\//g, '');
  let cursor = 0;
  let found = null;
  while (cursor < executable.length) {
    const open = executable.indexOf('{', cursor);
    if (open === -1) break;
    const close = executable.indexOf('}', open + 1);
    if (close === -1) break;
    const boundary = Math.max(executable.lastIndexOf('}', open - 1), executable.lastIndexOf('{', open - 1));
    const selector = executable.slice(boundary + 1, open).trim();
    if (selector === exactSelector) found = executable.slice(open + 1, close);
    cursor = close + 1;
  }
  assert.notEqual(found, null, `${exactSelector} rule not found`);
  return Object.fromEntries(found.split(';').map(part => part.trim()).filter(Boolean).map(part => {
    const colon = part.indexOf(':');
    assert.ok(colon > 0, `invalid declaration in ${exactSelector}: ${part}`);
    return [part.slice(0, colon).trim(), part.slice(colon + 1).trim()];
  }));
}

// The CEREBRUM connectome view renders the agent message-bus in the brain hull.
// mapConnectome() is the pure projection from the /network/synapses payload onto
// the MRI render contract; these tests pin the invariants the renderer relies on:
// neuron degree + synapse weight normalization, ghost-edge rejection, and a safe
// empty state.

const payload = {
  neurons: [
    { agent_id: 'alice', name: 'Alice', role: 'planner', domain: 'ops' },
    { agent_id: 'bob',   name: 'Bob',   role: 'worker',  domain: 'ops' },
    { agent_id: 'carol', name: 'Carol', role: 'worker',  domain: 'research' },
  ],
  synapses: [
    { from_agent: 'alice', to_agent: 'bob',   count: 5, last_fired: '2026-08-12T10:00:00Z' },
    { from_agent: 'bob',   to_agent: 'alice', count: 2, last_fired: '2026-08-12T09:00:00Z' },
    { from_agent: 'bob',   to_agent: 'carol', count: 1, last_fired: '2026-08-12T08:00:00Z' },
  ],
};

test('neurons become nodes with normalized degree (busiest = 1)', () => {
  const g = mapConnectome(payload);
  assert.equal(g.nodes.length, 3);
  const by = Object.fromEntries(g.nodes.map(n => [n.agent_id, n]));
  // total traffic (in+out): bob 5+2+1=8 (busiest), alice 5+2=7, carol 1
  assert.equal(by.bob._w, 8);
  assert.equal(by.alice._w, 7);
  assert.equal(by.carol._w, 1);
  assert.equal(by.bob._deg, 1);
  assert.equal(by.alice._deg, 7 / 8);
  assert.equal(by.carol._deg, 1 / 8);
  assert.ok(g.nodes.every(n => n.isNeuron === true));
});

test('node fields map from the payload with sensible fallbacks', () => {
  const g = mapConnectome({
    neurons: [
      { agent_id: 'x', name: 'X', role: 'r', domain: 'd' },
      { agent_id: 'y' }, // no name/role/domain
    ],
    synapses: [],
  });
  const by = Object.fromEntries(g.nodes.map(n => [n.agent_id, n]));
  assert.equal(by.x.label, 'X');
  assert.equal(by.x.domain, 'd');
  assert.equal(by.x.agent_domain, 'd');
  assert.equal(by.x.role, 'r');
  // label falls back to agent_id, domain falls back to role then 'agent'
  assert.equal(by.y.label, 'y');
  assert.equal(by.y.domain, 'agent');
  assert.equal(by.y.agent_domain, '', 'display details must not mislabel the role fallback as a real domain');
  assert.equal(by.y.role, '');
});

test('synapses become weighted links normalized by the busiest edge', () => {
  const g = mapConnectome(payload);
  assert.equal(g.links.length, 3);
  const by = Object.fromEntries(g.links.map(l => [`${l.source}>${l.target}`, l]));
  assert.equal(by['agent:alice>agent:bob'].count, 5);
  assert.equal(by['agent:alice>agent:bob']._w, 1);        // busiest edge
  assert.equal(by['agent:bob>agent:alice']._w, 2 / 5);
  assert.equal(by['agent:bob>agent:carol']._w, 1 / 5);
  assert.ok(g.links.every(l => l.link_type === 'synapse'));
  assert.equal(by['agent:alice>agent:bob'].last_fired, '2026-08-12T10:00:00Z');
});

test('direction is preserved: A->B is distinct from B->A', () => {
  const g = mapConnectome(payload);
  const keys = g.links.map(l => `${l.source}>${l.target}`);
  assert.ok(keys.includes('agent:alice>agent:bob'));
  assert.ok(keys.includes('agent:bob>agent:alice'));
});

test('selected-agent connections preserve sent/received direction and visible peers', () => {
  const graph = mapConnectome({
    neurons: [
      { agent_id: 'alice', name: 'Alice' },
      { agent_id: 'bob', name: 'Bob' },
      { agent_id: 'carol', name: 'Carol' },
    ],
    synapses: [
      { from_agent: 'alice', to_agent: 'bob', count: 7, last_fired: '2026-08-16T10:00:00Z' },
      { from_agent: 'bob', to_agent: 'alice', count: 2, last_fired: '2026-08-16T11:00:00Z' },
      { from_agent: 'carol', to_agent: 'alice', count: 4, last_fired: '2026-08-16T09:00:00Z' },
      { from_agent: 'hidden', to_agent: 'alice', count: 99, last_fired: '2026-08-16T12:00:00Z' },
    ],
  });
  assert.deepEqual(agentConnections(graph, 'alice'), [
    { peer_id:'bob', peer_node_id:'agent:bob', peer_name:'Bob', peer_domain:'', sent:7, received:2, total:9, last_fired:'2026-08-16T11:00:00Z' },
    { peer_id:'carol', peer_node_id:'agent:carol', peer_name:'Carol', peer_domain:'', sent:0, received:4, total:4, last_fired:'2026-08-16T09:00:00Z' },
  ]);
  assert.deepEqual(agentConnections(graph, 'missing'), []);
});

test('connection inspection keeps object endpoints asymmetric and coalesces only the same peer', () => {
  const graph = mapConnectome({
    neurons: [
      { agent_id: 'alice', name: 'Alice' },
      { agent_id: 'bob', name: 'Bob' },
      { agent_id: 'carol', name: 'Carol' },
    ],
    synapses: [
      { from_agent: 'alice', to_agent: 'bob', count: 3, last_fired: '2026-08-16T10:00:00Z' },
      { from_agent: 'bob', to_agent: 'alice', count: 11, last_fired: '2026-08-16T12:00:00Z' },
      { from_agent: 'alice', to_agent: 'carol', count: 5, last_fired: '2026-08-16T11:00:00Z' },
    ],
  });
  const byID = Object.fromEntries(graph.nodes.map(node => [node.id, node]));
  // ForceGraph mutates link endpoints from ids to node objects after binding.
  graph.links.forEach(link => {
    link.source = byID[link.source];
    link.target = byID[link.target];
  });

  assert.deepEqual(agentConnections(graph, 'alice'), [
    { peer_id:'bob', peer_node_id:'agent:bob', peer_name:'Bob', peer_domain:'', sent:3, received:11, total:14, last_fired:'2026-08-16T12:00:00Z' },
    { peer_id:'carol', peer_node_id:'agent:carol', peer_name:'Carol', peer_domain:'', sent:5, received:0, total:5, last_fired:'2026-08-16T11:00:00Z' },
  ], 'reversing either sent/received branch or flattening both directions must fail');
  assert.deepEqual(agentConnections(graph, 'bob'), [
    { peer_id:'alice', peer_node_id:'agent:alice', peer_name:'Alice', peer_domain:'', sent:11, received:3, total:14, last_fired:'2026-08-16T12:00:00Z' },
  ], 'the same directed rows must invert when the inspected endpoint changes');
});

test('nodes expose visible directional traffic, distinct peers, and strongest connection', () => {
  const g = mapConnectome(payload);
  const by = Object.fromEntries(g.nodes.map(n => [n.agent_id, n]));
  assert.deepEqual(
    { incoming:by.alice._incoming, outgoing:by.alice._outgoing, peers:by.alice._peers },
    { incoming:2, outgoing:5, peers:1 },
  );
  assert.deepEqual(
    { incoming:by.bob._incoming, outgoing:by.bob._outgoing, peers:by.bob._peers },
    { incoming:5, outgoing:3, peers:2 },
  );
  assert.equal(by.bob._strongest_peer, 'alice');
  assert.equal(by.bob._strongest_peer_traffic, 7,
    'strongest connection combines retained traffic in both directions');
  assert.deepEqual(
    { incoming:by.carol._incoming, outgoing:by.carol._outgoing, peers:by.carol._peers },
    { incoming:1, outgoing:0, peers:1 },
  );
});

test('a self-synapse contributes directional traffic but not a connected peer', () => {
  const [node] = mapConnectome({
    neurons:[{agent_id:'solo'}],
    synapses:[{from_agent:'solo',to_agent:'solo',count:3}],
  }).nodes;
  assert.equal(node._incoming,3); assert.equal(node._outgoing,3);
  assert.equal(node._peers,0); assert.equal(node._strongest_peer,'');
});

test('edges to unknown agents are dropped (no ghost nodes)', () => {
  const g = mapConnectome({
    neurons: [{ agent_id: 'alice', name: 'Alice' }],
    synapses: [
      { from_agent: 'alice', to_agent: 'ghost', count: 9 }, // ghost not registered
      { from_agent: 'ghost', to_agent: 'alice', count: 9 },
    ],
  });
  assert.equal(g.nodes.length, 1);
  assert.equal(g.links.length, 0, 'both endpoints must be registered neurons');
  // a dropped edge must not leak into the neuron's traffic weight
  assert.equal(g.nodes[0]._w, 0);
});

test('empty / null / malformed payloads yield a safe empty connectome', () => {
  for (const p of [null, undefined, {}, { neurons: null, synapses: 'nope' }]) {
    const g = mapConnectome(p);
    assert.deepEqual(g.nodes, []);
    assert.deepEqual(g.links, []);
    assert.equal(g.total, 0);
    assert.equal(g.connectome, true);
  }
});

test('a connectome with neurons but zero traffic degrades gracefully', () => {
  const g = mapConnectome({
    neurons: [{ agent_id: 'a', name: 'A' }, { agent_id: 'b', name: 'B' }],
    synapses: [],
  });
  assert.equal(g.nodes.length, 2);
  assert.ok(g.nodes.every(n => n._deg === 0 && n._w === 0));
  assert.equal(g.links.length, 0);
});

test('mode switches invalidate initial and reload responses from the old view', () => {
  const loads = createGraphLoadCoordinator();
  const initialMemory = loads.begin('memory');
  loads.invalidate();
  const connectome = loads.begin('connectome');

  assert.equal(loads.isCurrent(initialMemory, 'connectome'), false,
    'a slow initial memory response must not render after switching to connectome');
  assert.equal(loads.isCurrent(connectome, 'connectome'), true);

  const slowReload = loads.begin('connectome');
  loads.invalidate();
  const memory = loads.begin('memory');
  assert.equal(loads.isCurrent(slowReload, 'memory'), false,
    'a slow connectome reload must not render after switching back to memory');
  assert.equal(loads.isCurrent(memory, 'memory'), true);
});

test('renderer wires mode invalidation into both initial acquisition and reloads', () => {
  const initialStart = mriSource.indexOf('function acquireInitialGraph()');
  const initialEnd = mriSource.indexOf('\n  acquireInitialGraph();', initialStart);
  assert.ok(initialStart >= 0 && initialEnd > initialStart, 'initial graph acquisition must remain explicit');
  const initialAcquire = mriSource.slice(initialStart, initialEnd);
  assert.match(initialAcquire, /const request = graphLoads\.begin\(mode\);/,
    'initial acquisition must capture its source mode and generation');
  assert.match(initialAcquire, /fetchActive\(request\.mode\)/,
    'initial acquisition must fetch from the captured mode');
  assert.match(initialAcquire, /graphLoads\.isCurrent\(request, mode\)/,
    'a stale initial response must fail closed after a mode change');
  assert.match(mriSource, /function setMode\(next\)\{[\s\S]*graphLoads\.invalidate\(\);[\s\S]*else acquireInitialGraph\(\);/,
    'a toggle must invalidate old work and refetch even before Graph exists');
});

test('connectome guidance is reachable without adding another floating panel', () => {
  const templateStart = mriSource.indexOf('root.innerHTML = `');
  const templateEnd = mriSource.indexOf('`;\n  container.appendChild(root);', templateStart);
  assert.ok(templateStart >= 0 && templateEnd > templateStart, 'renderer root template must remain inspectable');
  const rootTemplate = mriSource.slice(templateStart, templateEnd);

  const panelClasses = [...rootTemplate.matchAll(/class="panel ([^"]+)"/g)].map(match => match[1]).sort();
  assert.deepEqual(panelClasses, ['hud', 'legend', 'scan'],
    'the renderer must retain only its established scan, legend, and HUD panels');
  assert.doesNotMatch(rootTemplate, /\bmodeCap\b|mode-cap/,
    'connectome mode must not create a free-floating explanatory panel');
  const executableMri = mriSource
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/.*$/gm, '');
  const childMutations = [...executableMri.matchAll(
    /\b(root|container)\.(appendChild|append|prepend|insertBefore|replaceChildren|insertAdjacentElement|insertAdjacentHTML|before|after|replaceWith)\s*\(([^;\n]*)\)/g,
  )].map(match => `${match[1]}.${match[2]}(${match[3].trim()})`);
  assert.deepEqual(childMutations, ['container.appendChild(root)', 'root.appendChild(p)'],
    'the mount root and established Explore panel are the only root-level insertions, through any DOM insertion API');
  assert.match(mriSource, /p\.className = 'panel explore'; root\.appendChild\(p\)/,
    'the dynamic-root allow-list must stay bound to the click-to-explore panel');

  assert.match(rootTemplate, /class="lg-detail guide-connectome" hidden>[\s\S]*Agents are neurons[\s\S]*Click one to bloom its memories/i,
    'standalone MRI must keep connectome guidance inside its existing reading legend');
  const modeChromeStart = mriSource.indexOf('function updateModeChrome(');
  const modeChromeEnd = mriSource.indexOf('function setMode(', modeChromeStart);
  assert.doesNotMatch(mriSource.slice(modeChromeStart,modeChromeEnd), /legend\.hidden\s*=|\.legend[^\n]*hidden/,
    'connectome mode must not hide the standalone reading guide to make room for agent details');
  const standaloneMountStart = mriPageSource.indexOf("mountMriBrain(document.getElementById('mount')");
  const standaloneMountEnd = mriPageSource.indexOf('});', standaloneMountStart);
  assert.ok(standaloneMountStart >= 0 && standaloneMountEnd > standaloneMountStart,
    'standalone MRI mount options must remain inspectable');
  const standaloneMount = mriPageSource.slice(standaloneMountStart, standaloneMountEnd);
  assert.match(standaloneMount, /showScan:\s*true/);
  assert.doesNotMatch(standaloneMount, /showDomainLegend:\s*false|allowConnectome:\s*false/,
    'standalone MRI must keep both the connectome toggle and its reading guide enabled');
  assert.match(rootTemplate, /<button type="button" class="lg-toggle" aria-expanded="false">/,
    'the standalone reading guide must be keyboard reachable');
  assert.match(rootTemplate, /<button type="button" class="btn b-mode" aria-label="Connectome view" aria-pressed="false">/,
    'the mode toggle must be a keyboard-reachable button');
  assert.match(mriSource, /class="sr-status" role="status" aria-live="polite"/,
    'mode changes must have a non-visual screen-reader announcement target');

  const guideStart = appSource.indexOf('showGuide && html`<section class="brain-domain-guide"');
  const guideEnd = appSource.indexOf('</section>`}', guideStart);
  assert.ok(guideStart >= 0 && guideEnd > guideStart, 'dashboard guide template must remain inspectable');
  const dashboardGuide = appSource.slice(guideStart, guideEnd);
  assert.match(dashboardGuide, /<b>Connectome mode:<\/b> agents are neurons/,
    'the dashboard How to read guide must retain the connectome explanation');
  assert.match(appSource, /class="brain-domain-reset"/,
    'the mobile-only action hiding rule needs an explicit Reset target');
  const executableCss = cssSource.replace(/\/\*[\s\S]*?\*\//g, '');
  const mobileBlocks = [];
  let mobileCursor = 0;
  while ((mobileCursor = executableCss.indexOf('@media (max-width: 760px)', mobileCursor)) !== -1) {
    const block = bracedBlock(executableCss, '@media (max-width: 760px)', mobileCursor);
    mobileBlocks.push(block.body);
    mobileCursor = block.end;
  }
  const inventoryMobile = mobileBlocks.find(block => block.includes('.brain-domain-head-actions .brain-domain-reset'));
  assert.ok(inventoryMobile, 'the executable 760px mobile block must target Reset explicitly');
  assert.equal(cssDeclarations(inventoryMobile, '.brain-domain-head-actions .brain-domain-reset').display, 'none',
    'mobile may hide Reset, not the How to read button');
  assert.doesNotMatch(executableCss, /\.brain-domain-head-actions button:first-child\s*\{\s*display:\s*none;/,
    'mobile must not hide whichever action happens to be first');
});

test('connectome exposes persistent, keyboard-reachable agent identity details', () => {
  assert.match(mriSource, /<aside class="agent-inspector" aria-label="Connectome agent details" aria-hidden="true">/,
    'selected details must be a labelled nonmodal landmark');
  assert.match(mriSource, /<select class="ai-select" aria-label="Browse connected visible agents">/,
    'connected visible neurons need a compact keyboard and touch selection path');
  assert.match(mriSource, /<button type="button" class="ai-close" aria-label="Close agent details">/,
    'persistent details need a real accessible close button');
  assert.match(mriSource, /class="tip" role="tooltip" aria-hidden="true"/,
    'transient hover details must expose tooltip semantics');
  assert.match(mriSource, /\.onNodeClick\([^\n]*selectNeuron\(n\)/,
    'a canvas click must enter the same persistent selection path as the picker');
  assert.match(mriSource, /const onKeyDown=e=>\{ if\(e\.key==='Escape' && selectedAgentID\)/,
    'Escape must dismiss a selected agent');
  assert.match(mriSource, /subs\.push\(\(\)=>document\.removeEventListener\('keydown',onKeyDown\)\)/,
    'the global Escape listener must be cleaned up with the renderer');
});

test('connectome makes neuron clicks primary and keeps the fallback picker connection-scoped', () => {
  assert.match(mriSource, /Click a neuron for details, or choose a connected visible agent/,
    'the visible instruction must lead with direct neuron interaction');
  assert.match(mriSource, /n\.isNeuron && n\.agent_id && \(\(n\._peers \|\| 0\) > 0 \|\| n\.agent_id === selectedAgentID\)/,
    'the fallback picker must exclude dormant roster noise while preserving a selected isolated neuron');
  assert.match(mriSource, /nodes\.sort\(\(a,b\)=>\(b\._w\|\|0\)-\(a\._w\|\|0\)\|\|agentName\(a\)\.localeCompare\(agentName\(b\)\)\)/,
    'connected agents must be ordered by visible traffic before name');
  assert.match(mriSource, /const nodeVal = n => n\.isNeuron\s*\? 3\.0 \+ \(n\._deg\|\|0\)\*7/,
    'even dormant neurons need a practical minimum canvas click target');
  assert.match(mriSource, /\.onNodeClick\([^\n]*selectNeuron\(n\)/,
    'clicking the enlarged neuron must enter the persistent detail and relationship path');
  assert.match(mriSource, /selectedAgentID = n\.agent_id; selectedAgentNode = n;[\s\S]{0,300}populateAgentPicker\(rendered\);[\s\S]{0,100}\$\('\.ai-select'\)\.value = selectedAgentID/,
    'a clicked isolated neuron must be added to the picker before it becomes the keyboard return target');
});

test('connectome click dispatch has one hit-tested owner and tolerates small pointer drift', () => {
  assert.doesNotMatch(mriSource, /root\.addEventListener\('click'/,
    'a DOM click fallback races ForceGraph deferred hit-testing and must not dismiss a real node click');
  assert.match(mriSource, /\.clickAfterDrag\(true\)/,
    'ForceGraph must deliver the candidate click so the renderer can apply its explicit tolerance');
  const body = bracedBlock(mriSource, 'function graphClickWithinTolerance').body;
  const withinTolerance = new Function('e', 'graphPointerDown', body);
  assert.equal(withinTolerance({ clientX:102, clientY:101 }, { x:100, y:100 }), true,
    'a two-pixel mouse wobble must still select the neuron');
  assert.equal(withinTolerance({ clientX:110, clientY:100 }, { x:100, y:100 }), false,
    'a real orbit drag must not become a click');
  assert.match(mriSource, /\.onBackgroundClick\(e=>\{ if\(graphClickWithinTolerance\(e\)\) exitFocus\(\); \}\)/,
    'only the graph library raycast may classify a background click');
});

test('connectome bounds domain metadata and keeps traffic ahead of optional raw policy', () => {
  const traffic = mriSource.indexOf('<div class="ai-label">Visible retained traffic</div>');
  const domain = mriSource.indexOf('<div class="ai-label">Domain access metadata</div>');
  assert.ok(traffic !== -1 && domain > traffic,
    'raw access metadata must not bury the primary traffic and relationship details');
  assert.match(mriSource, /<details class="ai-domain-details"><summary class="ai-domain"><\/summary><pre class="ai-domain-full"><\/pre><\/details>/,
    'the full access value must remain available behind an explicit bounded disclosure');
  assert.match(mriSource, /\.ai-domain-full\{max-height:180px;overflow:auto/,
    'expanded policy metadata must scroll inside a bounded region');
  assert.match(mriSource, /agentDomainSummary\(n\)/,
    'the inspector and hover path must use the bounded domain summary');
});

test('directed-link inspection anchors the sender unless an endpoint is already selected', () => {
  const body = bracedBlock(mriSource, 'function selectDirectedLink').body;
  const inspect = new Function(
    'l', 'rendered', 'selectedAgentNode', 'selectedAgentID', 'selectNeuron', 'pinAgentConnection',
    body,
  );
  const alice = { id:'agent:alice', agent_id:'alice', label:'Alice', isNeuron:true };
  const bob = { id:'agent:bob', agent_id:'bob', label:'Bob', isNeuron:true };
  const rendered = { nodes:[alice, bob] };
  const link = { source:'agent:alice', target:'agent:bob', link_type:'synapse', count:9 };
  const selected = [], pinned = [];

  inspect(link, rendered, null, null, node => selected.push(node.agent_id), peer => pinned.push(peer));
  assert.deepEqual(selected, ['alice'], 'clicking an unanchored A→B edge must inspect its sender');
  assert.deepEqual(pinned, ['bob'], 'the receiver must become the pinned peer');

  selected.length = 0; pinned.length = 0;
  inspect(link, rendered, bob, 'bob', node => selected.push(node.agent_id), peer => pinned.push(peer));
  assert.deepEqual(selected, [], 'clicking an incident edge must not replace the selected endpoint');
  assert.deepEqual(pinned, ['alice'], 'the opposite endpoint must be pinned when the receiver is selected');

  selected.length = 0; pinned.length = 0;
  inspect({ ...link, link_type:'focus' }, rendered, null, null,
    node => selected.push(node.agent_id), peer => pinned.push(peer));
  assert.deepEqual([selected, pinned], [[], []], 'transient memory links are not agent traffic');
});

test('memory mutations request a selected-memory refresh while traffic ticks reuse it', () => {
  const reloadBody = bracedBlock(mriSource, 'const reload = () =>').body;
  let cleared = null, scheduled = null;
  const load = () => {};
  const runReload = new Function(
    'mode', 'selectedAgentID', 'selectedMemoryRefreshPending', 'clearTimeout', 't', 'setTimeout', 'load',
    `${reloadBody}\nreturn { selectedMemoryRefreshPending, t };`,
  );
  const afterMutation = runReload(
    'connectome', 'alice', false,
    timer => { cleared = timer; }, 41,
    (callback, delay) => { scheduled = { callback, delay }; return 42; }, load,
  );
  assert.equal(afterMutation.selectedMemoryRefreshPending, true,
    'removing the mutation-only refresh flag must fail');
  assert.equal(cleared, 41);
  assert.deepEqual(scheduled, { callback:load, delay:3000 });
  assert.equal(afterMutation.t, 42);

  const withoutSelection = runReload(
    'connectome', null, false, () => {}, null, (callback, delay) => ({ callback, delay }), load,
  );
  assert.equal(withoutSelection.selectedMemoryRefreshPending, false,
    'memory churn must not invent a selected-agent refresh');

  assert.match(mriSource,
    /\['remember','forget','reinstate','cocommit','import','update','consensus'\]\.forEach\(eventName=>\{\s*subs\.push\(opts\.sse\.on\(eventName, reload\)\);\s*\}\)/,
    'every memory mutation event must share the cache-invalidating reload path');

  const pulseBody = bracedBlock(mriSource, 'const pulse = () =>').body;
  let pulseScheduled = null, pulseLoads = [];
  const runPulse = new Function('mode', 'clearTimeout', 'ct', 'setTimeout', 'load', `${pulseBody}\nreturn ct;`);
  runPulse('connectome', () => {}, null, (callback, delay) => {
    pulseScheduled = { callback, delay }; return 77;
  }, tick => pulseLoads.push(tick));
  assert.equal(pulseScheduled.delay, 900);
  pulseScheduled.callback();
  assert.deepEqual(pulseLoads, [true],
    'traffic ticks must use load(true), not the selected-memory invalidation callback');
});

test('access invalidation ignores event payload and fences pre-access graph and memory responses', () => {
  const body = bracedBlock(mriSource, 'const reauthorize=()=>').body;
  assert.match(mriSource, /const reauthorize=\(\)=>\{/,
    'access payloads are untrusted invalidation ticks, never replacement graph or identity data');
  const calls = [];
  const graphLoads = { invalidate(){ calls.push('graph-invalidate'); } };
  const bloomLoads = { invalidate(){ calls.push('bloom-invalidate'); } };
  const run = new Function(
    'eventPayload', 'clearTimeout', 't', 'selectedMemoryRefreshPending', 'mode', 'selectedAgentID',
    'graphLoads', 'bloomLoads', 'clearBloom', 'hideTip', 'focusId', 'focusSet', 'clearFocusMarker',
    'selectedAgentNode', 'selectedMemoryState', 'renderAgentInspector', 'Graph', 'rendered',
    'refreshCounts', 'populateAgentPicker', '$', 'document', 'load',
    `${body}\nreturn { selectedMemoryRefreshPending, focusId, focusSet, selectedAgentNode, selectedMemoryState };`,
  );
  const state = run(
    { detail:{ nodes:[{ agent_id:'attacker' }], engrams:[{ content:'inject' }] } },
    () => calls.push('timer-clear'), 12, true, 'connectome', 'alice', graphLoads, bloomLoads,
    () => calls.push('bloom-clear'), () => calls.push('tip-hide'), 'agent:alice', new Set(['agent:alice']),
    () => calls.push('marker-clear'), { agent_id:'alice' }, { status:'ready', memories:[{ id:'secret' }] },
    value => calls.push(value === null ? 'inspector-hide' : 'payload-rendered'),
    { graphData:value => calls.push(value.nodes.length===0?'graph-clear':'payload-rendered') }, null,
    value => calls.push(value.nodes.length===0?'counts-clear':'payload-rendered'),
    value => calls.push(value.nodes.length===0?'picker-clear':'payload-rendered'),
    () => null, { activeElement:null }, () => calls.push('load'),
  );
  assert.deepEqual(calls, [
    'timer-clear', 'graph-invalidate', 'bloom-invalidate', 'bloom-clear', 'tip-hide',
    'marker-clear', 'inspector-hide', 'graph-clear', 'counts-clear', 'picker-clear', 'load',
  ], 'access must invalidate both request generations before any fresh authorized load');
  assert.deepEqual(state, {
    selectedMemoryRefreshPending:false,
    focusId:null,
    focusSet:null,
    selectedAgentNode:null,
    selectedMemoryState:{ status:'loading' },
  });
  assert.doesNotMatch(calls.join(' '), /payload|attacker|inject|secret/,
    'no access-event field may populate UI or graph state');
});

test('selection dismissal hides the large inspector and returns keyboard focus to the picker', () => {
  const renderStart=mriSource.indexOf('function renderAgentInspector(');
  const renderEnd=mriSource.indexOf('function clearAgentSelection(',renderStart);
  const render=mriSource.slice(renderStart,renderEnd);
  assert.match(render,/classList\.toggle\('visible', mode === 'connectome' && chosen\)/,
    'the expanded panel must exist only while an agent is selected');
  assert.match(mriSource,/\.ai-close'\)\.onclick=\(\)=>\{ exitFocus\(\); \$\('\.ai-select'\)\.focus\(\); \}/);
  assert.match(mriSource,/e\.key==='Escape'[\s\S]{0,100}exitFocus\(\); \$\('\.ai-select'\)\.focus\(\)/);
  assert.match(mriSource,/t\.closest\('\.panel,\.agent-browser,\.agent-inspector'\)/,
    'picker events must be chrome, never graph-background dismissal');
});

test('a live reload restores cached selected memories or restarts an interrupted bloom', () => {
  const start=mriSource.indexOf('function restoreSelectedAgent(');
  const end=mriSource.indexOf('function setHullOpacity(',start);
  const body=mriSource.slice(start,end);
  assert.match(body,/applyEngramBloom\(Graph\.graphData\(\), selectedMemoryState\.memories\|\|\[\], selected, focusSet, placeNear\)/,
    'ready memories must be recomposed after graph replacement strips transient nodes');
  const loadBody=bracedBlock(mriSource,'function load(').body;
  assert.match(loadBody,/if\(refreshSelectedMemory\|\|selectedMemoryState&&selectedMemoryState\.status==='updating'\)\{\s*bloomEngrams\(selectedAgentNode,\{preserve:true\}\);/,
    'one owner must restart a mutation or interrupted updating bloom');
  assert.match(loadBody,/else if\(selectedMemoryState&&selectedMemoryState\.status==='loading'\)\{\s*bloomEngrams\(selectedAgentNode\);/,
    'a fresh loading selection must restart after graph replacement');
  assert.doesNotMatch(loadBody,/selectedMemoryState\.status==='error'[\s\S]{0,120}bloomEngrams/,
    'explicit engram errors must wait for Retry rather than hammering the endpoint on every live tick');
  assert.match(mriSource,/restoreSelectedAgent\(d\)/,
    'successful reload reconciliation must invoke the tested restore path');
});

test('empty memory results retain projection and continuation caveats', () => {
  const start=mriSource.indexOf('function renderMemoryState(');
  const end=mriSource.indexOf('function renderAgentInspector(',start);
  const body=mriSource.slice(start,end);
  assert.match(body,/!memories\.length[\s\S]*state\.partial[\s\S]*temporarily hidden/);
  assert.match(body,/!memories\.length[\s\S]*state\.continuation[\s\S]*More may exist/);
});

test('agent tooltip is clamped, escaped, and includes identity plus visible traffic', () => {
  const showStart = mriSource.indexOf('function showTip(');
  const showEnd = mriSource.indexOf('function onMove(', showStart);
  const show = mriSource.slice(showStart, showEnd);
  assert.match(show, /escapeHtml\(agentName\(n\)\)/);
  assert.match(show, /escapeHtml\(agentRole\(n\)\)/);
  assert.match(show, /escapeHtml\(agentDomainSummary\(n\)\)/,
    'the tooltip must never render unbounded raw access metadata');
  assert.match(show, /escapeHtml\(id\)/, 'canonical agent identity must be escaped');
  assert.match(show, /n\._incoming/); assert.match(show, /n\._outgoing/); assert.match(show, /n\._peers/);
  const positionStart = mriSource.indexOf('function positionTip(');
  const positionEnd = mriSource.indexOf('function showTip(', positionStart);
  const position = mriSource.slice(positionStart, positionEnd);
  assert.match(position, /tip\.offsetWidth/); assert.match(position, /tip\.offsetHeight/);
  assert.match(position, /Math\.max\(pad,Math\.min\(r\.width-tip\.offsetWidth-pad,left\)\)/,
    'horizontal tooltip placement must stay inside the renderer');
  assert.match(position, /Math\.max\(pad,Math\.min\(r\.height-tip\.offsetHeight-pad,top\)\)/,
    'vertical tooltip placement must stay inside the renderer');
});

test('connectome mode suppresses memory-only empty and unavailable overlays', () => {
  assert.match(mriSource,
    /container\.dispatchEvent\(new CustomEvent\('sage:mri-mode-change',[\s\S]{0,100}detail: \{ mode \}/,
    'the renderer must expose its actual active mode to the host');
  assert.match(appSource,
    /element\.addEventListener\('sage:mri-mode-change', onModeChange\)/,
    'the dashboard must subscribe to renderer mode changes');
  assert.match(appSource, /const showingMemoryView = mriMode === 'memory'/);
  const overlayStart = appSource.indexOf('const showingMemoryView = mriMode');
  const overlayEnd = appSource.indexOf('// Global tooltips state', overlayStart);
  const overlayBlock = appSource.slice(overlayStart, overlayEnd);
  assert.equal((overlayBlock.match(/showingMemoryView &&/g) || []).length, 3,
    'all three memory-only overlays must be gated by the active MRI mode');
});

test('mode controls retain visible pressed and keyboard-focus styling in both themes', () => {
  const styleStart = mriSource.indexOf('const STYLE = `');
  const styleEnd = mriSource.indexOf('`;\n\nfunction injectStyleOnce', styleStart);
  assert.ok(styleStart >= 0 && styleEnd > styleStart, 'the executable MRI stylesheet must remain inspectable');
  const style = mriSource.slice(styleStart, styleEnd);
  const darkPressed = cssDeclarations(style, '.mrib .hud .b-mode[aria-pressed="true"]');
  const lightPressed = cssDeclarations(style, ':root[data-theme="light"] .mrib .hud .b-mode[aria-pressed="true"]');
  const focus = cssDeclarations(style, '.mrib .hud .btn:focus-visible,.mrib .lg-toggle:focus-visible');

  assert.deepEqual(darkPressed, { background: '#0e2943', 'border-color': '#39d0ff' });
  assert.deepEqual(lightPressed, { background: '#dff5fb', 'border-color': '#0e7490' });
  assert.deepEqual(focus, { outline: '2px solid #39d0ff', 'outline-offset': '2px' });
});

test('memory mode isolates typed reasoning links behind the ◈ filter toggle', () => {
  // supersedes + duplicates were previously uncoloured — they fell through to the
  // "related" style. They must now be first-class typed link types.
  const linkTypes = bracedBlock(mriSource, 'const LINK_TYPES =').body;
  for (const t of ['supersedes', 'duplicates']) {
    assert.match(linkTypes, new RegExp(`\\b${t}:\\s*\\{[^}]*typed:\\s*true`),
      `${t} must be a first-class typed link type with its own colour`);
  }

  // linkVisibleFor is the filter itself: pass-through when off; when on, ONLY the
  // domain-grouping + lineage scaffolding are hidden — every typed reasoning link
  // (plus transient focus + connectome synapse/bridge) stays visible.
  const start = mriSource.indexOf('function linkVisibleFor(');
  assert.notEqual(start, -1, 'linkVisibleFor() not found');
  const open = mriSource.indexOf('{', start);
  let depth = 0, body = '';
  for (let i = open; i < mriSource.length; i++) {
    if (mriSource[i] === '{') depth++;
    else if (mriSource[i] === '}' && --depth === 0) { body = mriSource.slice(open + 1, i); break; }
  }
  const visible = (typedOnly, link_type) => new Function('typedOnly', 'l', body)(typedOnly, { link_type });
  for (const t of ['domain', 'parent', 'supersedes', 'contradicts', 'focus', 'synapse']) {
    assert.equal(visible(false, t), true, `${t} is visible when the filter is off`);
  }
  assert.equal(visible(true, 'domain'), false, 'domain grouping hidden when isolating typed links');
  assert.equal(visible(true, 'parent'), false, 'lineage hidden when isolating typed links');
  for (const t of ['supersedes', 'contradicts', 'refines', 'supports', 'causes', 'precedes',
                   'related', 'duplicates', 'focus', 'synapse', 'engram-bridge']) {
    assert.equal(visible(true, t), true, `${t} stays visible when isolating typed links`);
  }

  // The toggle button, its wiring, and its pressed styling must all be present.
  assert.match(mriSource, /class="btn b-typed"[^>]*aria-pressed="false"/, 'the ◈ typed-links toggle must exist');
  const handler = mriSource.indexOf("$('.b-typed').onclick");
  assert.notEqual(handler, -1, 'the ◈ toggle must be wired');
  assert.match(mriSource.slice(handler, handler + 500), /Graph\.linkVisibility\(linkVisibleFor\)/,
    'toggling the filter must re-evaluate link visibility');
  assert.match(mriSource, /\.b-typed\[aria-pressed="true"\]\{[^}]*background:/,
    'the isolate-typed pressed state must stay visible');

  // The filter is memory-mode only — connectome mode has no domain/lineage edges.
  assert.match(mriSource, /typedFilterBtn = \$\('\.b-typed'\); if \(typedFilterBtn\) typedFilterBtn\.hidden = connectome;/,
    'the typed-links toggle must be hidden in connectome mode');
});

test('mode chrome exposes coherent toggle state, active guidance, and live status', () => {
  const functionBody = name => {
    const start = mriSource.indexOf(`function ${name}(`);
    assert.notEqual(start, -1, `${name}() not found`);
    const open = mriSource.indexOf('{', start);
    let depth = 0;
    for (let i = open; i < mriSource.length; i++) {
      if (mriSource[i] === '{') depth++;
      else if (mriSource[i] === '}' && --depth === 0) return mriSource.slice(open + 1, i);
    }
    assert.fail(`${name}() body did not close`);
  };

  const body = functionBody('updateModeChrome');
  const runChrome = new Function('$', 'root', 'mode', 'announce', 'hideTip', 'renderAgentInspector', 'selectedAgentNode', body);
  const elements = {
    '.b-mode': { textContent: '', attrs: {}, setAttribute(k, v) { this.attrs[k] = v; } },
    '.lg-title': { textContent: '' },
    '.guide-memory': { hidden: false },
    '.guide-connectome': { hidden: true },
    '.sr-status': { textContent: '' },
  };
  const labels = [{ textContent: '' }, { textContent: '' }, { textContent: '' }];
  const root = { querySelectorAll: () => labels };
  const $ = selector => elements[selector];

  runChrome($, root, 'connectome', true, () => {}, () => {}, null);
  assert.equal(elements['.b-mode'].textContent, '◉ connectome',
    'a toggle button keeps one visible label; aria-pressed carries its state');
  assert.equal(elements['.b-mode'].attrs['aria-label'], 'Connectome view',
    'aria-pressed needs a stable toggle name, not a changing action label');
  assert.equal(elements['.b-mode'].attrs['aria-pressed'], 'true');
  assert.match(mriSource, /\.b-mode\[aria-pressed="true"\]\{[^}]*background:/,
    'pressed state must remain visible without changing the toggle label');
  assert.equal(elements['.guide-memory'].hidden, true);
  assert.equal(elements['.guide-connectome'].hidden, false);
  assert.match(elements['.sr-status'].textContent, /Connectome view/);
  assert.deepEqual(labels.map(label => label.textContent), ['neurons', 'synapses', 'hubs']);

  runChrome($, root, 'memory', true, () => {}, () => {}, null);
  assert.equal(elements['.b-mode'].textContent, '◉ connectome');
  assert.equal(elements['.b-mode'].attrs['aria-label'], 'Connectome view');
  assert.equal(elements['.b-mode'].attrs['aria-pressed'], 'false');
  assert.equal(elements['.guide-memory'].hidden, false);
  assert.equal(elements['.guide-connectome'].hidden, true);
  assert.match(elements['.sr-status'].textContent, /Memory view/);

  const executableSetMode = functionBody('setMode')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/.*$/gm, '');
  const modeAnnouncements = [];
  const runSetMode = new Function(
    'next', 'mode', 'allowConnectome', 'graphLoads', 'bloomLoads', 'connectomeActivity',
    'connectomeReloadIntent', 'neuronBirths', 'currentDomain', 'container', 'CustomEvent', 'leaveFocusForGraphReplacement', 'clearAgentSelection',
    'hideExplorePanel', 'clearFocusMarker', 'updateModeChrome', 'hullState', '$',
    'sliderUnits', 'setHullOpacity', 'Graph', 'load', 'zoomOut', 'acquireInitialGraph',
    executableSetMode,
  );
  const resettable = () => ({ reset() {} });
  let focusLeaves = 0;
  const modeEvents = [];
  function FakeCustomEvent(type, init) { this.type = type; this.detail = init.detail; }
  runSetMode(
    'connectome', 'memory', true, { invalidate() {} }, { invalidate() {} }, resettable(),
    resettable(), resettable(), null, { dispatchEvent(event) { modeEvents.push(event); } }, FakeCustomEvent,
    () => { focusLeaves++; }, () => {}, () => {}, () => {},
    announce => modeAnnouncements.push(announce), { valueFor: () => 0.03 }, () => null,
    value => value, () => {}, null, () => {}, () => {}, () => {},
  );
  assert.deepEqual(modeAnnouncements, [true],
    'the executable mode transition must invoke the announcing chrome path exactly once');
  assert.equal(focusLeaves, 1,
    'the executable mode transition must strip any distributed-engram bloom exactly once');
  assert.deepEqual(modeEvents.map(event => [event.type, event.detail.mode]),
    [['sage:mri-mode-change', 'connectome']],
    'the host must receive the actual active mode so memory-only overlays can hide');
});

// Live firing pulses only the synapses that actually carried a message. The
// server tick is contentless by design, so which edges fired is derived here by
// diffing two AUTHORIZED snapshots — meaning a client can only ever pulse an
// edge the RBAC-filtered endpoint was willing to show it.
test('diffConnectomeActivity reports a brand new synapse as fired', () => {
  const fired = diffConnectomeActivity(
    [],
    [{ source: 'a', target: 'b', count: 1, last_fired: '2026-01-01T00:00:00Z' }],
  );
  assert.deepEqual(fired, ['a\u0000b']);
});

test('diffConnectomeActivity reports a risen count as fired', () => {
  const prev = [{ source: 'a', target: 'b', count: 1, last_fired: 't1' }];
  const next = [{ source: 'a', target: 'b', count: 2, last_fired: 't1' }];
  assert.deepEqual(diffConnectomeActivity(prev, next), ['a\u0000b']);
});

// Needed because a burst can land inside one timestamp granularity, and a send
// that coincides with a retention prune can leave the count unmoved.
test('diffConnectomeActivity reports an advanced last_fired as fired', () => {
  const prev = [{ source: 'a', target: 'b', count: 5, last_fired: '2026-01-01T00:00:00Z' }];
  const next = [{ source: 'a', target: 'b', count: 5, last_fired: '2026-01-01T00:00:09Z' }];
  assert.deepEqual(diffConnectomeActivity(prev, next), ['a\u0000b']);
});

test('diffConnectomeActivity reports an unchanged synapse as quiet', () => {
  const same = [{ source: 'a', target: 'b', count: 3, last_fired: 't9' }];
  assert.deepEqual(diffConnectomeActivity(same, same), []);
});

// Retained-row pruning lowers a count without any message being sent. Treating
// that as activity would animate traffic that did not happen.
test('diffConnectomeActivity does not treat a pruned count as firing', () => {
  const prev = [{ source: 'a', target: 'b', count: 9, last_fired: 't1' }];
  const next = [{ source: 'a', target: 'b', count: 2, last_fired: 't1' }];
  assert.deepEqual(diffConnectomeActivity(prev, next), []);
});

// After force-simulation binding, link endpoints are node objects rather than
// id strings. The diff must key identically in both shapes or every edge would
// read as new on the first pulse after a render.
test('diffConnectomeActivity keys object endpoints the same as string ids', () => {
  const prev = [{ source: 'a', target: 'b', count: 4, last_fired: 't1' }];
  const next = [{ source: { id: 'a' }, target: { id: 'b' }, count: 4, last_fired: 't1' }];
  assert.deepEqual(diffConnectomeActivity(prev, next), []);
});

test('initial connectome acquisition establishes a baseline without firing', () => {
  const activity = createConnectomeActivityTracker();
  const initial = [{ source: 'a', target: 'b', count: 7, last_fired: 't7' }];
  assert.deepEqual(activity.observe(initial, false), []);
});

test('ordinary reload updates the baseline without firing', () => {
  const activity = createConnectomeActivityTracker();
  activity.observe([{ source: 'a', target: 'b', count: 1, last_fired: 't1' }], false);
  assert.deepEqual(activity.observe(
    [{ source: 'a', target: 'b', count: 2, last_fired: 't2' }], false,
  ), []);
});

test('connectome tick fires only edges changed since the latest authorized baseline', () => {
  const activity = createConnectomeActivityTracker();
  activity.observe([{ source: 'a', target: 'b', count: 1, last_fired: 't1' }], false);
  assert.deepEqual(activity.observe(
    [{ source: 'a', target: 'b', count: 2, last_fired: 't2' }], true,
  ), ['a\u0000b']);
});

test('tick arriving during an ordinary load keeps the pre-tick baseline', () => {
  const activity = createConnectomeActivityTracker();
  const before = [{ source: 'a', target: 'b', count: 1, last_fired: 't1' }];
  const after = [{ source: 'a', target: 'b', count: 2, last_fired: 't2' }];
  activity.observe(before, false);
  assert.deepEqual(activity.observe(after, false, true), [],
    'ordinary response must not pulse or consume a pending tick');
  assert.deepEqual(activity.observe(after, true), ['a\u0000b'],
    'queued tick refetch must still observe the firing transition');
});

test('tick intent survives failed retry and an ordinary reload during backoff', () => {
  const activity = createConnectomeActivityTracker();
  const intent = createConnectomeReloadIntent();
  const before = [{ source: 'a', target: 'b', count: 1, last_fired: 't1' }];
  const after = [{ source: 'a', target: 'b', count: 2, last_fired: 't2' }];
  activity.observe(before, false);

  intent.requestTick();
  const failedTickGeneration = intent.begin('connectome');
  assert.equal(failedTickGeneration, 1);
  intent.settle('connectome', failedTickGeneration, false);
  assert.equal(intent.isPending('connectome'), true,
    'failed tick fetch must retain intent throughout retry backoff');

  // A remember/forget refresh can cancel the retry timer. It must inherit the
  // pending tick instead of advancing the baseline as an ordinary reload.
  const ordinaryDuringBackoff = intent.begin('connectome');
  assert.equal(ordinaryDuringBackoff, 1);
  assert.deepEqual(activity.observe(after, ordinaryDuringBackoff > 0), ['a\u0000b']);
  intent.settle('connectome', ordinaryDuringBackoff, true);
  assert.equal(intent.isPending('connectome'), false);
  assert.deepEqual(activity.observe(after, intent.begin('connectome')), [],
    'a satisfied tick must pulse exactly once');
});

test('a second tick is not acknowledged by an older in-flight generation', () => {
  const activity = createConnectomeActivityTracker();
  const intent = createConnectomeReloadIntent();
  const before = [{ source: 'a', target: 'b', count: 1, last_fired: 't1' }];
  const afterTick1 = [{ source: 'a', target: 'b', count: 2, last_fired: 't2' }];
  const afterTick2 = [{ source: 'a', target: 'b', count: 3, last_fired: 't3' }];
  activity.observe(before, false);

  intent.requestTick();
  const generation1 = intent.begin('connectome');
  intent.requestTick();
  assert.deepEqual(activity.observe(afterTick1, generation1 > 0), ['a\u0000b']);
  intent.settle('connectome', generation1, true);
  assert.equal(intent.isPending('connectome'), true,
    'tick 1 response must not consume tick 2, which arrived after it began');

  const generation2 = intent.begin('connectome');
  assert.equal(generation2, 2);
  assert.deepEqual(activity.observe(afterTick2, generation2 > 0), ['a\u0000b']);
  intent.settle('connectome', generation2, true);
  assert.equal(intent.isPending('connectome'), false);
});

// The renderer subscribes to the contentless tick and refetches the authorized
// snapshot; it must never read edge data off the event itself.
test('mri-brain refetches the authorized snapshot on a connectome tick', () => {
  assert.match(mriSource, /opts\.sse\.on\('connectome'/,
    'the renderer must subscribe to the connectome tick');
  assert.match(mriSource, /load\(true\)/,
    'only the connectome tick path may request a firing diff');
  assert.match(mriSource, /markConnectomeFiring\(d, tickAware, connectomeReloadIntent\.isPending\(request\.mode\) && !tickAware\)/,
    'the fired-edge diff must run on the applied snapshot');
  assert.match(mriSource, /if \(fromConnectomeTick\) connectomeReloadIntent\.requestTick\(\)/,
    'the event must persist tick intent independently of a retry timer');
  assert.match(mriSource, /scheduleGraphRetry\(load\)/,
    'retries must consult persistent tick intent when they begin');
  assert.match(mriSource, /connectomeReloadIntent\.settle\(request\.mode, tickGeneration, true\)/,
    'a successful production fetch must acknowledge exactly its captured generation');
});
