import { fedPipeContactsGet } from './api.js';
import { filterFederationAgents } from './federation-directory.js';

export const agentLabel = agent => agent.display_name || agent.registered_name || String(agent.agent_id || '').slice(0, 12);

// A node's colour is derived from its chain id, never assigned and never
// negotiated: both operators hash the same id to the same swatch, so a peer
// looks identical on both screens and nothing has to be stored or synced. The
// palette is picked for contrast on the dark map, and colour is always a
// redundant cue here — the machine name and the state word are text — so the
// map still reads correctly with any colour vision or in a screenshot.
export const FEDERATION_NODE_PALETTE = Object.freeze([
    '#5ee0c8', '#7fb2ff', '#ffc06a', '#c3a2ff',
    '#ff9db0', '#9ae06a', '#64d2ff', '#ffa8e8',
]);

export function nodeColor(id) {
    const value = String(id || '');
    let hash = 0x811c9dc5;
    for (let index = 0; index < value.length; index += 1) {
        hash ^= value.charCodeAt(index);
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return FEDERATION_NODE_PALETTE[hash % FEDERATION_NODE_PALETTE.length];
}

// One node name rendered inside a 47px circle: at most two short lines, then a
// truncated second line with an ellipsis. "SAGE" told the operator nothing —
// every node wore the same word — while the machine name is the identity they
// actually reason about, so the name goes in the circle and the full value
// stays available in the title, the aria-label and the inspector.
export function nodeLabel(name) {
    // Break after a hyphen, dot or underscore rather than dropping it, so
    // "STUDIO-MACMINI" wraps as "STUDIO-" / "MACMINI" instead of silently
    // turning into two unrelated words.
    const words = String(name || '').trim().split(/\s+/)
        .flatMap(chunk => chunk.match(/[^\s._-]+[._-]?/g) || [])
        .filter(Boolean);
    if (!words.length) return { lines: ['This node'], fontSize: 17 };
    const join = list => list.reduce((acc, word) => (!acc ? word : acc + (/[._-]$/.test(acc) ? '' : ' ') + word), '');
    const width = 12;
    const lines = [];
    let current = [];
    let consumed = 0;
    for (const word of words) {
        consumed += 1;
        const candidate = [...current, word];
        if (!current.length || join(candidate).length <= width) { current = candidate; continue; }
        lines.push(join(current));
        current = [word];
        if (lines.length === 2) break;
    }
    if (lines.length < 2 && current.length) lines.push(join(current));
    const rendered = lines.map(line => (line.length > width ? `${line.slice(0, width - 1)}…` : line));
    const droppedWord = consumed < words.length;
    if (rendered.length && (droppedWord || rendered.some(line => line.length > width))) {
        const last = rendered[rendered.length - 1];
        if (!last.endsWith('…')) rendered[rendered.length - 1] = `${last.slice(0, width - 1)}…`;
    }
    const longest = rendered.reduce((max, line) => Math.max(max, line.length), 0);
    return { lines: rendered, fontSize: rendered.length === 1 ? (longest <= 7 ? 21 : 16) : 13 };
}
export function activityLabel(item) {
    if (item.state === 'failed') return item.kind === 'result' ? 'Reply failed' : 'Delivery failed';
    if (item.state === 'received') return item.kind === 'result' ? 'Reply received' : 'Received here';
    if (item.state === 'delivered') return item.kind === 'result' ? 'Reply delivered' : 'Delivered';
    return item.kind === 'result' ? 'Reply queued' : 'Queued';
}
export function reconcileActivity(previous, items) {
    const next = new Map(items.map(item => [item.id, `${item.state}:${item.at}`]));
    // The first/reconnected snapshot is historical context, never a burst of
    // pretend live traffic. Retain only this bounded window for deduplication.
    const changed = previous ? items.filter(item => previous.get(item.id) !== next.get(item.id)) : [];
    return { next, changed };
}
export function constellationLayout(nodes) {
    const count = nodes.length - 1;
    return nodes.map((node, index) => {
        const angle = count <= 2 ? (index - 1) * Math.PI : (index - 1) * Math.PI * 2 / Math.max(count, 1) - Math.PI / 2;
        const x = index === 0 ? (count === 1 ? 340 : 600) : (count === 1 ? 850 : 600 + 500 * Math.cos(angle));
        const y = index === 0 || count === 1 ? 380 : 380 + 440 * Math.sin(angle);
        return { ...node, x, y, agents: (node.agents || []).map((agent, i, all) => {
            const a = i * Math.PI * 2 / Math.max(all.length, 1) - Math.PI / 2;
            const radius = all.length > 18 ? 105 + (i % 2) * 27 : 112;
            return { ...agent, x: x + radius * Math.cos(a), y: y + radius * Math.sin(a), nodeID: node.id };
        }) };
    });
}

// Ambient movement is decorative, independent of transport and presence.
export function driftConstellation(nodes, seconds) {
    return nodes.map(node => ({ ...node, agents: node.agents.map((agent, index) => {
        const phase = index * 2.399963;
        const dx = agent.x - node.x, dy = agent.y - node.y;
        const angle = Math.atan2(dy, dx) + seconds * .045 + (Math.sin(seconds / 7 + phase) - Math.sin(phase)) * .025;
        const radius = Math.hypot(dx, dy) + (Math.sin(seconds / 5 + phase) - Math.sin(phase)) * 3;
        return { ...agent, x: node.x + Math.cos(angle) * radius, y: node.y + Math.sin(angle) * radius };
    }) }));
}

export function permittedNodePair(enabled, remote, localAgent, remoteAgent) {
        const localContact = remote.localGrant?.contacts?.find(a => a.agent_id === localAgent.agent_id);
        return enabled && remote.known && !remote.conn?.sharing_paused && !remote.grant?.paused && !remote.localGrant?.paused &&
            localContact?.authorization_mode === 'node-messaging-v1' && remoteAgent.authorization_mode === 'node-messaging-v1' &&
            localContact.accepting && localContact.available && remoteAgent.accepting && remoteAgent.available;
}

export function FederationConnectome({ connections, statuses, localChain, localName, enabled, onManage, onPause, onRevoke, busyChain }) {
    const html = window.html;
    const { useState, useEffect, useRef } = preactHooks;
    const [directories, setDirectories] = useState({});
    const [page, setPage] = useState(0);
    const [query, setQuery] = useState('');
    const [view, setView] = useState('graph');
    const [selection, setSelection] = useState(null);
    const [camera, setCamera] = useState({ x: 0, y: 0, zoom: 1 });
    const [activity, setActivity] = useState([]);
    const [pulses, setPulses] = useState([]);
    const [streamState, setStreamState] = useState('Connecting');
    const [notice, setNotice] = useState('');
    const [cursors, setCursors] = useState({});
    const [motionOn, setMotionOn] = useState(true);
    const [reducedMotion, setReducedMotion] = useState(() => window.matchMedia('(prefers-reduced-motion: reduce)').matches);
    const [agentHovered, setAgentHovered] = useState(false);
    const [mapDragging, setMapDragging] = useState(false);
    const [mapFocused, setMapFocused] = useState(false);
    const [ambientTime, setAmbientTime] = useState(0);
    const ambientClock = useRef(0);
    const ambientRunning = motionOn && !reducedMotion && enabled && view === 'graph' && !agentHovered && !mapFocused && !mapDragging;
    useEffect(() => {
        const media = window.matchMedia('(prefers-reduced-motion: reduce)');
        const changed = () => setReducedMotion(media.matches);
        media.addEventListener('change', changed);
        return () => media.removeEventListener('change', changed);
    }, []);
    useEffect(() => {
        if (!ambientRunning) return;
        let frame = 0, last = performance.now();
        const animate = now => {
            if (document.hidden) { frame = 0; return; }
            if (now - last >= 50) {
                ambientClock.current += Math.min(now - last, 100) / 1000;
                last = now; setAmbientTime(ambientClock.current);
            }
            frame = requestAnimationFrame(animate);
        };
        const visibility = () => { cancelAnimationFrame(frame); frame = 0; if (!document.hidden) { last = performance.now(); frame = requestAnimationFrame(animate); } };
        visibility(); document.addEventListener('visibilitychange', visibility);
        return () => { cancelAnimationFrame(frame); document.removeEventListener('visibilitychange', visibility); };
    }, [ambientRunning]);
    const svgRef = useRef(null);
    const drag = useRef(null);
    const activitySeen = useRef(null);
    const pageSize = 6;
    const lastPage = Math.max(0, Math.ceil(connections.length / pageSize) - 1);
    const activePage = Math.min(page, lastPage);
    const visibleConnections = connections.slice(activePage * pageSize, (activePage + 1) * pageSize);
    const localSourceChain = visibleConnections[0]?.remote_chain_id;
    useEffect(() => { if (page !== activePage) { setPage(activePage); setSelection(null); } }, [page, activePage]);
    const chainsKey = visibleConnections.map(c => c.remote_chain_id).join('|');
    const cursorKey = JSON.stringify(cursors);
    useEffect(() => {
        let live = true, timer;
        const refresh = async () => {
            // A bounded two-worker queue; an offline peer cannot cause a burst
            // of directory requests or erase a different peer's last result.
            const queue = [...visibleConnections];
            const worker = async () => { while (live && queue.length) {
                const conn = queue.shift(), chain = conn.remote_chain_id;
                try {
                    const data = await fedPipeContactsGet(chain, true, '', cursors[chain] || {});
                    if (live) setDirectories(old => ({ ...old, [chain]: { data, stale: false } }));
                } catch {
                    if (live) setDirectories(old => ({ ...old, [chain]: { data: null, stale: true } }));
                }
            } };
            await Promise.all([worker(), worker()]);
            if (live) timer = setTimeout(refresh, 10000);
        };
        setDirectories({});
        if (enabled) refresh();
        return () => { live = false; clearTimeout(timer); };
    }, [chainsKey, cursorKey, enabled]);
    useEffect(() => {
        if (!enabled) { setStreamState('Paused'); setActivity([]); setPulses([]); activitySeen.current = null; return; }
        let es, timer, live = true;
        const connect = () => {
            if (!live || document.hidden) return;
            es = new EventSource('/v1/dashboard/federation/activity');
            es.addEventListener('federation_activity', event => {
                try {
                    const data = JSON.parse(event.data);
                    if (!Array.isArray(data.items)) return;
                    const items = data.items.slice(0, 100).filter(item => item && typeof item.id === 'string' && typeof item.chain_id === 'string' && ['inbound','outbound'].includes(item.direction) && ['send','result'].includes(item.kind) && ['pending','received','delivered','failed'].includes(item.state) && Number.isFinite(Date.parse(item.at)));
                    const delta = reconcileActivity(activitySeen.current, items);
                    activitySeen.current = delta.next;
                    setActivity(items); setStreamState('Live');
                    if (delta.changed.length) setPulses(delta.changed.slice(0, 12).map(item => ({ ...item, key: `${item.id}:${item.state}:${Date.now()}` })));
                } catch { setStreamState('Unavailable'); }
            });
            es.onerror = () => {
                es.close(); setStreamState('Reconnecting');
                // Catch-up after a gap is a snapshot, not proof of live traffic.
                activitySeen.current = null; setPulses([]);
                timer = setTimeout(connect, 1500);
            };
        };
        const visibility = () => { clearTimeout(timer); if (es) es.close(); activitySeen.current = null; setPulses([]); setStreamState(document.hidden ? 'Paused' : 'Connecting'); if (!document.hidden) connect(); };
        connect(); document.addEventListener('visibilitychange', visibility);
        return () => { live = false; clearTimeout(timer); if (es) es.close(); document.removeEventListener('visibilitychange', visibility); activitySeen.current = null; };
    }, [enabled]);
    useEffect(() => { if (!pulses.length) return; const timer = setTimeout(() => setPulses([]), 2500); return () => clearTimeout(timer); }, [pulses]);
    const remoteNodes = visibleConnections.map(conn => {
        const entry = directories[conn.remote_chain_id];
        const data = entry && entry.data;
        const status = statuses[conn.remote_chain_id];
        return { id: conn.remote_chain_id, name: conn.peer_name || conn.remote_chain_id, conn,
            state: !enabled ? 'Federation off' : conn.sharing_paused ? 'Paused' : status?.reachable ? 'Reachable' : status?.checking ? 'Checking' : 'Unreachable',
            agents: data?.remote_known ? data.remote_contacts?.contacts || [] : [],
            grant: data?.remote_known ? data.remote_contacts : null,
            localGrant: data?.local_node_contacts,
            known: !!data?.remote_known, stale: entry?.stale };
    });
    // A local continuation cursor is agreement-bound: never silently switch
    // its source peer when a different request wins a race or fails.
    const firstData = directories[localSourceChain]?.data;
    const localGrant = firstData?.local_node_contacts;
    const localNode = { id: localChain || 'local', name: localName || localChain || 'Viewing this node', state: enabled ? 'Viewing this node' : 'Federation off', agents: localGrant?.contacts || [], grant: localGrant, known: !!localGrant, local: true };
    const nodes = driftConstellation(constellationLayout([localNode, ...remoteNodes].map(node => {
        const loadedAgents = node.agents;
        const filtered = filterFederationAgents(loadedAgents, node.name.toLowerCase().includes(query.trim().toLowerCase()) ? '' : query);
        const selected = selection?.nodeID === node.id ? loadedAgents.find(a => a.agent_id === selection.agentID) : null;
        const candidates = selected && !filtered.slice(0, 24).some(a => a.agent_id === selected.agent_id) ? [selected, ...filtered.filter(a => a.agent_id !== selected.agent_id)] : filtered;
        return { ...node, loadedAgents, matchingCount: filtered.length, agents: candidates.slice(0, 24) };
    })), ambientTime);
    // Preserve readable hit targets: fit the cluster extents, then let users
    // focus/zoom. Never stack six clusters or hundreds of agent targets.
    const halfWidth = Math.max(600, ...nodes.map(n => Math.abs(n.x - 600) + 185));
    const halfHeight = Math.max(380, ...nodes.map(n => Math.abs(n.y - 380) + 225));
    const mapViewBox = `${600-halfWidth} ${380-halfHeight} ${halfWidth*2} ${halfHeight*2}`;
    // Names are fitted once per node, not per render pass: the circle is 47px
    // in a fixed viewBox, so the label is a property of the identity, not of the
    // current zoom or camera.
    const nodeLabels = new Map(nodes.map(node => [node.id, nodeLabel(node.name)]));
    const matches = new Set(nodes.flatMap(n => filterFederationAgents(n.loadedAgents, query).map(a => `${n.id}:${a.agent_id}`)));
    const selectedNode = nodes.find(n => n.id === selection?.nodeID);
    const selectedAgent = selectedNode?.agents.find(a => a.agent_id === selection?.agentID);
    const selectedConn = selectedNode?.conn;
    const select = (node, agent) => { setSelection({ nodeID: node.id, agentID: agent?.agent_id }); };
    const focus = (node, agent) => { select(node, agent); const target = agent || node; setCamera({ x: 600 - target.x, y: 380 - target.y, zoom: 1 }); };
    const nodeMatches = n => !!query.trim() && n.name.toLowerCase().includes(query.trim().toLowerCase());
    const nodeVisible = n => !query.trim() || nodeMatches(n) || n.loadedAgents.some(a => matches.has(`${n.id}:${a.agent_id}`));
    const copy = async agent => { try { await navigator.clipboard.writeText(agent.address); setNotice('Message address copied.'); } catch { setNotice('Could not copy. Select and copy the address in the panel.'); } };
    const navigate = (node, side, cursor) => {
        const chain = node.local ? localSourceChain : node.id;
        if (!chain) return;
        setCursors(old => ({ ...old, [chain]: { ...(old[chain] || {}), [side]: cursor } })); setSelection(null);
    };
    const point = event => {
        const matrix = svgRef.current?.getScreenCTM();
        if (!matrix) return { x: 0, y: 0 };
        return new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse());
    };
    const activityNodes = item => {
        const remote = nodes.find(n => n.id === item.chain_id);
        if (!remote) return null;
        const sourceNode = item.direction === 'inbound' ? remote : nodes[0];
        const targetNode = item.direction === 'inbound' ? nodes[0] : remote;
        const source = sourceNode.agents.find(a => a.agent_id === item.source);
        const target = targetNode.agents.find(a => a.agent_id === item.target);
        return source && target ? { source, target, sourceNode, targetNode } : null;
    };
    const shownActivity = activity.filter(item => (!selectedConn || item.chain_id === selectedConn.remote_chain_id) && (!selectedAgent || item.source === selectedAgent.agent_id || item.target === selectedAgent.agent_id));
    const access = (node, agent) => !enabled ? 'Federation off' : !node.local && (node.conn?.sharing_paused || node.grant?.paused || node.localGrant?.paused) ? 'Connection paused' : agent.accepting && agent.available ? 'Messaging allowed' : 'Messaging blocked';
    // Directory membership is not a pairwise permission proof. Draw only
    // negotiated node-messaging relations with both current per-peer grants;
    // legacy/export and linked modes need their own relation resolver.
    const permittedAgents = !selectedAgent ? [] : selectedNode.local
        ? nodes.slice(1).flatMap(remote => remote.agents.filter(agent => permittedNodePair(enabled, remote, selectedAgent, agent)))
        : nodes[0].agents.filter(agent => permittedNodePair(enabled, selectedNode, agent, selectedAgent));
    const keySelect = (event, node, agent) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(node, agent); } };
    return html`<section class="fed-connectome" aria-label="Federation connectome">
        <header class="fc-heading"><div><span class="fc-eyebrow">YOUR FEDERATION</span><h2>Independent brains. Connected agents.</h2><p>Explore a node or agent. Messaging follows trust; memory sharing stays your choice.</p></div><span class=${`fc-live ${streamState === 'Live' ? 'is-live' : ''}`}>${streamState} activity</span></header>
        <div class="fc-toolbar"><label><span>Find an agent or node</span><input type="search" placeholder="Search this view…" value=${query} onInput=${e => setQuery(e.target.value)} /></label>
            <div class="fc-view-switch" role="group" aria-label="Federation view"><button class="btn" aria-pressed=${view === 'graph'} onClick=${() => setView('graph')}>Connectome</button><button class="btn" aria-pressed=${view === 'list'} onClick=${() => setView('list')}>List</button></div>
            <span class="muted">${connections.length} trusted ${connections.length === 1 ? 'node' : 'nodes'} · ${nodes.reduce((n, node) => n + node.loadedAgents.length, 0)} agents loaded</span>
        </div>
        <div class="fc-workspace"><div class="fc-map-area">
        ${view === 'graph' ? html`<div class=${`fc-map ${ambientRunning ? 'fc-ambient-running' : 'fc-ambient-paused'}`}
            onMouseMove=${event => setAgentHovered(!!event.target.closest('[data-entity="agent"]'))} onMouseLeave=${() => setAgentHovered(false)}
            onFocusCapture=${event => setMapFocused(!!event.target.closest('[data-entity]') && event.target.matches(':focus-visible'))} onBlurCapture=${event => { setMapFocused(false); }}><svg ref=${svgRef} viewBox=${mapViewBox} aria-label="Interactive federation map. Tab to a node or agent and press Enter for details."
            onPointerDown=${event => { if (event.target.closest('[data-entity]')) return; setMapDragging(true); drag.current = { ...point(event), camera }; event.currentTarget.setPointerCapture(event.pointerId); }}
            onPointerMove=${event => { if (!drag.current) return; const p = point(event); setCamera({ ...drag.current.camera, x: drag.current.camera.x + p.x - drag.current.x, y: drag.current.camera.y + p.y - drag.current.y }); }}
            onPointerUp=${() => { drag.current = null; setMapDragging(false); }} onPointerCancel=${() => { drag.current = null; setMapDragging(false); }}>
            <defs>${nodes.map((node, index) => html`<radialGradient id=${`fc-halo-${index}`}><stop offset="0" stop-color=${nodeColor(node.id)} stop-opacity=".15"/><stop offset="1" stop-color=${nodeColor(node.id)} stop-opacity="0"/></radialGradient>`)}</defs>
            <g transform=${`translate(${camera.x} ${camera.y}) translate(600 380) scale(${camera.zoom}) translate(-600 -380)`}>
            ${nodes.slice(1).map(node => html`<g key=${node.id} class=${`fc-trust ${node.state === 'Reachable' ? 'is-reachable' : ''}`} style=${`--fc-node:${nodeColor(node.id)}`}>
                <path d=${`M ${nodes[0].x} ${nodes[0].y} Q 600 ${node.y - 75} ${node.x} ${node.y}`} />
                <path class="fc-link-hit" data-entity="connection" role="button" tabindex="0" aria-label=${`Connection to ${node.name}`} d=${`M ${nodes[0].x} ${nodes[0].y} Q 600 ${node.y - 75} ${node.x} ${node.y}`} onClick=${() => select(node)} onKeyDown=${e => keySelect(e, node)} />
            </g>`)}
            ${permittedAgents.map(a => html`<line class="fc-permitted" x1=${selectedAgent.x} y1=${selectedAgent.y} x2=${a.x} y2=${a.y} />`)}
            ${nodes.map((node, index) => {
                const label = nodeLabels.get(node.id) || nodeLabel(node.name);
                const labelTop = label.lines.length === 1 ? node.y + 2 : node.y - 8;
                return html`<g key=${node.id} class=${`fc-cluster ${nodeVisible(node) ? '' : 'is-dim'}`} style=${`--fc-node:${nodeColor(node.id)}`}>
                <circle cx=${node.x} cy=${node.y} r="168" fill=${`url(#fc-halo-${index})`}/>
                <ellipse class="fc-orbit" cx=${node.x} cy=${node.y} rx="143" ry="143"/>
                ${node.agents.map(agent => html`<g key=${agent.agent_id} class=${`fc-agent ${access(node,agent) === 'Messaging allowed' ? '' : 'is-blocked'} ${selectedAgent?.agent_id === agent.agent_id && selectedNode?.id === node.id ? 'is-selected' : ''} ${query.trim() && !nodeMatches(node) && !matches.has(`${node.id}:${agent.agent_id}`) ? 'is-dim' : ''}`}>
                    <line x1=${node.x} y1=${node.y} x2=${agent.x} y2=${agent.y}/>
                    <g data-entity="agent" role="button" tabindex="0" aria-label=${`${agentLabel(agent)} on ${node.name}. ${access(node,agent)}`} onClick=${() => select(node,agent)} onKeyDown=${e => keySelect(e,node,agent)}>
                        <circle class="fc-agent-hit" cx=${agent.x} cy=${agent.y} r="15"/><circle class="fc-agent-core" cx=${agent.x} cy=${agent.y} r="5"/><title>${agentLabel(agent)} · ${access(node,agent)}</title>
                        ${(query && matches.has(`${node.id}:${agent.agent_id}`) || selectedAgent?.agent_id === agent.agent_id) && html`<text class="fc-agent-label" x=${agent.x} y=${agent.y - 19} text-anchor="middle">${agentLabel(agent).slice(0,30)}</text>`}
                    </g>
                </g>`)}
                <g data-entity="node" class=${`fc-node ${selectedNode?.id === node.id && !selectedAgent ? 'is-selected' : ''}`} role="button" tabindex="0" aria-label=${`${node.name}. ${node.state}. ${node.loadedAgents.length} agents loaded`} onClick=${() => select(node)} onKeyDown=${e => keySelect(e,node)}>
                    <circle class="fc-node-ring" cx=${node.x} cy=${node.y} r="47"/>
                    <title>${`${node.name} · ${node.state} · ${node.loadedAgents.length} agents loaded`}</title>
                    <text class="fc-node-label" x=${node.x} y=${labelTop} text-anchor="middle" style=${`font-size:${label.fontSize}px`}>${label.lines[0]}</text>
                    ${label.lines.length > 1 && html`<text class="fc-node-label" x=${node.x} y=${node.y + 11} text-anchor="middle" style=${`font-size:${label.fontSize}px`}>${label.lines[1]}</text>`}
                    <text class="fc-node-count" x=${node.x} y=${node.y + (label.lines.length === 1 ? 22 : 31)} text-anchor="middle">${node.loadedAgents.length} agents</text>
                    <text class="fc-node-status" x=${node.x} y=${node.y + 183} text-anchor="middle">${node.state}${!node.known ? ' · directory unavailable' : ''}</text>
                    ${node.matchingCount > node.agents.length && html`<text class="fc-node-status" x=${node.x} y=${node.y + 201} text-anchor="middle">+${node.matchingCount - node.agents.length} more · search or use List</text>`}
                </g>
            </g>`; })}
            ${pulses.map(item => { const endpoints = activityNodes(item); if (!endpoints) return null; return html`<g key=${item.key} class=${`fc-pulse fc-pulse-${item.state}`} aria-hidden="true"><path pathLength="1" d=${`M ${endpoints.source.x} ${endpoints.source.y} L ${endpoints.target.x} ${endpoints.target.y}`} /></g>`; })}
            </g></svg><div class="fc-map-tools"><button class="btn" aria-pressed=${motionOn && !reducedMotion} disabled=${reducedMotion} title="Decorative movement; pauses over an agent, during keyboard inspection, or while dragging. It does not indicate agent presence." onClick=${() => setMotionOn(value => !value)}>${reducedMotion ? 'Reduced motion' : motionOn ? 'Pause motion' : 'Resume motion'}</button><button class="btn" aria-label="Zoom in" onClick=${() => setCamera(c => ({ ...c, zoom: Math.min(2.5,c.zoom + .2) }))}>+</button><button class="btn" aria-label="Zoom out" onClick=${() => setCamera(c => ({ ...c, zoom: Math.max(.5,c.zoom - .2) }))}>−</button><button class="btn" onClick=${() => setCamera({ x:0,y:0,zoom:1 })}>Fit view</button></div><div class="fc-map-hint">Drag space to pan · select a dot to inspect an agent</div></div>` : html`<div class="fc-list">${nodes.filter(nodeVisible).map(node => html`<section key=${node.id}><button class="fc-list-node" onClick=${() => select(node)}>${node.name}<small>${node.state}</small></button>${filterFederationAgents(node.loadedAgents,nodeMatches(node) ? '' : query).map(agent => html`<button class="fc-list-agent" key=${agent.agent_id} onClick=${() => select(node,agent)}><strong>${agentLabel(agent)}</strong><small>${access(node,agent)}</small></button>`)}</section>`)}</div>`}
        <div class="fc-legend"><span>● Agent</span><span>─ Trusted connection</span><span>┄ Allowed messaging on selection</span><span>Pulse = message status update</span><span>Gentle drift is decorative</span></div>
        <div class="fc-pagination">${nodes.filter(n => n.grant?.next_cursor || (n.local ? cursors[localSourceChain]?.local : cursors[n.id]?.remote)).map(n => html`<div key=${n.id}><span>${n.name}</span><button class="btn btn-sm" onClick=${() => navigate(n,n.local ? 'local':'remote','')}>First agents</button>${n.grant?.next_cursor && html`<button class="btn btn-sm" onClick=${() => navigate(n,n.local ? 'local':'remote',n.grant.next_cursor)}>Next agents</button>`}</div>`)}
            ${connections.length > pageSize && html`<button class="btn" disabled=${activePage === 0} onClick=${() => { setPage(activePage-1);setSelection(null); }}>Previous nodes</button><span>Node page ${activePage+1}</span><button class="btn" disabled=${(activePage+1)*pageSize>=connections.length} onClick=${() => {setPage(activePage+1);setSelection(null);}}>Next nodes</button>`}</div>
        <p class="fc-footnote">The map shows up to 24 matching agents per node. List shows all loaded agents; search brings matching agents onto the map. Search covers loaded agents and nodes. Use Next agents or Next nodes for more. Listed agents may be idle; membership is not presence.</p>
        </div><aside class="fc-inspector" aria-label="Federation selection details">
            ${selectedNode ? html`<div class="fc-inspector-selection"><span class="fc-eyebrow">${selectedAgent ? 'AGENT' : 'SAGE NODE'}</span><h3>${selectedAgent ? agentLabel(selectedAgent) : selectedNode.name}</h3><p>${selectedAgent ? access(selectedNode,selectedAgent) : selectedNode.state}</p>
                ${selectedAgent && html`<p class="muted">${selectedAgent.provider || 'Agent'} · ${selectedNode.name}</p><label>Exact message address<input readonly value=${selectedAgent.address || ''}/></label><button class="btn btn-primary" disabled=${!selectedAgent.address} onClick=${() => copy(selectedAgent)}>Copy message address</button><p class="fc-footnote">Use this address in your agent’s messaging tool. Memory access is configured separately.</p>`}
                ${selectedConn && html`<button class="btn btn-primary" onClick=${() => onManage(selectedConn)}>Manage memory sharing</button><button class="btn" disabled=${busyChain === selectedConn.remote_chain_id || !enabled} onClick=${() => onPause(selectedConn,!selectedConn.sharing_paused)}>${selectedConn.sharing_paused ? 'Resume connection' : 'Pause connection'}</button><button class="btn btn-danger" disabled=${busyChain === selectedConn.remote_chain_id} onClick=${() => onRevoke(selectedConn)}>Remove trusted connection…</button><p class="fc-footnote">Pause stops sharing and work requests while preserving trust. Removing trust requires pairing again.</p>`}
                ${selectedNode.local && html`<p class="fc-footnote">Select a connected SAGE to manage its sharing or remove its trusted connection.</p>`}
                <button class="btn btn-sm" onClick=${() => { setView('graph'); focus(selectedNode, selectedAgent); }}>Focus on map</button><button class="btn btn-sm" onClick=${() => setSelection(null)}>Clear selection</button></div>` : html`<div class="fc-inspector-empty"><span class="fc-eyebrow">EXPLORE THE NETWORK</span><h3>Every dot is an agent.</h3><p>Select a brain, agent, or connection to see its permissions and controls.</p><p>Solid links connect trusted SAGEs. Select an agent to see who it can message. Older SAGE versions may need an update to show these paths.</p></div>`}
            <div class="fc-activity"><h3>Message activity <small>${streamState}</small></h3><p class="fc-footnote">Recent message and reply delivery on this SAGE. Up to 100 records from the last 24 hours.</p>
            ${shownActivity.slice(0,8).map(item => { const e = activityNodes(item); return html`<div class="fc-activity-row" key=${item.id}><strong>${activityLabel(item)}</strong><small>${e ? `${agentLabel(e.source)} → ${agentLabel(e.target)}` : `${item.direction === 'inbound' ? 'From' : 'To'} ${connections.find(c => c.remote_chain_id === item.chain_id)?.peer_name || 'connected node'}`}</small><time>${new Date(item.at).toLocaleTimeString()}</time></div>`; })}
            ${!shownActivity.length && html`<p class="muted">${streamState === 'Live' ? 'No recent activity in this view.' : 'Waiting for activity status…'}</p>`}</div>
        </aside></div>${notice && html`<p role="status">${notice}</p>`}
    </section>`;
}
