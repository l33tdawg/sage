// CEREBRUM — Your SAGE Brain
import { SSEClient } from './sse.js';
import { fetchStats, fetchGraph, fetchMemories, deleteMemory, updateMemory, fetchHealth, fetchValidators, fetchMcpConfig, checkAuth, login, recoverVault, lockSession, importMemories, importPreview, importConfirm, fetchCleanupSettings, saveCleanupSettings, runCleanup, fetchAgents, fetchAgent, createAgent, updateAgent, removeAgent, downloadBundle, fetchTemplates, fetchRedeployStatus, startRedeploy, createPairingCode, rotateAgentKey, fetchBootInstructions, saveBootInstructions, fetchLedgerStatus, enableLedger, changeLedgerPassphrase, disableLedger, fetchTags, fetchMemoryTags, setMemoryTags, fetchAutostart, setAutostart, checkForUpdate, applyUpdate, restartServer, fetchReranker, saveReranker, testReranker, fetchTasks, updateTaskStatus, createTask, assignTask, fetchUnregisteredAgents, mergeAgent, fetchRecallSettings, saveRecallSettings, fetchAgentTags, transferTag, transferDomain, bulkUpdateMemories, fetchMemoryMode, saveMemoryMode, fetchPipeline, fetchPipelineStats, sendPipelineNote, fetchGovProposals, fetchGovProposalDetail, submitGovProposal, submitGovVote, wizardCheckCloudflared, wizardInstallCloudflared, wizardStartLogin, wizardLoginStatus, wizardCreateTunnel, wizardMintToken, connectProvider, connectRemoteUrl,
embeddingsStatus, checkOllamaEmbed, pullEmbedModel, reembedMemories, reembedProgress, enableSemanticEmbeddings,
joinHostInterfaces, enableNetworkMode, joinHostStart, joinHostStatus, joinHostApprove, joinHostAbort,
joinGuestStart, joinGuestStatus, joinGuestCancel, joinGuestRestart,
fedConnections, fedRevoke, fedPeerStatus, fedHostCreate, fedHostScanReturn, fedHostStatus, fedHostApprove, fedHostAbort, fedGuestScan, fedGuestRequest, fedGuestStatus, fedGuestConfirm } from './api.js';

import { mountMriBrain } from './mri-brain.js';

const { h, render, createContext } = preact;
const { useState, useEffect, useRef, useCallback, useContext } = preactHooks;
const html = window.html;

// Product version shown in the CEREBRUM header. Bump at release; the precise
// build string (git describe) stays available on the backend.
const SAGE_VERSION = 'v11.0';

// MriView — the 3D MRI memory-brain, rendered natively (the dashboard's
// X-Frame-Options/CSP forbid iframing, so we mount the shared renderer
// directly). Reads the same /v1/dashboard/memory/graph the 2D brain uses.
function MriView({ sse }) {
    const ref = useRef(null);
    const [mriEmpty, setMriEmpty] = useState(false);
    useEffect(() => {
        let alive = true;
        fetchStats().then(s => { if (alive) setMriEmpty((s.total_memories || 0) === 0); }).catch(() => {});
        return () => { alive = false; };
    }, [sse]);
    useEffect(() => {
        if (!ref.current) return;
        const cleanup = mountMriBrain(ref.current, {
            fetchUrl: '/v1/dashboard/memory/graph?status=all&limit=500',
            showScan: false,
            sse,
        });
        return cleanup;
    }, [sse]);
    return html`<div class="mri-wrap">
        <div class="mri-stage" ref=${ref}></div>
        ${mriEmpty && html`<div class="brain-empty-overlay">
            <${EmptyState} icon="brain"
                headline="Your brain is empty"
                hint="No memories to visualize yet. Import an existing export or connect an agent over MCP, and the MRI will render them in 3D."
                actionLabel="Import memories"
                onAction=${() => { window.location.hash = '#/import'; }} />
        </div>`}
    </div>`;
}

// Global tooltips state — persisted in localStorage
const TooltipsContext = createContext(false);
function useTooltips() { return useContext(TooltipsContext); }

// HelpTip — contextual help tooltip, only visible when tooltips are enabled in settings
function HelpTip({ text, align }) {
    const enabled = useTooltips();
    const [show, setShow] = useState(false);
    if (!enabled) return null;
    return html`<span class="help-tip"
        onMouseEnter=${() => setShow(true)} onMouseLeave=${() => setShow(false)}
        onFocus=${() => setShow(true)} onBlur=${() => setShow(false)}>
        <span class="help-tip-trigger" tabIndex="0">?</span>
        ${show && html`<span class="help-tip-popup ${align ? 'align-' + align : ''}">${text}</span>`}
    </span>`;
}

// PageHelp — contextual "?" button that opens the CEREBRUM guide to a specific section.
// Place this in any page header to give users one-click access to relevant help.
function PageHelp({ section, label }) {
    return html`<button class="page-help-btn" onClick=${() => window.__sageOpenHelp && window.__sageOpenHelp(section)}
        title=${label || 'Help for this page'}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
    </button>`;
}

// Domain color palette — vibrant, distinct hues for every domain.
// Known domains get hand-picked colors; unknown domains get a stable
// hash-derived color so each domain is always the same hue.
const DOMAIN_COLORS = {
    crypto: '#06b6d4',
    vuln_intel: '#ef4444',
    challenge_generation: '#8b5cf6',
    solver_feedback: '#10b981',
    calibration: '#f59e0b',
    infrastructure: '#3b82f6',
    general: '#6b7280',
    security: '#f43f5e',
    exploit: '#ec4899',
    'sage-project': '#a78bfa',
    'sage-release': '#2dd4bf',
    'sage-development': '#f472b6',
    'sage-roadmap': '#fbbf24',
    'sage-security': '#fb923c',
    'sage-distribution': '#38bdf8',
    'go-debugging': '#34d399',
    'user-preferences': '#c084fc',
};

const _domainColorCache = {};

function hslToHex(h, s, l) {
    s /= 100; l /= 100;
    const a = s * Math.min(l, 1 - l);
    const f = n => { const k = (n + h / 30) % 12; return l - a * Math.max(-1, Math.min(k - 3, 9 - k, 1)); };
    return '#' + [f(0), f(8), f(4)].map(x => Math.round(x * 255).toString(16).padStart(2, '0')).join('');
}

function getDomainColor(domain) {
    if (!domain) return '#64748b';
    const lower = domain.toLowerCase();

    // Check known domains first
    for (const [key, color] of Object.entries(DOMAIN_COLORS)) {
        if (lower === key || lower.includes(key)) return color;
    }

    // Hash-based color for unknown domains — stable across sessions
    if (_domainColorCache[lower]) return _domainColorCache[lower];
    let hash = 0;
    for (let i = 0; i < lower.length; i++) {
        hash = lower.charCodeAt(i) + ((hash << 5) - hash);
    }
    // Use HSL for vibrant colors, then convert to hex (canvas gradients need hex for alpha suffix)
    const hue = ((hash % 360) + 360) % 360;
    const color = hslToHex(hue, 70, 60);
    _domainColorCache[lower] = color;
    return color;
}

function timeAgo(dateStr) {
    const now = Date.now();
    const then = new Date(dateStr).getTime();
    const diff = Math.floor((now - then) / 1000);
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
}

function confidenceColor(v) {
    if (v >= 0.7) return '#10b981';
    if (v >= 0.4) return '#f59e0b';
    return '#ef4444';
}

// describeEmbedder normalizes the /v1/dashboard/health embedder block into the
// shape the status pill + system-status row need. Handles three server tiers:
//   1. v6.8.8+:   health.embedder = { provider, model, dimension, ready,
//                                     semantic, online }
//   2. v6.8.7:    health.ollama only ("running" | "offline"). Treat as Ollama
//                 even though that may be wrong — pre-fix server can't tell us
//                 otherwise, and the misreport this exists to fix is exactly
//                 this case. Once upstream ships the fix this branch becomes
//                 dead code for new deployments.
//   3. degraded:  no health at all → unknown.
function describeEmbedder(health) {
    // displayName is the short noun the System Status row renders as the
    // dependency name (the "Ollama" column equivalent). It's deliberately not
    // derived from shortLabel.split(' ')[0] — that worked for "Ollama" /
    // "Hash" / "OpenAI-compatible" but produced "No" for the
    // "No embedder configured" case.
    const displayName = {
        'ollama': 'Ollama',
        'openai-compatible': 'OpenAI-compatible',
        'hash': 'Hash provider',
        'none': 'Embedder',
        'unknown': 'Embedder',
    };
    const emb = health?.embedder;
    if (emb && emb.provider) {
        const p = emb.provider;
        const online = !!emb.online;
        let label;
        switch (p) {
            case 'ollama':
                label = `Ollama ${online ? 'connected' : 'offline'}`;
                break;
            case 'openai-compatible':
                label = `OpenAI-compatible ${online ? 'connected' : 'offline'}`;
                break;
            case 'hash':
                // Hash has no upstream and no semantic meaning. Make the
                // operator-facing label say so loudly — it's a test/fallback
                // mode and a long-running deployment on it is almost always
                // a misconfiguration.
                label = 'Hash provider (no semantic search)';
                break;
            case 'none':
                label = 'No embedder configured';
                break;
            default:
                label = `${p} ${online ? 'connected' : 'offline'}`;
        }
        // Append model + dimension when known. Hash and "none" don't get a
        // model suffix — they don't talk to anything.
        const detailParts = [];
        if (emb.model && p !== 'hash' && p !== 'none') detailParts.push(emb.model);
        if (emb.dimension && p !== 'none') detailParts.push(`${emb.dimension}-dim`);
        const detail = detailParts.length ? ` (${detailParts.join(', ')})` : '';
        return {
            provider: p,
            online,
            label: label + detail,
            shortLabel: label,
            displayName: displayName[p] || p,
            detail: detailParts.join(', '),
        };
    }
    // Pre-v6.8.8 fallback: only the legacy ollama string is available. The
    // value "n/a" means a newer server explicitly told us not to use this
    // field — treat as unknown so we don't paint a false Ollama status.
    const legacy = health?.ollama;
    if (legacy === 'running') {
        return { provider: 'ollama', online: true, label: 'Ollama connected', shortLabel: 'Ollama connected', displayName: displayName.ollama, detail: '' };
    }
    if (legacy === 'offline') {
        return { provider: 'ollama', online: false, label: 'Ollama offline', shortLabel: 'Ollama offline', displayName: displayName.ollama, detail: '' };
    }
    return { provider: 'unknown', online: false, label: 'Embedder status unknown', shortLabel: 'Embedder unknown', displayName: displayName.unknown, detail: '' };
}

// SVG Icons
const icons = {
    brain: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2C9.5 2 7.5 3.5 7 5.5C5.5 5.5 4 7 4 9c0 1.5.8 2.8 2 3.5C5.5 13.5 5 15 5.5 16.5c.5 1 1.5 2 3 2.5l.5 1c.3.6 1 1 1.7 1h2.6c.7 0 1.4-.4 1.7-1l.5-1c1.5-.5 2.5-1.5 3-2.5.5-1.5 0-3-.5-4C19.2 11.8 20 10.5 20 9c0-2-1.5-3.5-3-3.5C16.5 3.5 14.5 2 12 2z"/><path d="M12 2v19" opacity="0.3"/><path d="M8 8c-1 0-2 .5-2 1.5" opacity="0.5"/><path d="M16 8c1 0 2 .5 2 1.5" opacity="0.5"/></svg>`,
    search: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
    settings: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.32 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>`,
    import: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>`,
    help: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
    network: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="5" r="3"/><circle cx="5" cy="19" r="3"/><circle cx="19" cy="19" r="3"/><line x1="12" y1="8" x2="5" y2="16"/><line x1="12" y1="8" x2="19" y2="16"/><line x1="5" y1="19" x2="19" y2="19" opacity="0.3"/></svg>`,
    tasks: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>`,
    pipeline: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 12h4"/><path d="M16 12h4"/><rect x="8" y="8" width="8" height="8" rx="2"/><path d="M12 4v4"/><path d="M12 16v4"/><circle cx="2" cy="12" r="1" fill="currentColor"/><circle cx="22" cy="12" r="1" fill="currentColor"/><circle cx="12" cy="2" r="1" fill="currentColor"/><circle cx="12" cy="22" r="1" fill="currentColor"/></svg>`,
    // Two interlinked rings — a handshake/connection between two networks.
    // Deliberately NOT the quorum "network" node-graph glyph (federation-join
    // ADDS a connection and deletes nothing; quorum-join wipes node state).
    federation: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="8" cy="12" r="5"/><circle cx="16" cy="12" r="5"/></svg>`,
    // Speedometer gauge — the node control board. Deliberately distinct from the
    // tasks 2x2 grid glyph so the two are not confused in the sidebar.
    overview: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20a8 8 0 1 1 16 0"/><path d="M12 20l4-5"/><circle cx="12" cy="20" r="1.3" fill="currentColor"/></svg>`,
};

// ============================================================================
// Brain Visualization (Canvas)
// ============================================================================

function BrainView({ sse, onSelectMemory, timelineFilter }) {
    const canvasRef = useRef(null);
    const stateRef = useRef({
        nodes: [], edges: [], simulation: null,
        camera: { x: 0, y: 0, zoom: 0.6 },
        mouse: { x: 0, y: 0, dragging: false, dragStart: null, hoveredNode: null },
        filterDomains: new Set(),
        searchFilter: '',
        animTime: 0,
        pulseNodes: new Map(),
        // Focus mode: click a node to see its domain group arranged in a timeline row
        focusDomain: null,
        focusTransition: 0, // 0 = normal, 1 = fully focused
        focusTargetPositions: new Map(), // node id → {x, y} for timeline arrangement
    });
    const [stats, setStats] = useState(null);
    const [domains, setDomains] = useState([]);
    const [filterDomains, setFilterDomains] = useState(new Set());
    const [domainsCollapsed, setDomainsCollapsed] = useState(true); // collapse the domain filter strip by default (it can be 200+ chips)
    const [graphLoaded, setGraphLoaded] = useState(false); // first graph fetch done — gate a "synthesizing" hint so the canvas never just looks blank
    const [searchText, setSearchText] = useState('');
    const [searchOpen, setSearchOpen] = useState(false);
    const [tooltip, setTooltip] = useState(null);
    const [sseConnected, setSseConnected] = useState(false);
    const [agentFilter, setAgentFilter] = useState(''); // '' = all agents
    const [agentList, setAgentList] = useState([]);
    const [focusedDomain, setFocusedDomain] = useState(null); // for UI display
    // Domain transfer state
    const [domainTransfer, setDomainTransfer] = useState(null); // { domain, sourceAgentId }
    const [domainTransferring, setDomainTransferring] = useState(false);
    // Selection state for bulk operations (only active in focus mode)
    const [selectedMemories, setSelectedMemories] = useState(new Set());
    const [bulkAction, setBulkAction] = useState(null); // 'domain' | 'tag' | 'agent' | null
    const [bulkInput, setBulkInput] = useState('');
    const [bulkBusy, setBulkBusy] = useState(false);
    const selectedRef = useRef(new Set()); // mirror for canvas access
    // Draggable stats panel
    const [statsPos, setStatsPos] = useState(() => {
        try { const s = JSON.parse(localStorage.getItem('sage_stats_pos')); if (s && s.x != null) return s; } catch {}
        return null; // null = default CSS position
    });
    const statsDragRef = useRef({ dragging: false, startX: 0, startY: 0, startPosX: 0, startPosY: 0 });

    const registeredAgentsRef = useRef([]);

    // Load graph data + agents
    useEffect(() => {
        fetchAgents().then(data => {
            registeredAgentsRef.current = data.agents || [];
            loadData();
        }).catch(() => loadData());
        const interval = setInterval(loadData, 30000);
        return () => clearInterval(interval);
    }, []);

    async function loadData() {
        try {
            const [graphData, statsData] = await Promise.all([fetchGraph(), fetchStats()]);
            const s = stateRef.current;

            // Preserve positions of existing nodes
            const existingPositions = {};
            for (const n of s.nodes) {
                existingPositions[n.id] = { x: n.x, y: n.y, vx: n.vx, vy: n.vy };
            }

            const nodes = (graphData.nodes || []).map(n => {
                const existing = existingPositions[n.id];
                return {
                    ...n,
                    x: existing ? existing.x : (Math.random() - 0.5) * 600,
                    y: existing ? existing.y : (Math.random() - 0.5) * 400,
                    vx: existing ? existing.vx : 0,
                    vy: existing ? existing.vy : 0,
                    radius: 6 + n.confidence * 14,
                    color: getDomainColor(n.domain),
                };
            });

            s.nodes = nodes;
            s.edges = graphData.edges || [];
            setStats(statsData);
            setGraphLoaded(true);

            const domainSet = new Set(nodes.map(n => n.domain).filter(Boolean));
            setDomains([...domainSet].sort());

            // Merge registered agents with agents discovered from graph data
            const registered = registeredAgentsRef.current;
            const knownIds = new Set(registered.map(a => a.agent_id));
            const graphAgentIds = new Set(nodes.map(n => n.agent).filter(Boolean));
            const merged = [...registered];
            for (const aid of graphAgentIds) {
                if (!knownIds.has(aid)) {
                    merged.push({ agent_id: aid, name: aid.slice(0, 8) + '…', avatar: '🤖' });
                }
            }
            setAgentList(merged);
        } catch (err) {
            // retry on next interval; clear the loading hint so it doesn't stick
            // forever on a transient error (an empty canvas is honest; the 30s
            // interval and SSE events will repopulate).
            setGraphLoaded(true);
        }
    }

    // SSE events
    useEffect(() => {
        if (!sse) return;
        const unsub1 = sse.on('connection', (data) => setSseConnected(data.connected));
        const unsub2 = sse.on('remember', (data) => {
            stateRef.current.pulseNodes.set(data.memory_id, { start: performance.now(), type: 'remember' });
            loadData();
        });
        const unsub3 = sse.on('recall', (data) => {
            stateRef.current.pulseNodes.set(data.memory_id, { start: performance.now(), type: 'recall' });
        });
        const unsub4 = sse.on('forget', (data) => {
            stateRef.current.pulseNodes.set(data.memory_id, { start: performance.now(), type: 'forget' });
            // Remove node from local state after fade (or immediately if tab was backgrounded)
            setTimeout(() => {
                const s = stateRef.current;
                s.nodes = s.nodes.filter(n => n.id !== data.memory_id);
                s.pulseNodes.delete(data.memory_id);
            }, 2200);
        });
        return () => { unsub1(); unsub2(); unsub3(); unsub4(); };
    }, [sse]);

    // Canvas rendering loop
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let animFrame;
        let lastFrameTime = performance.now();

        function resize() {
            const rect = canvas.parentElement.getBoundingClientRect();
            canvas.width = rect.width * devicePixelRatio;
            canvas.height = rect.height * devicePixelRatio;
            canvas.style.width = rect.width + 'px';
            canvas.style.height = rect.height + 'px';
            ctx.scale(devicePixelRatio, devicePixelRatio);
        }
        resize();
        window.addEventListener('resize', resize);

        function tick() {
            // Schedule next frame FIRST — if anything throws, the loop survives
            animFrame = requestAnimationFrame(tick);
            const s = stateRef.current;
            const W = canvas.width / devicePixelRatio;
            const H = canvas.height / devicePixelRatio;
            const now = performance.now();
            const frameDelta = now - lastFrameTime;
            lastFrameTime = now;

            // Skip physics if tab was backgrounded (>200ms gap) — just redraw current state
            const skipPhysics = frameDelta > 200;
            s.animTime = now;

            // Update filter state from React
            s.filterDomains = filterDomains;
            s.searchFilter = searchText.toLowerCase();
            s.agentFilter = agentFilter;
            s.timelineFilter = timelineFilter;

            // Force simulation — skip if returning from backgrounded tab
            if (!skipPhysics) simulateForces(s, W, H);

            // Clear
            ctx.save();
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            ctx.scale(devicePixelRatio, devicePixelRatio);
            ctx.clearRect(0, 0, W, H);

            // Smooth camera pan (lerp toward target if set)
            const cam = s.camera;
            if (s._cameraTarget && !skipPhysics) {
                const lerpSpeed = 0.08;
                cam.x += (s._cameraTarget.x - cam.x) * lerpSpeed;
                cam.y += (s._cameraTarget.y - cam.y) * lerpSpeed;
                cam.zoom += (s._cameraTarget.zoom - cam.zoom) * lerpSpeed;
                // Clear target when close enough
                if (Math.abs(cam.x - s._cameraTarget.x) < 0.5 &&
                    Math.abs(cam.y - s._cameraTarget.y) < 0.5) {
                    s._cameraTarget = null;
                }
            }

            // Camera transform
            ctx.translate(W / 2 + cam.x, H / 2 + cam.y);
            ctx.scale(cam.zoom, cam.zoom);

            // Precompute node timestamps and search fields once (used by filter and focus)
            if (!s._nodeTimestamps || s._nodeTimestamps.size !== s.nodes.length) {
                s._nodeTimestamps = new Map();
                for (const n of s.nodes) {
                    const ct = n.created_at || n.createdAt;
                    s._nodeTimestamps.set(n.id, ct ? new Date(ct).getTime() : 0);
                }
            }
            if (!s._searchFields || s._searchFields.size !== s.nodes.length) {
                s._searchFields = new Map();
                for (const n of s.nodes) {
                    s._searchFields.set(n.id, [
                        n.content ? n.content.toLowerCase() : '',
                        n.domain ? n.domain.toLowerCase() : '',
                        n.memory_type ? n.memory_type.toLowerCase() : '',
                        n.agent ? n.agent.toLowerCase() : '',
                    ].join('\0'));
                }
            }

            // Determine visible nodes (cached — only recompute when filters change)
            if (!s._filterCache || s._filterCache.filterDomains !== s.filterDomains ||
                s._filterCache.searchFilter !== s.searchFilter || s._filterCache.agentFilter !== s.agentFilter ||
                s._filterCache.timelineFilter !== s.timelineFilter || s._filterCache.nodeCount !== s.nodes.length) {
                const filtered = [];
                for (const n of s.nodes) {
                    if (s.filterDomains.size > 0 && !s.filterDomains.has(n.domain)) continue;
                    if (s.agentFilter && n.agent !== s.agentFilter) continue;
                    if (s.searchFilter && !s._searchFields.get(n.id).includes(s.searchFilter)) continue;
                    if (s.timelineFilter && s.timelineFilter.length > 0) {
                        const nodeTime = s._nodeTimestamps.get(n.id);
                        if (!nodeTime) continue;
                        let inRange = false;
                        for (const range of s.timelineFilter) {
                            if (nodeTime >= range.from && nodeTime <= range.to) { inRange = true; break; }
                        }
                        if (!inRange) continue;
                    }
                    filtered.push(n);
                }
                s._visibleNodes = filtered;
                s._visibleIds = new Set(filtered.map(n => n.id));
                s._filterCache = { filterDomains: s.filterDomains, searchFilter: s.searchFilter,
                    agentFilter: s.agentFilter, timelineFilter: s.timelineFilter, nodeCount: s.nodes.length };
            }
            const visibleNodes = s._visibleNodes;
            const visibleIds = s._visibleIds;
            const searchMatch = null;

            // Focus mode: animate transition (linear progress, eased in draw)
            if (s.focusDomain) {
                s.focusTransition = Math.min(1, s.focusTransition + 0.035);
                s._wasInFocus = true;
            } else {
                // Snap-back: when exiting focus, give returning nodes a velocity kick
                if (s._wasInFocus && s.focusTransition > 0.3) {
                    s._wasInFocus = false;
                    s._forceFrame = 0;
                    // Compute cloud centroid from ALL nodes
                    let cx = 0, cy = 0;
                    for (const n of s.nodes) { cx += n.x; cy += n.y; }
                    cx /= (s.nodes.length || 1);
                    cy /= (s.nodes.length || 1);
                    // Kick ALL nodes — focused ones snap back from grid, others scatter
                    for (const n of s.nodes) {
                        const target = s.focusTargetPositions.get(n.id);
                        if (target) {
                            // Focused node: start from grid position, spring toward center
                            n.x = target.x;
                            n.y = target.y;
                        }
                        // Give every node a velocity impulse
                        const dx = cx - n.x;
                        const dy = cy - n.y;
                        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                        const speed = target ? (6 + Math.random() * 4) : (1 + Math.random() * 2);
                        n.vx = (dx / dist) * speed + (Math.random() - 0.5) * 3;
                        n.vy = (dy / dist) * speed + (Math.random() - 0.5) * 3;
                    }
                }
                s.focusTransition = Math.max(0, s.focusTransition - 0.08);
                // Clean up target positions once transition is fully done
                if (s.focusTransition === 0) {
                    s.focusTargetPositions.clear();
                }
            }

            // Compute focus target positions (grid layout: L→R, old→new, wrapping rows)
            // Grid is anchored at the cloud centroid; camera pans to center it in the visible area
            if (s.focusDomain && s.focusTransition > 0) {
                if (s._focusCacheDomain !== s.focusDomain || s._focusCacheCount !== visibleNodes.length) {
                    const domainNodes = visibleNodes
                        .filter(n => n.domain === s.focusDomain)
                        .sort((a, b) => {
                            const ta = s._nodeTimestamps ? s._nodeTimestamps.get(a.id) || 0 : new Date(a.created_at || a.createdAt || 0).getTime();
                            const tb = s._nodeTimestamps ? s._nodeTimestamps.get(b.id) || 0 : new Date(b.created_at || b.createdAt || 0).getTime();
                            return ta - tb;
                        });
                    const zoom = s.camera?.zoom || 0.6;
                    // Cloud centroid in world coords (stable anchor point)
                    let cloudCx = 0, cloudCy = 0;
                    for (const n of visibleNodes) { cloudCx += n.x; cloudCy += n.y; }
                    cloudCx /= (visibleNodes.length || 1);
                    cloudCy /= (visibleNodes.length || 1);
                    const spacing = 50;
                    const viewW = W / zoom;
                    const cols = Math.max(3, Math.floor(viewW * 0.7 / spacing));
                    const rows = Math.ceil(domainNodes.length / cols);
                    const gridW = (cols - 1) * spacing;
                    const gridH = (rows - 1) * spacing;
                    s.focusTargetPositions.clear();
                    domainNodes.forEach((n, i) => {
                        const col = i % cols;
                        const row = Math.floor(i / cols);
                        s.focusTargetPositions.set(n.id, {
                            x: cloudCx - gridW / 2 + col * spacing,
                            y: cloudCy - gridH / 2 + row * spacing,
                        });
                    });
                    // Pan camera so cloud centroid appears at the visible viewport center
                    // (accounting for stats panel overlay on the left)
                    const statsEl = document.querySelector('.stats-panel:not(.stats-dragged)');
                    let visibleShiftPx = 0;
                    if (statsEl) {
                        const sr = statsEl.getBoundingClientRect();
                        const cr = canvas.getBoundingClientRect();
                        const overlapPx = Math.max(0, sr.right - cr.left);
                        if (overlapPx > 0 && overlapPx < W * 0.5) visibleShiftPx = overlapPx / 2;
                    }
                    s._cameraTarget = {
                        x: visibleShiftPx - cloudCx * zoom,
                        y: -cloudCy * zoom,
                        zoom: zoom,
                    };
                    s._focusCacheDomain = s.focusDomain;
                    s._focusCacheCount = visibleNodes.length;
                }
            }

            // Draw edges
            const nodeMap = {};
            for (const n of s.nodes) nodeMap[n.id] = n;
            for (const e of s.edges) {
                const src = nodeMap[e.source];
                const tgt = nodeMap[e.target];
                if (!src || !tgt) continue;
                if (!visibleIds.has(src.id) || !visibleIds.has(tgt.id)) continue;

                let alpha = 0.08;
                if (searchMatch) {
                    if (searchMatch.has(src.id) || searchMatch.has(tgt.id)) alpha = 0.3;
                    else alpha = 0.02;
                }

                // Pulse animation on recall
                const pulseSrc = s.pulseNodes.get(src.id);
                const pulseTgt = s.pulseNodes.get(tgt.id);
                if (pulseSrc && pulseSrc.type === 'recall' && (now - pulseSrc.start) < 2000) {
                    alpha = 0.8 * (1 - (now - pulseSrc.start) / 2000);
                }
                if (pulseTgt && pulseTgt.type === 'recall' && (now - pulseTgt.start) < 2000) {
                    alpha = Math.max(alpha, 0.8 * (1 - (now - pulseTgt.start) / 2000));
                }

                ctx.beginPath();
                ctx.moveTo(src.x, src.y);
                ctx.lineTo(tgt.x, tgt.y);
                ctx.strokeStyle = src.color;
                ctx.globalAlpha = alpha;
                ctx.lineWidth = 0.5;
                ctx.stroke();
                ctx.globalAlpha = 1;
            }

            // Draw nodes
            for (const n of visibleNodes) {
                let dim = false;
                if (searchMatch && !searchMatch.has(n.id)) dim = true;

                // Focus mode dimming
                const isFocused = s.focusDomain && n.domain === s.focusDomain;
                const focusT = s.focusTransition;
                if (focusT > 0 && !isFocused) {
                    dim = true;
                }

                const pulse = s.pulseNodes.get(n.id);
                let extraGlow = 0;
                let fadeOut = 1;

                if (pulse) {
                    // Use wall-clock elapsed, capped to avoid jumps when tab is backgrounded
                    const rawElapsed = now - pulse.start;
                    const elapsed = Math.min(rawElapsed, pulse.type === 'forget' ? 2200 : 3000);
                    if (elapsed > 3000) {
                        s.pulseNodes.delete(n.id);
                    } else if (pulse.type === 'remember') {
                        extraGlow = Math.max(0, 1 - elapsed / 1500) * 20;
                    } else if (pulse.type === 'recall') {
                        extraGlow = Math.max(0, 1 - elapsed / 2000) * 15;
                    } else if (pulse.type === 'forget') {
                        fadeOut = Math.max(0, 1 - elapsed / 2000);
                        if (rawElapsed > 2200) fadeOut = 0; // Snap to gone if tab was backgrounded
                    }
                }

                // Organic drift (reduced in focus mode)
                const driftAmt = focusT > 0 && isFocused ? 0.1 : 0.3;
                const drift = Math.sin(now / 2000 + n.x * 0.01) * driftAmt;

                // In focus mode, lerp focused nodes toward grid positions (Minority Report fly-in)
                let drawX = n.x;
                let drawY = n.y + drift;
                if (focusT > 0 && isFocused) {
                    const target = s.focusTargetPositions.get(n.id);
                    if (target) {
                        // Stagger: each node starts its animation slightly later based on grid position
                        const idx = [...s.focusTargetPositions.keys()].indexOf(n.id);
                        const stagger = Math.min(0.3, idx * 0.015);
                        const t = Math.max(0, Math.min(1, (focusT - stagger) / (1 - stagger)));
                        // Ease-out cubic for dramatic deceleration
                        const eased = 1 - Math.pow(1 - t, 3);
                        drawX = n.x + (target.x - n.x) * eased;
                        drawY = (n.y + drift) + (target.y - (n.y + drift)) * eased;
                    }
                }
                const r = n.radius;

                const dimAlpha = focusT > 0 ? 0.06 : 0.15;
                ctx.globalAlpha = dim ? dimAlpha * fadeOut : fadeOut;

                // Glow (optimized: solid arc instead of per-frame gradient)
                const glowSize = r + 8 + extraGlow + Math.sin(now / 1000 + n.x) * 2;
                ctx.fillStyle = n.color;
                const glowAlpha = (dim ? (focusT > 0 ? 0.02 : 0.05) : 0.15) * fadeOut;
                ctx.globalAlpha = glowAlpha;
                ctx.beginPath();
                ctx.arc(drawX, drawY, glowSize, 0, Math.PI * 2);
                ctx.fill();
                ctx.globalAlpha = dim ? dimAlpha * fadeOut : fadeOut;

                // Node body
                ctx.beginPath();
                ctx.arc(drawX, drawY, r, 0, Math.PI * 2);
                ctx.fillStyle = n.color;
                ctx.globalAlpha = (dim ? (focusT > 0 ? 0.08 : 0.2) : 0.85) * fadeOut;
                ctx.fill();

                // Focus mode: draw date label and tag pills below focused nodes
                if (focusT > 0.5 && isFocused) {
                    const ct = n.created_at || n.createdAt;
                    if (ct) {
                        const d = new Date(ct);
                        const label = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                        ctx.globalAlpha = focusT * 0.8;
                        ctx.font = '9px system-ui, sans-serif';
                        ctx.fillStyle = n.color;
                        ctx.textAlign = 'center';
                        ctx.fillText(label, drawX, drawY + r + 14);
                    }
                    // Tag indicator dots
                    if (n.tags && n.tags.length > 0) {
                        const tagY = drawY + r + 22;
                        const dotR = 3;
                        const dotSpacing = 8;
                        const startX = drawX - ((n.tags.length - 1) * dotSpacing) / 2;
                        for (let ti = 0; ti < Math.min(n.tags.length, 5); ti++) {
                            ctx.beginPath();
                            ctx.arc(startX + ti * dotSpacing, tagY, dotR, 0, Math.PI * 2);
                            ctx.fillStyle = '#8b5cf6';
                            ctx.globalAlpha = focusT * 0.9;
                            ctx.fill();
                        }
                        // Show first tag name if only one
                        if (n.tags.length <= 2) {
                            ctx.globalAlpha = focusT * 0.6;
                            ctx.font = '8px system-ui, sans-serif';
                            ctx.fillStyle = '#8b5cf6';
                            ctx.textAlign = 'center';
                            ctx.fillText(n.tags.join(', '), drawX, tagY + 11);
                        }
                    }
                }

                // Inner highlight
                ctx.beginPath();
                ctx.arc(drawX - r * 0.2, drawY - r * 0.2, r * 0.4, 0, Math.PI * 2);
                ctx.fillStyle = 'rgba(255,255,255,0.25)';
                ctx.globalAlpha = (dim ? (focusT > 0 ? 0.04 : 0.1) : 0.5) * fadeOut;
                ctx.fill();

                // Remember ring animation
                if (pulse && pulse.type === 'remember') {
                    const elapsed = now - pulse.start;
                    const ringR = r + (elapsed / 1500) * 40;
                    const ringAlpha = Math.max(0, 1 - elapsed / 1500);
                    ctx.beginPath();
                    ctx.arc(drawX, drawY, ringR, 0, Math.PI * 2);
                    ctx.strokeStyle = n.color;
                    ctx.globalAlpha = ringAlpha * 0.6;
                    ctx.lineWidth = 2;
                    ctx.stroke();
                }

                // Selection indicator (focus mode only)
                if (focusT > 0.3 && isFocused && selectedRef.current.has(n.id)) {
                    // Bright selection ring
                    ctx.beginPath();
                    ctx.arc(drawX, drawY, r + 5, 0, Math.PI * 2);
                    ctx.strokeStyle = '#ffffff';
                    ctx.globalAlpha = 0.9;
                    ctx.lineWidth = 2.5;
                    ctx.stroke();
                    // Checkmark
                    ctx.globalAlpha = 1;
                    ctx.fillStyle = '#10b981';
                    ctx.beginPath();
                    ctx.arc(drawX + r - 2, drawY - r + 2, 6, 0, Math.PI * 2);
                    ctx.fill();
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 1.5;
                    ctx.beginPath();
                    ctx.moveTo(drawX + r - 5, drawY - r + 2);
                    ctx.lineTo(drawX + r - 2, drawY - r + 5);
                    ctx.lineTo(drawX + r + 2, drawY - r - 1);
                    ctx.stroke();
                }

                ctx.globalAlpha = 1;
            }

            // Hover highlight — strong white outer glow
            if (s.mouse.hoveredNode && visibleIds.has(s.mouse.hoveredNode.id)) {
                const n = s.mouse.hoveredNode;
                // Compute drawn position (same as in node drawing)
                let hx = n.x, hy = n.y;
                const focusTH = s.focusTransition || 0;
                if (focusTH > 0 && s.focusDomain && n.domain === s.focusDomain) {
                    const target = s.focusTargetPositions.get(n.id);
                    if (target) {
                        hx = n.x + (target.x - n.x) * focusTH;
                        hy = n.y + (target.y - n.y) * focusTH;
                    }
                }
                // Outer glow ring (soft)
                ctx.beginPath();
                ctx.arc(hx, hy, n.radius + 14, 0, Math.PI * 2);
                ctx.fillStyle = '#ffffff';
                ctx.globalAlpha = 0.1;
                ctx.fill();
                // Mid glow ring
                ctx.beginPath();
                ctx.arc(hx, hy, n.radius + 8, 0, Math.PI * 2);
                ctx.fillStyle = '#ffffff';
                ctx.globalAlpha = 0.15;
                ctx.fill();
                // Inner bright ring
                ctx.beginPath();
                ctx.arc(hx, hy, n.radius + 4, 0, Math.PI * 2);
                ctx.strokeStyle = '#ffffff';
                ctx.globalAlpha = 0.8;
                ctx.lineWidth = 2.5;
                ctx.stroke();
                ctx.globalAlpha = 1;
            }

            ctx.restore();
        }

        animFrame = requestAnimationFrame(tick);
        return () => {
            cancelAnimationFrame(animFrame);
            window.removeEventListener('resize', resize);
        };
    }, [filterDomains, searchText, agentFilter, timelineFilter]);

    // Mouse interactions
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        function screenToWorld(sx, sy) {
            const s = stateRef.current;
            const W = canvas.width / devicePixelRatio;
            const H = canvas.height / devicePixelRatio;
            return {
                x: (sx - W / 2 - s.camera.x) / s.camera.zoom,
                y: (sy - H / 2 - s.camera.y) / s.camera.zoom,
            };
        }

        function findNode(wx, wy) {
            const s = stateRef.current;
            const focusT = s.focusTransition || 0;
            const visibleIds = s._visibleIds;
            // Only allow hovering/clicking nodes that are visible (respects agent tab, domain filter, search, timeline)
            for (let i = s.nodes.length - 1; i >= 0; i--) {
                const n = s.nodes[i];
                if (visibleIds && !visibleIds.has(n.id)) continue;
                // Skip non-focused nodes when in focus mode
                if (focusT > 0.3 && s.focusDomain && n.domain !== s.focusDomain) continue;
                // Use drawn position (accounting for focus mode lerp)
                let nx = n.x;
                let ny = n.y;
                if (focusT > 0 && s.focusDomain && n.domain === s.focusDomain) {
                    const target = s.focusTargetPositions.get(n.id);
                    if (target) {
                        nx = n.x + (target.x - n.x) * focusT;
                        ny = n.y + (target.y - n.y) * focusT;
                    }
                }
                const dx = nx - wx;
                const dy = ny - wy;
                if (dx * dx + dy * dy < (n.radius + 4) * (n.radius + 4)) return n;
            }
            return null;
        }

        function onMouseMove(e) {
            const rect = canvas.getBoundingClientRect();
            const sx = e.clientX - rect.left;
            const sy = e.clientY - rect.top;
            const s = stateRef.current;
            s.mouse.x = sx;
            s.mouse.y = sy;

            if (s.mouse.dragging) {
                s.camera.x += e.movementX;
                s.camera.y += e.movementY;
                return;
            }

            const w = screenToWorld(sx, sy);
            const node = findNode(w.x, w.y);
            s.mouse.hoveredNode = node;
            canvas.style.cursor = node ? 'pointer' : 'grab';

            if (node) {
                setTooltip({
                    x: e.clientX + 12,
                    y: e.clientY + 12,
                    node,
                });
            } else {
                setTooltip(null);
            }
        }

        function onMouseDown(e) {
            if (e.button === 0) {
                stateRef.current.mouse.dragging = true;
                stateRef.current.mouse.dragStart = { x: e.clientX, y: e.clientY };
                canvas.style.cursor = 'grabbing';
            }
        }

        // Double-click timer for focus mode (single-click = select, double-click = detail)
        let clickTimer = null;
        let lastClickNode = null;

        function onMouseUp(e) {
            const s = stateRef.current;
            const wasDragging = s.mouse.dragging;
            s.mouse.dragging = false;

            if (wasDragging && s.mouse.dragStart) {
                const dx = e.clientX - s.mouse.dragStart.x;
                const dy = e.clientY - s.mouse.dragStart.y;
                if (Math.abs(dx) < 4 && Math.abs(dy) < 4) {
                    if (s.mouse.hoveredNode) {
                        const clickedDomain = s.mouse.hoveredNode.domain;
                        if (s.focusDomain === clickedDomain) {
                            // In focus mode: single-click opens/refreshes detail panel
                            const node = s.mouse.hoveredNode;
                            onSelectMemory(node);
                        } else {
                            // First click on different domain: enter focus mode
                            s.focusDomain = clickedDomain;
                            s.focusTransition = 0;
                            s.focusTargetPositions.clear();
                            s._focusCacheDomain = null;
                            setFocusedDomain(clickedDomain);
                            setSelectedMemories(new Set());
                            selectedRef.current = new Set();
                        }
                    } else {
                        // Clicked empty space: exit focus mode and clear selection
                        if (s.focusDomain) {
                            s.focusDomain = null;
                            // Don't clear focusTargetPositions here — snap-back animation needs them
                            setFocusedDomain(null);
                            setSelectedMemories(new Set());
                            selectedRef.current = new Set();
                            setBulkAction(null);
                        }
                        onSelectMemory(null);
                    }
                }
            }
            canvas.style.cursor = s.mouse.hoveredNode ? 'pointer' : 'grab';
        }

        function onWheel(e) {
            e.preventDefault();
            const s = stateRef.current;
            const factor = e.deltaY > 0 ? 0.9 : 1.1;
            s.camera.zoom = Math.max(0.1, Math.min(5, s.camera.zoom * factor));
        }

        canvas.addEventListener('mousemove', onMouseMove);
        canvas.addEventListener('mousedown', onMouseDown);
        canvas.addEventListener('mouseup', onMouseUp);
        canvas.addEventListener('mouseleave', () => { setTooltip(null); stateRef.current.mouse.dragging = false; });
        canvas.addEventListener('wheel', onWheel, { passive: false });

        return () => {
            canvas.removeEventListener('mousemove', onMouseMove);
            canvas.removeEventListener('mousedown', onMouseDown);
            canvas.removeEventListener('mouseup', onMouseUp);
            canvas.removeEventListener('mouseleave', () => {});
            canvas.removeEventListener('wheel', onWheel);
        };
    }, [onSelectMemory]);

    function toggleDomain(d) {
        setFilterDomains(prev => {
            const next = new Set(prev);
            if (next.has(d)) next.delete(d);
            else next.add(d);
            return next;
        });
    }

    const handleDomainTransfer = async (targetAgentId) => {
        if (!domainTransfer) return;
        setDomainTransferring(true);
        try {
            const res = await transferDomain(domainTransfer.sourceAgentId, targetAgentId, domainTransfer.domain);
            if (res.error) { showToast(res.error, 'error'); setDomainTransferring(false); return; }
            showToast(res.message || `${res.memories_moved} memories transferred`, 'success');
            setDomainTransfer(null);
            loadData();
        } catch (e) { showToast('Transfer failed: ' + e.message, 'error'); }
        setDomainTransferring(false);
    };

    const handleBulkAction = async () => {
        if (selectedMemories.size === 0 || !bulkAction) return;
        const ids = [...selectedMemories];
        setBulkBusy(true);
        try {
            if (bulkAction === 'domain') {
                const domain = bulkInput.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
                if (!domain) { showToast('Enter a domain name', 'warning'); setBulkBusy(false); return; }
                const res = await bulkUpdateMemories(ids, { domain });
                if (res.error) { showToast(res.error, 'error'); } else {
                    showToast(`${res.updated} memories moved to ${domain}`, 'success');
                    setSelectedMemories(new Set()); selectedRef.current = new Set();
                    setBulkAction(null); setBulkInput(''); loadData();
                }
            } else if (bulkAction === 'tag') {
                const tag = bulkInput.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
                if (!tag) { showToast('Enter a tag name', 'warning'); setBulkBusy(false); return; }
                const res = await bulkUpdateMemories(ids, { addTags: [tag] });
                if (res.error) { showToast(res.error, 'error'); } else {
                    showToast(`Tag "${tag}" added to ${res.updated} memories`, 'success');
                    setSelectedMemories(new Set()); selectedRef.current = new Set();
                    setBulkAction(null); setBulkInput(''); loadData();
                }
            }
        } catch (e) { showToast('Bulk operation failed: ' + e.message, 'error'); }
        setBulkBusy(false);
    };

    const handleBulkReassign = async (targetAgentId) => {
        if (selectedMemories.size === 0) return;
        setBulkBusy(true);
        try {
            const ids = [...selectedMemories];
            const res = await bulkUpdateMemories(ids, { agent: targetAgentId });
            if (res.error) { showToast(res.error, 'error'); } else {
                showToast(`${res.updated} memories reassigned`, 'success');
                setSelectedMemories(new Set()); selectedRef.current = new Set();
                setBulkAction(null); loadData();
            }
        } catch (e) { showToast('Reassign failed: ' + e.message, 'error'); }
        setBulkBusy(false);
    };

    return html`
        ${agentList.length > 0 && html`
            <div class="agent-tab-bar">
                <button class="agent-tab ${agentFilter === '' ? 'active' : ''}"
                        onClick=${() => setAgentFilter('')}>
                    All
                </button>
                ${[...agentList].sort((a, b) => {
                    // Admins first, then registered members, then unregistered
                    const order = r => r === 'admin' ? 0 : r ? 1 : 2;
                    return order(a.role) - order(b.role);
                }).map(a => html`
                    <button class="agent-tab ${agentFilter === a.agent_id ? 'active' : ''} ${a.role === 'admin' ? 'admin' : ''} ${!a.role ? 'unregistered' : ''}"
                            onClick=${() => setAgentFilter(agentFilter === a.agent_id ? '' : a.agent_id)}
                            title=${a.role ? `${a.name} (${a.role}) — ${a.agent_id}` : `Unregistered agent — ${a.agent_id}`}>
                        <span class="agent-tab-avatar">${a.avatar || '🤖'}</span>
                        ${a.name}
                        ${a.role === 'admin' ? html`<span class="agent-role-badge admin">★</span>` : ''}
                        ${!a.role ? html`<span class="agent-role-badge unknown">?</span>` : ''}
                    </button>
                `)}
            </div>
        `}
        <div class="domain-bar ${domainsCollapsed ? 'collapsed' : ''}">
            <button class="domain-toggle" onClick=${() => setDomainsCollapsed(c => !c)}
                    title=${domainsCollapsed ? 'Show domain filters' : 'Hide domain filters'}>
                ${domainsCollapsed ? '▸' : '▾'} domains · ${domains.length}
            </button>
            <${HelpTip} text="Click a domain to filter the graph. Click again to show all." />
            ${(domainsCollapsed ? [...filterDomains] : domains).map(d => html`
                <button class="domain-pill ${filterDomains.has(d) ? 'active' : ''}"
                        style="color: ${getDomainColor(d)}; ${filterDomains.has(d) ? `background: ${getDomainColor(d)}20` : ''}"
                        onClick=${() => toggleDomain(d)}>
                    ${d}
                </button>
            `)}
            ${domainsCollapsed && filterDomains.size === 0 ? html`<span class="domain-hint">click to filter by domain</span>` : ''}
        </div>
        <div class="brain-container">
            <canvas ref=${canvasRef} class="brain-canvas"></canvas>
            ${!graphLoaded ? html`<div class="brain-loading">◍ synthesizing memory graph…</div>` : ''}
            ${graphLoaded && stats && (stats.total_memories || 0) === 0 ? html`
                <div class="brain-empty-overlay">
                    <${EmptyState} icon="brain"
                        headline="Your brain is empty"
                        hint="No memories have been committed yet. Import an existing export or connect an agent over MCP, and they'll appear here as a living graph."
                        actionLabel="Import memories"
                        onAction=${() => { window.location.hash = '#/import'; }} />
                </div>
            ` : ''}

            <div class="nav-pad">
                <button class="nav-btn nav-up" onClick=${() => { stateRef.current.camera.y += 60; }} title="Pan up">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M6 2L1 8h10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-left" onClick=${() => { stateRef.current.camera.x += 60; }} title="Pan left">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M2 6l6-5v10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-center" onClick=${() => { stateRef.current.camera.zoom = 0.6; stateRef.current.camera.x = 0; stateRef.current.camera.y = 0; }} title="Reset view">
                    <svg width="12" height="12" viewBox="0 0 12 12"><circle cx="6" cy="6" r="3" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-right" onClick=${() => { stateRef.current.camera.x -= 60; }} title="Pan right">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M10 6L4 1v10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-down" onClick=${() => { stateRef.current.camera.y -= 60; }} title="Pan down">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M6 10L1 4h10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-zin" onClick=${() => { stateRef.current.camera.zoom = Math.min(5, stateRef.current.camera.zoom * 1.3); }} title="Zoom in">
                    <svg width="14" height="14" viewBox="0 0 14 14"><line x1="7" y1="3" x2="7" y2="11" stroke="currentColor" stroke-width="2"/><line x1="3" y1="7" x2="11" y2="7" stroke="currentColor" stroke-width="2"/></svg>
                </button>
                <button class="nav-btn nav-zout" onClick=${() => { stateRef.current.camera.zoom = Math.max(0.1, stateRef.current.camera.zoom / 1.3); }} title="Zoom out">
                    <svg width="14" height="14" viewBox="0 0 14 14"><line x1="3" y1="7" x2="11" y2="7" stroke="currentColor" stroke-width="2"/></svg>
                </button>
            </div>

            <div class="graph-search ${searchOpen ? 'open' : ''} ${searchText ? 'has-filter' : ''}">
                <button class="graph-search-toggle" onClick=${() => { setSearchOpen(!searchOpen); if (searchOpen && !searchText) setSearchOpen(false); }}
                        title="Search & filter memories">
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="6.5" cy="6.5" r="4.5"/><line x1="10" y1="10" x2="14" y2="14"/>
                    </svg>
                    ${!searchOpen && searchText ? html`<span class="search-badge"></span>` : null}
                </button>
                ${searchOpen && html`
                    <input class="graph-search-input" type="text"
                           placeholder="Filter by content, domain, type, or agent..."
                           value=${searchText} onInput=${e => setSearchText(e.target.value)}
                           ref=${el => { if (el) el.focus(); }} />
                    ${searchText && html`
                        <button class="graph-search-clear" onClick=${() => { setSearchText(''); }}>×</button>
                    `}
                `}
            </div>

            ${focusedDomain && html`
                <div class="focus-indicator">
                    <span style="color: ${getDomainColor(focusedDomain)}">
                        ${focusedDomain}
                    </span>
                    ${selectedMemories.size > 0 ? html`
                        <span class="focus-selection-count">${selectedMemories.size} selected</span>
                        <button class="focus-action-btn" onClick=${() => setBulkAction('domain')}>Move Domain</button>
                        <button class="focus-action-btn" onClick=${() => setBulkAction('tag')}>Add Tag</button>
                        ${registeredAgentsRef.current.length > 1 ? html`
                            <button class="focus-action-btn" onClick=${() => setBulkAction('agent')}>Reassign</button>
                        ` : ''}
                        <button class="focus-action-btn deselect" onClick=${() => {
                            setSelectedMemories(new Set());
                            selectedRef.current = new Set();
                        }}>Deselect</button>
                    ` : html`
                        <button class="focus-action-btn select-all" onClick=${() => {
                            const s = stateRef.current;
                            const ids = new Set();
                            for (const n of s.nodes) {
                                if (n.domain === focusedDomain) ids.add(n.id);
                            }
                            setSelectedMemories(ids);
                            selectedRef.current = ids;
                        }}>Select All</button>
                    `}
                    <button class="focus-exit-btn" onClick=${() => {
                        stateRef.current.focusDomain = null;
                        // Don't clear focusTargetPositions — snap-back animation needs them
                        setFocusedDomain(null);
                        setSelectedMemories(new Set());
                        selectedRef.current = new Set();
                        setBulkAction(null);
                        onSelectMemory(null);
                    }}>Exit Focus</button>
                    <span class="focus-hint">${selectedMemories.size > 0 ? 'Choose an action above' : 'Click bubbles to select · Double-click for details'}</span>
                </div>
            `}

            ${stats && html`
                <div class="stats-panel ${statsPos ? 'stats-dragged' : ''}"
                     style=${statsPos ? `left:${statsPos.x}px;top:${statsPos.y}px;bottom:auto;` : ''}>
                    <h3 class="stats-drag-handle"
                        onMouseDown=${(e) => {
                            // Only drag from the h3 header bar
                            if (e.target.tagName === 'BUTTON') return;
                            const panel = e.currentTarget.parentElement;
                            const rect = panel.getBoundingClientRect();
                            const startX = e.clientX;
                            const startY = e.clientY;
                            const origLeft = rect.left;
                            const origTop = rect.top;
                            let lastPos = null;
                            function onMove(ev) {
                                lastPos = {
                                    x: origLeft + (ev.clientX - startX),
                                    y: origTop + (ev.clientY - startY),
                                };
                                setStatsPos(lastPos);
                            }
                            function onUp() {
                                document.removeEventListener('mousemove', onMove);
                                document.removeEventListener('mouseup', onUp);
                                if (lastPos) {
                                    try { localStorage.setItem('sage_stats_pos', JSON.stringify(lastPos)); } catch {}
                                }
                            }
                            document.addEventListener('mousemove', onMove);
                            document.addEventListener('mouseup', onUp);
                            e.preventDefault();
                            e.stopPropagation();
                        }}>Memory Stats
                        ${statsPos ? html`<button class="stats-reset-btn" onClick=${(e) => {
                            e.stopPropagation();
                            setStatsPos(null);
                            try { localStorage.removeItem('sage_stats_pos'); } catch {}
                        }} title="Reset position">Reset</button>` : ''}
                    </h3>
                    <div class="stat-row">
                        <span class="stat-label">Total</span>
                        <span class="stat-value">${stats.total_memories || 0}</span>
                    </div>
                    ${stats.by_domain && Object.entries(stats.by_domain)
                        .sort((a, b) => b[1] - a[1])
                        .slice(0, 25)
                        .map(([d, c]) => html`
                        <div class="stat-bar-container">
                            <span style="color: ${getDomainColor(d)}; font-size: 11px; min-width: 80px; text-transform: uppercase; letter-spacing: 0.5px;">${d}</span>
                            <div class="stat-bar">
                                <div class="stat-bar-fill" style="width: ${stats.total_memories ? (c / stats.total_memories * 100) : 0}%; background: ${getDomainColor(d)};"></div>
                            </div>
                            <span class="stat-bar-label">${c}</span>
                        </div>
                    `)}
                    ${stats.by_domain && Object.keys(stats.by_domain).length > 25 ? html`
                        <div style="font-size:11px;color:var(--text-muted);margin-top:6px;text-align:center;">Showing top 25 of ${Object.keys(stats.by_domain).length} domains</div>
                    ` : ''}
                    ${stats.last_activity && html`
                        <div class="stat-row" style="margin-top: 6px; border-top: 1px solid var(--border); padding-top: 8px;">
                            <span class="stat-label">Last activity</span>
                            <span class="stat-value" style="font-size: 12px;">${timeAgo(stats.last_activity)}</span>
                        </div>
                    `}
                </div>
            `}

            ${tooltip && html`
                <div class="tooltip" style="left: ${tooltip.x}px; top: ${tooltip.y}px;">
                    <div class="tooltip-domain" style="color: ${getDomainColor(tooltip.node.domain)}; background: ${getDomainColor(tooltip.node.domain)}20;">${tooltip.node.domain || 'unknown'}</div>
                    <div class="tooltip-content">${tooltip.node.content ? tooltip.node.content.slice(0, 200) : 'No content'}${tooltip.node.content && tooltip.node.content.length > 200 ? '...' : ''}</div>
                    <div class="tooltip-meta">
                        <span class="tooltip-meta-item">${tooltip.node.memory_type || tooltip.node.memoryType || 'memory'}</span>
                        <span class="tooltip-meta-sep"></span>
                        <span class="tooltip-meta-item" style="color: ${confidenceColor(tooltip.node.confidence)};">${(tooltip.node.confidence * 100).toFixed(0)}%</span>
                        <span class="tooltip-meta-sep"></span>
                        <span class="tooltip-meta-item">${timeAgo(tooltip.node.created_at || tooltip.node.createdAt)}</span>
                    </div>
                    ${tooltip.node.tags && tooltip.node.tags.length > 0 && html`
                        <div class="tooltip-tags">
                            ${tooltip.node.tags.map(t => html`<span class="tooltip-tag">${t}</span>`)}
                        </div>
                    `}
                    <div class="tooltip-hint">${focusedDomain ? 'Click to select · Double-click for details' : 'Click to focus domain · Double-click for details'}</div>
                </div>
            `}

            ${domainTransfer && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setDomainTransfer(null); }}>
                    <div class="wizard-modal" style="max-width:480px;">
                        <div class="wizard-header">
                            <h2>Transfer Domain Memories</h2>
                            <button class="detail-close" onClick=${() => setDomainTransfer(null)}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:16px;">
                                Transfer all <span style="color:${getDomainColor(domainTransfer.domain)};font-weight:600;">${domainTransfer.domain}</span> memories
                                from <strong>${domainTransfer.sourceAgentName}</strong> to:
                            </p>
                            <div style="display:flex;flex-direction:column;gap:8px;">
                                ${registeredAgentsRef.current.filter(a => a.status !== 'removed' && a.agent_id !== domainTransfer.sourceAgentId).map(a => html`
                                    <button class="merge-target-btn" onClick=${() => handleDomainTransfer(a.agent_id)} disabled=${domainTransferring}>
                                        <span>${a.avatar || '\u{1F916}'}</span>
                                        <span>${a.name}</span>
                                        <span class="agent-role-badge ${a.role}" style="margin-left:auto;">${a.role}</span>
                                    </button>
                                `)}
                            </div>
                            ${domainTransferring && html`<p style="color:var(--primary);font-size:12px;margin-top:12px;">Submitting to blockchain consensus...</p>`}
                        </div>
                    </div>
                </div>
            `}

            ${bulkAction && (bulkAction === 'domain' || bulkAction === 'tag') && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) { setBulkAction(null); setBulkInput(''); } }}>
                    <div class="wizard-modal" style="max-width:420px;">
                        <div class="wizard-header">
                            <h2>${bulkAction === 'domain' ? 'Move to Domain' : 'Add Tag'}</h2>
                            <button class="detail-close" onClick=${() => { setBulkAction(null); setBulkInput(''); }}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:12px;">
                                ${bulkAction === 'domain' ? 'Move' : 'Tag'} <strong>${selectedMemories.size}</strong> selected ${selectedMemories.size === 1 ? 'memory' : 'memories'}${bulkAction === 'domain' ? ' to:' : ' with:'}
                            </p>
                            <div style="display:flex;gap:8px;align-items:center;">
                                <input class="tag-input" type="text" style="flex:1;font-size:14px;padding:8px 12px;"
                                    placeholder=${bulkAction === 'domain' ? 'e.g. sage-architecture' : 'e.g. important'}
                                    value=${bulkInput} onInput=${e => setBulkInput(e.target.value)}
                                    onKeyDown=${e => { if (e.key === 'Enter') handleBulkAction(); }}
                                    ref=${el => { if (el) setTimeout(() => el.focus(), 50); }}
                                />
                                <button class="btn btn-primary" onClick=${handleBulkAction} disabled=${bulkBusy || !bulkInput.trim()}>
                                    ${bulkBusy ? '...' : bulkAction === 'domain' ? 'Move' : 'Add'}
                                </button>
                            </div>
                            ${bulkAction === 'domain' && domains.length > 0 && html`
                                <div style="margin-top:12px;display:flex;flex-wrap:wrap;gap:6px;">
                                    ${domains.filter(d => d !== focusedDomain).slice(0, 12).map(d => html`
                                        <button class="domain-pill" style="color:${getDomainColor(d)};font-size:11px;padding:3px 8px;cursor:pointer;"
                                            onClick=${() => { setBulkInput(d); }}>
                                            ${d}
                                        </button>
                                    `)}
                                </div>
                            `}
                        </div>
                    </div>
                </div>
            `}

            ${bulkAction === 'agent' && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setBulkAction(null); }}>
                    <div class="wizard-modal" style="max-width:480px;">
                        <div class="wizard-header">
                            <h2>Reassign Memories</h2>
                            <button class="detail-close" onClick=${() => setBulkAction(null)}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:16px;">
                                Reassign <strong>${selectedMemories.size}</strong> selected ${selectedMemories.size === 1 ? 'memory' : 'memories'} to:
                            </p>
                            <div style="display:flex;flex-direction:column;gap:8px;">
                                ${registeredAgentsRef.current.filter(a => a.status !== 'removed').map(a => html`
                                    <button class="merge-target-btn" onClick=${() => handleBulkReassign(a.agent_id)} disabled=${bulkBusy}>
                                        <span>${a.avatar || '\u{1F916}'}</span>
                                        <span>${a.name}</span>
                                        <span class="agent-role-badge ${a.role}" style="margin-left:auto;">${a.role}</span>
                                    </button>
                                `)}
                            </div>
                            ${bulkBusy && html`<p style="color:var(--primary);font-size:12px;margin-top:12px;">Processing...</p>`}
                        </div>
                    </div>
                </div>
            `}
        </div>
    `;
}

// Simple force simulation
function simulateForces(state, W, H) {
    const nodes = state.nodes;
    const edges = state.edges;
    if (nodes.length === 0) return;

    if (!state._forceFrame) state._forceFrame = 0;
    state._forceFrame++;

    const dt = 0.3;
    const repulsion = 800;
    const attraction = 0.005;
    const centerGravity = 0.002;
    const damping = 0.92;

    // Build node index (cache it)
    if (!state._nodeIdx || state._nodeIdxLen !== nodes.length) {
        state._nodeIdx = {};
        for (let i = 0; i < nodes.length; i++) state._nodeIdx[nodes[i].id] = i;
        state._nodeIdxLen = nodes.length;
    }
    const nodeIdx = state._nodeIdx;

    // Repulsion — grid-based spatial partitioning for O(n) avg case
    const cellSize = 300;
    const grid = new Map();
    for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i];
        const cx = Math.floor(n.x / cellSize);
        const cy = Math.floor(n.y / cellSize);
        const key = cx + ',' + cy;
        if (!grid.has(key)) grid.set(key, []);
        grid.get(key).push(i);
    }
    for (const [key, cell] of grid) {
        const [cx, cy] = key.split(',').map(Number);
        // Check own cell + 4 neighbors (right, below, below-left, below-right)
        const neighbors = [
            key,
            (cx+1)+','+cy,
            cx+','+(cy+1),
            (cx-1)+','+(cy+1),
            (cx+1)+','+(cy+1),
        ];
        for (const nk of neighbors) {
            const other = grid.get(nk);
            if (!other) continue;
            const isSelf = nk === key;
            for (let ci = 0; ci < cell.length; ci++) {
                const startJ = isSelf ? ci + 1 : 0;
                for (let cj = startJ; cj < other.length; cj++) {
                    const a = nodes[cell[ci]], b = nodes[other[cj]];
                    const dx = b.x - a.x;
                    const dy = b.y - a.y;
                    const distSq = dx * dx + dy * dy;
                    if (distSq > 90000) continue; // 300^2
                    const dist = Math.sqrt(distSq) || 1;
                    const force = repulsion / distSq;
                    const fx = (dx / dist) * force;
                    const fy = (dy / dist) * force;
                    a.vx -= fx * dt;
                    a.vy -= fy * dt;
                    b.vx += fx * dt;
                    b.vy += fy * dt;
                }
            }
        }
    }

    // Attraction along edges
    for (const e of edges) {
        const si = nodeIdx[e.source];
        const ti = nodeIdx[e.target];
        if (si === undefined || ti === undefined) continue;
        const a = nodes[si], b = nodes[ti];
        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const fx = dx * attraction;
        const fy = dy * attraction;
        a.vx += fx * dt;
        a.vy += fy * dt;
        b.vx -= fx * dt;
        b.vy -= fy * dt;
    }

    // Zoom-repulsion: when zoomed in, push nodes away from camera center (ball pit effect)
    // Disabled during focus mode — focused nodes need to stay in their timeline positions
    const cam = state.camera;
    if (cam && cam.zoom > 1.0 && !state.focusDomain) {
        // Camera center in world coords
        const viewCX = -cam.x / cam.zoom;
        const viewCY = -cam.y / cam.zoom;
        // Strength scales with zoom level — deeper zoom = stronger push
        const zoomForce = (cam.zoom - 1.0) * 150;
        // Only affect nodes within a radius that shrinks as you zoom in (visible area)
        const viewRadius = Math.max(W, H) / (cam.zoom * 1.5);
        for (const n of nodes) {
            const dx = n.x - viewCX;
            const dy = n.y - viewCY;
            const dist = Math.sqrt(dx * dx + dy * dy) || 1;
            if (dist < viewRadius) {
                // Stronger push for nodes closer to center
                const proximity = 1 - (dist / viewRadius);
                const push = zoomForce * proximity * proximity;
                n.vx += (dx / dist) * push * dt;
                n.vy += (dy / dist) * push * dt;
            }
        }
    }

    // Center gravity + damping + ambient drift + integration
    const time = state._forceFrame * 0.015;
    for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i];
        n.vx -= n.x * centerGravity;
        n.vy -= n.y * centerGravity;
        n.vx *= damping;
        n.vy *= damping;
        // Ambient drift — layered sine waves per node for organic floating motion
        const p = i * 2.399; // golden angle offset per node
        const drift = 0.15;
        n.vx += (Math.sin(p + time) + Math.sin(p * 0.7 + time * 1.3) * 0.5) * drift;
        n.vy += (Math.cos(p * 0.8 + time * 0.9) + Math.cos(p * 1.1 + time * 0.7) * 0.5) * drift;
        n.x += n.vx * dt;
        n.y += n.vy * dt;
    }

    // Reset force frame when camera changes (ball pit needs to recalculate)
    if (cam && (state._lastZoom !== cam.zoom || state._lastCamX !== cam.x || state._lastCamY !== cam.y)) {
        state._forceFrame = 0;
        state._lastZoom = cam.zoom;
        state._lastCamX = cam.x;
        state._lastCamY = cam.y;
    }
}

// ============================================================================
// ============================================================================
// Toast Notification System
// ============================================================================

const toastState = { listeners: [], toasts: [] };
let toastIdCounter = 0;

function showToast(message, type = 'info', duration = 4000) {
    const id = ++toastIdCounter;
    const toast = { id, message, type, removing: false };
    toastState.toasts = [...toastState.toasts, toast];
    toastState.listeners.forEach(fn => fn(toastState.toasts));
    if (duration > 0) {
        setTimeout(() => dismissToast(id), duration);
    }
    return id;
}

function dismissToast(id) {
    toastState.toasts = toastState.toasts.map(t =>
        t.id === id ? { ...t, removing: true } : t
    );
    toastState.listeners.forEach(fn => fn(toastState.toasts));
    setTimeout(() => {
        toastState.toasts = toastState.toasts.filter(t => t.id !== id);
        toastState.listeners.forEach(fn => fn(toastState.toasts));
    }, 250);
}

function ToastContainer() {
    const [toasts, setToasts] = useState([]);

    useEffect(() => {
        toastState.listeners.push(setToasts);
        return () => {
            toastState.listeners = toastState.listeners.filter(fn => fn !== setToasts);
        };
    }, []);

    if (toasts.length === 0) return null;

    const icons = { success: '\u2713', error: '\u2717', warning: '\u26A0', info: '\u2139' };

    return html`
        <div class="toast-container" role="status" aria-live="polite" aria-atomic="true">
            ${toasts.map(t => html`
                <div key=${t.id} class="toast ${t.type} ${t.removing ? 'removing' : ''}">
                    <span class="toast-icon">${icons[t.type] || icons.info}</span>
                    <span class="toast-content">${t.message}</span>
                    <button class="toast-close" onClick=${() => dismissToast(t.id)}>\u00D7</button>
                </div>
            `)}
        </div>
    `;
}

// Memory Detail Panel
// ============================================================================

function TagEditor({ memoryId }) {
    const [tags, setTags] = useState([]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (!memoryId) return;
        fetchMemoryTags(memoryId).then(data => {
            setTags(data.tags || []);
            setLoading(false);
        }).catch(() => setLoading(false));
    }, [memoryId]);

    async function addTag() {
        const tag = input.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
        if (!tag || tags.includes(tag)) { setInput(''); return; }
        const newTags = [...tags, tag];
        setTags(newTags);
        setInput('');
        await setMemoryTags(memoryId, newTags);
    }

    async function removeTag(tag) {
        const newTags = tags.filter(t => t !== tag);
        setTags(newTags);
        await setMemoryTags(memoryId, newTags);
    }

    function handleKeyDown(e) {
        if (e.key === 'Enter') { e.preventDefault(); addTag(); }
    }

    if (loading) return html`<span style="font-size:12px;color:var(--text-dim);">Loading tags...</span>`;

    return html`
        <div class="tag-editor">
            <div class="tag-chips">
                ${tags.map(t => html`
                    <span class="tag-chip">
                        ${t}
                        <span class="tag-chip-remove" onClick=${() => removeTag(t)}>x</span>
                    </span>
                `)}
            </div>
            <div class="tag-input-row">
                <input class="tag-input" type="text" placeholder="Add tag..."
                    value=${input} onInput=${e => setInput(e.target.value)}
                    onKeyDown=${handleKeyDown} />
                <button class="tag-add-btn" onClick=${addTag} disabled=${!input.trim()}>+</button>
            </div>
        </div>
    `;
}

function MemoryDetail({ memory, onClose, onDelete, onNavigate }) {
    const [confirming, setConfirming] = useState(false);
    const [agentInfo, setAgentInfo] = useState(null);
    const [visible, setVisible] = useState(false);
    const [lastMemory, setLastMemory] = useState(null);
    const [editingDomain, setEditingDomain] = useState(false);
    const [domainInput, setDomainInput] = useState('');
    const [domainOverride, setDomainOverride] = useState(null);
    const [savingDomain, setSavingDomain] = useState(false);
    const [domainList, setDomainList] = useState([]);

    // Existing domains for the change-domain picker.
    useEffect(() => {
        fetchStats().then(d => { if (d && d.by_domain) setDomainList(Object.keys(d.by_domain).sort()); }).catch(() => {});
    }, []);

    // Keep last memory data for closing animation
    useEffect(() => {
        setConfirming(false); // never carry an armed delete across to a different memory
        setEditingDomain(false);
        setDomainOverride(null); // drop any per-memory domain edit when a different memory opens
        if (memory) {
            setLastMemory(memory);
            // Double-rAF: first frame renders the element off-screen, second triggers transition
            requestAnimationFrame(() => {
                requestAnimationFrame(() => setVisible(true));
            });
        } else {
            setVisible(false);
        }
    }, [memory]);

    const displayMemory = memory || lastMemory;
    const agentId = displayMemory?.agent || displayMemory?.submitting_agent;
    useEffect(() => {
        if (!agentId) return;
        fetchAgents().then(data => {
            const agents = data.agents || [];
            const match = agents.find(a => a.agent_id === agentId);
            if (match) setAgentInfo(match);
        }).catch(() => {});
    }, [agentId]);

    // After closing animation completes, clear the last memory
    function handleTransitionEnd() {
        if (!visible && !memory) setLastMemory(null);
    }

    if (!displayMemory) return null;

    async function handleDelete() {
        if (!confirming) { setConfirming(true); return; }
        await deleteMemory(m.id);
        if (onDelete) onDelete(m.id);
        onClose();
    }

    async function saveDomain() {
        const nd = domainInput.trim();
        const current = domainOverride || m.domain;
        if (!nd || nd === current) { setEditingDomain(false); return; }
        setSavingDomain(true);
        try {
            const res = await updateMemory(m.id || m.memory_id, { domain: nd });
            if (res && res.error) { showToast(res.error, 'error'); }
            else { setDomainOverride(nd); setEditingDomain(false); showToast(`Domain changed to ${nd}`, 'success'); }
        } catch (e) { showToast('Failed to change domain: ' + (e.message || e), 'error'); }
        setSavingDomain(false);
    }

    // Use displayMemory for rendering (keeps last data during close animation)
    const m = displayMemory;
    const conf = m.confidence;
    const color = getDomainColor(m.domain);

    return html`
        <div class="detail-overlay ${visible ? 'open' : ''}" onTransitionEnd=${handleTransitionEnd}>
            <div class="detail-header">
                <h2>Memory Detail</h2>
                <button class="detail-close" onClick=${onClose}>×</button>
            </div>
            <div class="detail-body">
                <div class="detail-section">
                    <label>Content</label>
                    <div class="detail-content">${m.content || 'No content available'}</div>
                </div>

                <div class="detail-section">
                    <label>Confidence</label>
                    <div class="confidence-bar-container">
                        <div class="confidence-bar">
                            <div class="confidence-bar-fill" style="width: ${conf * 100}%; background: ${confidenceColor(conf)};"></div>
                        </div>
                        <span class="confidence-value" style="color: ${confidenceColor(conf)}">${(conf * 100).toFixed(0)}%</span>
                    </div>
                </div>

                <div class="detail-meta">
                    <div class="detail-meta-item">
                        <label>Domain</label>
                        ${editingDomain ? html`
                            <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
                                <input list="md-domain-list" class="filter-select" style="padding:4px 8px;font-size:12px;min-width:130px;"
                                    value=${domainInput} autofocus
                                    onInput=${e => setDomainInput(e.target.value)}
                                    onKeyDown=${e => { if (e.key === 'Enter') saveDomain(); if (e.key === 'Escape') setEditingDomain(false); }} />
                                <datalist id="md-domain-list">${domainList.map(d => html`<option value=${d}></option>`)}</datalist>
                                <button class="btn" style="padding:3px 9px;font-size:11px;" disabled=${savingDomain} onClick=${saveDomain}>${savingDomain ? 'Saving...' : 'Save'}</button>
                                <button class="btn btn-secondary" style="padding:3px 9px;font-size:11px;" disabled=${savingDomain} onClick=${() => setEditingDomain(false)}>Cancel</button>
                            </div>
                        ` : html`
                            <span style="display:inline-flex;align-items:center;gap:8px;">
                                <span class="domain-badge" style="background: ${color}20; color: ${color};">${domainOverride || m.domain}</span>
                                <button class="btn-link" style="background:none;border:none;color:var(--primary);cursor:pointer;font-size:11px;padding:0;" title="Move this memory to a different domain"
                                    onClick=${() => { setDomainInput(domainOverride || m.domain || ''); setEditingDomain(true); }}>Change</button>
                            </span>
                        `}
                    </div>
                    <div class="detail-meta-item">
                        <label>Type</label>
                        <span class="value">${m.memory_type || m.memoryType || 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Status</label>
                        <span class="value">${m.status}</span>
                    </div>
                    ${(m.corroboration_count != null) && html`
                        <div class="detail-meta-item">
                            <label>Corroborations</label>
                            <span class="value" title="How many agents have independently backed this memory toward consensus">◍ ${m.corroboration_count || 0}</span>
                        </div>
                    `}
                    <div class="detail-meta-item">
                        <label>Created</label>
                        <span class="value">${m.created_at ? timeAgo(m.created_at) : 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Agent</label>
                        ${agentInfo ? html`
                            <span class="value agent-detail-link" onClick=${() => {
                                if (onNavigate) onNavigate('network');
                                onClose();
                            }} title="View agent on Network page">
                                <span style="margin-right:4px;">${agentInfo.avatar || '🤖'}</span>
                                <span>${agentInfo.name}</span>
                                <span class="agent-role-badge" style="margin-left:6px;font-size:9px;padding:1px 5px;">${agentInfo.role}</span>
                                <span style="margin-left:4px;font-size:10px;color:var(--primary);">→</span>
                            </span>
                        ` : html`
                            <span class="value" style="font-size: 11px; word-break: break-all;">${m.agent || m.submitting_agent || 'unknown'}</span>
                        `}
                    </div>
                    <div class="detail-meta-item">
                        <label>Memory ID</label>
                        <span class="value" style="font-size: 10px; word-break: break-all;">${m.id || m.memory_id}</span>
                    </div>
                    ${m.content_hash && html`
                        <div class="detail-meta-item">
                            <label>Content Hash</label>
                            <span class="value" style="font-size: 10px; word-break: break-all; font-family: var(--font-mono, monospace);">${typeof m.content_hash === 'string' ? m.content_hash : btoa(String.fromCharCode(...new Uint8Array(m.content_hash)))}</span>
                        </div>
                    `}
                    ${m.committed_at && html`
                        <div class="detail-meta-item">
                            <label>Committed</label>
                            <span class="value">${timeAgo(m.committed_at)}</span>
                        </div>
                    `}
                    ${m.provider && html`
                        <div class="detail-meta-item">
                            <label>Provider</label>
                            <span class="value">${m.provider}</span>
                        </div>
                    `}
                </div>

                <div class="detail-section" style="margin-top: 16px;">
                    <label>Tags</label>
                    ${html`<${TagEditor} memoryId=${m.id || m.memory_id} />`}
                </div>

                <div class="detail-section" style="margin-top: 24px; padding-top: 16px; border-top: 1px solid var(--border);">
                    <button class="btn btn-danger" onClick=${handleDelete}>
                        ${confirming ? 'Confirm Delete' : 'Forget Memory'}
                    </button>
                    ${confirming && html`<span style="font-size: 12px; color: var(--danger); margin-left: 12px;">Click again to confirm</span>`}
                </div>
            </div>
        </div>
    `;
}

// ============================================================================
// Tasks / Kanban Page
// ============================================================================

const TASK_COLUMNS = [
    { key: 'planned', label: 'Planned', color: 'var(--text-muted)', icon: '○' },
    { key: 'in_progress', label: 'In Progress', color: 'var(--warning)', icon: '◉' },
    { key: 'done', label: 'Done', color: 'var(--accent)', icon: '✓' },
    { key: 'dropped', label: 'Dropped', color: 'var(--danger)', icon: '✕' },
];

function TasksPage({ sse }) {
    const [tasks, setTasks] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [domainFilter, setDomainFilter] = useState('');
    const [domains, setDomains] = useState([]);
    const [agentFilter, setAgentFilter] = useState('');
    const [dragging, setDragging] = useState(null);
    const [dragOver, setDragOver] = useState(null);
    const [showAddForm, setShowAddForm] = useState(false);
    const [newContent, setNewContent] = useState('');
    const [newDomain, setNewDomain] = useState('');
    const [adding, setAdding] = useState(false);
    const [showOldDone, setShowOldDone] = useState(false);
    const [agentList, setAgentList] = useState([]);
    const draggingRef = useRef(false);
    const reloadTimer = useRef(null);
    const agentName = (id) => { const a = agentList.find(x => x.agent_id === id); return a ? a.name : (id ? id.slice(0, 8) : ''); };

    useEffect(() => {
        loadTasks();
        fetchAgents().then(d => setAgentList(d.agents || [])).catch(() => {});
    }, []);

    async function doAssign(taskId, assignee) {
        setTasks(prev => prev.map(t => t.memory_id === taskId ? { ...t, assignee } : t)); // optimistic
        try {
            const res = await assignTask(taskId, assignee);
            if (res && res.error) { showToast('Could not assign: ' + res.error, 'error'); loadTasks(); }
            else showToast(assignee ? `Assigned to ${agentName(assignee)}` : 'Task unassigned', 'success');
        } catch (e) { showToast('Could not assign task', 'error'); loadTasks(); }
    }

    // Debounced reload that never fires mid-drag. Called by SSE events AND by
    // drag-end, so an update that arrived during a drag (incl. a cancelled drag
    // that produces no server event) is still picked up once the drag settles.
    const scheduleReload = () => {
        clearTimeout(reloadTimer.current);
        reloadTimer.current = setTimeout(() => { if (!draggingRef.current) loadTasks(); }, 500);
    };

    // Live refresh: agents create and move tasks through the same shared store, so
    // refresh on task/remember/forget events instead of leaving the board stale.
    useEffect(() => {
        if (!sse) return;
        const subs = [sse.on('task', scheduleReload), sse.on('remember', scheduleReload), sse.on('forget', scheduleReload)];
        return () => { clearTimeout(reloadTimer.current); subs.forEach(u => u && u()); };
    }, [sse]);

    async function loadTasks() {
        setLoading(true);
        setError(null);
        try {
            const data = await fetchTasks({ all: true, limit: 200 });
            const items = data.tasks || [];
            setTasks(items);
            const ds = [...new Set(items.map(t => t.domain_tag).filter(Boolean))].sort();
            setDomains(ds);
        } catch (e) { setTasks([]); setError(e && e.message ? e.message : 'Failed to load tasks'); }
        setLoading(false);
    }

    async function moveTask(taskId, newStatus) {
        // Optimistic update
        setTasks(prev => prev.map(t => t.memory_id === taskId ? { ...t, task_status: newStatus } : t));
        try {
            const res = await updateTaskStatus(taskId, newStatus);
            if (res && res.error) {
                showToast('Could not move task: ' + res.error, 'error');
                loadTasks(); // revert on error
            }
        } catch (e) {
            showToast('Could not move task: ' + (e.message || 'network error'), 'error');
            loadTasks(); // revert on error
        }
    }

    async function handleAddTask(e) {
        e.preventDefault();
        if (!newContent.trim()) return;
        setAdding(true);
        try {
            const res = await createTask(newContent.trim(), newDomain.trim() || 'general');
            if (res && res.error) {
                showToast('Could not add task: ' + res.error, 'error');
            } else {
                showToast('Task added to Planned', 'success');
                setNewContent('');
                setNewDomain('');
                setShowAddForm(false);
                loadTasks();
            }
        } catch (err) {
            showToast('Could not add task: ' + (err.message || 'network error'), 'error');
            // stay on form so user can retry
        }
        setAdding(false);
    }

    // Filter out done/dropped items older than 7 days unless showOldDone is on
    function isRecentDone(task) {
        if (task.task_status !== 'done' && task.task_status !== 'dropped') return true;
        if (showOldDone) return true;
        const created = new Date(task.created_at);
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        return created > sevenDaysAgo;
    }

    function handleDragStart(e, task) {
        draggingRef.current = true; // pause live-refresh while a drag is in flight
        setDragging(task.memory_id);
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', task.memory_id);
    }

    function handleDragOver(e, colKey) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        setDragOver(colKey);
    }

    function handleDragLeave() {
        setDragOver(null);
    }

    function handleDrop(e, colKey) {
        e.preventDefault();
        draggingRef.current = false;
        const taskId = e.dataTransfer.getData('text/plain');
        if (taskId && colKey) {
            moveTask(taskId, colKey);
        }
        setDragging(null);
        setDragOver(null);
        scheduleReload(); // pick up any refresh deferred during the drag
    }

    const taskAuthors = [...new Set(tasks.map(t => t.provider).filter(Boolean))].sort();
    const filtered = tasks
        .filter(t => !domainFilter || t.domain_tag === domainFilter)
        .filter(t => !agentFilter || (agentFilter === '__human__' ? !t.provider : t.provider === agentFilter))
        .filter(isRecentDone);
    const hiddenDoneCount = tasks.filter(t => (t.task_status === 'done' || t.task_status === 'dropped') && !isRecentDone(t)).length;

    return html`
        <div class="tasks-page">
            <div class="tasks-header">
                <div>
                    <h2 class="tasks-title">Task Board <${HelpTip} text="Each card is a governed memory tagged [TASK], flowing through consensus like any other memory. Drag a card between columns, or use the play / check / drop buttons to move it through Planned, In Progress, Done, and Dropped." /><${PageHelp} section="getting-started" label="How SAGE memories work" /></h2>
                    <div class="network-header-sub">Plan work as memories - drag cards across the board or use the card buttons to change status.</div>
                </div>
                <div class="tasks-filters">
                    <select class="filter-select" value=${domainFilter} onChange=${e => setDomainFilter(e.target.value)}>
                        <option value="">All domains</option>
                        ${domains.map(d => html`<option value=${d}>${d}</option>`)}
                    </select>
                    ${taskAuthors.length > 0 && html`
                        <select class="filter-select" value=${agentFilter} onChange=${e => setAgentFilter(e.target.value)} title="Filter by who created the task">
                            <option value="">All authors</option>
                            <option value="__human__">You (dashboard)</option>
                            ${taskAuthors.map(p => html`<option value=${p}>${p}</option>`)}
                        </select>
                    `}
                    <button class="btn" onClick=${() => setShowAddForm(!showAddForm)} title="Add task" style="font-weight:bold;">+ Add</button>
                    <button class="btn" onClick=${loadTasks} title="Refresh">↻</button>
                </div>
            </div>
            ${showAddForm && html`
                <form class="task-add-form" onSubmit=${handleAddTask}>
                    <input class="task-add-input" type="text" placeholder="What needs to be done?"
                        value=${newContent} onInput=${e => setNewContent(e.target.value)}
                        autoFocus disabled=${adding} />
                    <input class="task-add-domain" type="text" placeholder="domain (optional)"
                        value=${newDomain} onInput=${e => setNewDomain(e.target.value)}
                        disabled=${adding} list="domain-suggestions" />
                    <datalist id="domain-suggestions">
                        ${domains.map(d => html`<option value=${d} />`)}
                    </datalist>
                    <button class="btn task-add-submit" type="submit" disabled=${adding || !newContent.trim()}>
                        ${adding ? '...' : 'Add'}
                    </button>
                    <button class="btn" type="button" onClick=${() => setShowAddForm(false)}>Cancel</button>
                </form>
            `}
            ${hiddenDoneCount > 0 && !showOldDone && html`
                <div class="tasks-old-done-bar">
                    <span>${hiddenDoneCount} older completed task${hiddenDoneCount !== 1 ? 's' : ''} hidden</span>
                    <button class="btn" onClick=${() => setShowOldDone(true)}>Show all</button>
                </div>
            `}
            ${showOldDone && html`
                <div class="tasks-old-done-bar">
                    <span>Showing all tasks</span>
                    <button class="btn" onClick=${() => setShowOldDone(false)}>Hide old</button>
                </div>
            `}
            ${error && html`
                <div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.4);color:#ef4444;padding:12px 16px;border-radius:8px;margin-bottom:12px;display:flex;align-items:center;gap:12px;font-size:13px;">
                    <span style="flex:1;">Couldn't load tasks: ${error}</span>
                    <button class="btn" onClick=${loadTasks}>Retry</button>
                </div>
            `}
            ${filtered.length === 0 && !loading && !error ? html`
                <${EmptyState} icon="tasks"
                    headline=${tasks.length === 0 ? 'No tasks yet' : 'No tasks in this view'}
                    hint=${tasks.length === 0
                        ? 'Tasks are governed memories tagged [TASK]. Add one, then drag it across Planned, In progress, and Done as work moves.'
                        : 'No tasks match the current domain filter. Clear the filter or add a task to this domain.'}
                    actionLabel="Add a task"
                    onAction=${() => setShowAddForm(true)} />
            ` : html`
            <div class="kanban-board">
                ${TASK_COLUMNS.map(col => {
                    const colTasks = filtered.filter(t => t.task_status === col.key);
                    return html`
                        <div class="kanban-column ${dragOver === col.key ? 'drag-over' : ''}"
                             onDragOver=${e => handleDragOver(e, col.key)}
                             onDragLeave=${handleDragLeave}
                             onDrop=${e => handleDrop(e, col.key)}>
                            <div class="kanban-column-header">
                                <span class="kanban-column-icon" style="color:${col.color}">${col.icon}</span>
                                <span class="kanban-column-label">${col.label}</span>
                                <span class="kanban-column-count">${colTasks.length}</span>
                            </div>
                            <div class="kanban-cards">
                                ${colTasks.map(task => html`
                                    <div class="kanban-card ${dragging === task.memory_id ? 'dragging' : ''}"
                                         draggable="true"
                                         onDragStart=${e => handleDragStart(e, task)}
                                         onDragEnd=${() => { draggingRef.current = false; scheduleReload(); }}>
                                        <div class="kanban-card-content">${task.content.replace(/^\[TASK\]\s*/i, '')}</div>
                                        <div class="kanban-card-meta">
                                            <span class="domain-badge" style="background:${getDomainColor(task.domain_tag)}20;color:${getDomainColor(task.domain_tag)};font-size:10px;padding:2px 6px;">
                                                ${task.domain_tag}
                                            </span>
                                            ${task.provider
                                                ? html`<span title="Created by an agent" style="font-size:10px;color:var(--text-dim);">🤖 ${task.provider}</span>`
                                                : html`<span title="Created from the dashboard" style="font-size:10px;color:var(--primary);">You</span>`}
                                            <span style="font-size:11px;color:var(--text-muted);margin-left:auto;">${timeAgo(task.created_at)}</span>
                                        </div>
                                        ${agentList.length > 0 && html`
                                            <div class="kanban-card-assign" style="margin-top:6px;display:flex;align-items:center;gap:4px;">
                                                <span style="font-size:10px;color:var(--text-muted);">${task.assignee ? '👤' : 'Assign:'}</span>
                                                <select style="flex:1;font-size:10px;padding:2px 4px;background:var(--bg-secondary);color:var(--text);border:1px solid var(--border);border-radius:4px;"
                                                    value=${task.assignee || ''}
                                                    onClick=${e => e.stopPropagation()}
                                                    onChange=${e => { e.stopPropagation(); doAssign(task.memory_id, e.target.value); }}
                                                    title=${task.assignee ? 'Assigned to ' + agentName(task.assignee) : 'Assign this task to an agent'}>
                                                    <option value="">Unassigned</option>
                                                    ${agentList.map(a => html`<option value=${a.agent_id}>${a.name}</option>`)}
                                                </select>
                                            </div>
                                        `}
                                        ${col.key !== 'done' && col.key !== 'dropped' ? html`
                                            <div class="kanban-card-actions">
                                                ${col.key === 'planned' && html`
                                                    <button class="kanban-action" title="Start" onClick=${() => moveTask(task.memory_id, 'in_progress')}>▶</button>
                                                `}
                                                ${col.key === 'in_progress' && html`
                                                    <button class="kanban-action" title="Done" onClick=${() => moveTask(task.memory_id, 'done')}>✓</button>
                                                `}
                                                <button class="kanban-action kanban-action-drop" title="Drop" onClick=${() => moveTask(task.memory_id, 'dropped')}>✕</button>
                                            </div>
                                        ` : null}
                                    </div>
                                `)}
                                ${colTasks.length === 0 && html`
                                    <div class="kanban-empty">${loading ? 'Loading...' : 'No tasks'}</div>
                                `}
                            </div>
                        </div>
                    `;
                })}
            </div>`}
        </div>
    `;
}

// ============================================================================
// Search Page
// ============================================================================

function SearchPage({ onSelectMemory }) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [agentFilter, setAgentFilter] = useState('');
    const [agents, setAgents] = useState([]);
    const [domainFilter, setDomainFilter] = useState('');
    const [domains, setDomains] = useState([]);
    const [tagFilter, setTagFilter] = useState('');
    const [allTags, setAllTags] = useState([]);
    const [statusFilter, setStatusFilter] = useState('');
    const [selected, setSelected] = useState(() => new Set());
    const [bulkDomain, setBulkDomain] = useState('');
    const [bulkTag, setBulkTag] = useState('');
    const [bulkBusy, setBulkBusy] = useState(false);
    const searchTimer = useRef(null);

    const toggleSelect = (id, e) => {
        if (e) e.stopPropagation();
        setSelected(prev => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id); else next.add(id);
            return next;
        });
    };
    const clearSelection = () => setSelected(new Set());

    async function runBulk(fn, okMsg) {
        const ids = Array.from(selected);
        if (!ids.length) return;
        setBulkBusy(true);
        try {
            await fn(ids);
            showToast(okMsg.replace('%n', ids.length), 'success');
            clearSelection();
            setBulkDomain(''); setBulkTag('');
            loadMemories(query, agentFilter, domainFilter, tagFilter);
        } catch (e) { showToast('Bulk action failed: ' + (e.message || e), 'error'); }
        setBulkBusy(false);
    }
    const bulkMoveDomain = () => { const d = bulkDomain.trim(); if (!d) return; runBulk(ids => bulkUpdateMemories(ids, { domain: d }), `Moved %n memories to ${d}`); };
    const bulkAddTag = () => { const t = bulkTag.trim(); if (!t) return; runBulk(ids => bulkUpdateMemories(ids, { addTags: [t] }), `Tagged %n memories "${t}"`); };
    const bulkDeprecate = () => { if (!window.confirm(`Deprecate ${selected.size} selected memories? They stay on-chain but are marked deprecated.`)) return; runBulk(ids => Promise.all(ids.map(id => deleteMemory(id))), 'Deprecated %n memories'); };

    useEffect(() => {
        loadMemories();
        fetchAgents().then(data => setAgents(data.agents || [])).catch(() => {});
        fetchStats().then(data => { if (data.by_domain) setDomains(Object.keys(data.by_domain).sort()); }).catch(() => {});
        fetchTags().then(data => setAllTags(data.tags || [])).catch(() => {});
        return () => clearTimeout(searchTimer.current); // drop any pending debounced search on unmount
    }, []);

    async function loadMemories(search, agent, domain, tag, status = statusFilter) {
        setLoading(true);
        setError(null);
        try {
            const params = { limit: search ? 200 : 100, sort: 'newest' };
            if (agent) params.agent = agent;
            if (domain) params.domain = domain;
            if (tag) params.tag = tag;
            if (status) params.status = status;
            if (search) params.q = search; // real server-side FTS/keyword search over the whole base
            const data = await fetchMemories(params);
            const memories = data.memories || [];
            setResults(memories);
            setTotal(data.total || memories.length);
        } catch (err) {
            setResults([]);
            setError(err && err.message ? err.message : 'Failed to load memories');
        }
        setLoading(false);
    }

    function handleSearch(e) {
        const v = e.target.value;
        setQuery(v);
        // Debounce: search now hits the server (FTS/keyword), so don't fire per keystroke.
        clearTimeout(searchTimer.current);
        searchTimer.current = setTimeout(() => loadMemories(v, agentFilter, domainFilter, tagFilter), 250);
    }

    function handleAgentFilter(e) {
        const v = e.target.value;
        setAgentFilter(v);
        loadMemories(query, v, domainFilter, tagFilter);
    }

    function handleDomainFilter(e) {
        const v = e.target.value;
        setDomainFilter(v);
        loadMemories(query, agentFilter, v, tagFilter);
    }

    function handleTagFilter(e) {
        const v = e.target.value;
        setTagFilter(v);
        loadMemories(query, agentFilter, domainFilter, v);
    }

    function handleStatusFilter(e) {
        const v = e.target.value;
        setStatusFilter(v);
        loadMemories(query, agentFilter, domainFilter, tagFilter, v);
    }

    return html`
        <div class="search-page">
            <input class="search-page-input" type="text" placeholder="Search memories..."
                   value=${query} onInput=${handleSearch} />
            <div class="search-filters">
                <${HelpTip} text="Full-text search across your whole memory base by content or domain - relevance-ranked when the full-text index is available, keyword-matched otherwise. Domain, tag, and agent filters narrow the set." />
                <${PageHelp} section="search" label="Search & Import guide" />
                <select class="filter-select" value=${domainFilter} onChange=${handleDomainFilter}>
                    <option value="">All domains</option>
                    ${domains.map(d => html`<option value=${d}>${d}</option>`)}
                </select>
                <select class="filter-select" value=${statusFilter} onChange=${handleStatusFilter} title="Filter by memory lifecycle status">
                    <option value="">All statuses</option>
                    <option value="committed">Committed</option>
                    <option value="proposed">Proposed</option>
                    <option value="deprecated">Deprecated</option>
                </select>
                ${allTags.length > 0 && html`
                    <select class="filter-select" value=${tagFilter} onChange=${handleTagFilter}>
                        <option value="">All tags</option>
                        ${allTags.map(t => html`<option value=${t.tag}>${t.tag} (${t.count})</option>`)}
                    </select>
                `}
                ${agents.length > 0 && html`
                    <select class="filter-select" value=${agentFilter} onChange=${handleAgentFilter}>
                        <option value="">All agents</option>
                        ${agents.map(a => html`<option value=${a.agent_id}>${a.name} (${a.agent_id.slice(0, 8)}...)</option>`)}
                    </select>
                `}
                <span style="font-size: 12px; color: var(--text-muted); align-self: center;">${loading ? 'Searching...' : `${results.length} ${results.length === 1 ? 'memory' : 'memories'}`}</span>
            </div>
            <div class="memory-list">
                ${error && html`
                    <div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.4);color:#ef4444;padding:12px 16px;border-radius:8px;margin-bottom:12px;display:flex;align-items:center;gap:12px;font-size:13px;">
                        <span style="flex:1;">Couldn't load memories: ${error}</span>
                        <button class="btn" onClick=${() => loadMemories(query, agentFilter, domainFilter, tagFilter)}>Retry</button>
                    </div>
                `}
                ${selected.size > 0 && html`
                    <div style="position:sticky;top:0;z-index:5;display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:var(--bg-elevated);border:1px solid var(--primary);border-radius:8px;padding:10px 12px;margin-bottom:12px;">
                        <span style="font-weight:700;color:var(--primary);white-space:nowrap;">${selected.size} selected</span>
                        <input list="bulk-domain-list" class="filter-select" style="min-width:130px;" placeholder="Move to domain..." value=${bulkDomain} onInput=${e => setBulkDomain(e.target.value)} onKeyDown=${e => { if (e.key === 'Enter') bulkMoveDomain(); }} />
                        <datalist id="bulk-domain-list">${domains.map(d => html`<option value=${d}></option>`)}</datalist>
                        <button class="btn" disabled=${bulkBusy || !bulkDomain.trim()} onClick=${bulkMoveDomain}>Move</button>
                        <input class="filter-select" style="min-width:110px;" placeholder="Add tag..." value=${bulkTag} onInput=${e => setBulkTag(e.target.value)} onKeyDown=${e => { if (e.key === 'Enter') bulkAddTag(); }} />
                        <button class="btn" disabled=${bulkBusy || !bulkTag.trim()} onClick=${bulkAddTag}>Tag</button>
                        <button class="btn btn-danger" disabled=${bulkBusy} onClick=${bulkDeprecate}>Deprecate</button>
                        <button class="btn btn-secondary" style="margin-left:auto;" onClick=${clearSelection}>Clear</button>
                    </div>
                `}
                ${results.map(m => { const mid = m.memory_id; const isSel = selected.has(mid); return html`
                    <div class="memory-card" style=${isSel ? 'outline:2px solid var(--primary);outline-offset:-1px;' : ''} onClick=${() => onSelectMemory({
                        id: m.memory_id,
                        content: m.content,
                        domain: m.domain_tag,
                        confidence: m.confidence_score,
                        status: m.status,
                        memory_type: m.memory_type,
                        created_at: m.created_at,
                        agent: m.submitting_agent,
                        corroboration_count: m.corroboration_count,
                        content_hash: m.content_hash,
                        committed_at: m.committed_at,
                        provider: m.provider,
                    })}>
                        <div class="memory-card-header">
                            <input type="checkbox" style="cursor:pointer;width:15px;height:15px;flex:none;" checked=${isSel} onClick=${e => toggleSelect(mid, e)} title="Select for bulk actions" />
                            <span class="domain-badge" style="background: ${getDomainColor(m.domain_tag)}20; color: ${getDomainColor(m.domain_tag)};">
                                ${m.domain_tag}
                            </span>
                            <span style="font-size: 12px; font-weight: 600; color: ${confidenceColor(m.confidence_score)};margin-left:auto;">
                                ${(m.confidence_score * 100).toFixed(0)}%
                            </span>
                        </div>
                        <div class="memory-card-content">${m.content || 'No content'}</div>
                        <div class="memory-card-footer">
                            <span>${m.memory_type} | ${m.status}</span>
                            <span>${m.created_at ? timeAgo(m.created_at) : ''}</span>
                        </div>
                    </div>
                `; })}
                ${results.length === 0 && !loading && !error && html`
                    ${(query || agentFilter || domainFilter || tagFilter)
                        ? html`<${EmptyState} icon="search"
                            headline="No memories match"
                            hint="Full-text search across your whole memory base by content or domain. Try a shorter or different term, or clear the filters."
                            actionLabel="Clear search"
                            onAction=${() => { setQuery(''); setAgentFilter(''); setDomainFilter(''); setTagFilter(''); setStatusFilter(''); clearSelection(); loadMemories('', '', '', '', ''); }} />`
                        : html`<${EmptyState} icon="brain"
                            headline="No memories yet"
                            hint="Your brain is empty. Import an existing memory export or connect an agent over MCP, and committed memories will show up here to search."
                            actionLabel="Import memories"
                            onAction=${() => { window.location.hash = '#/import'; }} />`}
                `}
            </div>
        </div>
    `;
}

// ============================================================================
// Synaptic Ledger (Encryption) Component
// ============================================================================

function SynapticLedger() {
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(true);
    const [view, setView] = useState('status'); // status | enable | change | disable
    const [passphrase, setPassphrase] = useState('');
    const [confirmPassphrase, setConfirmPassphrase] = useState('');
    const [oldPassphrase, setOldPassphrase] = useState('');
    const [newPassphrase, setNewPassphrase] = useState('');
    const [error, setError] = useState(null);
    const [busy, setBusy] = useState(false);
    const [recoveryKey, setRecoveryKey] = useState(null);

    const loadStatus = useCallback(async () => {
        try {
            const data = await fetchLedgerStatus();
            setStatus(data);
        } catch (e) {
            setStatus({ enabled: false });
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { loadStatus(); }, []);

    const downloadRecoveryKey = (key) => {
        const date = new Date().toISOString().slice(0, 10);
        const filename = `synaptic-ledger-recovery-${date}.txt`;
        const content = [
            '╔══════════════════════════════════════════════════════════════╗',
            '║              SYNAPTIC LEDGER — RECOVERY KEY                ║',
            '╚══════════════════════════════════════════════════════════════╝',
            '',
            'This is your Synaptic Ledger recovery key. If you forget your',
            'passphrase, this key can restore access to your encrypted',
            'memories. Without it, encrypted data is UNRECOVERABLE.',
            '',
            '  KEEP THIS FILE SAFE. STORE IT OFFLINE.',
            '  DO NOT SHARE IT. DO NOT COMMIT IT TO GIT.',
            '',
            '────────────────────────────────────────────────────────',
            '',
            key,
            '',
            '────────────────────────────────────────────────────────',
            '',
            `Generated: ${new Date().toISOString()}`,
            'Algorithm: AES-256-GCM',
            'KDF: Argon2id',
            'Application: (S)AGE — Sovereign Agent Governed Experience',
            '',
        ].join('\n');
        const blob = new Blob([content], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
    };

    const handleEnable = async () => {
        if (passphrase.length < 8) { setError('Passphrase must be at least 8 characters'); return; }
        if (passphrase !== confirmPassphrase) { setError('Passphrases do not match'); return; }
        setError(null);
        setBusy(true);
        try {
            const res = await enableLedger(passphrase);
            if (res.error) { setError(res.error); setBusy(false); return; }
            setRecoveryKey(res.recovery_key);
            setPassphrase('');
            setConfirmPassphrase('');
            loadStatus();
        } catch (e) { setError(e.message); }
        setBusy(false);
    };

    const handleChangePassphrase = async () => {
        if (newPassphrase.length < 8) { setError('New passphrase must be at least 8 characters'); return; }
        setError(null);
        setBusy(true);
        try {
            const res = await changeLedgerPassphrase(oldPassphrase, newPassphrase);
            if (res.error) { setError(res.error); setBusy(false); return; }
            setRecoveryKey(res.recovery_key);
            setOldPassphrase('');
            setNewPassphrase('');
        } catch (e) { setError(e.message); }
        setBusy(false);
    };

    const handleDisable = async () => {
        setError(null);
        setBusy(true);
        try {
            const res = await disableLedger(passphrase);
            if (res.error) { setError(res.error); setBusy(false); return; }
            setPassphrase('');
            setView('status');
            loadStatus();
        } catch (e) { setError(e.message); }
        setBusy(false);
    };

    if (loading) return html`<div style="color:var(--text-muted);">Loading...</div>`;

    // Recovery key display (shown after enable or passphrase change)
    if (recoveryKey) {
        return html`
            <div>
                <h3 style="color:var(--accent);margin-bottom:12px;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                    </svg>
                    Recovery Key Generated
                </h3>
                <div class="warning-banner" style="margin-bottom:16px;">
                    ⚠ Save this recovery key NOW. It will not be shown again. If you lose your passphrase and this key, your encrypted memories are unrecoverable.
                </div>
                <div style="background:var(--bg-deep);border:1px solid var(--border-light);border-radius:var(--radius);padding:12px;font-family:monospace;font-size:11px;word-break:break-all;color:var(--text-dim);margin-bottom:16px;max-height:100px;overflow-y:auto;">
                    ${recoveryKey}
                </div>
                <div style="display:flex;gap:8px;">
                    <button class="btn btn-primary" onClick=${() => downloadRecoveryKey(recoveryKey)}>
                        Download Recovery Key
                    </button>
                    <button class="btn" onClick=${() => {
                        navigator.clipboard.writeText(recoveryKey);
                    }}>Copy to Clipboard</button>
                    <button class="btn" onClick=${() => {
                        setRecoveryKey(null);
                        // After first-time encryption enable, send to lock screen
                        if (window.__sageLock) {
                            window.__sageLock();
                        } else {
                            setView('status');
                        }
                    }}>
                        I've saved it — Lock Now
                    </button>
                </div>
            </div>
        `;
    }

    const enabled = status?.enabled;

    // Enable form
    if (view === 'enable') {
        return html`
            <div>
                <h3>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                    </svg>
                    Enable Synaptic Ledger
                </h3>
                <p style="font-size:13px;color:var(--text-dim);margin:12px 0;line-height:1.6;">
                    All new memories will be encrypted at rest with AES-256-GCM. You'll need this passphrase to unlock your brain on startup. A recovery key will be generated — save it somewhere safe.
                </p>
                ${error && html`<div class="import-error" style="margin-bottom:12px;">${error}</div>`}
                <div class="wizard-field">
                    <label>Passphrase</label>
                    <input class="wizard-input" type="password" placeholder="Minimum 8 characters"
                        value=${passphrase} onInput=${e => setPassphrase(e.target.value)} />
                </div>
                <div class="wizard-field">
                    <label>Confirm Passphrase</label>
                    <input class="wizard-input" type="password" placeholder="Type it again"
                        value=${confirmPassphrase} onInput=${e => setConfirmPassphrase(e.target.value)}
                        onKeyDown=${e => { if (e.key === 'Enter') handleEnable(); }} />
                </div>
                <div style="display:flex;gap:8px;margin-top:4px;">
                    <button class="btn btn-primary" onClick=${handleEnable} disabled=${busy || !passphrase}>
                        ${busy ? 'Encrypting...' : 'Enable Encryption'}
                    </button>
                    <button class="btn" onClick=${() => { setView('status'); setError(null); }}>Cancel</button>
                </div>
            </div>
        `;
    }

    // Change passphrase form
    if (view === 'change') {
        return html`
            <div>
                <h3>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                    </svg>
                    Change Passphrase
                </h3>
                <p style="font-size:13px;color:var(--text-dim);margin:12px 0;line-height:1.6;">
                    Your existing memories stay readable — the underlying encryption key doesn't change, only the passphrase that protects it. A new recovery key will be generated.
                </p>
                ${error && html`<div class="import-error" style="margin-bottom:12px;">${error}</div>`}
                <div class="wizard-field">
                    <label>Current Passphrase</label>
                    <input class="wizard-input" type="password" placeholder="Your current passphrase"
                        value=${oldPassphrase} onInput=${e => setOldPassphrase(e.target.value)} />
                </div>
                <div class="wizard-field">
                    <label>New Passphrase</label>
                    <input class="wizard-input" type="password" placeholder="Minimum 8 characters"
                        value=${newPassphrase} onInput=${e => setNewPassphrase(e.target.value)}
                        onKeyDown=${e => { if (e.key === 'Enter') handleChangePassphrase(); }} />
                </div>
                <div style="display:flex;gap:8px;margin-top:4px;">
                    <button class="btn btn-primary" onClick=${handleChangePassphrase} disabled=${busy || !oldPassphrase || !newPassphrase}>
                        ${busy ? 'Changing...' : 'Change Passphrase'}
                    </button>
                    <button class="btn" onClick=${() => { setView('status'); setError(null); }}>Cancel</button>
                </div>
            </div>
        `;
    }

    // Disable confirmation
    if (view === 'disable') {
        return html`
            <div>
                <h3 style="color:var(--danger);">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                    </svg>
                    Disable Synaptic Ledger
                </h3>
                <p style="font-size:13px;color:var(--text-dim);margin:12px 0;line-height:1.6;">
                    New memories will no longer be encrypted. Existing encrypted memories remain protected and readable. Enter your passphrase to confirm.
                </p>
                ${error && html`<div class="import-error" style="margin-bottom:12px;">${error}</div>`}
                <div class="wizard-field">
                    <label>Passphrase</label>
                    <input class="wizard-input" type="password" placeholder="Confirm your passphrase"
                        value=${passphrase} onInput=${e => setPassphrase(e.target.value)}
                        onKeyDown=${e => { if (e.key === 'Enter') handleDisable(); }} />
                </div>
                <div style="display:flex;gap:8px;margin-top:4px;">
                    <button class="btn btn-danger" onClick=${handleDisable} disabled=${busy || !passphrase}>
                        ${busy ? 'Disabling...' : 'Disable Encryption'}
                    </button>
                    <button class="btn" onClick=${() => { setView('status'); setError(null); }}>Cancel</button>
                </div>
            </div>
        `;
    }

    // Status view (default)
    return html`
        <div>
            <h3>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                    <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                </svg>
                Synaptic Ledger <${HelpTip} text="Encrypts all memories at rest with AES-256-GCM. You'll need to enter your passphrase each session. If you lose it, use your recovery key to reset it." />
            </h3>
            ${enabled ? html`
                <div style="margin:12px 0;">
                    <div class="settings-row">
                        <span class="label">Status</span>
                        <span class="value" style="color:var(--accent);">Encrypted</span>
                    </div>
                    <div class="settings-row">
                        <span class="label">Algorithm</span>
                        <span class="value">${status.algorithm}</span>
                    </div>
                    <div class="settings-row">
                        <span class="label">Key Derivation</span>
                        <span class="value">${status.kdf}</span>
                    </div>
                    <div class="settings-row">
                        <span class="label">Vault</span>
                        <span class="value" style="font-family:monospace;font-size:12px;">${status.vault_path}</span>
                    </div>
                </div>
                <div style="display:flex;gap:8px;">
                    <button class="btn" onClick=${() => setView('change')}>Change Passphrase</button>
                    <button class="btn btn-danger" onClick=${() => setView('disable')}>Disable</button>
                </div>
            ` : html`
                <div style="margin:12px 0;">
                    <div class="settings-row">
                        <span class="label">Status</span>
                        <span class="value" style="color:var(--text-muted);">Not encrypted</span>
                    </div>
                    <p style="font-size:12px;color:var(--text-muted);margin:8px 0;line-height:1.5;">
                        Enable the Synaptic Ledger to encrypt all memories at rest with AES-256-GCM. If your device is lost or compromised, your memories stay private.
                    </p>
                </div>
                <button class="btn btn-primary" onClick=${() => setView('enable')}>Enable Encryption</button>
            `}
        </div>
    `;
}

// ============================================================================
// Software Update Component
// ============================================================================

function SoftwareUpdate() {
    const [updateInfo, setUpdateInfo] = useState(null);
    const [checking, setChecking] = useState(false);
    const [updating, setUpdating] = useState(false);
    const [error, setError] = useState(null);
    const [installed, setInstalled] = useState(false);
    const [restarting, setRestarting] = useState(false);

    // Step-by-step progress from SSE
    const [steps, setSteps] = useState([]);
    const [currentStep, setCurrentStep] = useState(null);

    const doCheck = async () => {
        setChecking(true);
        setError(null);
        try {
            const data = await checkForUpdate();
            if (data.error) setError(data.error);
            setUpdateInfo(data);
        } catch (e) {
            setError('Failed to check for updates');
        }
        setChecking(false);
    };

    useEffect(() => { doCheck(); }, []);

    // Listen for SSE update events when an update is in progress
    useEffect(() => {
        if (!updating) return;
        const es = new EventSource('/v1/dashboard/events');
        const handler = (e) => {
            try {
                const outer = JSON.parse(e.data);
                const data = outer.data || outer;
                const { step, status, message } = data;

                if (step === 'complete' && status === 'done') {
                    setInstalled(true);
                    setUpdating(false);
                    setCurrentStep(null);
                    return;
                }

                setCurrentStep({ step, status, message });

                if (status === 'error') {
                    setError(message);
                    setUpdating(false);
                    return;
                }

                // Build step list for visual progress
                setSteps(prev => {
                    const existing = prev.findIndex(s => s.step === step);
                    if (existing >= 0) {
                        const updated = [...prev];
                        updated[existing] = { step, status, message };
                        return updated;
                    }
                    return [...prev, { step, status, message }];
                });
            } catch (err) { /* ignore parse errors */ }
        };
        es.addEventListener('update', handler);
        return () => { es.removeEventListener('update', handler); es.close(); };
    }, [updating]);

    const doUpdate = async () => {
        if (!updateInfo?.download_url) {
            setError('No download URL available for your platform');
            return;
        }
        setUpdating(true);
        setError(null);
        setSteps([]);
        setCurrentStep(null);
        try {
            const res = await applyUpdate(updateInfo.download_url);
            if (!res.ok) {
                setError(res.error || 'Update failed to start');
                setUpdating(false);
            }
            // If ok, progress comes via SSE events
        } catch (e) {
            setError('Update failed: ' + e.message);
            setUpdating(false);
        }
    };

    const doRestart = async () => {
        setRestarting(true);
        try { await restartServer(); } catch (e) { /* expected */ }
        // Server will restart — poll until back
        setTimeout(() => {
            const poll = setInterval(() => {
                fetch('/health').then(r => {
                    if (r.ok) { clearInterval(poll); window.location.reload(); }
                }).catch(() => {});
            }, 1000);
        }, 2000);
    };

    const formatSize = (bytes) => {
        if (!bytes) return '';
        if (bytes < 1048576) return (bytes / 1024).toFixed(0) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
    };

    const stepIcon = (status) => {
        if (status === 'done') return html`<span style="color:var(--accent-green)">✓</span>`;
        if (status === 'error') return html`<span style="color:var(--accent-red)">✗</span>`;
        if (status === 'active') return html`<span class="spinner" style="width:12px;height:12px"></span>`;
        return html`<span style="color:var(--text-dim)">·</span>`;
    };

    const stepLabel = (step) => {
        const labels = { download: 'Download', verify: 'Verify checksum', extract: 'Extract binary', install: 'Install' };
        return labels[step] || step;
    };

    return html`
        <div class="settings-section update-section">
            <h3>
                <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:-2px;margin-right:6px">
                    <path d="M8 2v8M5 7l3 3 3-3" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/>
                    <path d="M3 12h10" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/>
                </svg>
                Software Update
            </h3>

            <div class="update-status">
                <div class="update-version-row">
                    <span class="label">Current Version</span>
                    <span class="value mono">${updateInfo?.current_version || '...'}</span>
                </div>

                ${updateInfo?.latest_version && html`
                    <div class="update-version-row">
                        <span class="label">Latest Version</span>
                        <span class="value mono ${updateInfo.update_available ? 'update-highlight' : ''}">${updateInfo.latest_version}</span>
                    </div>
                `}

                ${updateInfo?.platform && html`
                    <div class="update-version-row">
                        <span class="label">Platform</span>
                        <span class="value mono">${updateInfo.platform}</span>
                    </div>
                `}
            </div>

            ${error && html`<div class="update-error">${error}</div>`}

            ${!updateInfo?.update_available && updateInfo?.latest_version && !error && !installed && !updateInfo?.restart_required && html`
                <div class="update-current">You're up to date.</div>
            `}

            ${updateInfo?.restart_required && !installed && !updating && html`
                <div class="update-available">
                    <div class="update-release-name">Update installed — restart needed</div>
                    <span class="update-size">On disk: ${updateInfo.disk_version} · Running: ${updateInfo.current_version}</span>
                </div>
            `}

            ${updateInfo?.update_available && !updateInfo?.restart_required && !installed && !updating && html`
                <div class="update-available">
                    <div class="update-release-name">${updateInfo.release_name || 'New version available'}</div>
                    ${updateInfo.download_size ? html`<span class="update-size">${formatSize(updateInfo.download_size)}</span>` : null}
                    ${updateInfo.release_url && html`
                        <a class="update-notes-link" href="${updateInfo.release_url}" target="_blank" rel="noopener">Release notes →</a>
                    `}
                </div>
            `}

            ${(updating || installed) && steps.length > 0 && html`
                <div class="update-steps">
                    ${steps.map(s => html`
                        <div class="update-step ${s.status}" key=${s.step}>
                            <span class="update-step-icon">${stepIcon(s.status)}</span>
                            <span class="update-step-label">${stepLabel(s.step)}</span>
                            <span class="update-step-msg">${s.message}</span>
                        </div>
                    `)}
                </div>
            `}

            <div class="update-actions">
                ${!installed && !updating && html`
                    <button class="btn btn-secondary" onClick=${doCheck} disabled=${checking}>
                        ${checking ? 'Checking...' : 'Check for Updates'}
                    </button>
                `}

                ${updateInfo?.update_available && !updateInfo?.restart_required && !installed && !updating && html`
                    <button class="btn btn-primary" onClick=${doUpdate}>
                        Update Now
                    </button>
                `}

                ${updating && html`
                    <button class="btn btn-primary" disabled>
                        <span class="spinner"></span> Updating...
                    </button>
                `}

                ${(installed || updateInfo?.restart_required) && !restarting && !updating && html`
                    <button class="btn btn-primary update-restart-btn" onClick=${doRestart}>
                        Restart to Apply
                    </button>
                `}

                ${restarting && html`
                    <button class="btn btn-primary" disabled>
                        <span class="spinner"></span> Restarting...
                    </button>
                `}
            </div>
        </div>
    `;
}

// ============================================================================
// Boot Instructions Component
// ============================================================================

function BootInstructions() {
    const [instructions, setInstructions] = useState('');
    const [original, setOriginal] = useState('');
    const [saving, setSaving] = useState(false);
    const [saved, setSaved] = useState(false);

    useEffect(() => {
        fetchBootInstructions().then(data => {
            setInstructions(data.instructions || '');
            setOriginal(data.instructions || '');
        }).catch(() => {});
    }, []);

    const handleSave = async () => {
        if (saving) return;
        setSaving(true);
        setSaved(false);
        try {
            const res = await saveBootInstructions(instructions);
            if (res.ok) {
                setOriginal(instructions);
                setSaved(true);
                setTimeout(() => setSaved(false), 2000);
            }
        } catch (e) { /* ignore */ }
        setSaving(false);
    };

    const dirty = instructions !== original;

    return html`
        <div class="settings-section full-width boot-instructions-section">
            <h3>
                <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:-2px;margin-right:6px">
                    <path d="M8 1L2 4v4c0 3.5 2.6 6.8 6 7.9 3.4-1.1 6-4.4 6-7.9V4L8 1z" stroke="currentColor" fill="none" stroke-width="1.5"/>
                    <path d="M6 8l2 2 3-4" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
                Boot Instructions <${HelpTip} text="Custom instructions injected into every MCP inception. Use this to configure agent behavior on startup — like pulling reflections, checking tasks, or setting personality." />
            </h3>
            <p style="font-size:12px;color:var(--text-dim);margin-bottom:12px;">
                These instructions are appended to every <code style="background:var(--bg-elevated);padding:1px 4px;border-radius:3px;">sage_inception</code> response.
                Connected AI agents will follow them at the start of each session.
            </p>
            <textarea
                class="boot-textarea"
                placeholder="e.g. Pull yesterday's last reflection before starting. Always check pending tasks first. Use a friendly but professional tone."
                value=${instructions}
                onInput=${e => setInstructions(e.target.value)}
                rows="5"
            ></textarea>
            <div style="display:flex;align-items:center;gap:8px;margin-top:8px;">
                <button class="btn btn-primary" onClick=${handleSave} disabled=${!dirty || saving}>
                    ${saving ? 'Saving...' : saved ? 'Saved!' : 'Save'}
                </button>
                ${dirty && !saving && html`<span style="font-size:11px;color:var(--warning);">Unsaved changes</span>`}
            </div>
        </div>
    `;
}

// ============================================================================
// Cleanup Settings Component
// ============================================================================

function AutostartToggle() {
    const [autostart, setAutostartState] = useState(null);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        fetchAutostart().then(res => {
            if (res.supported) setAutostartState(res);
        }).catch(() => {});
    }, []);

    if (!autostart || !autostart.supported) return null;

    async function handleToggle() {
        setLoading(true);
        try {
            const res = await setAutostart(!autostart.enabled);
            if (!res.error) {
                setAutostartState(res);
            }
        } catch (e) {
            // ignore
        } finally {
            setLoading(false);
        }
    }

    return html`
        <div class="settings-row">
            <span class="label">Open at Login</span>
            <span class="value" style="display:flex;align-items:center;gap:8px;">
                <label class="toggle-switch" onClick=${(e) => e.stopPropagation()}>
                    <input type="checkbox" checked=${autostart.enabled}
                        disabled=${loading}
                        onChange=${handleToggle} />
                    <span class="toggle-slider"></span>
                </label>
                <span style="color:var(--text-dim);font-size:12px;">${loading ? 'Saving...' : autostart.enabled ? 'On' : 'Off'}</span>
            </span>
        </div>
    `;
}

function MemoryMode() {
    const [mode, setMode] = useState('full');
    const [saving, setSaving] = useState(false);
    const [saved, setSaved] = useState(false);

    useEffect(() => {
        fetchMemoryMode().then(data => {
            if (data.mode) setMode(data.mode);
        }).catch(() => {});
    }, []);

    async function handleChange(newMode) {
        if (saving || newMode === mode) return;
        setSaving(true);
        setSaved(false);
        try {
            const res = await saveMemoryMode(newMode);
            if (res.ok) {
                setMode(newMode);
                setSaved(true);
                setTimeout(() => setSaved(false), 2000);
            }
        } catch (e) { /* ignore */ }
        setSaving(false);
    }

    const modes = [
        {
            id: 'full',
            label: 'Full',
            desc: 'sage_turn fires every turn — maximum recall and context building.',
            who: 'Best for: power users, long-running projects, agents that need deep continuity.',
        },
        {
            id: 'bookend',
            label: 'Bookend',
            desc: 'Inception at session start, reflect at end. No per-turn calls.',
            who: 'Best for: cost-conscious users, shorter sessions, when you want memory without the overhead.',
        },
        {
            id: 'on-demand',
            label: 'On-Demand',
            desc: 'No automatic SAGE calls. You say "recall" or "reflect" to trigger manually.',
            who: 'Best for: maximum token savings, users who want full control, quick one-off tasks.',
        },
    ];

    return html`
        <h3>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                <path d="M12 2a7 7 0 0 1 7 7c0 2.4-1.2 4.5-3 5.7V17a2 2 0 0 1-2 2h-4a2 2 0 0 1-2-2v-2.3C6.2 13.5 5 11.4 5 9a7 7 0 0 1 7-7z"/>
                <line x1="10" y1="22" x2="14" y2="22"/>
            </svg>
            Memory Mode <${HelpTip} text="Controls when SAGE saves and recalls memories during a conversation. Affects token usage and how much context your AI agent carries between turns." />
        </h3>
        <p style="font-size:12px;color:var(--text-dim);margin:0 0 14px">
            Controls how your AI agents interact with SAGE during conversations. Higher engagement means richer memory but more token usage.
        </p>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
            ${modes.map(m => html`
                <button class="btn ${mode === m.id ? 'btn-primary' : ''}"
                        onClick=${() => handleChange(m.id)}
                        disabled=${saving}
                        style="flex:1;min-width:180px;padding:12px 14px;text-align:left;border:1px solid ${mode === m.id ? 'var(--accent)' : 'var(--border)'};border-radius:8px;background:${mode === m.id ? 'var(--accent-bg, rgba(99,102,241,0.1))' : 'var(--bg-elevated)'};cursor:pointer">
                    <div style="font-weight:600;margin-bottom:4px">
                        ${mode === m.id ? '● ' : '○ '}${m.label}
                    </div>
                    <div style="font-size:11px;color:var(--text-dim);font-weight:normal;margin-bottom:6px">
                        ${m.desc}
                    </div>
                    <div style="font-size:10px;color:var(--text-muted);font-weight:normal;font-style:italic">
                        ${m.who}
                    </div>
                </button>
            `)}
        </div>
        ${saving && html`<div style="margin-top:8px;font-size:12px;color:var(--text-dim)">Saving and syncing hooks...</div>`}
        ${saved && html`<div style="margin-top:8px;font-size:12px;color:#10b981">✓ Saved — mode synced to hooks. Takes effect on next session.</div>`}
    `;
}

function RecallSettings() {
    const [topK, setTopK] = useState(5);
    const [minConfidence, setMinConfidence] = useState(95);
    const [saving, setSaving] = useState(false);
    const [saved, setSaved] = useState(false);

    useEffect(() => {
        fetchRecallSettings().then(data => {
            if (data.top_k) setTopK(data.top_k);
            if (data.min_confidence) setMinConfidence(data.min_confidence);
        }).catch(() => {});
    }, []);

    async function handleSave() {
        setSaving(true);
        setSaved(false);
        try {
            await saveRecallSettings(topK, minConfidence);
            setSaved(true);
            setTimeout(() => setSaved(false), 2000);
        } catch (e) { /* ignore */ }
        setSaving(false);
    }

    return html`
        <h3>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
            </svg>
            Memory Recall
        </h3>
        <p style="font-size:12px;color:var(--text-dim);margin:0 0 12px">
            Controls how your AI agents retrieve memories via MCP tools (sage_recall, sage_turn).
        </p>
        <div class="settings-row" style="align-items:flex-start">
            <div style="flex:1">
                <span class="label">Results per query (k)</span>
                <div style="font-size:11px;color:var(--text-dim);margin-top:2px">How many memories are returned per recall. Higher = more context but slower.</div>
            </div>
            <div style="display:flex;align-items:center;gap:10px;min-width:180px">
                <input type="range" min="4" max="10" value=${topK}
                    onInput=${e => setTopK(parseInt(e.target.value))}
                    style="flex:1;accent-color:var(--accent)" />
                <span class="value" style="min-width:24px;text-align:center;font-weight:600">${topK}</span>
            </div>
        </div>
        <div class="settings-row" style="align-items:flex-start;margin-top:8px">
            <div style="flex:1">
                <span class="label">Minimum confidence</span>
                <div style="font-size:11px;color:var(--text-dim);margin-top:2px">Only return memories above this confidence threshold. Lower = broader but noisier recall.</div>
            </div>
            <div style="display:flex;align-items:center;gap:10px;min-width:180px">
                <input type="range" min="85" max="100" value=${minConfidence}
                    onInput=${e => setMinConfidence(parseInt(e.target.value))}
                    style="flex:1;accent-color:var(--accent)" />
                <span class="value" style="min-width:36px;text-align:center;font-weight:600">${minConfidence}%</span>
            </div>
        </div>
        <div style="margin-top:12px;display:flex;align-items:center;gap:8px">
            <button class="btn" onClick=${handleSave} disabled=${saving}>
                ${saving ? 'Saving...' : 'Save'}
            </button>
            ${saved && html`<span style="color:#10b981;font-size:12px">Saved</span>`}
        </div>
    `;
}

function CleanupSettings() {
    const [config, setConfig] = useState(null);
    const [saving, setSaving] = useState(false);
    const [lastRun, setLastRun] = useState(null);
    const [lastResult, setLastResult] = useState(null);
    const [cleanupRunning, setCleanupRunning] = useState(false);
    const [cleanupResult, setCleanupResult] = useState(null);
    const [expanded, setExpanded] = useState(false);

    useEffect(() => {
        fetchCleanupSettings().then(data => {
            if (data.config) setConfig(data.config);
            if (data.last_run) setLastRun(data.last_run);
            if (data.last_result) {
                try { setLastResult(JSON.parse(data.last_result)); } catch(e) {}
            }
        }).catch(() => {});
    }, []);

    const updateField = (field, value) => {
        setConfig(prev => ({ ...prev, [field]: value }));
    };

    const handleSave = async () => {
        if (!config || saving) return;
        setSaving(true);
        try {
            const res = await saveCleanupSettings(config);
            if (res.config) setConfig(res.config);
        } catch(e) {}
        setSaving(false);
    };

    const handleDryRun = async () => {
        setCleanupRunning(true);
        setCleanupResult(null);
        try {
            const res = await runCleanup(true);
            setCleanupResult(res);
        } catch(e) {
            setCleanupResult({ error: 'Failed to run preview' });
        }
        setCleanupRunning(false);
    };

    const [confirmCleanup, setConfirmCleanup] = useState(false);
    const handleCleanup = async () => {
        if (!confirmCleanup) {
            setConfirmCleanup(true);
            setTimeout(() => setConfirmCleanup(false), 5000);
            return;
        }
        setConfirmCleanup(false);
        setCleanupRunning(true);
        setCleanupResult(null);
        try {
            const res = await runCleanup(false);
            setCleanupResult(res);
            setLastRun(new Date().toISOString());
            setLastResult(res);
        } catch(e) {
            setCleanupResult({ error: 'Failed to run cleanup' });
        }
        setCleanupRunning(false);
    };

    if (!config) return null;

    return html`
        <div class="settings-section cleanup-section">
            <h3 style="display:flex;align-items:center;justify-content:space-between;cursor:pointer" onClick=${() => setExpanded(!expanded)}>
                <span>
                    <svg width="16" height="16" viewBox="0 0 16 16" style="vertical-align:-2px;margin-right:6px">
                        <path d="M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13zM8 5v3.5l2.5 1.5" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/>
                    </svg>
                    Memory Auto-Cleanup <${HelpTip} text="Automatically removes stale observations and low-confidence memories over time. Keeps your brain focused on high-value knowledge." align="right" />
                </span>
                <span style="font-size:12px;color:var(--text-muted)">${expanded ? '▲' : '▼'}</span>
            </h3>

            <div class="cleanup-description">
                <p style="color:var(--text-dim);font-size:13px;line-height:1.5;margin:8px 0">
                    Automatically deprecate stale memories whose confidence has decayed below a threshold,
                    or observations that have outlived their usefulness.
                </p>
            </div>

            <!-- Master toggle — always visible -->
            <div class="settings-row" style="padding:12px 0;border-bottom:1px solid var(--border)">
                <div style="flex:1">
                    <span class="label" style="font-weight:600">Enable Auto-Cleanup</span>
                    <div class="setting-help">
                        <span style="color:var(--accent);font-size:11px;font-weight:500">ON:</span>
                        <span style="color:var(--text-dim);font-size:11px"> SAGE periodically removes stale session observations and low-confidence memories. Good for long-running agents that accumulate thousands of memories.</span>
                    </div>
                    <div class="setting-help" style="margin-top:2px">
                        <span style="color:var(--danger);font-size:11px;font-weight:500">OFF:</span>
                        <span style="color:var(--text-dim);font-size:11px"> Nothing is ever auto-removed. You control what stays. Best if you want complete history or have a small memory set.</span>
                    </div>
                </div>
                <label class="toggle-switch">
                    <input type="checkbox" checked=${config.enabled}
                        onChange=${(e) => {
                            const newVal = e.target.checked;
                            updateField('enabled', newVal);
                            saveCleanupSettings({ ...config, enabled: newVal }).catch(() => {});
                        }} />
                    <span class="toggle-slider"></span>
                </label>
            </div>

            <!-- Quick actions — always visible -->
            <div style="display:flex;gap:8px;padding:12px 0;flex-wrap:wrap;align-items:center">
                <button class="btn ${confirmCleanup ? '' : 'btn-danger'}" onClick=${handleCleanup} disabled=${cleanupRunning}
                    style="font-size:12px;${confirmCleanup ? 'background:var(--danger);color:#fff;animation:pulse 1s infinite' : ''}">
                    ${cleanupRunning ? 'Cleaning...' : confirmCleanup ? 'Click again to confirm' : 'Clean Synaptic Ledger'}
                </button>
                <button class="btn" onClick=${handleDryRun} disabled=${cleanupRunning} style="font-size:12px">
                    ${cleanupRunning ? 'Running...' : 'Preview'}
                </button>
                ${lastRun && html`<span style="font-size:11px;color:var(--text-muted)">Last run: ${new Date(lastRun).toLocaleString()}</span>`}
            </div>

            <!-- Cleanup result — always visible -->
            ${cleanupResult && html`
                <div class="cleanup-result" style="margin-bottom:12px;padding:12px;background:var(--bg-surface);border:1px solid var(--border);border-radius:var(--radius)">
                    ${cleanupResult.error ? html`
                        <span style="color:var(--danger)">${cleanupResult.error}</span>
                    ` : html`
                        <div style="font-size:13px;color:var(--text-dim)">
                            <strong>${cleanupResult.dry_run ? 'Preview' : 'Done'}:</strong>
                            ${cleanupResult.deprecated || 0} memories ${cleanupResult.dry_run ? 'would be' : ''} deprecated
                            (${cleanupResult.checked || 0} checked)
                        </div>
                    `}
                </div>
            `}

            ${expanded && html`
                <!-- Observation TTL -->
                <div class="settings-row setting-detail" style="padding:12px 0;border-bottom:1px solid var(--border)">
                    <div style="flex:1">
                        <span class="label">Observation TTL</span>
                        <div class="setting-help">
                            <span style="color:var(--text-dim);font-size:11px">
                                How many days before general observations are auto-deprecated.
                                Observations are things like "user asked about X" or "noticed pattern Y" — useful short-term, less so after a week.
                            </span>
                        </div>
                        <div class="setting-help" style="margin-top:2px">
                            <span style="color:var(--text-muted);font-size:11px;font-style:italic">
                                Example: Set to 7 days if your agent logs dozens of observations per session. Set to 30+ if observations are rare and valuable.
                            </span>
                        </div>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px">
                        <input type="range" min="1" max="90" value=${config.observation_ttl_days}
                            onInput=${(e) => updateField('observation_ttl_days', parseInt(e.target.value))}
                            style="width:120px" />
                        <span class="value" style="min-width:50px;text-align:right">${config.observation_ttl_days}d</span>
                    </div>
                </div>

                <!-- Session TTL -->
                <div class="settings-row setting-detail" style="padding:12px 0;border-bottom:1px solid var(--border)">
                    <div style="flex:1">
                        <span class="label">Session Context TTL</span>
                        <div class="setting-help">
                            <span style="color:var(--text-dim);font-size:11px">
                                How many days before session-context observations expire. These are ephemeral notes like
                                "user said good morning" or "started new session" — they clutter your memory fast.
                            </span>
                        </div>
                        <div class="setting-help" style="margin-top:2px">
                            <span style="color:var(--text-muted);font-size:11px;font-style:italic">
                                Example: Set to 1-2 days for aggressive cleanup. Set to 7 if you want a week of session history.
                            </span>
                        </div>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px">
                        <input type="range" min="1" max="30" value=${config.session_ttl_days}
                            onInput=${(e) => updateField('session_ttl_days', parseInt(e.target.value))}
                            style="width:120px" />
                        <span class="value" style="min-width:50px;text-align:right">${config.session_ttl_days}d</span>
                    </div>
                </div>

                <!-- Stale Threshold -->
                <div class="settings-row setting-detail" style="padding:12px 0;border-bottom:1px solid var(--border)">
                    <div style="flex:1">
                        <span class="label">Stale Confidence Threshold</span>
                        <div class="setting-help">
                            <span style="color:var(--text-dim);font-size:11px">
                                Memories whose computed confidence drops below this value get auto-deprecated.
                                Confidence decays naturally over time — facts decay slowly (~139 day half-life),
                                while observations decay faster.
                            </span>
                        </div>
                        <div class="setting-help" style="margin-top:2px">
                            <span style="color:var(--text-muted);font-size:11px;font-style:italic">
                                Example: 0.10 is conservative (only removes very stale memories). 0.25 is aggressive (removes anything that's lost 75% confidence).
                            </span>
                        </div>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px">
                        <input type="range" min="1" max="50" value=${Math.round(config.stale_threshold * 100)}
                            onInput=${(e) => updateField('stale_threshold', parseInt(e.target.value) / 100)}
                            style="width:120px" />
                        <span class="value" style="min-width:50px;text-align:right">${(config.stale_threshold * 100).toFixed(0)}%</span>
                    </div>
                </div>

                <!-- Cleanup Interval -->
                <div class="settings-row setting-detail" style="padding:12px 0;border-bottom:1px solid var(--border)">
                    <div style="flex:1">
                        <span class="label">Cleanup Interval</span>
                        <div class="setting-help">
                            <span style="color:var(--text-dim);font-size:11px">
                                How often the background cleanup runs (in hours). Lower = more frequent checks.
                            </span>
                        </div>
                        <div class="setting-help" style="margin-top:2px">
                            <span style="color:var(--text-muted);font-size:11px;font-style:italic">
                                Example: 24h (once a day) is fine for most users. Set to 1h if you're generating memories rapidly.
                            </span>
                        </div>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px">
                        <input type="range" min="1" max="168" value=${config.cleanup_interval_hours}
                            onInput=${(e) => updateField('cleanup_interval_hours', parseInt(e.target.value))}
                            style="width:120px" />
                        <span class="value" style="min-width:50px;text-align:right">${config.cleanup_interval_hours}h</span>
                    </div>
                </div>

                <!-- Save button -->
                <div style="display:flex;gap:8px;margin-top:16px;flex-wrap:wrap">
                    <button class="btn btn-primary" onClick=${handleSave} disabled=${saving}>
                        ${saving ? 'Saving...' : 'Save Settings'}
                    </button>
                    <button class="btn" onClick=${handleDryRun} disabled=${cleanupRunning}>
                        ${cleanupRunning ? 'Running...' : 'Preview Cleanup'}
                    </button>
                    <button class="btn btn-danger" onClick=${handleCleanup} disabled=${cleanupRunning}>
                        Run Cleanup Now
                    </button>
                </div>

                <!-- Cleanup result -->
                ${cleanupResult && html`
                    <div class="cleanup-result" style="margin-top:12px;padding:12px;background:var(--bg-surface);border:1px solid var(--border);border-radius:var(--radius)">
                        ${cleanupResult.error ? html`
                            <span style="color:var(--danger)">${cleanupResult.error}</span>
                        ` : html`
                            <div style="font-size:13px;color:var(--text-dim)">
                                <strong style="color:var(--text)">${cleanupResult.dry_run ? 'Preview' : 'Cleanup Complete'}</strong>
                                <span style="margin-left:8px">
                                    Checked: ${cleanupResult.checked} ·
                                    ${cleanupResult.dry_run ? 'Would deprecate' : 'Deprecated'}: <strong style="color:${cleanupResult.deprecated > 0 ? 'var(--warning)' : 'var(--accent)'}">${cleanupResult.deprecated}</strong>
                                </span>
                            </div>
                            ${cleanupResult.deprecated_ids && cleanupResult.deprecated_ids.length > 0 && html`
                                <div style="margin-top:8px;font-size:11px;color:var(--text-muted);max-height:100px;overflow-y:auto">
                                    ${cleanupResult.deprecated_ids.map(id => html`<div style="font-family:monospace">${id.substring(0, 8)}...</div>`)}
                                </div>
                            `}
                        `}
                    </div>
                `}

                <!-- Last run info -->
                ${lastRun && html`
                    <div style="margin-top:8px;font-size:11px;color:var(--text-muted)">
                        Last cleanup: ${new Date(lastRun).toLocaleString()}
                        ${lastResult && lastResult.deprecated != null ? html` · Deprecated: ${lastResult.deprecated}` : ''}
                    </div>
                `}
            `}
        </div>
    `;
}

// ============================================================================
// Settings Page
// ============================================================================

// EmbeddingsSetupModal — turn on SAGE's bundled semantic memory (Ollama +
// nomic-embed-text). Walks a non-technical operator through: detect Ollama →
// download the model if missing → re-embed existing memories (progress) → switch
// the node to the semantic embedder (a brief restart). The embedder is LOCKED to
// Ollama + nomic-embed-text — this only turns the bundled one on, it's not a
// "pick your embedder" screen.
function EmbeddingsSetupModal({ onClose, onDone }) {
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(true);
    const [step, setStep] = useState('intro'); // intro | need-ollama | pull | reembed | enable | done
    const [err, setErr] = useState(null);
    const [pullLog, setPullLog] = useState('');
    const [pulling, setPulling] = useState(false);
    const [prog, setProg] = useState(null); // { done, total }
    const [reembedding, setReembedding] = useState(false);
    const [enabling, setEnabling] = useState(false);
    const [warn, setWarn] = useState(null); // non-blocking notice (e.g. partial re-embed failures)
    const [skipped, setSkipped] = useState(0); // memories that couldn't be read/embedded
    const busy = pulling || reembedding || enabling;

    useEffect(() => {
        (async () => {
            try {
                // Re-attach if a background re-embed is already running (reopened
                // modal, or it kept going after the modal was closed).
                const prog = await reembedProgress().catch(() => null);
                if (prog && prog.running) {
                    setStatus(await embeddingsStatus().catch(() => null));
                    setProg({ done: prog.done || 0, total: prog.total || 0 });
                    setStep('reembed');
                    setLoading(false);
                    pollReembed();
                    return;
                }
                const s = await embeddingsStatus();
                setStatus(s);
                if (s.is_semantic && s.need_reembed === 0) setStep('done');
                else if (!s.ollama_running) setStep('need-ollama');
                else if (!s.model_available) setStep('pull');
                else setStep('intro');
            } catch (e) { setErr(e.message); }
            setLoading(false);
        })();
    }, []);

    // Read a text/plain "key: value\n" stream, dispatching each line.
    const readStream = async (res, onLine) => {
        const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = '';
        for (;;) {
            const { value, done } = await reader.read(); if (done) break;
            buf += dec.decode(value, { stream: true });
            let i; while ((i = buf.indexOf('\n')) >= 0) {
                const l = buf.slice(0, i); buf = buf.slice(i + 1);
                const c = l.indexOf(':'); if (c > 0) onLine(l.slice(0, c).trim(), l.slice(c + 1).trim());
            }
        }
    };

    const doPull = async () => {
        setPulling(true); setErr(null); setPullLog('starting…');
        let streamErr = null;
        try {
            const res = await pullEmbedModel();
            await readStream(res, (k, v) => {
                if (k === 'status') setPullLog(v);
                if (k === 'error') { streamErr = v; setErr(v); }
            });
            const s = await checkOllamaEmbed();
            if (s.model_available) setStep('reembed');
            else if (!streamErr) setErr('The model download did not finish. Try again.');
        } catch (e) { setErr(e.message || String(e)); }
        setPulling(false);
    };

    // pollReembed watches the SERVER-SIDE background job to completion. The job
    // survives client hiccups (backgrounded tab, network blip), so we just poll
    // its snapshot every second and mirror it into the progress bar.
    const pollReembed = async () => {
        setReembedding(true); setErr(null); setWarn(null);
        try {
            for (;;) {
                const p = await reembedProgress();
                setProg({ done: p.done || 0, total: p.total || 0 });
                setSkipped(p.skipped || 0);
                if (p.error) { setErr(p.error); break; }        // stay on reembed; error shown
                if (!p.running) { setStep('enable'); break; }    // finished
                await new Promise(r => setTimeout(r, 1000));
            }
        } catch (e) { setErr('Lost contact with the node: ' + (e.message || e)); }
        setReembedding(false);
    };

    const doReembed = async () => {
        setErr(null); setWarn(null); setProg({ done: 0, total: status?.need_reembed || 0 });
        try {
            const s = await reembedMemories();               // starts (or re-attaches to) the job
            setProg({ done: s.done || 0, total: s.total || 0 });
        } catch (e) { setErr(e.message || String(e)); return; }
        pollReembed();
    };

    const doEnable = async () => {
        setEnabling(true); setErr(null);
        try { await enableSemanticEmbeddings(); setStep('done'); }
        catch (e) { setErr(e.message); setEnabling(false); }
    };

    const pct = prog && prog.total > 0 ? Math.round((prog.done / prog.total) * 100) : 0;

    return html`
        <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget && !pulling && !reembedding && !enabling) onClose(); }}>
            <div class="wizard-modal" style="max-width:560px;max-height:85vh;display:flex;flex-direction:column;">
                <div class="wizard-header">
                    <h2>Turn on smart memory</h2>
                    <button class="detail-close" onClick=${() => { if (!busy) onClose(); }} disabled=${busy} title=${busy ? 'Please wait — a task is running' : 'Close'}>×</button>
                </div>
                <div class="wizard-body" style="overflow-y:auto;flex:1;padding:20px;line-height:1.55;">
                    ${loading && html`<p style="color:var(--text-dim);text-align:center;padding:24px;">Checking…</p>`}

                    ${!loading && step === 'intro' && html`
                        <p>Right now SAGE finds memories by matching <strong>keywords</strong>. Smart memory lets it find them by <strong>meaning</strong> — so "what did I decide about pricing" also surfaces notes that never used those exact words.</p>
                        <p style="color:var(--text-dim);font-size:13px;">It runs entirely on your machine using the bundled model (Ollama + nomic-embed-text). Nothing leaves your computer.</p>
                        <div class="summary-card" style="padding:14px;margin:14px 0;">
                            <div style="font-size:13px;">To switch on, SAGE will re-read your <strong>${status?.need_reembed ?? status?.total_memories ?? 0} memories</strong> once (a few minutes), then restart briefly. Your memories aren't changed or deleted.</div>
                        </div>
                        <button class="btn btn-primary" style="width:100%;padding:12px;" onClick=${() => setStep('reembed')}>Turn on smart memory</button>
                    `}

                    ${!loading && step === 'need-ollama' && html`
                        <div class="warning-banner" style="margin-bottom:12px;">Ollama isn't running yet.</div>
                        <p>SAGE's smart memory needs <strong>Ollama</strong> — a small free app that runs the AI model locally.</p>
                        <ol style="color:var(--text-dim);font-size:13px;line-height:1.8;">
                            <li>Install it from <a href="https://ollama.com/download" target="_blank" rel="noopener" style="color:var(--accent);">ollama.com/download</a> and open it.</li>
                            <li>Come back here and click <em>Check again</em>.</li>
                        </ol>
                        <button class="btn btn-primary" onClick=${async () => {
                            try {
                                const s = await checkOllamaEmbed();
                                if (!s.ollama_running) setErr('Still can\'t reach Ollama — make sure the app is open.');
                                else { setErr(null); setStep(s.model_available ? 'reembed' : 'pull'); }
                            } catch (e) { setErr('Could not check Ollama: ' + (e.message || e)); }
                        }}>Check again</button>
                    `}

                    ${!loading && step === 'pull' && html`
                        <p>SAGE needs to download its memory model <code>nomic-embed-text</code> once (~275 MB). This is a one-time download.</p>
                        ${pulling
                            ? html`<div style="display:flex;align-items:center;gap:8px;color:var(--accent);margin:12px 0;"><span class="spinner" style="width:14px;height:14px;"></span> ${pullLog || 'Downloading…'}</div>`
                            : html`<button class="btn btn-primary" onClick=${doPull}>Download the model</button>`}
                    `}

                    ${!loading && step === 'reembed' && html`
                        ${!reembedding && (!prog || prog.done === 0) && html`
                            <p>Re-reading your <strong>${status?.need_reembed ?? 0} memories</strong> so SAGE understands their meaning. This takes a few minutes.</p>
                            <div style="font-size:12px;color:var(--text-muted);margin-bottom:14px;">Search results may be incomplete while this runs — it's briefly upgrading. Keep this window open.</div>
                            <button class="btn btn-primary" style="width:100%;padding:12px;" onClick=${doReembed}>Start</button>
                        `}
                        ${(reembedding || (prog && prog.done > 0)) && html`
                            <p style="margin-top:0;">${reembedding ? 'Re-reading your memories…' : 'Done re-reading.'}</p>
                            <div style="background:var(--bg-elev);border-radius:8px;height:12px;overflow:hidden;margin:10px 0;">
                                <div style="height:100%;width:${pct}%;background:var(--accent);transition:width .3s;"></div>
                            </div>
                            <div style="font-size:12px;color:var(--text-muted);text-align:center;">${prog ? `${prog.done} / ${prog.total}` : ''} ${pct ? `(${pct}%)` : ''}</div>
                        `}
                    `}

                    ${!loading && step === 'enable' && html`
                        <div style="text-align:center;">
                            <div style="font-size:34px;margin-bottom:6px;">✓</div>
                            <h3 style="margin:0 0 8px;color:var(--accent-green);">Memories re-read</h3>
                            <p style="color:var(--text-dim);">Last step: switch SAGE over to smart memory. It restarts briefly (about 10-20s) — unlock the vault when it's back.</p>
                            ${skipped > 0 && html`<div class="warning-banner" style="margin:10px 0;font-size:12px;text-align:left;">
                                ${skipped} memor${skipped === 1 ? 'y' : 'ies'} couldn't be read — ${skipped === 1 ? 'it was' : 'they were'} encrypted with a previous vault key, so ${skipped === 1 ? 'it' : 'they'} can't be searched no matter the embedder. Since ${skipped === 1 ? "it's" : "they're"} unreadable, you can safely deprecate ${skipped === 1 ? 'it' : 'them'} from the Memories page — or, if you still have your original vault's recovery key, restore that to get ${skipped === 1 ? 'it' : 'them'} back.
                            </div>`}
                            <button class="btn btn-primary" onClick=${doEnable} disabled=${enabling}>${enabling ? 'Switching…' : 'Finish & restart'}</button>
                        </div>
                    `}

                    ${!loading && step === 'done' && html`
                        <div style="text-align:center;padding:12px 0;">
                            <div style="font-size:38px;margin-bottom:8px;">🧠</div>
                            <h3 style="margin:0 0 8px;color:var(--accent-green);">Smart memory is ${enabling ? 'turning on' : 'on'}</h3>
                            <p style="color:var(--text-dim);">${status?.is_semantic ? 'This node is already using semantic search.' : 'SAGE is restarting to finish. Unlock the vault when it returns, and semantic search will be live.'}</p>
                        </div>
                    `}

                    ${warn && html`<div class="warning-banner" style="margin-top:14px;">${warn}</div>`}
                    ${err && html`<div class="import-error" style="margin-top:14px;">${err}</div>`}
                </div>
                <div class="wizard-footer">
                    <button class="btn" onClick=${() => { if (busy) return; if (onDone) onDone(); onClose(); }} disabled=${busy}>${step === 'done' ? 'Done' : 'Close'}</button>
                </div>
            </div>
        </div>
    `;
}

// RerankerControl - a real on/off toggle + URL/model config + Test for the
// optional cross-encoder reranker. Applied live (no restart) and persisted.
// The reranker is a bring-your-own add-on (not the bundled embedder), so unlike
// the embedder it IS configurable.
function RerankerControl() {
    const [cfg, setCfg] = useState(null);
    const [url, setUrl] = useState('');
    const [model, setModel] = useState('');
    const [busy, setBusy] = useState(false);
    const [testResult, setTestResult] = useState(null);
    const [msg, setMsg] = useState('');
    const [, bumpRender] = useState(0); // force the controlled toggle to resync even when a guard bails
    useEffect(() => {
        fetchReranker().then(c => { setCfg(c); setUrl(c.url || ''); setModel(c.model || ''); })
            .catch(() => setCfg({ enabled: false, url: '', model: '' }));
    }, []);
    const inputStyle = 'background:var(--bg-elev);border:1px solid var(--border);border-radius:4px;padding:6px 8px;color:var(--text);font-size:12px;width:100%;';
    if (!cfg) return html`<div class="settings-row"><span class="label">Reranker</span><span class="value" style="color:var(--text-muted)">Loading...</span></div>`;
    const save = async (enabled) => {
        bumpRender(n => n + 1); // guarantee a re-render so the toggle reflects cfg.enabled even when the URL guard bails
        if (enabled && !url.trim()) { setMsg('Enter a reranker URL first.'); return; }
        setBusy(true); setMsg('');
        try {
            const next = await saveReranker({ enabled, url: url.trim(), model: model.trim() });
            setCfg(next); setUrl(next.url || ''); setModel(next.model || ''); setMsg(enabled ? 'Reranker on.' : 'Reranker off.');
        } catch (e) { setMsg('Save failed: ' + (e.message || 'error')); }
        setBusy(false);
    };
    const test = async () => {
        if (!url.trim()) { setMsg('Enter a reranker URL to test.'); return; }
        setBusy(true); setTestResult(null); setMsg('');
        try { setTestResult(await testReranker({ url: url.trim(), model: model.trim() })); }
        catch (e) { setTestResult({ ok: false, error: e.message || 'error' }); }
        setBusy(false);
    };
    return html`
        <div>
            <div class="settings-row">
                <span class="label"><span class="status-dot ${cfg.enabled ? 'active' : 'inactive'}"></span> Reranker</span>
                <span class="value" style="display:flex;align-items:center;gap:8px;">
                    <span style="color:${cfg.enabled ? '#10b981' : '#6b7280'};font-size:12px;">${cfg.enabled ? 'On' : 'Off'}</span>
                    <label class="toggle-switch" onClick=${(e) => e.stopPropagation()}>
                        <input type="checkbox" checked=${cfg.enabled} disabled=${busy} onChange=${(e) => save(e.target.checked)} />
                        <span class="toggle-slider"></span>
                    </label>
                </span>
            </div>
            <div style="margin-top:10px;display:flex;flex-direction:column;gap:8px;max-width:560px;">
                <input style=${inputStyle} placeholder="Reranker URL (e.g. http://localhost:8081)" value=${url} onInput=${e => setUrl(e.target.value)} />
                <input style=${inputStyle} placeholder="Model (default BAAI/bge-reranker-v2-m3)" value=${model} onInput=${e => setModel(e.target.value)} />
                <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                    <button class="btn" disabled=${busy} onClick=${test}>Test connection</button>
                    <button class="btn" disabled=${busy} onClick=${() => save(true)}>Save & enable</button>
                    ${testResult ? html`<span style="font-size:12px;color:${testResult.ok ? '#10b981' : '#ef4444'};">${testResult.ok ? 'Reachable' : ('Failed: ' + String(testResult.error || '').slice(0, 100))}</span>` : ''}
                    ${msg ? html`<span style="font-size:12px;color:var(--text-muted);">${msg}</span>` : ''}
                </div>
                <div style="font-size:11px;color:var(--text-muted);">Optional. Point at a TEI-compatible reranker server to re-score recall results for sharper relevance. Off by default; the bundled embedder works fine without it.</div>
            </div>
        </div>
    `;
}

// RestartNodeButton - a confirm-armed node restart. The handler already exists
// (POST /v1/dashboard/settings/update/restart); restarting re-locks the vault.
function RestartNodeButton() {
    const [arming, setArming] = useState(false);
    const [busy, setBusy] = useState(false);
    const armRef = useRef(null);
    useEffect(() => () => { if (armRef.current) clearTimeout(armRef.current); }, []);
    const doRestart = async () => {
        if (!arming) {
            setArming(true);
            armRef.current = setTimeout(() => setArming(false), 4000);
            return;
        }
        if (armRef.current) clearTimeout(armRef.current);
        setArming(false); setBusy(true);
        try { await restartServer(); } catch (e) {}
    };
    return html`
        <div class="settings-row">
            <span class="label">Restart node</span>
            <span class="value">
                <button class="btn" disabled=${busy} style=${arming ? 'border-color:#f59e0b;color:#f59e0b;' : ''} onClick=${doRestart}>
                    ${busy ? 'Restarting...' : (arming ? 'Click again to confirm' : 'Restart')}
                </button>
            </span>
        </div>
        ${arming ? html`<div style="font-size:11px;color:#f59e0b;margin-top:4px;">Restarting re-locks the vault - you will need to unlock again. The node is briefly offline.</div>` : ''}
    `;
}

function SettingsPage() {
    const [settingsTab, setSettingsTab] = useState('overview');
    const [stats, setStats] = useState(null);
    const [health, setHealth] = useState(null);
    const [showEmbedSetup, setShowEmbedSetup] = useState(false); // semantic-memory setup modal
    const [embStatus, setEmbStatus] = useState(null);            // GET /embeddings/status (need_reembed etc)
    const refreshEmb = useCallback(() => { embeddingsStatus().then(setEmbStatus).catch(() => {}); }, []);
    useEffect(() => { refreshEmb(); }, [refreshEmb]);
    const [updateAvailable, setUpdateAvailable] = useState(false);
    const [mcpConfig, setMcpConfig] = useState(null);      // GET /v1/mcp-config - copy-paste block
    const [mcpConfigErr, setMcpConfigErr] = useState(false);
    const [mcpCopied, setMcpCopied] = useState(false);
    const [agents, setAgents] = useState([]);                // local agents using this node (Overview tab)
    const [peerAgentFilter, setPeerAgentFilter] = useState('all');

    // Fetch health with live polling every 3s
    useEffect(() => {
        const poll = () => {
            fetchHealth().then(h => {
                setHealth(h);
            }).catch(() => {});
            fetchStats().then(setStats).catch(() => {});
            fetchAgents().then(a => setAgents(a?.agents || [])).catch(() => {});
        };
        poll();
        const iv = setInterval(poll, 3000);
        return () => clearInterval(iv);
    }, []);

    // Background update check on page load + every 12 hours
    useEffect(() => {
        const doCheck = () => checkForUpdate().then(data => {
            if (data && data.update_available) setUpdateAvailable(true);
        }).catch(() => {});
        doCheck();
        const iv = setInterval(doCheck, 12 * 60 * 60 * 1000);
        return () => clearInterval(iv);
    }, []);

    // Fetch the copy-paste MCP config block once (System > Connection).
    useEffect(() => {
        fetchMcpConfig().then(c => setMcpConfig(c)).catch(() => setMcpConfigErr(true));
    }, []);

    // Countdown ticker — force re-render every 100ms for smooth display
    const [, setTick] = useState(0);
    useEffect(() => {
        const iv = setInterval(() => setTick(t => t + 1), 100);
        return () => clearInterval(iv);
    }, []);

    const ver = health?.version || 'dev';
    const encrypted = health?.encrypted || false;
    const chain = health?.chain || null;
    // Embedder status. v6.8.8+ servers return a structured `embedder` block;
    // older servers only set the `ollama` string. Derive a normalized view
    // so the row below this can render any provider without branching twice.
    const embedderStatus = describeEmbedder(health);
    const uptimeRaw = health?.uptime || '';
    const uptimeBaseSec = useRef(0);
    const [uptimeOffset, setUptimeOffset] = useState(0);

    // Sync base uptime when health refreshes
    useEffect(() => {
        if (uptimeRaw) {
            uptimeBaseSec.current = parseUptimeSec(uptimeRaw);
            setUptimeOffset(0);
        }
    }, [uptimeRaw]);

    // Tick uptime every second
    useEffect(() => {
        const iv = setInterval(() => setUptimeOffset(o => o + 1), 1000);
        return () => clearInterval(iv);
    }, []);

    const uptime = uptimeRaw ? formatUptime(uptimeBaseSec.current + uptimeOffset) : '--';

    // Format countdown — compute from block_time relative to now. Post-v10.5.0
    // (issue #40) an idle chain stops minting empty blocks, so a long gap since
    // the last block is normal, not a stall: past CHAIN_IDLE_AFTER_MS we render
    // an "idle — waiting for activity" state instead of a countdown that loops
    // forever and reads like a stalled node.
    const BLOCK_INTERVAL_MS = 5000;
    const CHAIN_IDLE_AFTER_MS = 30000;
    const blockElapsed = chain?.block_time ? Date.now() - new Date(chain.block_time).getTime() : null;
    const chainIdle = blockElapsed !== null && blockElapsed > CHAIN_IDLE_AFTER_MS;
    const liveCountdown = blockElapsed !== null && !chainIdle ? BLOCK_INTERVAL_MS - (blockElapsed % BLOCK_INTERVAL_MS) : null;
    const countdownDisplay = liveCountdown !== null ? (liveCountdown / 1000).toFixed(1) + 's' : '--';
    const countdownPct = liveCountdown !== null ? Math.min(100, (liveCountdown / BLOCK_INTERVAL_MS) * 100) : 0;

    // Status indicator dot
    const statusDot = (active) => html`
        <span class="status-dot ${active ? 'active' : 'inactive'}"></span>
    `;

    // Helper: format nanosecond duration to human-readable
    const formatDuration = (nsStr) => {
        const ns = parseInt(nsStr);
        if (isNaN(ns)) return '--';
        const hours = Math.floor(ns / 3.6e12);
        const mins = Math.floor((ns % 3.6e12) / 6e10);
        if (hours > 24) return Math.floor(hours / 24) + 'd ' + (hours % 24) + 'h';
        if (hours > 0) return hours + 'h ' + mins + 'm';
        return mins + 'm';
    };

    const formatBytes = (bytesStr) => {
        const b = parseInt(bytesStr);
        if (isNaN(b)) return '0 B';
        if (b < 1024) return b + ' B';
        if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
        return (b / 1048576).toFixed(1) + ' MB';
    };

    const peers = chain?.peer_list || [];

    // Connection-exposure derived state for the System > Connection section.
    // (The reranker control is self-contained in <RerankerControl/>.)
    const restAddr = health?.rest_addr || '';
    const restLocalOnly = restAddr.startsWith('127.0.0.1') || restAddr.startsWith('localhost') || restAddr.startsWith('[::1]');
    const restNetworkExposed = !!restAddr && !restLocalOnly;
    const copyMcp = async () => {
        try { await navigator.clipboard.writeText(JSON.stringify(mcpConfig, null, 2)); setMcpCopied(true); setTimeout(() => setMcpCopied(false), 1500); } catch (e) {}
    };

    const tabs = [
        { id: 'overview', label: 'Overview', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M4 4h3v3H4zM9 4h3v3H9zM4 9h3v3H4zM9 9h3v3H9z" fill="currentColor" opacity="0.8"/><path d="M2 2h12v12H2z" stroke="currentColor" fill="none" stroke-width="1.5" rx="2"/></svg>` },
        { id: 'connection', label: 'Connection', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M6 10l4-4M6.5 4.5l1-1a2.5 2.5 0 013.5 3.5l-1 1M9.5 11.5l-1 1a2.5 2.5 0 01-3.5-3.5l1-1" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/></svg>` },
        { id: 'recall', label: 'Recall', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><circle cx="7" cy="7" r="4.5" stroke="currentColor" fill="none" stroke-width="1.5"/><path d="M11 11l3 3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>` },
        { id: 'security', label: 'Security', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M8 1L2 4v4c0 4 3 6 6 7 3-1 6-3 6-7V4L8 1z" stroke="currentColor" fill="none" stroke-width="1.5"/></svg>` },
        { id: 'maintenance', label: 'Maintenance', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M11 2a3 3 0 00-3 3.9L3 11v2h2l5.1-5A3 3 0 1011 2z" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linejoin="round"/></svg>` },
        { id: 'updates', label: 'Updates', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M8 2v8M5 7l3 3 3-3" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/><path d="M3 12h10" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/></svg>` },
    ];

    return html`
        <div class="settings-page">
            ${showEmbedSetup && html`<${EmbeddingsSetupModal} onClose=${() => setShowEmbedSetup(false)} onDone=${() => { fetchHealth().then(setHealth).catch(() => {}); refreshEmb(); }} />`}
            <div class="settings-tabs">
                ${tabs.map(t => html`
                    <button class="settings-tab ${settingsTab === t.id ? 'active' : ''}"
                            onClick=${() => setSettingsTab(t.id)}>
                        ${t.icon}
                        <span>${t.label}</span>
                        ${t.id === 'updates' && updateAvailable ? html`<span class="update-badge" title="Update available"></span>` : ''}
                    </button>
                `)}
                <${PageHelp} section="settings" label="Settings guide" />
            </div>

            ${settingsTab === 'overview' && html`
                <div class="settings-tab-content">
                    <!-- Chain Health -->
                    <div class="settings-section chain-health-section">
                        <h3>
                            Chain Health <${HelpTip} text="Your BFT consensus chain status. Blocks are produced every ~5 seconds while there is activity; since v10.5.0 an idle chain stops minting empty blocks. All validators must agree on memory operations." />
                        </h3>
                        ${chain ? html`
                            <div class="chain-stats-grid">
                                <div class="chain-stat-card" title="Total number of blocks committed to the chain.">
                                    <div class="chain-stat-value block-height">${Number(chain.block_height || 0).toLocaleString()}</div>
                                    <div class="chain-stat-label">Block Height</div>
                                </div>
                                ${chainIdle ? html`
                                    <div class="chain-stat-card" title="No new block for over 30s. Since v10.5.0 an idle chain stops minting empty blocks — height advances when new transactions arrive.">
                                        <div class="chain-stat-value countdown-value" style="color:var(--text-dim)">Idle</div>
                                        <div class="chain-stat-label">Next Block</div>
                                        <div style="font-size:10px;color:var(--text-muted);margin-top:4px">waiting for activity</div>
                                    </div>
                                ` : html`
                                    <div class="chain-stat-card" title="Countdown to the next block (~5s intervals while there is activity).">
                                        <div class="chain-stat-value countdown-value">${countdownDisplay}</div>
                                        <div class="chain-stat-label">Next Block</div>
                                        <div class="countdown-bar"><div class="countdown-fill" style="width: ${countdownPct}%"></div></div>
                                    </div>
                                `}
                                <div class="chain-stat-card" title="Connected SAGE nodes in quorum mode. 0 = solo.">
                                    <div class="chain-stat-value">${chain.peers || '0'}</div>
                                    <div class="chain-stat-label">Peers</div>
                                </div>
                                <div class="chain-stat-card" title="This validator's voting power in BFT consensus.">
                                    <div class="chain-stat-value">${chain.voting_power || '0'}</div>
                                    <div class="chain-stat-label">Voting Power</div>
                                </div>
                            </div>
                            <div class="chain-details">
                                <div class="settings-row"><span class="label">Chain ID</span><span class="value chain-id-value">${chain.chain_id || '--'}</span></div>
                                <div class="settings-row"><span class="label">Node</span><span class="value">${chain.moniker || '--'}</span></div>
                                <div class="settings-row"><span class="label">Syncing</span><span class="value" style="color: ${chain.catching_up ? '#ef4444' : '#10b981'}">${chain.catching_up ? 'Catching up...' : 'In sync'}</span></div>
                                <div class="settings-row"><span class="label">Last Block</span><span class="value">${chain.block_time ? new Date(chain.block_time).toLocaleTimeString() : '--'}${chainIdle ? html` <span style="color:var(--text-muted)">(chain idle — not a stall)</span>` : ''}</span></div>
                            </div>
                        ` : html`<div class="chain-offline">${statusDot(false)} <span>Chain unavailable — CometBFT not running</span></div>`}
                    </div>

                    <div class="settings-grid">
                        <!-- System Status -->
                        <div class="settings-section">
                            <h3>System Status</h3>
                            <div class="settings-row"><span class="label">${statusDot(true)} SAGE</span><span class="value" style="color:#10b981">Running</span></div>
                            <div class="settings-row"><span class="label">${statusDot(embedderStatus.online)} ${embedderStatus.displayName}</span><span class="value" style="color: ${embedderStatus.online ? '#10b981' : '#6b7280'}" title="${embedderStatus.detail || ''}">${embedderStatus.online ? (embedderStatus.detail ? embedderStatus.detail : 'Connected') : 'Offline'}</span></div>
                            ${(embedderStatus.provider === 'hash' || (embStatus && embStatus.need_reembed > 0)) && html`
                                <div class="settings-row" style="align-items:center;">
                                    <span class="label" style="color:var(--text-dim);font-size:12px;">${embedderStatus.provider === 'hash'
                                        ? 'Only keyword matching is on — turn on semantic (meaning-based) search.'
                                        : `${embStatus.need_reembed} memor${embStatus.need_reembed === 1 ? 'y' : 'ies'} still need re-reading to be searchable by meaning.`}</span>
                                    <button class="btn btn-primary" style="padding:6px 14px;font-size:12px;" onClick=${() => setShowEmbedSetup(true)}>${embedderStatus.provider === 'hash' ? 'Turn on smart memory →' : 'Finish setup →'}</button>
                                </div>
                            `}
                            <div class="settings-row"><span class="label">${statusDot(encrypted)} Synaptic Ledger Encryption</span><span class="value" style="color: ${encrypted ? '#10b981' : '#6b7280'}">${encrypted ? 'AES-256-GCM' : 'Off'}</span></div>
                            <div class="settings-row"><span class="label">Version</span><span class="value">${ver}</span></div>
                            <div class="settings-row"><span class="label">Uptime</span><span class="value">${uptime}</span></div>
                            <div class="settings-row"><span class="label">API Endpoint</span><span class="value">${window.location.origin}</span></div>
                        </div>

                        <!-- Memory Statistics -->
                        ${stats ? html`
                            <div class="settings-section">
                                <h3>Memory Statistics</h3>
                                <div class="settings-row"><span class="label">Total Memories</span><span class="value">${stats.total_memories || 0}</span></div>
                                ${stats.by_status && Object.entries(stats.by_status).map(([s, c]) => html`
                                    <div class="settings-row"><span class="label">${s}</span><span class="value">${c}</span></div>
                                `)}
                                ${stats.db_size_bytes != null && html`
                                    <div class="settings-row"><span class="label">DB Size</span><span class="value">${(stats.db_size_bytes / 1024 / 1024).toFixed(1)} MB</span></div>
                                `}
                            </div>
                        ` : html`<div></div>`}

                    </div>

                    <!-- Peers & agents (combined, filtered) - peers are other nodes,
                         agents are the local identities using this node. Peers are
                         usually empty on a single machine, so we show both here. -->
                    <div class="settings-section">
                        <h3>
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                                <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/>
                                <path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>
                            </svg>
                            Peers & agents <${HelpTip} text="Everything connected to this node: peer SAGE nodes (other machines in quorum) and the local agents that read and write memory here. A single-machine node has no peers - that is normal." />
                        </h3>
                        <div style="display:flex;gap:6px;margin-bottom:14px;">
                            ${[['all', 'All'], ['peers', 'Peers'], ['agents', 'Agents']].map(([f, lbl]) => html`
                                <button class="btn" style="padding:4px 12px;font-size:12px;${peerAgentFilter === f ? 'border-color:var(--accent);color:var(--accent);' : ''}" onClick=${() => setPeerAgentFilter(f)}>${lbl}${f === 'peers' ? ` (${peers.length})` : (f === 'agents' ? ` (${agents.length})` : '')}</button>
                            `)}
                        </div>
                        ${(() => {
                            const now = Date.now();
                            const showPeers = peerAgentFilter !== 'agents';
                            const showAgents = peerAgentFilter !== 'peers';
                            const rows = [];
                            if (showPeers) peers.forEach(p => rows.push(html`
                                <div class="peer-card" key=${'peer-' + (p.id || p.remote_ip)}>
                                    <div class="peer-header">
                                        <span class="status-dot active"></span>
                                        <span class="peer-moniker">${p.moniker || 'unknown'}</span>
                                        <span class="peer-badge">peer · ${p.outbound ? 'outbound' : 'inbound'}</span>
                                    </div>
                                    <div class="peer-meta">
                                        <span class="peer-meta-label">IP</span><span class="peer-meta-value">${p.remote_ip}</span>
                                        <span class="peer-meta-label">Connected</span><span class="peer-meta-value">${formatDuration(p.duration)}</span>
                                        <span class="peer-meta-label">Sent</span><span class="peer-meta-value">${formatBytes(p.bytes_sent)}</span>
                                        <span class="peer-meta-label">Received</span><span class="peer-meta-value">${formatBytes(p.bytes_recv)}</span>
                                    </div>
                                </div>
                            `));
                            if (showAgents) agents.forEach(a => {
                                const ageMs = a.last_seen ? (now - new Date(a.last_seen).getTime()) : Infinity;
                                const dot = ageMs < 5 * 60 * 1000 ? '#10b981' : (ageMs < 60 * 60 * 1000 ? '#f59e0b' : '#6b7280');
                                rows.push(html`
                                    <div class="peer-card" key=${'agent-' + a.agent_id}>
                                        <div class="peer-header">
                                            <span class="status-dot" style="background:${dot};"></span>
                                            <span class="peer-moniker">${a.name || (a.agent_id || '').slice(0, 8)}</span>
                                            <span class="peer-badge">agent${a.provider ? ' · ' + a.provider : ''}</span>
                                        </div>
                                        <div class="peer-meta">
                                            <span class="peer-meta-label">Role</span><span class="peer-meta-value">${a.role || '--'}</span>
                                            <span class="peer-meta-label">Memories</span><span class="peer-meta-value">${(a.memory_count || 0).toLocaleString()}</span>
                                            <span class="peer-meta-label">Last seen</span><span class="peer-meta-value">${a.last_seen ? timeAgo(a.last_seen) : 'never'}</span>
                                        </div>
                                    </div>
                                `);
                            });
                            return rows.length ? rows : html`<div class="peer-empty">${peerAgentFilter === 'peers' ? 'No peer nodes connected - this is a single-machine node (normal).' : (peerAgentFilter === 'agents' ? 'No agents registered on this node yet.' : 'No peers or agents yet.')}</div>`;
                        })()}
                    </div>
                </div>
            `}

            ${settingsTab === 'connection' && html`
                <div class="settings-tab-content">
                    <div class="settings-section">
                        <h3>Connection <${HelpTip} text="How this node is reached. REST is the HTTP API your dashboard and tools use; MCP is the local command your AI client runs to plug in. Share these when connecting an Agent or another client." /></h3>
                        <p style="color:var(--text-muted);font-size:0.85rem;margin-bottom:16px;">Endpoints other tools and Agents use to reach this node.</p>
                        ${html`<${ChatGPTCopyField} label="REST API address" value=${window.location.origin} />`}
                        ${restAddr ? html`<${ChatGPTCopyField} label="REST listen address" value=${restAddr} />` : ''}
                        ${restNetworkExposed ? html`<div style="color:#f59e0b;font-size:11px;margin:2px 0 12px;">This address is reachable on your network, not just this machine.</div>` : ''}
                        <div style="margin-top:16px;">
                            <div style="font-size:13px;font-weight:600;margin-bottom:6px;">MCP configuration</div>
                            <div style="color:var(--text-muted);font-size:0.85rem;margin-bottom:10px;">Paste this into your AI client's MCP settings so it can use this node as its memory. SAGE's MCP runs as a local command (stdio), so there is nothing to open on your firewall.</div>
                            ${mcpConfig ? html`
                                <pre style="background:var(--bg-elev);padding:12px;border-radius:4px;font-size:12px;overflow-x:auto;white-space:pre;margin:0 0 8px;">${JSON.stringify(mcpConfig, null, 2)}</pre>
                                <button class="btn" onClick=${copyMcp}>${mcpCopied ? 'Copied!' : 'Copy config'}</button>
                            ` : (mcpConfigErr ? html`<div style="color:var(--text-muted);font-size:12px;">Config unavailable - is the node running?</div>` : html`<div style="color:var(--text-muted);font-size:12px;">Loading...</div>`)}
                        </div>
                    </div>
                </div>
            `}

            ${settingsTab === 'recall' && html`
                <div class="settings-tab-content">
                    <div class="settings-section">
                        <h3>Recall & embeddings <${HelpTip} text="How this node finds and returns memories. Controls how much context your Agent pulls each turn, and the status of the models that power semantic search." /></h3>
                        ${html`<${MemoryMode} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        ${html`<${RecallSettings} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        ${html`<${BootInstructions} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        <h3>Engine status <${HelpTip} text="The embedding model that turns memories into vectors for semantic recall (bundled with SAGE, not configurable), plus the optional reranker that re-scores recall results (bring your own)." /></h3>
                        <div class="settings-row"><span class="label">${statusDot(embedderStatus.online)} Embedding model</span><span class="value" style="color: ${embedderStatus.online ? '#10b981' : '#6b7280'}" title="${embedderStatus.detail || ''}">${embedderStatus.displayName || (embedderStatus.online ? 'Connected' : 'Offline')}${embedderStatus.detail ? ' · ' + embedderStatus.detail : ''}</span></div>
                        <div style="font-size:11px;color:var(--text-muted);margin:-4px 0 14px;">The embedding model is managed by SAGE and is not configurable here.</div>
                        ${html`<${RerankerControl} />`}
                    </div>
                </div>
            `}

            ${settingsTab === 'security' && html`
                <div class="settings-tab-content">
                    <div class="settings-section ledger-section">
                        ${html`<${SynapticLedger} />`}
                    </div>
                </div>
            `}

            ${settingsTab === 'maintenance' && html`
                <div class="settings-tab-content">
                    <div class="settings-section">
                        <h3>Memory cleanup <${HelpTip} text="Prune stale, low-confidence memories so recall stays sharp. Always Preview before you Clean - Clean is destructive." /></h3>
                        ${html`<${CleanupSettings} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        <h3>Export & Backup</h3>
                        <p style="color:var(--text-muted);font-size:0.85rem;margin-bottom:16px;">
                            Download a full backup of your memories in JSONL format. This file can be re-imported on a new machine via the Import page to restore your brain.
                        </p>
                        <div class="settings-row">
                            <span class="label">Export all memories (JSONL - re-importable)</span>
                            <button class="btn" onClick=${() => { window.open('/v1/dashboard/export', '_blank'); }}>Download Backup</button>
                        </div>
                        <p style="color:var(--text-muted);font-size:0.85rem;margin-top:12px;">
                            To restore, go to the <strong>Import</strong> page (sidebar) and upload your <code>.jsonl</code> backup file. All domains, types, and metadata are preserved.
                        </p>
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        <h3>Node <${HelpTip} text="Restart this SAGE node. Restarting re-locks the vault (you will need to unlock again) and briefly takes the node offline." /></h3>
                        ${html`<${RestartNodeButton} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        <h3>Preferences</h3>
                        <div class="settings-row">
                            <span class="label">Contextual Tooltips</span>
                            <span class="value" style="display:flex;align-items:center;gap:8px;">
                                <label class="toggle-switch" onClick=${(e) => e.stopPropagation()}>
                                    <input type="checkbox" checked=${window.__sageTooltips?.enabled}
                                        onChange=${() => window.__sageTooltips?.toggle()} />
                                    <span class="toggle-slider"></span>
                                </label>
                                <span style="color:var(--text-dim);font-size:12px;">${window.__sageTooltips?.enabled ? 'On' : 'Off'}</span>
                            </span>
                        </div>
                        ${html`<${AutostartToggle} />`}
                    </div>
                </div>
            `}

            ${settingsTab === 'updates' && html`
                <div class="settings-tab-content">
                    <div class="settings-section">
                        ${html`<${SoftwareUpdate} />`}
                    </div>
                    <div class="settings-section" style="margin-top:16px">
                        <h3>About</h3>
                        <div class="settings-row"><span class="label">Full Name</span><span class="value">(Sovereign) Agent Governed Experience</span></div>
                        <div class="settings-row"><span class="label">Author</span><span class="value">Dhillon Andrew Kannabhiran</span></div>
                        <div class="settings-row"><span class="label">License</span><span class="value">Apache 2.0</span></div>
                        <div class="settings-row"><span class="label">GitHub</span><span class="value"><a href="https://github.com/l33tdawg/sage" target="_blank" style="color:var(--accent)">l33tdawg/sage</a></span></div>
                        <div class="settings-row"><span class="label">Website</span><span class="value"><a href="https://l33tdawg.github.io/sage/" target="_blank" style="color:var(--accent)">l33tdawg.github.io/sage</a></span></div>
                        <div class="settings-row"><span class="label">Architecture</span><span class="value">CometBFT v0.38 + SQLite + Ed25519</span></div>
                        <div class="settings-row"><span class="label">Connect Guide</span><span class="value"><a href="https://l33tdawg.github.io/sage/connect.html" target="_blank" style="color:var(--accent)">How to connect your AI</a></span></div>
                    </div>
                </div>
            `}
        </div>
    `;
}

// ============================================================================
// Import Page
// ============================================================================

function ImportPage({ sse }) {
    const [selectedFile, setSelectedFile] = useState(null);
    const [dragging, setDragging] = useState(false);
    const [importing, setImporting] = useState(false);
    const [previewing, setPreviewing] = useState(false);
    const [preview, setPreview] = useState(null); // { import_id, total, source, previews }
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const [suggestion, setSuggestion] = useState(null);
    const [progress, setProgress] = useState(null);
    const fileInputRef = useRef(null);

    // Listen for import progress SSE events
    useEffect(() => {
        if (!sse) return;
        const unsub = sse.on('import', (data) => {
            const p = data.data || data;
            if (p.phase === 'complete') {
                setProgress(null);
            } else {
                setProgress(p);
            }
        });
        return unsub;
    }, [sse]);

    function handleDragOver(e) {
        e.preventDefault();
        e.stopPropagation();
        setDragging(true);
    }

    function handleDragLeave(e) {
        e.preventDefault();
        e.stopPropagation();
        setDragging(false);
    }

    function handleDrop(e) {
        e.preventDefault();
        e.stopPropagation();
        setDragging(false);
        const file = e.dataTransfer.files[0];
        if (file && (file.name.endsWith('.json') || file.name.endsWith('.jsonl') || file.name.endsWith('.zip') || file.name.endsWith('.md') || file.name.endsWith('.txt'))) {
            setSelectedFile(file);
            setResult(null);
            setError(null);
            setPreview(null);
        } else {
            setError('Please drop a .json, .jsonl, .zip, .md, or .txt file.');
        }
    }

    function handleFileSelect(e) {
        const file = e.target.files[0];
        if (file) {
            setSelectedFile(file);
            setResult(null);
            setError(null);
            setPreview(null);
        }
    }

    async function handlePreview() {
        if (!selectedFile || previewing || importing) return;
        setPreviewing(true);
        setError(null);
        setResult(null);
        setSuggestion(null);
        setPreview(null);
        try {
            const res = await importPreview(selectedFile);
            if (res.error === 'unstructured_document') {
                setSuggestion(res.suggestion || res.message);
            } else if (res.error) {
                setError(res.error);
            } else {
                setPreview(res);
            }
        } catch (err) {
            setError(err.message || 'Preview failed. Please try again.');
        } finally {
            setPreviewing(false);
        }
    }

    async function handleConfirmImport() {
        if (!preview || importing) return;
        setImporting(true);
        setError(null);
        try {
            const res = await importConfirm(preview.import_id);
            if (res.error) {
                setError(res.error);
            } else {
                setResult(res);
                setPreview(null);
            }
        } catch (err) {
            setError(err.message || 'Import failed. Please try again.');
        } finally {
            setImporting(false);
        }
    }

    function handleCancelImport() {
        setPreview(null);
    }

    function formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    return html`
        <div class="import-page">
            <div class="import-header">
                <h2>Import Memories</h2>
                <p class="import-subtitle">Bring your AI conversations into SAGE</p>
            </div>

            <div class="provider-cards">
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <circle cx="12" cy="12" r="10"/>
                            <path d="M8 12l2-4h4l2 4-2 4h-4l-2-4z"/>
                        </svg>
                    </div>
                    <h3>ChatGPT</h3>
                    <p>Export from <strong>Settings</strong> > <strong>Data Controls</strong> > <strong>Export Data</strong>. Upload the ZIP.</p>
                    <span class="provider-file-type">.zip</span>
                </div>
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                            <path d="M2 17l10 5 10-5"/>
                            <path d="M2 12l10 5 10-5"/>
                        </svg>
                    </div>
                    <h3>Claude.ai</h3>
                    <p>Export from <strong>Settings</strong> > <strong>Privacy</strong> > <strong>Export Data</strong>. Upload the JSON.</p>
                    <span class="provider-file-type">.json</span>
                </div>
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
                        </svg>
                    </div>
                    <h3>Gemini</h3>
                    <p>Export from <strong>Google Takeout</strong> > <strong>My Activity</strong> > <strong>Gemini Apps</strong>. Upload the JSON.</p>
                    <span class="provider-file-type">.json</span>
                </div>
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
                        </svg>
                    </div>
                    <h3>Grok</h3>
                    <p>Export from <strong>accounts.x.ai/data</strong>. Upload the <strong>prod-grok-backend.json</strong> file.</p>
                    <span class="provider-file-type">.json</span>
                </div>
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <path d="M4 4h16v16H4z"/>
                            <path d="M8 8h8M8 12h6M8 16h4"/>
                        </svg>
                    </div>
                    <h3>Claude Code</h3>
                    <p>Upload session <strong>.jsonl</strong> files from <strong>~/.claude/projects/</strong> or <strong>MEMORY.md</strong> files.</p>
                    <span class="provider-file-type">.jsonl .md .txt</span>
                </div>
                <div class="provider-card">
                    <div class="provider-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <circle cx="12" cy="12" r="10"/>
                            <path d="M12 6v6l4 2"/>
                        </svg>
                    </div>
                    <h3>Any AI / API</h3>
                    <p>Works with <strong>OpenAI API</strong>, <strong>Mistral</strong>, <strong>DeepSeek</strong>, browser extensions, and any <strong>role/content</strong> JSON format.</p>
                    <span class="provider-file-type">.json .jsonl</span>
                </div>
                <div class="provider-card" style="border-color: rgba(6,182,212,0.4)">
                    <div class="provider-icon" style="color: var(--accent)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="32" height="32">
                            <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
                        </svg>
                    </div>
                    <h3>SAGE Backup</h3>
                    <p><strong>Restore from backup.</strong> Upload a <strong>.jsonl</strong> export from Settings > Export. All domains, types, and metadata are preserved.</p>
                    <span class="provider-file-type">.jsonl</span>
                </div>
            </div>

            <div class="drop-zone ${dragging ? 'drop-zone-active' : ''} ${selectedFile ? 'drop-zone-has-file' : ''} ${importing ? 'drop-zone-disabled' : ''}"
                 onDragOver=${!importing ? handleDragOver : undefined}
                 onDragLeave=${!importing ? handleDragLeave : undefined}
                 onDrop=${!importing ? handleDrop : undefined}
                 onClick=${() => !importing && fileInputRef.current && fileInputRef.current.click()}>
                <input type="file" ref=${fileInputRef} accept=".json,.jsonl,.zip,.md,.txt"
                       style="display:none" onChange=${handleFileSelect} />
                ${selectedFile ? html`
                    <div class="drop-zone-file">
                        <svg viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2" width="28" height="28">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                            <polyline points="14 2 14 8 20 8"/>
                        </svg>
                        <div>
                            <div class="drop-zone-filename">${selectedFile.name}</div>
                            <div class="drop-zone-filesize">${formatFileSize(selectedFile.size)}</div>
                        </div>
                    </div>
                ` : html`
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="40" height="40" style="opacity:0.5">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                        <polyline points="17 8 12 3 7 8"/>
                        <line x1="12" y1="3" x2="12" y2="15"/>
                    </svg>
                    <p class="drop-zone-text">Drop your export file here or click to browse</p>
                    <span class="drop-zone-hint">Accepts .zip, .json, .jsonl, .md, and .txt files</span>
                `}
            </div>

            ${!preview && html`
                <div class="import-actions">
                    <button class="btn import-btn ${previewing ? 'importing' : ''}"
                            disabled=${!selectedFile || previewing || importing}
                            onClick=${handlePreview}>
                        ${previewing ? html`
                            <span class="import-spinner"></span> Scanning...
                        ` : 'Scan File'}
                    </button>
                </div>
            `}

            ${preview && !importing && !result && html`
                <div class="import-preview-card fade-in">
                    <div class="import-preview-header">
                        <svg viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2" width="22" height="22">
                            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                            <polyline points="14 2 14 8 20 8"/>
                        </svg>
                        <div>
                            <h3 style="margin:0;color:var(--text)">Found <span style="color:var(--accent)">${preview.total}</span> memories</h3>
                            <span style="font-size:12px;color:var(--text-dim)">Source: ${preview.source}</span>
                        </div>
                    </div>
                    ${preview.previews && preview.previews.length > 0 && html`
                        <div class="import-preview-samples">
                            ${preview.previews.map((p, i) => html`
                                <div class="import-preview-sample">
                                    <span class="import-preview-num">${i + 1}</span>
                                    <span class="import-preview-domain">${p.domain}</span>
                                    <span class="import-preview-text">${p.content}</span>
                                </div>
                            `)}
                            ${preview.total > 10 && html`
                                <div class="import-preview-more">...and ${preview.total - 10} more</div>
                            `}
                        </div>
                    `}
                    <div class="import-preview-actions">
                        <button class="btn import-btn" onClick=${handleConfirmImport}>
                            Confirm Import (${preview.total})
                        </button>
                        <button class="btn btn-secondary" onClick=${handleCancelImport}>Cancel</button>
                    </div>
                </div>
            `}

            ${(importing && !progress) && html`
                <div class="import-progress fade-in">
                    <div class="import-progress-header">
                        <span><span class="deploy-spinner" style="width:12px;height:12px;border-width:1.5px;margin-right:8px;vertical-align:middle;"></span>Starting import...</span>
                    </div>
                    <div class="import-progress-hint">Preparing to commit each memory through BFT consensus. This begins in a moment.</div>
                </div>
            `}

            ${(importing && progress) && html`
                <div class="import-progress fade-in">
                    <div class="import-progress-header">
                        <span>Processing memories on-chain...</span>
                        <span class="import-progress-count">${progress.current || 0} / ${progress.total || 0}</span>
                    </div>
                    <div class="import-progress-bar-track">
                        <div class="import-progress-bar-fill" style="width: ${progress.total ? Math.round(((progress.current || 0) / progress.total) * 100) : 0}%"></div>
                    </div>
                    <div class="import-progress-detail">
                        <span>${progress.imported || 0} imported</span>
                        ${progress.skipped > 0 ? html`<span> · ${progress.skipped} skipped</span>` : ''}
                    </div>
                    <div class="import-progress-hint">Each memory goes through BFT consensus on your chain. Watch Chain Activity to see them being committed.</div>
                </div>
            `}

            ${error && html`
                <div class="import-error">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
                        <circle cx="12" cy="12" r="10"/>
                        <line x1="15" y1="9" x2="9" y2="15"/>
                        <line x1="9" y1="9" x2="15" y2="15"/>
                    </svg>
                    <span>${error}</span>
                </div>
            `}

            ${suggestion && html`
                <div class="import-suggestion fade-in">
                    <div class="import-suggestion-header">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="22" height="22">
                            <circle cx="12" cy="12" r="10"/>
                            <line x1="12" y1="16" x2="12" y2="12"/>
                            <line x1="12" y1="8" x2="12.01" y2="8"/>
                        </svg>
                        <h3>Not quite right for import</h3>
                    </div>
                    <p class="import-suggestion-text">${suggestion}</p>
                    <div class="import-suggestion-example">
                        <strong>Better approach:</strong> Open your AI agent (Claude, ChatGPT, etc.) and ask it to read the document, then use SAGE MCP tools like <code>sage_remember</code> or <code>sage_reflect</code> to store the key insights as structured memories.
                    </div>
                </div>
            `}

            ${result && html`
                <div class="import-results fade-in">
                    <div class="import-results-header">
                        <svg viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2" width="24" height="24">
                            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                            <polyline points="22 4 12 14.01 9 11.01"/>
                        </svg>
                        <h3>Import Complete</h3>
                    </div>
                    <div class="import-results-stats">
                        ${result.imported != null && html`
                            <div class="import-stat">
                                <span class="import-stat-value">${result.imported}</span>
                                <span class="import-stat-label">memories imported${result.provider ? ` from ${result.provider}` : ''}</span>
                            </div>
                        `}
                        ${result.skipped != null && result.skipped > 0 && html`
                            <div class="import-stat">
                                <span class="import-stat-value import-stat-dim">${result.skipped}</span>
                                <span class="import-stat-label">skipped (duplicates or empty)</span>
                            </div>
                        `}
                        ${result.errors && result.errors.length > 0 && html`
                            <div class="import-stat">
                                <span class="import-stat-value import-stat-warn">${result.errors.length}</span>
                                <span class="import-stat-label">errors</span>
                            </div>
                            <div class="import-error-list">
                                ${result.errors.slice(0, 5).map(e => html`<div class="import-error-item">${e}</div>`)}
                                ${result.errors.length > 5 && html`<div class="import-error-item">...and ${result.errors.length - 5} more</div>`}
                            </div>
                        `}
                    </div>
                    <button class="btn import-view-btn" onClick=${() => { window.location.hash = '/'; }}>
                        View in Brain
                    </button>
                </div>
            `}
        </div>
    `;
}

// ============================================================================
// Timeline Bar
// ============================================================================

function TimelineBar({ selectedRanges, onSelectRange }) {
    const [buckets, setBuckets] = useState([]);

    useEffect(() => {
        import('./api.js').then(({ fetchTimeline }) => {
            fetchTimeline({ bucket: 'hour' }).then(data => {
                setBuckets(data.buckets || []);
            }).catch(() => {});
        });
    }, []);

    const maxCount = Math.max(1, ...buckets.map(b => b.count));

    function formatPeriod(period) {
        try {
            const d = new Date(period);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch (e) {
            return period;
        }
    }

    function toggleBucket(bucket) {
        if (!onSelectRange) return;
        const from = new Date(bucket.period).getTime();
        const to = from + 3600000; // 1 hour
        const existing = (selectedRanges || []).find(r => r.from === from);
        if (existing) {
            // Deselect
            onSelectRange(selectedRanges.filter(r => r.from !== from));
        } else {
            // Select (add to multi-select)
            onSelectRange([...(selectedRanges || []), { from, to }]);
        }
    }

    function isSelected(bucket) {
        if (!selectedRanges || selectedRanges.length === 0) return false;
        const from = new Date(bucket.period).getTime();
        return selectedRanges.some(r => r.from === from);
    }

    return html`
        <div class="timeline-bar">
            <span class="timeline-label">24h</span>
            <div class="timeline-track">
                ${buckets.map((b, i) => {
                    const pct = (b.count / maxCount) * 100;
                    const sel = isSelected(b);
                    return html`
                        <div class="timeline-bucket-bar ${sel ? 'selected' : ''}"
                             style="left: ${(i / Math.max(1, buckets.length)) * 100}%;
                                    width: ${100 / Math.max(1, buckets.length)}%;
                                    height: ${Math.max(pct, 4)}%;
                                    ${sel ? 'background: #22d3ee; opacity: 1;' : ''}"
                             onClick=${() => toggleBucket(b)}>
                            <div class="timeline-tooltip">
                                <span class="timeline-tooltip-count">${b.count}</span> memor${b.count === 1 ? 'y' : 'ies'}
                                <br/>
                                <span class="timeline-tooltip-time">${formatPeriod(b.period)}</span>
                                ${sel ? html`<br/><span style="color:var(--primary);font-size:10px;">Click to deselect</span>` : ''}
                            </div>
                        </div>
                    `;
                })}
            </div>
            <span class="timeline-label" style="text-align: right;">Now</span>
            ${selectedRanges && selectedRanges.length > 0 && html`
                <button class="timeline-clear-btn" onClick=${() => onSelectRange([])}
                        title="Clear time filter">Clear</button>
            `}
        </div>
    `;
}

// ============================================================================
// Chain Activity Log — collapsible real-time event stream
// ============================================================================

function ChainActivityLog({ sse }) {
    const [open, setOpen] = useState(false);
    const [events, setEvents] = useState([]);
    const [logHeight, setLogHeight] = useState(200);
    const [expandedEvent, setExpandedEvent] = useState(null);
    const maxEvents = 200;

    useEffect(() => {
        if (!sse) return;

        function addEvent(type, data) {
            const entry = {
                id: Date.now() + '-' + Math.random().toString(36).slice(2, 6),
                type,
                timestamp: new Date().toISOString(),
                data,
            };
            setEvents(prev => {
                const next = [entry, ...prev];
                return next.length > maxEvents ? next.slice(0, maxEvents) : next;
            });
        }

        let connectedOnce = false;
        const unsubs = [
            sse.on('remember', (data) => addEvent('remember', data)),
            sse.on('recall', (data) => addEvent('recall', data)),
            sse.on('forget', (data) => addEvent('forget', data)),
            sse.on('vote', (data) => addEvent('vote', data)),
            sse.on('consensus', (data) => addEvent('consensus', data)),
            sse.on('agent', (data) => addEvent('agent', data)),
            sse.on('connection', (data) => {
                // Track connection state internally but don't show in chain activity
                if (data.connected) {
                    connectedOnce = true;
                } else {
                    connectedOnce = false;
                }
            }),
        ];
        return () => unsubs.forEach(u => u());
    }, [sse]);

    function formatTs(ts) {
        try {
            const d = new Date(ts);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        } catch { return ts; }
    }

    const typeIcons = {
        remember: { icon: '+', color: '#10b981', label: 'Memory Stored' },
        recall: { icon: '?', color: '#06b6d4', label: 'Memory Retrieved' },
        forget: { icon: '-', color: '#ef4444', label: 'Memory Forgotten' },
        vote: { icon: 'V', color: '#f59e0b', label: 'Consensus Vote' },
        consensus: { icon: 'C', color: '#a78bfa', label: 'Consensus Reached' },
        agent: { icon: 'A', color: '#f472b6', label: 'Agent Activity' },
        connection: { icon: '*', color: '#8b5cf6', label: 'Connection' },
    };

    return html`
        <div class="chain-activity ${open ? 'open' : ''}">
            <button class="chain-activity-toggle" onClick=${() => setOpen(!open)}>
                <svg width="12" height="12" viewBox="0 0 12 12" style="transform: rotate(${open ? '180' : '0'}deg); transition: transform 0.2s;">
                    <path d="M2 4l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.5"/>
                </svg>
                <span>Chain Activity</span>
                ${events.length > 0 ? html`<span class="chain-activity-count">${events.length}</span>` : ''}
                ${!open && events.length > 0 ? html`
                    <span class="chain-activity-latest" style="color: ${(typeIcons[events[0]?.type] || typeIcons.connection).color}">
                        ${(typeIcons[events[0]?.type] || typeIcons.connection).label}
                        — ${formatTs(events[0]?.timestamp)}
                    </span>
                ` : ''}
            </button>
            ${open && html`
                <div class="chain-activity-log" style="max-height: ${logHeight}px;">
                    ${events.length === 0 ? html`
                        <div class="chain-activity-empty">Waiting for chain events...</div>
                    ` : events.map(ev => {
                        const info = typeIcons[ev.type] || typeIcons.connection;
                        const isExpanded = expandedEvent === ev.id;
                        const hasDetail = ev.data?.data?.full_content || ev.data?.data?.retrieved;
                        return html`
                            <div class="chain-event ${isExpanded ? 'expanded' : ''}" key=${ev.id}
                                 onClick=${() => hasDetail && setExpandedEvent(isExpanded ? null : ev.id)}>
                                <span class="chain-event-icon" style="background: ${info.color}20; color: ${info.color};">${info.icon}</span>
                                <span class="chain-event-time">${formatTs(ev.timestamp)}</span>
                                <span class="chain-event-type" style="color: ${info.color};">${info.label}</span>
                                <span class="chain-event-detail">
                                    ${ev.data?.memory_id ? html`<code>${ev.data.memory_id.slice(0, 12)}...</code>` : ''}
                                    ${ev.data?.domain ? html`<span class="chain-event-domain">${ev.data.domain}</span>` : ''}
                                    ${ev.data?.content ? html`<span class="chain-event-content">${ev.data.content.slice(0, 60)}${ev.data.content.length > 60 ? '...' : ''}</span>` : ''}
                                    ${ev.data?.connected !== undefined ? (ev.data.connected ? 'Connected' : 'Disconnected') : ''}
                                    ${ev.data?.agent_id ? html`<span>Agent: <code>${ev.data.agent_id.slice(0, 8)}...</code></span>` : ''}
                                </span>
                                ${hasDetail ? html`<span class="chain-event-chevron ${isExpanded ? 'open' : ''}">▸</span>` : ''}
                                ${isExpanded && ev.type === 'remember' && ev.data?.data?.full_content ? html`
                                    <div class="chain-event-expand" onClick=${(e) => e.stopPropagation()}>
                                        <div class="chain-event-expand-label">Full Content</div>
                                        <div class="chain-event-expand-content">${ev.data.data.full_content}</div>
                                        <div style="display:flex;gap:12px;margin-top:4px;">
                                            ${ev.data.data.memory_type ? html`<span style="font-size:10px;color:var(--text-muted);">Type: <strong>${ev.data.data.memory_type}</strong></span>` : ''}
                                            ${ev.data.data.confidence ? html`<span style="font-size:10px;color:var(--text-muted);">Confidence: <strong>${(ev.data.data.confidence * 100).toFixed(0)}%</strong></span>` : ''}
                                        </div>
                                    </div>
                                ` : ''}
                                ${isExpanded && ev.type === 'recall' && ev.data?.data?.retrieved ? html`
                                    <div class="chain-event-expand" onClick=${(e) => e.stopPropagation()}>
                                        <div class="chain-event-expand-label">Retrieved Memories (${ev.data.data.retrieved.length})</div>
                                        <div class="chain-event-retrieved">
                                            ${ev.data.data.retrieved.map((m, i) => html`
                                                <div class="chain-event-retrieved-item" key=${i}>
                                                    <code>${m.memory_id?.slice(0, 8)}...</code>
                                                    <span class="chain-event-domain" style="font-size:9px;">${m.domain}</span>
                                                    <span class="retrieved-content">${m.content?.slice(0, 150)}${m.content?.length > 150 ? '...' : ''}</span>
                                                </div>
                                            `)}
                                        </div>
                                    </div>
                                ` : ''}
                            </div>
                        `;
                    })}
                </div>
                <div class="chain-activity-resize-handle"
                     onMouseDown=${(e) => {
                         e.preventDefault();
                         e.stopPropagation();
                         const startY = e.clientY;
                         const startH = logHeight;
                         let lastHeight = startH;
                         let rafId = 0;
                         document.body.style.userSelect = 'none';
                         document.body.style.cursor = 'ns-resize';
                         function onMove(ev) {
                             ev.preventDefault();
                             const dy = ev.clientY - startY;
                             const newH = Math.max(80, Math.min(600, startH + dy));
                             if (newH !== lastHeight) {
                                 lastHeight = newH;
                                 cancelAnimationFrame(rafId);
                                 rafId = requestAnimationFrame(() => setLogHeight(newH));
                             }
                         }
                         function onUp() {
                             document.removeEventListener('mousemove', onMove);
                             document.removeEventListener('mouseup', onUp);
                             document.body.style.userSelect = '';
                             document.body.style.cursor = '';
                             cancelAnimationFrame(rafId);
                         }
                         document.addEventListener('mousemove', onMove);
                         document.addEventListener('mouseup', onUp);
                     }}></div>
            `}
        </div>
    `;
}

// ============================================================================
// Health Status Bar
// ============================================================================

function parseUptimeSec(uptimeStr) {
    if (!uptimeStr) return 0;
    let sec = 0;
    const h = uptimeStr.match(/(\d+)h/); if (h) sec += parseInt(h[1]) * 3600;
    const m = uptimeStr.match(/(\d+)m/); if (m) sec += parseInt(m[1]) * 60;
    const s = uptimeStr.match(/([\d.]+)s/); if (s) sec += Math.floor(parseFloat(s[1]));
    return sec;
}

function formatUptime(totalSec) {
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
}

function HealthBar() {
    const [health, setHealth] = useState(null);
    const [uptimeSec, setUptimeSec] = useState(0);
    const uptimeBaseRef = useRef(0);
    const uptimeTickRef = useRef(null);

    useEffect(() => {
        loadHealth();
        const interval = setInterval(loadHealth, 15000);
        return () => clearInterval(interval);
    }, []);

    // Tick uptime every second for real-time display
    useEffect(() => {
        uptimeTickRef.current = setInterval(() => {
            setUptimeSec(s => s + 1);
        }, 1000);
        return () => clearInterval(uptimeTickRef.current);
    }, []);

    async function loadHealth() {
        try {
            const data = await fetchHealth();
            setHealth(data);
            const parsed = parseUptimeSec(data.uptime);
            uptimeBaseRef.current = parsed;
            setUptimeSec(parsed);
        } catch (e) {
            setHealth(null);
        }
    }

    if (!health) return null;

    const embedderStatus = describeEmbedder(health);
    const totalMem = health.memories?.total_memories || 0;
    const domains = health.memories?.by_domain ? Object.keys(health.memories.by_domain).length : 0;

    return html`
        <div class="health-bar">
            <div class="health-item" title="${embedderStatus.detail || ''}">
                <div class="health-dot ${embedderStatus.online ? 'ok' : 'err'}"></div>
                <span>${embedderStatus.label}</span>
            </div>
            <div class="health-sep"></div>
            <div class="health-item">
                <span class="health-num">${totalMem}</span> memories <${HelpTip} text="Total committed memories across all domains and agents." />
            </div>
            <div class="health-sep"></div>
            <div class="health-item">
                <span class="health-num">${domains}</span> domains
            </div>
            <div class="health-sep"></div>
            <div class="health-item">
                <span style="color: var(--text-muted)">uptime</span> ${formatUptime(uptimeSec)} <${PageHelp} section="cerebrum-view" label="Cerebrum guide" />
            </div>
        </div>
    `;
}

// ============================================================================
// Help Overlay
// ============================================================================

function HelpOverlay({ onClose, initialSection }) {
    const [dontShow, setDontShow] = useState(false);
    const [openSection, setOpenSection] = useState(initialSection || null);
    const [animClass, setAnimClass] = useState('');

    function handleDismiss() {
        if (dontShow) {
            try { localStorage.setItem('sage-help-dismissed', '1'); } catch (e) {}
        }
        onClose();
    }

    const selectSection = (key) => {
        if (key === openSection) return;
        setAnimClass('guide-anim-out');
        setTimeout(() => {
            setOpenSection(key);
            setAnimClass('guide-anim-in');
            setTimeout(() => setAnimClass(''), 300);
        }, 200);
    };

    const sections = [
        {
            key: 'getting-started',
            title: 'Getting Started',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
            summary: 'First steps with CEREBRUM and your SAGE brain.',
            content: html`
                <p>CEREBRUM is the visual dashboard for your SAGE institutional memory. Every conversation your AI agents have builds knowledge here — validated by BFT consensus, scored by confidence, and organized by domain.</p>
                <div class="guide-steps">
                    <div class="guide-step"><span class="guide-step-num">1</span><div><strong>Connect your AI assistant</strong> — Add the MCP config from Settings to Claude Code, Cursor, or any MCP-compatible client. Your assistant will automatically call <code>sage_inception</code> on startup to load its memory.</div></div>
                    <div class="guide-step"><span class="guide-step-num">2</span><div><strong>Start a conversation</strong> — As you work with your assistant, it stores observations, facts, and inferences. Each memory goes through consensus validation before being committed.</div></div>
                    <div class="guide-step"><span class="guide-step-num">3</span><div><strong>Explore your brain</strong> — Open the Cerebrum view (brain icon) to see your memories as an interactive bubble visualization. Each bubble represents a memory — its size reflects confidence, its color represents the knowledge domain.</div></div>
                </div>
            `,
        },
        {
            key: 'cerebrum-view',
            title: 'Cerebrum View',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a7 7 0 0 0-7 7c0 2.38 1.19 4.47 3 5.74V17a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2v-2.26c1.81-1.27 3-3.36 3-5.74a7 7 0 0 0-7-7z"/></svg>`,
            summary: 'Interactive visualization of your memories as a neural map.',
            content: html`
                <p>The Cerebrum view is your brain's neural map — a force-directed graph where each bubble is a committed memory.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Bubble size</div>
                        <div class="guide-detail-desc">Reflects the memory's confidence score. Higher confidence = larger bubble. Confidence ranges from 0.0 to 1.0 and is determined by BFT consensus among validators.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Bubble color</div>
                        <div class="guide-detail-desc">Each domain gets a unique color. Memories in the same domain cluster together visually, making it easy to spot knowledge concentrations.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Navigation</div>
                        <div class="guide-detail-desc">Scroll to zoom in/out. Click and drag to pan. Use the navigation pad in the corner for precise movement.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Click-to-Focus</div>
                        <div class="guide-detail-desc">Click any bubble to focus its domain group. Other domains fade out, and the focused memories arrange in a timeline row sorted by creation date. Click a focused bubble again to open its detail panel. Click empty space to exit focus mode.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Domain filter</div>
                        <div class="guide-detail-desc">Click the colored domain pills at the top to filter. Only bubbles from selected domains will appear. Click again to remove the filter.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Agent tabs</div>
                        <div class="guide-detail-desc">Filter memories by agent. Admin agents appear first. Click an agent tab to see only their memories; click again to show all.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Interactive timeline</div>
                        <div class="guide-detail-desc">The bar at the bottom shows memory activity over the last 24 hours. Click any time bucket to filter the graph to only those hours. Multi-select by clicking multiple buckets. Click "Clear" to reset.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Memory Stats panel</div>
                        <div class="guide-detail-desc">Shows domain breakdown and totals. Grab the header to drag it anywhere on screen — position persists between sessions. Use the resize handle at the bottom-right to expand.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Chain Activity</div>
                        <div class="guide-detail-desc">The collapsible bar at the very bottom shows real-time chain events — memories stored, recalled, forgotten, and consensus votes. Drag the top edge to resize. Visible on all pages.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'domains',
            title: 'Domains & Memory Types',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>`,
            summary: 'How knowledge is categorized and what memory types mean.',
            content: html`
                <p>Domains are knowledge categories that your AI agents create dynamically based on conversation context. Instead of dumping everything into one bucket, agents tag each memory with a specific domain for precise recall.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Dynamic domains</div>
                        <div class="guide-detail-desc">Domains are created automatically. If you're debugging Go code, the agent creates "go-debugging". Discussing architecture? "sage-architecture". The more specific the domain, the better recall precision.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Facts <span style="color:var(--accent);">(0.95+)</span></div>
                        <div class="guide-detail-desc">Verified truths — architecture decisions, confirmed behaviors, proven solutions. These are high-confidence memories that represent ground truth.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Observations <span style="color:var(--primary);">(0.80+)</span></div>
                        <div class="guide-detail-desc">Things noticed during work — patterns, user preferences, what worked and what failed. These form the bulk of institutional knowledge.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Inferences <span style="color:var(--text-dim);">(0.60+)</span></div>
                        <div class="guide-detail-desc">Conclusions drawn — hypotheses, connections between facts. Lower confidence than facts, but valuable for building understanding over time.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'search',
            title: 'Search & Import',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
            summary: 'Full-text search across memories and importing from other AI platforms.',
            content: html`
                <p>The Search page provides full-text search across all your committed memories, with filtering by domain, status, and agent.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Full-text search</div>
                        <div class="guide-detail-desc">Type any keyword or phrase to match memory content and domain (case-insensitive exact match). Results are sorted newest first - there is no semantic ranking. Domain badges and confidence scores are shown inline.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Filters</div>
                        <div class="guide-detail-desc">Filter by domain (dropdown), memory status (committed, pending, deprecated), and agent (which agent submitted the memory). Combine filters for precision.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Import</div>
                        <div class="guide-detail-desc">Go to Import (upload icon in sidebar) to bring in conversation exports from ChatGPT, Claude, or Gemini. The importer parses conversation JSON, extracts knowledge, and submits it through the BFT consensus pipeline.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Delete</div>
                        <div class="guide-detail-desc">Click any memory to open its detail, then click Delete. Deleted memories are marked as "deprecated" — hidden from recall but not permanently erased, preserving the audit trail.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'network',
            title: 'Network & Agents',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="5" r="3"/><circle cx="5" cy="19" r="3"/><circle cx="19" cy="19" r="3"/><line x1="12" y1="8" x2="5" y2="16"/><line x1="12" y1="8" x2="19" y2="16"/></svg>`,
            summary: 'Manage agents on your SAGE chain — add peers, set roles, and control permissions.',
            content: html`
                <p>The Network page manages all agents participating in your SAGE consensus chain. Each agent is a separate identity — a different Claude Code project, machine, or assistant — that shares the same memory network.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Agent list</div>
                        <div class="guide-detail-desc">Each agent appears as a card showing its name, role badge, status indicator (green = active, yellow = pending, red = offline), memory count, and clearance level. Click any card to expand its detail view.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Adding an agent</div>
                        <div class="guide-detail-desc">Click the "+" card to launch the Add Agent wizard. Configure the agent's identity (name, avatar, role), permissions (clearance, domain access), and connection method:
                            <br/><br/>
                            <strong>Local Project</strong> (recommended) — For Claude Code sessions on this machine. The wizard shows a one-line install command: <code>sage-gui mcp install --token XXXX</code>. Run it in your project folder. The agent's key and config are set up automatically, and it connects with the exact identity and RBAC you configured. The token is one-time use and expires in 24 hours.
                            <br/><br/>
                            <strong>Download Bundle</strong> — For remote machines. Downloads a ZIP with keys and config to copy manually.
                            <br/><br/>
                            <strong>LAN Pairing</strong> — For agents on your local network. Generates a pairing code (valid 15 minutes). Run <code>sage-gui pair CODE</code> on the new machine.
                        </div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Per-project identity</div>
                        <div class="guide-detail-desc">Each Claude Code session in a different project folder automatically gets its own Ed25519 keypair — no shared keys between projects. Keys are stored at <code>~/.sage/agents/&lt;project-name&gt;-&lt;hash&gt;/agent.key</code>. This means your "sage" project, "levelupctf" project, and "cfp-directory" project each have distinct agents with separate memory attribution and permissions — all managed from this dashboard.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Claim token flow</div>
                        <div class="guide-detail-desc">The recommended way to onboard an agent: create it in the dashboard first (name, role, RBAC permissions), then copy the install command shown in the wizard. Run <code>sage-gui mcp install --token XXXX</code> in your project folder. The CLI claims the pre-configured identity and writes <code>.mcp.json</code>. On next session start, the agent connects with the exact identity and permissions you set up — no manual key wrangling needed. The claim token is single-use and expires after 24 hours.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Unregistered agents</div>
                        <div class="guide-detail-desc">Agents that submit memories via MCP but are not formally registered in the dashboard show up in the Brain view agent filter tabs with a dashed border and a "?" badge. Their memories are stored normally, but they lack a configured name, role, and permissions. You can link an unregistered agent to a dashboard identity at any time from the Network page.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Admin role indicator</div>
                        <div class="guide-detail-desc">Admin agents display a gold star (\u2605) next to their name in the agent filter tabs across the Brain and Search views. The admin is the primary identity that manages other agents' permissions, RBAC settings, and network configuration.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Overview tab</div>
                        <div class="guide-detail-desc">Shows the agent's identity info: name, status, memory count, clearance level, first/last seen timestamps, agent ID (Ed25519 public key), validator key, and bio. Click Edit to modify name and bio.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Activity tab</div>
                        <div class="guide-detail-desc">The Activity tab shows per-agent statistics — total memories contributed, domains active in, and a timeline of recent memory operations. Use this to monitor which agents are most active and what knowledge they're producing.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Key rotation</div>
                        <div class="guide-detail-desc">If an agent's key is compromised or you want to rotate keys proactively, use the Rotate Key button in the agent's Overview tab. This generates a new Ed25519 identity, re-attributes all existing memories to the new key in a single transaction, and triggers a chain redeployment. The old key is permanently retired. You'll need to distribute a new bundle to the agent afterwards.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Removing an agent</div>
                        <div class="guide-detail-desc">Click Remove in the action bar. On a single-node (personal) network this is instant. On a multi-node network the chain briefly reconfigures its validator set. Memories from the removed agent are preserved with their original attribution. You cannot remove the last admin.</div>
                    </div>
                </div>
                <div class="guide-callout">
                    <strong>Chain redeployment:</strong> Adding or removing agents requires updating the validator set. During redeployment, the chain pauses briefly, a backup is taken, the genesis is regenerated, and the chain restarts with the new validator set. Your memories in SQLite are never touched — only the consensus layer resets.
                </div>
            `,
        },
        {
            key: 'access-control',
            title: 'Access Control',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>`,
            summary: 'Roles, domain-level permissions, and clearance levels for each agent.',
            content: html`
                <p>The Access Control tab (inside each agent's expanded view) lets you configure exactly what each agent can read, write, and access.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Roles</div>
                        <div class="guide-detail-desc"><strong>Admin</strong> — Full access to all domains and network management. Can add/remove agents and modify settings. <strong>Member</strong> — Read and write within allowed domains only. Cannot manage other agents. <strong>Observer</strong> — Read-only access. Can view memories but cannot submit new ones.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Domain access matrix</div>
                        <div class="guide-detail-desc">A per-domain permission grid with read and write toggles for each domain. Use "All Read" / "All Write" / "Revoke All" for bulk operations. Enabling write automatically enables read. Admins bypass this matrix entirely (shown as "full access").</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Clearance levels</div>
                        <div class="guide-detail-desc">A 5-tier clearance system: Guest (0), Internal (1), Confidential (2), Secret (3), Top Secret (4). Clearance determines the sensitivity level of memories the agent can access. Higher clearance = access to more sensitive knowledge.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Enforcement</div>
                        <div class="guide-detail-desc">Domain access is enforced server-side on every memory submission. If an agent tries to write to a domain it doesn't have write access to, the request is rejected with a 403 error. This is cryptographically verified — agents sign every request with their Ed25519 key.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'on-chain-identity',
            title: 'On-Chain Agent Identity',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>`,
            summary: 'Agent registration, updates, and permissions validated by BFT consensus.',
            content: html`
                <p>Starting in v3.5, agent identity is a first-class on-chain concept. Every registration, metadata update, and permission change goes through CometBFT consensus — giving you auditability, tamper resistance, and federation readiness.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">How agents join</div>
                        <div class="guide-detail-desc">
                            <strong>Option 1: Dashboard-first (recommended)</strong> — Create the agent in the Network page with name, role, and RBAC. Copy the install command and run it in your project folder. The agent claims its pre-configured identity automatically.
                            <br/><br/>
                            <strong>Option 2: Auto-register</strong> — Just install MCP config (<code>sage-gui mcp install</code>) without a token. The agent self-registers on-chain during its first <code>sage_inception</code> call with a default identity. Configure permissions later from the dashboard.
                        </div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Auto-registration</div>
                        <div class="guide-detail-desc">Agents connecting via MCP automatically register on-chain during their first <code>sage_inception</code> call. The registration is idempotent — connecting again returns the existing record. If a claim token was used, the agent already has its identity.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">On-chain badge</div>
                        <div class="guide-detail-desc">Agents registered on-chain show a green "On-Chain" badge on their card, along with the block height where they were registered. Legacy agents (pre-v3.5) are auto-migrated on first boot.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Visible agents</div>
                        <div class="guide-detail-desc">In the Access Control tab, you can restrict which agents' memories are visible to a given agent. By default, all agents can see everything (open model). Set a JSON array of agent IDs to restrict visibility.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Permission enforcement</div>
                        <div class="guide-detail-desc">Memory operations check on-chain state (BadgerDB) first for clearance and domain access. If an agent isn't registered on-chain yet, the system falls back to the SQLite record. On-chain state is the source of truth.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Transaction types</div>
                        <div class="guide-detail-desc">Three new on-chain transactions: <strong>AgentRegister</strong> (self-registration), <strong>AgentUpdate</strong> (self-update of name/bio), and <strong>AgentSetPermission</strong> (admin sets clearance, domains, visibility). All are cryptographically signed.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Claim token lifecycle</div>
                        <div class="guide-detail-desc">When you create an agent via the dashboard, a one-time claim token is generated. Running <code>sage-gui mcp install --token XXXX</code> in a project folder does three things: generates an Ed25519 keypair at <code>~/.sage/agents/&lt;project-name&gt;-&lt;hash&gt;/agent.key</code>, claims the pre-configured on-chain identity (name, role, RBAC), and writes the <code>.mcp.json</code> config. The token is consumed on claim and cannot be reused. If expired, generate a new one from the agent's detail view in the dashboard.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Unregistered agents on-chain</div>
                        <div class="guide-detail-desc">Agents that auto-register (no claim token) get an on-chain record but appear in the Brain view agent tabs with a dashed border and "?" badge, indicating they lack a dashboard-configured identity. Their memories are valid and consensus-verified, but they operate with default permissions until an admin links them to a named identity or configures their RBAC from the Network page.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Admin role indicator</div>
                        <div class="guide-detail-desc">Admin agents are visually distinguished with a gold star (\u2605) in the agent filter tabs throughout the dashboard. The admin role is the only one that can execute <strong>AgentSetPermission</strong> transactions and manage other agents' clearance, domain access, and visibility settings.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'encryption',
            title: 'Synaptic Ledger (Encryption)',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/><circle cx="12" cy="16" r="1"/></svg>`,
            summary: 'Encrypt your memories at rest with a passphrase.',
            content: html`
                <p>The Synaptic Ledger (found in Settings) provides at-rest encryption for your entire memory store. When enabled, all memory content is encrypted using a key derived from your passphrase.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Enabling encryption</div>
                        <div class="guide-detail-desc">Go to Settings and find the Synaptic Ledger section. Enter a passphrase and confirm it. All existing memories will be encrypted in place. Future memories are encrypted automatically on commit.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Unlocking</div>
                        <div class="guide-detail-desc">When encryption is enabled, you'll see a lock screen when opening CEREBRUM. Enter your passphrase to unlock. The passphrase is held in memory for the session — it's never stored on disk.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Changing passphrase</div>
                        <div class="guide-detail-desc">You can change your passphrase in Settings. The underlying data key stays the same — only the wrapper changes. If you lose your passphrase, use your recovery key on the lock screen to reset it.</div>
                    </div>
                </div>
                <div class="guide-callout" style="border-color: var(--warning, #f59e0b);">
                    <strong>Important:</strong> Save your recovery key somewhere safe. If you lose your passphrase, the recovery key is the only way to regain access. There is no backdoor beyond the recovery key.
                </div>
            `,
        },
        {
            key: 'validators',
            title: 'Quality & Validation',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>`,
            summary: '4 in-process validators enforce memory quality through BFT consensus.',
            content: html`
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">How it works</div>
                        <div class="guide-detail-desc">Every memory passes through 4 application validators before committing. Each validator independently votes accept, reject, or abstain. A BFT quorum of 3/4 (meeting the 2/3 threshold) is required for a memory to be committed. Each validator signs its vote as a real transaction broadcast through CometBFT consensus.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Sentinel</div>
                        <div class="guide-detail-desc">Always accepts. Guarantees at least one positive vote for liveness -- ensures the system never deadlocks.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Dedup</div>
                        <div class="guide-detail-desc">Checks the SHA-256 content hash against all committed memories. Rejects exact duplicates. Abstains if the database lookup fails (fail-open).</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Quality</div>
                        <div class="guide-detail-desc">Rejects low-value content: memories shorter than 20 characters, greeting noise patterns ("user said hi", "session started", "brain online"), empty reflection headers, and bare markdown headers.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Consistency</div>
                        <div class="guide-detail-desc">Enforces metadata rules: minimum confidence of 0.3 for all types, minimum 0.7 for facts, and requires a non-empty domain tag.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Pre-validate API</div>
                        <div class="guide-detail-desc">POST /v1/memory/pre-validate lets you dry-run the validators without submitting on-chain. Returns per-validator decisions and quorum result. MCP tools use this automatically to reject low-quality memories before they hit the chain.</div>
                    </div>
                </div>
            `,
        },
        {
            key: 'pipeline',
            title: 'Pipeline',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 12h4"/><path d="M16 12h4"/><rect x="8" y="8" width="8" height="8" rx="2"/><path d="M12 4v4"/><path d="M12 16v4"/><circle cx="2" cy="12" r="1" fill="currentColor"/><circle cx="22" cy="12" r="1" fill="currentColor"/><circle cx="12" cy="2" r="1" fill="currentColor"/><circle cx="12" cy="22" r="1" fill="currentColor"/></svg>`,
            summary: 'Route work between AI agents — SAGE as a message bus.',
            content: html`
                <p>The Pipeline turns SAGE into an agent-to-agent message bus. Instead of copy-pasting between Claude, Perplexity, and ChatGPT, agents can send work to each other through SAGE. The pipeline is ephemeral — messages auto-expire, and only a journal summary persists as a memory.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Sending work</div>
                        <div class="guide-detail-desc">From any connected agent, call <code>sage_pipe(to="perplexity", intent="research", payload="...")</code>. The <strong>to</strong> field accepts a provider name (e.g. "perplexity", "chatgpt") or a specific agent_id hex string. The message is stored in SAGE and waits for the target agent to pick it up.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Receiving work</div>
                        <div class="guide-detail-desc">Agents discover pipeline items automatically on their next <code>sage_turn</code> call — no extra action needed. Items appear in the <code>pipe_inbox</code> field of the turn response. Agents can also explicitly call <code>sage_inbox</code> to check for work.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Returning results</div>
                        <div class="guide-detail-desc">After processing a pipeline item, the receiving agent calls <code>sage_pipe_result(pipe_id="...", result="...")</code>. This marks the pipe as completed and auto-creates a journal memory summarizing the exchange. The sending agent sees the result on their next <code>sage_turn</code>.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Routing</div>
                        <div class="guide-detail-desc"><strong>By provider:</strong> Address by name (e.g. "perplexity") — any agent with that provider can claim it. First-come-first-served for same-provider agents. <strong>By agent_id:</strong> Address with the 64-character hex ID — only that specific agent sees it.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Auto-claiming</div>
                        <div class="guide-detail-desc">When an agent views items via <code>sage_inbox</code>, they are automatically claimed — preventing other agents of the same provider from double-processing. This is atomic at the database level.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">TTL & expiry</div>
                        <div class="guide-detail-desc">Pipeline messages have a time-to-live (default: 60 minutes, max: 24 hours). Expired messages are automatically cleaned up. Completed messages are purged after 24 hours — only the journal summary persists as a committed memory.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Dashboard</div>
                        <div class="guide-detail-desc">The Pipeline page in CEREBRUM shows all messages with status filters (pending, claimed, completed, expired, failed). Click any message to expand and see the full payload, result, and metadata. Auto-refreshes every 10 seconds.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Auto-journal</div>
                        <div class="guide-detail-desc">When a pipe completes, SAGE automatically creates a one-line observation memory in the "agent-pipeline" domain summarizing who asked whom to do what, and a preview of the result. This gives you a persistent audit trail without storing the full payload.</div>
                    </div>
                </div>
                <div class="guide-callout">
                    <strong>Key difference from Tasks:</strong> Pipeline messages are ephemeral work routing between machines. Tasks are your persistent backlog tracked across sessions. Pipeline auto-expires; tasks persist until you mark them done.
                </div>
            `,
        },
        {
            key: 'settings',
            title: 'Settings & Maintenance',
            icon: html`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>`,
            summary: 'Auto-cleanup, MCP configuration, chain health, and peer info.',
            content: html`
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Auto-cleanup</div>
                        <div class="guide-detail-desc">Enable auto-cleanup to automatically remove stale observations and low-confidence memories over time. This keeps your brain lean and focused on high-value knowledge. Configure the cleanup interval and minimum confidence threshold.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">MCP config</div>
                        <div class="guide-detail-desc">The Settings page shows your MCP configuration snippet. Copy this into your AI client's MCP config file to connect your assistant to SAGE. The config includes the server URL and authentication details.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Chain health</div>
                        <div class="guide-detail-desc">Monitor your consensus chain: current block height, validator count, latest block time, and sync status. A healthy chain produces blocks every few seconds with all validators participating.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Peers</div>
                        <div class="guide-detail-desc">View connected peers on your SAGE network. Each peer is another node running sage-gui that participates in consensus. Peers sync memories and validate each other's submissions through BFT.</div>
                    </div>
                </div>
            `,
        },
    ];

    const activeSection = sections.find(s => s.key === openSection) || sections[0];

    return html`
        <div class="help-overlay" onClick=${(e) => { if (e.target === e.currentTarget) handleDismiss(); }}>
            <div class="help-modal guide-modal">
                <div class="help-modal-header">
                    <h2>CEREBRUM Guide</h2>
                    <button class="detail-close" onClick=${handleDismiss}>×</button>
                </div>
                <div class="guide-tabs">
                    ${sections.map(s => html`
                        <button key=${s.key}
                                class="guide-tab ${(openSection || sections[0].key) === s.key ? 'active' : ''}"
                                onClick=${() => selectSection(s.key)}
                                title=${s.summary}>
                            <span class="guide-tab-icon">${s.icon}</span>
                            <span class="guide-tab-label">${s.title}</span>
                        </button>
                    `)}
                </div>
                <div class="guide-tab-content">
                    <div class="guide-section-content ${animClass}">${activeSection.content}</div>
                </div>
                <div class="help-footer">
                    <label class="help-dismiss-check">
                        <input type="checkbox" checked=${dontShow}
                               onChange=${(e) => setDontShow(e.target.checked)} />
                        Don't show again
                    </label>
                    <button class="btn" style="background: var(--primary); color: #fff; border-color: var(--primary); font-weight: 600;"
                            onClick=${handleDismiss}>Got it</button>
                </div>
            </div>
        </div>
    `;
}

// ============================================================================
// Login Screen (shown when vault encryption requires auth)
// ============================================================================

function LoginScreen({ onSuccess, nextHint }) {
    const [passphrase, setPassphrase] = useState('');
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);
    const [showRecovery, setShowRecovery] = useState(false);
    const [recoveryKey, setRecoveryKey] = useState('');
    const [newPassphrase, setNewPassphrase] = useState('');
    const [recoverySuccess, setRecoverySuccess] = useState(false);

    async function handleSubmit(e) {
        e.preventDefault();
        if (!passphrase) return;
        setLoading(true);
        setError('');
        try {
            const res = await login(passphrase);
            if (res.ok) {
                onSuccess();
            } else {
                setError(res.error || 'Wrong passphrase');
            }
        } catch (err) {
            setError('Connection failed');
        }
        setLoading(false);
    }

    async function handleRecover(e) {
        e.preventDefault();
        if (!recoveryKey || !newPassphrase) return;
        if (newPassphrase.length < 8) { setError('New passphrase must be at least 8 characters'); return; }
        setLoading(true);
        setError('');
        try {
            const res = await recoverVault(recoveryKey.trim(), newPassphrase);
            if (res.ok) {
                setRecoverySuccess(true);
                setShowRecovery(false);
                setPassphrase(newPassphrase);
                // Auto-login with the new passphrase.
                const loginRes = await login(newPassphrase);
                if (loginRes.ok) {
                    onSuccess();
                }
            } else {
                setError(res.error || 'Recovery failed — check your recovery key');
            }
        } catch (err) {
            setError('Connection failed');
        }
        setLoading(false);
    }

    if (showRecovery) {
        return html`
            <div class="login-screen">
                <div class="login-card">
                    <div class="login-icon">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--accent, #a78bfa)" stroke-width="1.5">
                            <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>
                        </svg>
                    </div>
                    <h2 class="login-title">Vault Recovery</h2>
                    <p class="login-subtitle">Enter your recovery key to reset your passphrase.</p>
                    <form onSubmit=${handleRecover}>
                        <textarea
                            class="login-input"
                            placeholder="Paste your recovery key (base64)"
                            value=${recoveryKey}
                            onInput=${e => setRecoveryKey(e.target.value)}
                            rows="2"
                            style="resize: vertical; font-family: monospace; font-size: 0.85em;"
                            autofocus
                        />
                        <input
                            type="password"
                            class="login-input"
                            placeholder="New passphrase (min 8 characters)"
                            value=${newPassphrase}
                            onInput=${e => setNewPassphrase(e.target.value)}
                            style="margin-top: 8px;"
                        />
                        ${error && html`<div class="login-error">${error}</div>`}
                        <button type="submit" class="login-btn" disabled=${loading || !recoveryKey || !newPassphrase}>
                            ${loading ? 'Recovering...' : 'Reset Passphrase'}
                        </button>
                    </form>
                    <button class="login-recover-link" onClick=${() => { setShowRecovery(false); setError(''); }}>
                        Back to login
                    </button>
                </div>
            </div>
        `;
    }

    return html`
        <div class="login-screen">
            <div class="login-card">
                <div class="login-icon">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--accent, #a78bfa)" stroke-width="1.5">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                        <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                        <circle cx="12" cy="16" r="1"/>
                    </svg>
                </div>
                <h2 class="login-title">SAGE is Encrypted</h2>
                <p class="login-subtitle">Enter your vault passphrase to unlock CEREBRUM.</p>
                <form onSubmit=${handleSubmit}>
                    <input
                        type="password"
                        class="login-input"
                        placeholder="Vault passphrase"
                        value=${passphrase}
                        onInput=${e => setPassphrase(e.target.value)}
                        autofocus
                    />
                    ${error && html`<div class="login-error">${error}</div>`}
                    <button type="submit" class="login-btn" disabled=${loading || !passphrase}>
                        ${loading ? 'Unlocking...' : 'Unlock'}
                    </button>
                </form>
                ${recoverySuccess && html`<div class="login-success">Passphrase reset successfully!</div>`}
                <button class="login-recover-link" onClick=${() => { setShowRecovery(true); setError(''); }}>
                    Lost your passphrase? Use recovery key
                </button>
            </div>
        </div>
    `;
}

// ============================================================================
// Network Page — Agent Management
// ============================================================================

const CLEARANCE_LABELS = ['Public', 'Internal', 'Confidential', 'Secret', 'Top Secret'];

// Role metadata. Role is immutable after on-chain registration, so the Access
// tab renders it read-only (no editable selector) to avoid reporting a "Saved"
// that never reaches consensus.
const ROLE_META = {
    admin: { name: 'Admin', desc: 'Full access. Can manage agents and network settings.' },
    member: { name: 'Member', desc: 'Read/write within allowed domains. Cannot manage agents.' },
    observer: { name: 'Observer', desc: 'Read-only. Can view memories but cannot submit.' },
};
const AGENT_EMOJIS = ['🤖', '🧠', '🎙️', '🔬', '👤', '🛡️', '📡', '🔮', '🦉', '🐺', '🌐', '💎'];

const DEPLOY_PHASES = [
    { key: 'LOCK_ACQUIRED', label: 'Acquiring lock' },
    { key: 'BACKUP_CREATED', label: 'Creating backup' },
    { key: 'CHAIN_STOPPED', label: 'Stopping chain' },
    { key: 'GENESIS_GENERATED', label: 'Generating genesis' },
    { key: 'CHAIN_STATE_WIPED', label: 'Wiping chain state' },
    { key: 'CHAIN_RESTARTED', label: 'Restarting chain' },
    { key: 'CONSENSUS_VERIFIED', label: 'Verifying consensus' },
    { key: 'RBAC_CONFIGURED', label: 'Configuring access' },
    { key: 'COMPLETED', label: 'Complete' },
];

function DeployProgress({ currentPhase, status, error }) {
    const currentIdx = DEPLOY_PHASES.findIndex(p => p.key === currentPhase);
    const isFailed = status === 'failed';
    const isComplete = currentPhase === 'COMPLETED' && !isFailed;
    const isRunning = !isFailed && !isComplete;
    const [elapsed, setElapsed] = useState(0);

    useEffect(() => {
        if (!isRunning) return;
        const start = Date.now();
        const timer = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000);
        return () => clearInterval(timer);
    }, [isRunning]);

    return html`
        <div class="deploy-progress">
            ${isRunning && html`
                <div class="deploy-timer">
                    <span class="deploy-spinner" style="width:12px;height:12px;border-width:1.5px;"></span>
                    <span style="color:var(--text-muted);font-size:12px;">Elapsed: ${elapsed}s</span>
                </div>
            `}
            ${DEPLOY_PHASES.map((phase, i) => {
                let phaseStatus = 'pending';
                if (isFailed && i === currentIdx) phaseStatus = 'failed';
                else if (i < currentIdx || isComplete) phaseStatus = 'completed';
                else if (i === currentIdx) phaseStatus = 'in-progress';

                return html`
                    <div class="deploy-phase ${phaseStatus}" key=${phase.key}>
                        <div class="deploy-phase-icon">
                            ${phaseStatus === 'completed' && html`<span class="deploy-check">✓</span>`}
                            ${phaseStatus === 'in-progress' && html`<span class="deploy-spinner"></span>`}
                            ${phaseStatus === 'failed' && html`<span class="deploy-fail">✗</span>`}
                            ${phaseStatus === 'pending' && html`<span class="deploy-pending">○</span>`}
                        </div>
                        <span class="deploy-phase-label">${phase.label}</span>
                    </div>
                `;
            })}
            ${isFailed && error && html`
                <div class="deploy-error">${error}</div>
            `}
        </div>
    `;
}

// --- Domain Access Matrix ---
function DomainAccessMatrix({ domains, domainAccess, onChange, onAddDomain, disabled }) {
    const [filter, setFilter] = useState('');
    const [newDomain, setNewDomain] = useState('');
    // Merge externally-passed domains with any custom-added domains in domainAccess
    const allDomains = [...new Set([...domains, ...Object.keys(domainAccess)])].sort();
    const filtered = allDomains.filter(d => !filter || d.toLowerCase().includes(filter.toLowerCase()));

    const toggle = (domain, field) => {
        const cur = domainAccess[domain] || { read: false, write: false };
        const upd = { ...cur, [field]: !cur[field] };
        if (field === 'write' && upd.write) upd.read = true;
        if (field === 'read' && !upd.read) upd.write = false;
        onChange({ ...domainAccess, [domain]: upd });
    };
    const bulkSet = (field, val) => {
        const upd = { ...domainAccess };
        allDomains.forEach(d => {
            upd[d] = { ...(upd[d] || { read: false, write: false }), [field]: val };
            if (field === 'write' && val) upd[d].read = true;
            if (field === 'read' && !val) upd[d].write = false;
        });
        onChange(upd);
    };
    const handleAddDomain = () => {
        const cleaned = newDomain.trim().toLowerCase().replace(/[^a-z0-9._-]/g, '-');
        if (!cleaned) return;
        // Add with read+write enabled
        onChange({ ...domainAccess, [cleaned]: { read: true, write: true } });
        if (onAddDomain) onAddDomain(cleaned);
        setNewDomain('');
    };

    if (disabled) return html`<div class="domain-matrix"><div class="domain-matrix-empty" style="color:var(--accent);">Admins have full access to all domains.</div></div>`;

    return html`
        <div class="domain-matrix">
            <div class="domain-matrix-header">
                <input class="domain-matrix-search" type="text" placeholder="Filter domains..." value=${filter}
                    onInput=${e => setFilter(e.target.value)} onClick=${e => e.stopPropagation()} />
                <div class="domain-matrix-bulk">
                    <button onClick=${e => { e.stopPropagation(); bulkSet('read', true); }}>All Read</button>
                    <button onClick=${e => { e.stopPropagation(); bulkSet('write', true); }}>All Write</button>
                    <button onClick=${e => { e.stopPropagation(); bulkSet('read', false); }}>Revoke All</button>
                </div>
            </div>
            <div class="domain-matrix-add" onClick=${e => e.stopPropagation()}>
                <input class="domain-matrix-search" type="text" placeholder="Add new domain tag..."
                    value=${newDomain} onInput=${e => setNewDomain(e.target.value)}
                    onKeyDown=${e => { if (e.key === 'Enter') { e.preventDefault(); handleAddDomain(); } }} />
                <button class="domain-add-btn" onClick=${handleAddDomain} disabled=${!newDomain.trim()}>+ Add</button>
            </div>
            <div class="domain-matrix-columns"><span>Domain</span><span style="text-align:center;">Read</span><span style="text-align:center;">Write</span></div>
            <div class="domain-matrix-body">
                ${filtered.length === 0 && allDomains.length > 0 ? html`<div class="domain-matrix-no-results">No domains matching "${filter}"</div>` : ''}
                ${allDomains.length === 0 ? html`<div class="domain-matrix-empty">No domains yet. Add one above or they'll appear as memories are submitted.</div>` : ''}
                ${filtered.map(domain => {
                    const a = domainAccess[domain] || { read: false, write: false };
                    const isCustom = !domains.includes(domain);
                    return html`<div class="domain-matrix-row ${isCustom ? 'custom' : ''}" key=${domain}>
                        <div class="domain-matrix-domain">
                            <span class="domain-matrix-dot" style="background:${getDomainColor(domain)};"></span>
                            ${domain}
                            ${isCustom ? html`<span style="font-size:10px;color:var(--text-muted);margin-left:6px;">new</span>` : ''}
                        </div>
                        <div class="domain-matrix-toggle" onClick=${e => e.stopPropagation()}>
                            <label class="toggle-switch"><input type="checkbox" checked=${a.read} onChange=${() => toggle(domain, 'read')} /><span class="toggle-slider"></span></label>
                        </div>
                        <div class="domain-matrix-toggle" onClick=${e => e.stopPropagation()}>
                            <label class="toggle-switch"><input type="checkbox" checked=${a.write} onChange=${() => toggle(domain, 'write')} /><span class="toggle-slider"></span></label>
                        </div>
                    </div>`;
                })}
            </div>
        </div>
    `;
}

// --- Activity Tab ---
function ActivityTab({ agent }) {
    const [memories, setMemories] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        setLoading(true);
        fetchMemories({ agent: agent.agent_id, limit: 20, sort: 'newest' })
            .then(data => setMemories(data.memories || []))
            .catch(() => {})
            .finally(() => setLoading(false));
    }, [agent.agent_id]);

    const uniqueDomains = [...new Set(memories.map(m => m.domain))];

    return html`
        <div class="activity-tab">
            <div class="activity-stats-row">
                <div class="activity-stat-card">
                    <div class="activity-stat-value">${agent.memory_count || 0}</div>
                    <div class="activity-stat-label">Total Memories</div>
                </div>
                <div class="activity-stat-card">
                    <div class="activity-stat-value">${uniqueDomains.length}</div>
                    <div class="activity-stat-label">Domains</div>
                </div>
                <div class="activity-stat-card">
                    <div class="activity-stat-value">${agent.last_seen ? timeAgo(agent.last_seen) : 'Never'}</div>
                    <div class="activity-stat-label">Last Active</div>
                </div>
            </div>
            <div class="access-section-title">Recent Memories</div>
            ${loading ? html`<div style="text-align:center;padding:20px;color:var(--text-muted);">Loading...</div>` :
              memories.length === 0 ? html`<div class="activity-empty">No memories from this agent yet.</div>` : html`
                <div class="activity-memory-list">
                    ${memories.map(m => html`
                        <div class="activity-memory-item" key=${m.id}>
                            <span class="activity-memory-domain" style="background:${getDomainColor(m.domain)}20;color:${getDomainColor(m.domain)};">${m.domain}</span>
                            <span class="activity-memory-content">${m.key || (m.content || '').substring(0, 80)}</span>
                            <span class="activity-memory-time">${timeAgo(m.created_at)}</span>
                        </div>
                    `)}
                </div>
            `}
        </div>
    `;
}

// --- Network Page (Accordion) ---
function NetworkPage({ sse }) {
    const [agents, setAgents] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showWizard, setShowWizard] = useState(false);
    const [expandedId, setExpandedId] = useState(null);
    const [expandedTab, setExpandedTab] = useState('overview');
    const [showRemoveConfirm, setShowRemoveConfirm] = useState(null);
    const [redeployStatus, setRedeployStatus] = useState(null);
    const [allDomains, setAllDomains] = useState([]);
    const redeployPollRef = useRef(null);

    // Access control state
    const [editRole, setEditRole] = useState('');
    const [editClearance, setEditClearance] = useState(1);
    const [editDomainAccess, setEditDomainAccess] = useState({});
    const [accessDirty, setAccessDirty] = useState(false);
    const [accessSaved, setAccessSaved] = useState(false);
    const [editVisibleAgents, setEditVisibleAgents] = useState('');
    // Edit mode state
    const [editing, setEditing] = useState(false);
    const [editName, setEditName] = useState('');
    const [editBio, setEditBio] = useState('');
    // Key rotation state
    const [showRotateConfirm, setShowRotateConfirm] = useState(null);
    const [rotating, setRotating] = useState(false);
    // Unregistered agents state
    const [unregistered, setUnregistered] = useState([]);
    const [mergeTarget, setMergeTarget] = useState(null); // {source, target}
    const [merging, setMerging] = useState(false);
    // Tag transfer state
    const [tagTransfer, setTagTransfer] = useState(null); // { agentId, agentName, tags: [], step: 'tags'|'target', selectedTag: null }
    const [transferring, setTransferring] = useState(false);

    // External-client wizard state (v6.7.3). remoteWizardTarget tailors the
    // shared RemoteAccessWizard's final step: 'chatgpt' → OAuth connector fields,
    // 'tool' → bearer paste blocks for a remote IDE client.
    const [showChatGPTWizard, setShowChatGPTWizard] = useState(false);
    const [remoteWizardTarget, setRemoteWizardTarget] = useState('chatgpt');
    const [showCursorPanel, setShowCursorPanel] = useState(false);
    // Same-machine connect entry (Phase 5b-1)
    const [showConnectTool, setShowConnectTool] = useState(false);
    // LAN node-join guest entry (Phase 5b-3)
    const [showJoinGuest, setShowJoinGuest] = useState(false);

    // Governance state
    const [govProposals, setGovProposals] = useState([]);
    const [activeProposal, setActiveProposal] = useState(null);
    const [govHistoryOpen, setGovHistoryOpen] = useState(false);
    const [showGovModal, setShowGovModal] = useState(false);
    const [govSubmitting, setGovSubmitting] = useState(false);
    const [govVoting, setGovVoting] = useState(false);
    // Local record of votes cast from this dashboard, keyed by proposal_id. The
    // votes array from the detail endpoint has no self-marker, so we remember
    // our own decision here to render the "You voted" badge across reloads.
    const [myVotes, setMyVotes] = useState({});
    const [govNewOp, setGovNewOp] = useState('add_validator');
    const [govNewTarget, setGovNewTarget] = useState('');
    const [govNewPower, setGovNewPower] = useState('10');
    const [govNewReason, setGovNewReason] = useState('');
    const [currentHeight, setCurrentHeight] = useState(0);
    const govPollRef = useRef(null);

    const loadAgents = useCallback(async () => {
        try {
            const data = await fetchAgents();
            setAgents(data.agents || []);
        } catch (e) { console.error(e); }
        finally { setLoading(false); }
    }, []);

    const loadUnregistered = useCallback(async () => {
        try {
            const data = await fetchUnregisteredAgents();
            setUnregistered(data.unregistered || []);
        } catch (e) { console.error(e); }
    }, []);

    const loadGovProposals = useCallback(async () => {
        try {
            const data = await fetchGovProposals();
            const proposals = data.proposals || [];
            setGovProposals(proposals);
            const active = proposals.find(p => p.status === 'voting');
            setActiveProposal(active || null);
        } catch (e) { /* governance endpoint may not exist yet */ }
    }, []);

    useEffect(() => {
        loadAgents();
        loadUnregistered();
        loadGovProposals();
        fetchStats().then(data => {
            if (data?.by_domain) setAllDomains(Object.keys(data.by_domain).sort());
        }).catch(() => {});
        // Fetch current block height
        fetchHealth().then(h => {
            if (h?.chain?.block_height) setCurrentHeight(Number(h.chain.block_height));
        }).catch(() => {});
    }, []);

    // Poll active proposal detail + block height every 3s while one is active
    useEffect(() => {
        if (!activeProposal) {
            if (govPollRef.current) { clearInterval(govPollRef.current); govPollRef.current = null; }
            return;
        }
        if (govPollRef.current) return;
        govPollRef.current = setInterval(async () => {
            try {
                const [detail, health] = await Promise.all([
                    fetchGovProposalDetail(activeProposal.proposal_id),
                    fetchHealth()
                ]);
                if (detail) {
                    const base = detail.proposal || detail;
                    const votes = detail.votes || [];
                    const q = detail.quorum_progress || {};
                    let accept = 0, reject = 0, abstain = 0;
                    for (const v of votes) {
                        if (v.decision === 'accept') accept++;
                        else if (v.decision === 'reject') reject++;
                        else if (v.decision === 'abstain') abstain++;
                    }
                    setActiveProposal({
                        ...base,
                        accept_power: q.accept_power || 0,
                        total_power: q.total_power || 0,
                        accept_count: accept,
                        reject_count: reject,
                        abstain_count: abstain,
                    });
                    // If proposal is no longer voting, refresh the full list
                    if (base.status !== 'voting') {
                        loadGovProposals();
                    }
                }
                if (health?.chain?.block_height) setCurrentHeight(Number(health.chain.block_height));
            } catch (e) { /* ignore polling errors */ }
        }, 3000);
        return () => { if (govPollRef.current) { clearInterval(govPollRef.current); govPollRef.current = null; } };
    }, [activeProposal?.proposal_id, loadGovProposals]);

    // SSE governance events — auto-refresh on governance activity
    useEffect(() => {
        if (!sse) return;
        const unsub = sse.on('governance', () => {
            loadGovProposals();
            fetchHealth().then(h => {
                if (h?.chain?.block_height) setCurrentHeight(Number(h.chain.block_height));
            }).catch(() => {});
        });
        return unsub;
    }, [sse, loadGovProposals]);

    // Redeploy polling
    const startRedeployPoll = useCallback(() => {
        if (redeployPollRef.current) return;
        let sawRunning = false, ticks = 0;
        redeployPollRef.current = setInterval(async () => {
            try {
                ticks++;
                const s = await fetchRedeployStatus();
                setRedeployStatus(s);
                if (s && s.status === 'running') { sawRunning = true; return; }
                // Only end THIS run once we've seen it reach 'running' (guards a stale
                // prior-run verdict), with a ~15s safety stop if it never registers.
                if (sawRunning || ticks > 15 || !s) {
                    clearInterval(redeployPollRef.current);
                    redeployPollRef.current = null;
                    loadAgents();
                }
            } catch (e) { clearInterval(redeployPollRef.current); redeployPollRef.current = null; }
        }, 1000);
    }, [loadAgents]);

    useEffect(() => {
        fetchRedeployStatus().then(s => {
            if (s?.active === true) { setRedeployStatus(s); startRedeployPoll(); }
        }).catch(() => {});
        return () => { if (redeployPollRef.current) clearInterval(redeployPollRef.current); };
    }, []);

    const toggleExpand = useCallback((agent) => {
        if (expandedId === agent.agent_id) {
            setExpandedId(null); setEditing(false);
        } else {
            setExpandedId(agent.agent_id);
            setExpandedTab('overview');
            setEditing(false);
            setEditName(agent.name);
            setEditBio(agent.boot_bio || '');
            setEditRole(agent.role);
            setEditClearance(agent.clearance);
            setAccessDirty(false);
            setAccessSaved(false);
            setEditVisibleAgents(agent.visible_agents || '');
            // Parse domain_access
            let parsed = {};
            try {
                const arr = JSON.parse(agent.domain_access || '[]');
                arr.forEach(e => { parsed[e.domain] = { read: !!e.read, write: !!e.write }; });
            } catch (e) {}
            setEditDomainAccess(parsed);
        }
    }, [expandedId]);

    const handleAccessSave = useCallback(async (agentId) => {
        const arr = Object.entries(editDomainAccess)
            .filter(([_, v]) => v.read || v.write)
            .map(([domain, p]) => ({ domain, read: p.read, write: p.write }));
        try {
            const res = await updateAgent(agentId, { clearance: editClearance, domain_access: JSON.stringify(arr), visible_agents: editVisibleAgents });
            if (res.error) { showToast(res.error, 'error'); return; }
            if (res.on_chain_warning) {
                showToast('Access saved locally but on-chain RBAC grant failed - will auto-heal on next agent boot. (' + res.on_chain_warning + ')', 'warning', 8000);
            }
            loadAgents();
            setAccessDirty(false);
            setAccessSaved(true);
            setTimeout(() => setAccessSaved(false), 2000);
        } catch (e) { showToast('Failed to save access: ' + e.message, 'error'); }
    }, [editRole, editClearance, editDomainAccess, editVisibleAgents, loadAgents]);

    const handleOverviewSave = useCallback(async (agentId) => {
        try {
            const res = await updateAgent(agentId, { name: editName, boot_bio: editBio });
            if (res.error) { showToast(res.error, 'error'); return; }
            if (res.on_chain_warning) {
                showToast('Saved locally but on-chain sync failed - will auto-heal on next agent boot. (' + res.on_chain_warning + ')', 'warning', 8000);
            }
            loadAgents();
            setEditing(false);
        } catch (e) { showToast('Failed to save agent: ' + e.message, 'error'); }
    }, [editName, editBio, loadAgents]);

    const handleRemove = useCallback(async (agent) => {
        try {
            const res = await removeAgent(agent.agent_id, true);
            if (res.error) { showToast(res.error, 'error'); return; }
            const rdRes = await startRedeploy('remove_agent', agent.agent_id);
            if (rdRes.error) showToast('Agent removed but redeployment failed: ' + rdRes.error, 'warning');
            else { setRedeployStatus(rdRes); if (rdRes.status !== 'completed') startRedeployPoll(); } // single-node: instant, no poll
            setShowRemoveConfirm(null); setExpandedId(null); loadAgents();
        } catch (e) { showToast('Failed to remove agent', 'error'); }
    }, [loadAgents, startRedeployPoll]);

    const handleRotateKey = useCallback(async (agent) => {
        setRotating(true);
        try {
            const res = await rotateAgentKey(agent.agent_id);
            if (res.error) { showToast(res.error, 'error'); setRotating(false); return; }
            const rdRes = await startRedeploy('rotate_key', res.new_agent_id);
            if (rdRes.error) showToast('Key rotated but redeployment failed: ' + rdRes.error, 'warning');
            else { setRedeployStatus(rdRes); if (rdRes.status !== 'completed') startRedeployPoll(); } // single-node: instant, no poll
            setShowRotateConfirm(null); setExpandedId(null); loadAgents();
        } catch (e) { showToast('Failed to rotate key', 'error'); }
        setRotating(false);
    }, [loadAgents, startRedeployPoll]);

    const handleMerge = useCallback(async (sourceId, targetId) => {
        setMerging(true);
        try {
            const res = await mergeAgent(sourceId, targetId);
            if (res.error) { showToast(res.error, 'error'); setMerging(false); return; }
            showToast(res.message || 'Memories merged successfully.', 'success');
            setMergeTarget(null);
            loadAgents();
            loadUnregistered();
        } catch (e) { showToast('Failed to merge: ' + e.message, 'error'); }
        setMerging(false);
    }, [loadAgents, loadUnregistered]);

    const loadAgentTags = useCallback(async (agent) => {
        try {
            const data = await fetchAgentTags(agent.agent_id);
            setTagTransfer({
                agentId: agent.agent_id,
                agentName: agent.name || agent.agent_id.slice(0, 16),
                tags: data.tags || [],
                step: 'tags',
                selectedTag: null
            });
        } catch (e) { showToast('Failed to load agent tags', 'error'); }
    }, []);

    const handleTagTransfer = useCallback(async (targetId) => {
        if (!tagTransfer?.selectedTag) return;
        setTransferring(true);
        try {
            const res = await transferTag(tagTransfer.agentId, targetId, tagTransfer.selectedTag.tag);
            if (res.error) { showToast(res.error, 'error'); setTransferring(false); return; }
            showToast(res.message || `${res.memories_moved} memories transferred`, 'success');
            setTagTransfer(null);
            loadAgents();
            loadUnregistered();
        } catch (e) { showToast('Transfer failed: ' + e.message, 'error'); }
        setTransferring(false);
    }, [tagTransfer, loadAgents, loadUnregistered]);

    // Governance handlers
    const handleGovVote = useCallback(async (proposalId, decision) => {
        setGovVoting(true);
        try {
            const res = await submitGovVote(proposalId, decision);
            if (res.error) { showToast(res.error, 'error'); }
            else {
                showToast(`Vote submitted: ${decision}`, 'success');
                setMyVotes(m => ({ ...m, [proposalId]: decision }));
            }
            loadGovProposals();
        } catch (e) { showToast('Vote failed: ' + e.message, 'error'); }
        setGovVoting(false);
    }, [loadGovProposals]);

    const handleGovSubmit = useCallback(async () => {
        setGovSubmitting(true);
        try {
            const proposal = {
                operation: govNewOp,
                target_id: govNewTarget,
                reason: govNewReason,
            };
            if (govNewOp === 'add_validator' || govNewOp === 'update_power') {
                proposal.target_power = parseInt(govNewPower, 10) || 10;
            }
            const res = await submitGovProposal(proposal);
            if (res.error) { showToast(res.error, 'error'); }
            else {
                showToast('Proposal submitted successfully', 'success');
                setShowGovModal(false);
                setGovNewOp('add_validator');
                setGovNewTarget('');
                setGovNewPower('10');
                setGovNewReason('');
            }
            loadGovProposals();
        } catch (e) { showToast('Proposal failed: ' + e.message, 'error'); }
        setGovSubmitting(false);
    }, [govNewOp, govNewTarget, govNewPower, govNewReason, loadGovProposals]);

    // Governance helpers
    const govOpLabel = (op) => {
        const labels = { add_validator: 'Add Validator', remove_validator: 'Remove Validator', update_power: 'Update Power' };
        return labels[op] || op;
    };
    const govOpIcon = (op) => {
        const icons = { add_validator: '+', remove_validator: '-', update_power: '~' };
        return icons[op] || '?';
    };
    const resolveAgentName = (agentId) => {
        if (!agentId) return 'Unknown';
        const a = agents.find(a => a.agent_id === agentId);
        return a ? a.name : agentId.slice(0, 16) + '...';
    };
    const govStatusBadge = (status) => {
        return html`<span class="gov-status-badge gov-status-${status}">${status.charAt(0).toUpperCase() + status.slice(1)}</span>`;
    };
    const pastProposals = govProposals.filter(p => p.status !== 'voting');

    if (loading) return html`<div class="network-page"><p style="color:var(--text-muted);text-align:center;padding:40px;">Loading agents...</p></div>`;

    const isRedeploying = redeployStatus?.active === true;

    return html`
        <div class="network-page fade-in">
            ${isRedeploying && html`<div class="redeploy-banner"><span class="deploy-spinner"></span> Network reconfiguration in progress...${redeployStatus.operation ? ` (${redeployStatus.operation.replace(/_/g, ' ')})` : ''}</div>`}
            <div class="network-header">
                <div><h2>Agents <${HelpTip} text="Manage the agents on your own SAGE node. Each agent is a separate participant in BFT consensus with its own permissions. Click any agent to expand its details and access. (To connect your whole node to ANOTHER SAGE, use Federation.)" /><${PageHelp} section="network" label="Agents guide" /></h2><div class="network-header-sub">${agents.length} agent${agents.length !== 1 ? 's' : ''} on this node</div></div>
            </div>

            <div class="gov-section">
                <div class="gov-section-header">
                    <h3>Governance <${HelpTip} text="Validator governance allows network participants to propose and vote on changes to the validator set. Proposals require 2/3 quorum to pass." /></h3>
                    ${!activeProposal && html`<button class="gov-new-btn" onClick=${() => setShowGovModal(true)}>+ New Proposal</button>`}
                </div>

                ${activeProposal ? html`
                    <div class="gov-proposal-card active">
                        <div class="gov-proposal-header">
                            <div class="gov-proposal-op">
                                <span>${govOpIcon(activeProposal.operation)}</span>
                                ${govOpLabel(activeProposal.operation)}: ${resolveAgentName(activeProposal.target_agent_id)}
                            </div>
                            ${govStatusBadge('voting')}
                        </div>
                        <div class="gov-proposal-meta">
                            Proposed by: ${resolveAgentName(activeProposal.proposer_id)}${activeProposal.created_height ? ` \u00b7 Block #${activeProposal.created_height}` : ''}
                        </div>
                        ${activeProposal.reason && html`
                            <div class="gov-proposal-reason">"${activeProposal.reason}"</div>
                        `}
                        <div class="gov-vote-tally">
                            <span class="gov-vote-accept">Accept: ${activeProposal.accept_count || 0}</span>
                            <span class="gov-vote-reject">Reject: ${activeProposal.reject_count || 0}</span>
                            <span class="gov-vote-abstain">Abstain: ${activeProposal.abstain_count || 0}</span>
                        </div>
                        <div class="gov-quorum-row">
                            <div class="gov-quorum-bar">
                                <div class="gov-quorum-fill" style=${`width: ${Math.min(100, ((activeProposal.accept_power || 0) / Math.max(1, activeProposal.total_power || 1)) * 100)}%`}></div>
                                <div class="gov-quorum-threshold" style="left: 66.7%;" title="2/3 quorum threshold"></div>
                            </div>
                            <span class="gov-quorum-label">${activeProposal.accept_power || 0}/${activeProposal.total_power || '?'} power (need 2/3)</span>
                        </div>
                        ${activeProposal.expiry_height && currentHeight > 0 ? html`
                            <div class="gov-expiry">
                                Expires in: ${Math.max(0, activeProposal.expiry_height - currentHeight)} blocks
                                (~${Math.max(0, Math.round((activeProposal.expiry_height - currentHeight) * 3))}s)
                            </div>
                        ` : ''}
                        <div class="gov-vote-actions">
                            ${(myVotes[activeProposal.proposal_id] || activeProposal.my_vote) ? html`
                                <div class="gov-voted-badge">
                                    You voted: <strong>${myVotes[activeProposal.proposal_id] || activeProposal.my_vote}</strong>
                                </div>
                            ` : html`
                                <button class="gov-vote-btn accept" onClick=${() => handleGovVote(activeProposal.proposal_id, 'accept')} disabled=${govVoting}>Accept</button>
                                <button class="gov-vote-btn reject" onClick=${() => handleGovVote(activeProposal.proposal_id, 'reject')} disabled=${govVoting}>Reject</button>
                                <button class="gov-vote-btn abstain" onClick=${() => handleGovVote(activeProposal.proposal_id, 'abstain')} disabled=${govVoting}>Abstain</button>
                            `}
                        </div>
                    </div>
                ` : html`
                    <div class="gov-empty">
                        <div class="gov-empty-icon">${'\u{1F5F3}\uFE0F'}</div>
                        No active proposals
                    </div>
                `}

                ${pastProposals.length > 0 && html`
                    <button class="gov-history-toggle ${govHistoryOpen ? 'open' : ''}" onClick=${() => setGovHistoryOpen(!govHistoryOpen)}>
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                        Past Proposals (${pastProposals.length})
                    </button>
                    ${govHistoryOpen && html`
                        <div class="gov-history-list">
                            ${pastProposals.map(p => html`
                                <div class="gov-history-card" key=${p.proposal_id}>
                                    <div class="gov-history-info">
                                        <div class="gov-history-op">${govOpLabel(p.operation)}: ${resolveAgentName(p.target_agent_id)}</div>
                                        <div class="gov-history-detail">
                                            by ${resolveAgentName(p.proposer_id)}${p.created_height ? ` \u00b7 Block #${p.created_height}` : ''}
                                        </div>
                                    </div>
                                    <div class="gov-history-right">
                                        <div class="gov-history-votes">
                                            <span class="gov-vote-accept">${p.accept_count || 0}A</span>
                                            <span class="gov-vote-reject">${p.reject_count || 0}R</span>
                                        </div>
                                        ${govStatusBadge(p.status)}
                                    </div>
                                </div>
                            `)}
                        </div>
                    `}
                `}
            </div>

            ${showGovModal && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setShowGovModal(false); }}>
                    <div class="wizard-modal" style="max-width:520px;">
                        <div class="wizard-header">
                            <h2>New Governance Proposal</h2>
                            <button class="detail-close" onClick=${() => setShowGovModal(false)}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <div class="wizard-field">
                                <label>Operation</label>
                                <select class="wizard-select" value=${govNewOp} onChange=${e => setGovNewOp(e.target.value)}>
                                    <option value="add_validator">Add Validator</option>
                                    <option value="remove_validator">Remove Validator</option>
                                    <option value="update_power">Update Power</option>
                                </select>
                            </div>
                            <div class="wizard-field">
                                <label>Target Agent</label>
                                <select class="wizard-select" value=${govNewTarget} onChange=${e => setGovNewTarget(e.target.value)}>
                                    <option value="">Select agent...</option>
                                    ${agents.filter(a => a.status !== 'removed').map(a => html`
                                        <option value=${a.agent_id}>${a.name} (${a.role})</option>
                                    `)}
                                </select>
                            </div>
                            ${(govNewOp === 'add_validator' || govNewOp === 'update_power') && html`
                                <div class="wizard-field">
                                    <label>Voting Power</label>
                                    <input class="wizard-input" type="number" min="1" max="100" value=${govNewPower} onInput=${e => setGovNewPower(e.target.value)} />
                                </div>
                            `}
                            <div class="wizard-field">
                                <label>Reason</label>
                                <textarea class="wizard-textarea" placeholder="Why is this change needed?" value=${govNewReason} onInput=${e => setGovNewReason(e.target.value)} />
                            </div>
                        </div>
                        <div class="wizard-footer">
                            <button class="btn" onClick=${() => setShowGovModal(false)}>Cancel</button>
                            <button class="btn btn-primary" onClick=${handleGovSubmit} disabled=${govSubmitting || !govNewTarget || !govNewReason.trim()}>
                                ${govSubmitting ? 'Submitting...' : 'Submit Proposal'}
                            </button>
                        </div>
                    </div>
                </div>
            `}

            <div class="ext-clients-section">
                <div class="ext-clients-header">
                    <h3>Connect an AI tool <${HelpTip} text="Wire an AI tool to your SAGE brain. Pick where the tool runs and which tool it is - SAGE writes the config for you. No terminal needed." /></h3>
                </div>
                <div class="ext-clients-grid">
                    <div class="ext-client-card" role="button" tabIndex="0"
                        onClick=${() => setShowConnectTool(true)}
                        onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setShowConnectTool(true); } }}>
                        <div class="ext-client-icon">🔌</div>
                        <div class="ext-client-info">
                            <div class="ext-client-title">Connect an AI tool</div>
                            <div class="ext-client-desc">Claude Code, Codex, Cursor, Windsurf, or Claude Desktop - on this computer or another. We write the config for you.</div>
                        </div>
                        <div class="ext-client-cta">Get started →</div>
                    </div>
                    <div class="ext-client-card" role="button" tabIndex="0"
                        onClick=${() => setShowJoinGuest(true)}
                        onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setShowJoinGuest(true); } }}>
                        <div class="ext-client-icon">🔗</div>
                        <div class="ext-client-info">
                            <div class="ext-client-title">Join a network</div>
                            <div class="ext-client-desc">Make this computer part of another SAGE on your network - it keeps its own node but shares that network's memory.</div>
                        </div>
                        <div class="ext-client-cta">Paste a code →</div>
                    </div>
                </div>
            </div>

            <div class="ext-clients-section">
                <div class="ext-clients-header">
                    <h3>Connect external clients <${HelpTip} text="Wire SAGE up to ChatGPT, Cursor, Cline, or Claude Desktop. Local-first — your tunnel, your domain, your bearer. SAGE never proxies through anyone's cloud." /></h3>
                </div>
                <div class="ext-clients-grid">
                    <div class="ext-client-card" role="button" tabIndex="0"
                        onClick=${() => { setRemoteWizardTarget('chatgpt'); setShowChatGPTWizard(true); }}
                        onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setRemoteWizardTarget('chatgpt'); setShowChatGPTWizard(true); } }}>
                        <div class="ext-client-icon">🤖</div>
                        <div class="ext-client-info">
                            <div class="ext-client-title">Connect to ChatGPT</div>
                            <div class="ext-client-desc">6-click wizard: cloudflared tunnel + DNS + autostart + bearer token. Requires a domain on Cloudflare.</div>
                        </div>
                        <div class="ext-client-cta">Setup wizard →</div>
                    </div>
                    <div class="ext-client-card" role="button" tabIndex="0"
                        onClick=${() => setShowCursorPanel(true)}
                        onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setShowCursorPanel(true); } }}>
                        <div class="ext-client-icon">💻</div>
                        <div class="ext-client-info">
                            <div class="ext-client-title">Connect to Cursor / Cline / Claude Desktop</div>
                            <div class="ext-client-desc">Bearer-only — no public DNS needed. The most local-first option. Just mint a token.</div>
                        </div>
                        <div class="ext-client-cta">Show me how →</div>
                    </div>
                </div>
            </div>

            <div class="agent-list">
                ${agents.map(agent => {
                    const isExpanded = expandedId === agent.agent_id;
                    const isLastAdmin = agent.role === 'admin' && agents.filter(a => a.role === 'admin' && a.status !== 'removed').length <= 1;
                    return html`
                        <div key=${agent.agent_id}>
                            <div class="agent-card-row ${isExpanded ? 'expanded' : ''}"
                                onClick=${() => toggleExpand(agent)} role="button"
                                aria-expanded=${isExpanded} tabIndex="0"
                                onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleExpand(agent); } }}>
                                <div class="agent-row-identity">
                                    <div class="agent-avatar">${agent.avatar || '\u{1F916}'}</div>
                                    <div>
                                        <div class="agent-name">${agent.name}</div>
                                        <span class="agent-role-badge ${agent.role}">${agent.role}</span>
                                    </div>
                                </div>
                                <div class="agent-row-meta">
                                    <span style="display:flex;align-items:center;gap:6px;">
                                        <span class="agent-status-dot ${agent.status}"></span>
                                        ${agent.status}
                                        <${HelpTip} text="Green = active, Yellow = pending setup, Red = offline, Gray = removed." />
                                    </span>
                                    ${agent.on_chain_height > 0 ? html`<span class="on-chain-badge" title="Registered on-chain at block ${agent.on_chain_height}">On-Chain</span>` : ''}
                                    <span>${agent.memory_count || 0} memories</span>
                                    <span>Clearance: ${CLEARANCE_LABELS[agent.clearance] || 'Internal'}</span>
                                    ${agent.last_seen ? html`<span>${timeAgo(agent.last_seen)}</span>` : ''}
                                </div>
                                <svg class="agent-row-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                            </div>
                            <div class="agent-expanded ${isExpanded ? 'open' : ''}">
                                ${isExpanded && html`<div class="agent-expanded-inner">
                                    <div class="agent-tab-bar" role="tablist">
                                        <button class="agent-tab ${expandedTab === 'overview' ? 'active' : ''}" onClick=${e => { e.stopPropagation(); setExpandedTab('overview'); setEditing(false); }}>Overview</button>
                                        <button class="agent-tab ${expandedTab === 'access' ? 'active' : ''}" onClick=${e => { e.stopPropagation(); setExpandedTab('access'); setEditing(false); }}>Access Control</button>
                                        <button class="agent-tab ${expandedTab === 'activity' ? 'active' : ''}" onClick=${e => { e.stopPropagation(); setExpandedTab('activity'); setEditing(false); }}>Activity</button>
                                    </div>

                                    ${expandedTab === 'overview' && html`
                                        <div class="agent-overview-grid">
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">Name</span>
                                                ${editing ? html`<input class="wizard-input" value=${editName} onInput=${e => setEditName(e.target.value)} onClick=${e => e.stopPropagation()} />`
                                                    : html`<span class="agent-info-value">${agent.name}</span>`}
                                            </div>
                                            ${agent.registered_name && agent.registered_name !== agent.name ? html`<div class="agent-info-block">
                                                <span class="agent-info-label">Registered As</span>
                                                <span class="agent-info-value" style="color:var(--text-dim);font-style:italic;" title="Immutable on-chain identity from initial registration">${agent.registered_name}</span>
                                            </div>` : ''}
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">Status</span>
                                                <span class="agent-info-value" style="display:flex;align-items:center;gap:6px;">
                                                    <span class="agent-status-dot ${agent.status}"></span> ${agent.status}
                                                </span>
                                            </div>
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">Memories</span>
                                                <span class="agent-info-value" style="color:var(--primary);font-weight:700;">${agent.memory_count || 0}</span>
                                            </div>
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">Clearance</span>
                                                <span class="agent-info-value">${CLEARANCE_LABELS[agent.clearance]} (Level ${agent.clearance})</span>
                                            </div>
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">First Seen</span>
                                                <span class="agent-info-value">${agent.first_seen ? timeAgo(agent.first_seen) : 'Never'}</span>
                                            </div>
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">Last Seen</span>
                                                <span class="agent-info-value">${agent.last_seen ? timeAgo(agent.last_seen) : 'Never'}</span>
                                            </div>
                                            ${agent.provider ? html`<div class="agent-info-block">
                                                <span class="agent-info-label">Provider</span>
                                                <span class="agent-info-value">${agent.provider}</span>
                                            </div>` : ''}
                                            <div class="agent-info-block">
                                                <span class="agent-info-label">On-Chain</span>
                                                <span class="agent-info-value">${agent.on_chain_height > 0 ? html`<span class="on-chain-badge">Block ${agent.on_chain_height}</span>` : 'Not registered'}</span>
                                            </div>
                                            <div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Agent ID</span>
                                                <span class="agent-info-value mono">${agent.agent_id}</span>
                                            </div>
                                            ${agent.validator_pubkey ? html`<div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Validator Key</span>
                                                <span class="agent-info-value mono">${agent.validator_pubkey}</span>
                                            </div>` : ''}
                                            <div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Purpose</span>
                                                ${editing ? html`<textarea class="wizard-textarea" value=${editBio} onInput=${e => setEditBio(e.target.value)} onClick=${e => e.stopPropagation()} />`
                                                    : html`<span class="agent-info-value" style="font-weight:400;color:var(--text-dim);">${agent.boot_bio || 'No purpose set'}</span>`}
                                            </div>
                                        </div>
                                    `}

                                    ${expandedTab === 'access' && html`
                                        <div>
                                            <div class="access-section-title">Role <${HelpTip} text="Admins have full access to all domains and can manage the network. Members read/write in allowed domains only. Observers are read-only. Role is fixed when the agent is registered on-chain and cannot be changed here." /></div>
                                            <div class="role-selector" onClick=${e => e.stopPropagation()}>
                                                <div class="role-card selected ${agent.role}">
                                                    <div class="role-card-name">${(ROLE_META[agent.role] || {}).name || agent.role || 'Member'}</div>
                                                    <div class="role-card-desc">${(ROLE_META[agent.role] || {}).desc || ''}</div>
                                                </div>
                                            </div>
                                            <div style="color:var(--text-muted);font-size:12px;margin-top:6px;">
                                                Role is set at registration and cannot be changed here.
                                            </div>

                                            <div class="access-section-title">Domain Access <${HelpTip} text="Control which knowledge domains this agent can read from and write to. Enabling write automatically enables read. Enforced server-side on every request." /></div>
                                            <${DomainAccessMatrix}
                                                domains=${allDomains}
                                                domainAccess=${editDomainAccess}
                                                onChange=${(v) => { setEditDomainAccess(v); setAccessDirty(true); }}
                                                disabled=${editRole === 'admin'}
                                            />

                                            <div class="access-section-title">Clearance Level <${HelpTip} text="5 tiers from Guest (0) to Top Secret (4). Determines the sensitivity of memories this agent can access." /></div>
                                            <div class="clearance-row" onClick=${e => e.stopPropagation()}>
                                                <input type="range" min="0" max="4" value=${editClearance}
                                                    onInput=${e => { setEditClearance(parseInt(e.target.value)); setAccessDirty(true); }} />
                                                <span class="clearance-label">${CLEARANCE_LABELS[editClearance]}</span>
                                            </div>

                                            <div class="access-section-title">Visible Agents <${HelpTip} text="Control which agents' memories this agent can see. Leave empty or set to '*' for full visibility (default). Enter a JSON array of agent IDs to restrict." /></div>
                                            <div onClick=${e => e.stopPropagation()}>
                                                <input class="wizard-input" style="font-family:var(--mono,monospace);font-size:13px;"
                                                    placeholder='* (all agents visible)' value=${editVisibleAgents}
                                                    onInput=${e => { setEditVisibleAgents(e.target.value); setAccessDirty(true); }} />
                                                <div style="color:var(--text-muted);font-size:12px;margin-top:4px;">
                                                    Use <code>*</code> or leave empty for full visibility. Use a JSON array like <code>["agentId1","agentId2"]</code> to restrict.
                                                </div>
                                            </div>

                                            <div class="access-save-bar" onClick=${e => e.stopPropagation()}>
                                                ${accessDirty ? html`<span class="access-dirty">Unsaved changes</span>` : ''}
                                                ${accessSaved ? html`<span class="access-saved">Saved</span>` : ''}
                                                <button class="btn btn-primary" onClick=${() => handleAccessSave(agent.agent_id)} disabled=${!accessDirty}>Save</button>
                                            </div>
                                        </div>
                                    `}

                                    ${expandedTab === 'activity' && html`<${ActivityTab} agent=${agent} />`}

                                    ${expandedTab === 'overview' && html`
                                        <div class="agent-action-bar" onClick=${e => e.stopPropagation()}>
                                            ${editing ? html`
                                                <button class="btn btn-primary" onClick=${() => handleOverviewSave(agent.agent_id)}>Save</button>
                                                <button class="btn" onClick=${() => setEditing(false)}>Cancel</button>
                                            ` : html`
                                                <button class="btn" onClick=${() => setEditing(true)}>Edit</button>
                                                <button class="btn" onClick=${async () => {
                                                    const ok = await downloadBundle(agent.agent_id);
                                                    if (!ok) showToast('No bundle available for this agent. Bundles are only created when agents are added via the wizard.', 'warning');
                                                }}>Download Bundle</button>
                                                <button class="btn" onClick=${(e) => { e.stopPropagation(); loadAgentTags(agent); }} style="gap:6px;display:inline-flex;align-items:center;">Transfer by Tag</button>
                                                <button class="btn" onClick=${() => setShowRotateConfirm(agent)}>Rotate Key</button>
                                                ${isLastAdmin
                                                    ? html`<button class="btn btn-danger btn-disabled" disabled=${true} title="Cannot remove the last admin — network needs at least one admin">Remove</button>`
                                                    : html`<button class="btn btn-danger" onClick=${() => setShowRemoveConfirm(agent)}>Remove</button>`}
                                            `}
                                        </div>
                                    `}
                                </div>`}
                            </div>
                        </div>
                    `;
                })}

                <div class="agent-card-add" onClick=${() => setShowWizard(true)}>
                    <div class="agent-card-add-icon">+</div>
                    <div class="agent-card-add-label">Add Agent</div>
                </div>
            </div>

            ${unregistered.length > 0 && html`
                <div class="unregistered-section">
                    <h3 style="color:var(--text-muted);font-size:14px;font-weight:500;margin-bottom:6px;display:flex;align-items:center;gap:8px;">
                        <span style="color:var(--warning, #f5a623);">?</span> Leftover identities
                    </h3>
                    <p style="color:var(--text-dim);font-size:12px;margin:0 0 12px;max-width:760px;line-height:1.5;">
                        These IDs wrote memories but were never registered as agents — usually old or per-project keys (e.g. a Claude Code session in another folder). They're harmless. To tidy up, hand their memories to one of your registered agents as the new owner. <strong style="color:var(--text-muted);">Nothing is deleted</strong> — the memories move over and the leftover ID disappears from this list.
                    </p>
                    <div class="unregistered-list">
                        ${unregistered.map(u => html`
                            <div class="unregistered-card" key=${u.agent_id}>
                                <div class="unregistered-info">
                                    <span class="mono" style="font-size:12px;color:var(--text-dim);">${u.short_id}</span>
                                    <span style="font-size:12px;color:var(--text-muted);">${u.memory_count} memor${u.memory_count === 1 ? 'y' : 'ies'} · no owner</span>
                                </div>
                                <button class="merge-btn" onClick=${() => setMergeTarget({ source: u.agent_id, sourceShort: u.short_id, memoryCount: u.memory_count })}>
                                    Give to an agent & remove →
                                </button>
                            </div>
                        `)}
                    </div>
                </div>
            `}
            ${mergeTarget && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setMergeTarget(null); }}>
                    <div class="wizard-modal" style="max-width:480px;">
                        <div class="wizard-header">
                            <h2>Choose the new owner</h2>
                            <button class="detail-close" onClick=${() => setMergeTarget(null)}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:8px;">
                                The <strong>${mergeTarget.memoryCount}</strong> memor${mergeTarget.memoryCount === 1 ? 'y' : 'ies'} from
                                <code style="font-size:11px;">${mergeTarget.sourceShort}</code> will move to the agent you pick. Nothing is deleted, and this leftover ID then disappears.
                            </p>
                            <p style="color:var(--text-muted);font-size:12px;margin-bottom:16px;">
                                The reassignment is recorded on-chain (BFT consensus) and takes effect on the next block.
                            </p>
                            ${agents.filter(a => a.status !== 'removed').length === 0
                                ? html`<div class="import-error">You have no registered agents to hand these memories to yet. Add an agent first (the + Add Agent button above), then come back.</div>`
                                : html`<div style="display:flex;flex-direction:column;gap:8px;">
                                    ${agents.filter(a => a.status !== 'removed').map(a => html`
                                        <button class="merge-target-btn" onClick=${() => handleMerge(mergeTarget.source, a.agent_id)} disabled=${merging}>
                                            <span>${a.avatar || '\u{1F916}'}</span>
                                            <span>${a.name}</span>
                                            <span class="agent-role-badge ${a.role}" style="margin-left:auto;">${a.role}</span>
                                        </button>
                                    `)}
                                </div>`}
                            ${merging && html`<p style="color:var(--primary);font-size:12px;margin-top:12px;">Reassigning memories on-chain…</p>`}
                        </div>
                    </div>
                </div>
            `}
            ${tagTransfer && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setTagTransfer(null); }}>
                    <div class="wizard-modal" style="max-width:520px;">
                        <div class="wizard-header">
                            <h2>${tagTransfer.step === 'tags' ? 'Transfer Memories by Tag' : 'Select Target Agent'}</h2>
                            <button class="detail-close" onClick=${() => setTagTransfer(null)}>×</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            ${tagTransfer.step === 'tags' ? html`
                                <p style="color:var(--text-dim);margin-bottom:16px;">
                                    Select a tag from <strong>${tagTransfer.agentName}</strong> to transfer those memories to another agent.
                                </p>
                                ${tagTransfer.tags.length === 0 ? html`
                                    <p style="color:var(--text-muted);font-size:13px;font-style:italic;">No tagged memories found for this agent.</p>
                                ` : html`
                                    <div style="display:flex;flex-direction:column;gap:6px;max-height:320px;overflow-y:auto;">
                                        ${tagTransfer.tags.map(t => html`
                                            <button class="merge-target-btn" onClick=${() => setTagTransfer(prev => ({ ...prev, step: 'target', selectedTag: t }))}
                                                style="justify-content:space-between;">
                                                <span style="display:flex;align-items:center;gap:8px;">
                                                    <span class="tag-chip" style="margin:0;">${t.tag}</span>
                                                </span>
                                                <span style="color:var(--text-muted);font-size:12px;">${t.count} memor${t.count === 1 ? 'y' : 'ies'}</span>
                                            </button>
                                        `)}
                                    </div>
                                `}
                            ` : html`
                                <div style="margin-bottom:16px;">
                                    <button class="btn" onClick=${() => setTagTransfer(prev => ({ ...prev, step: 'tags', selectedTag: null }))}
                                        style="font-size:12px;padding:4px 12px;margin-bottom:12px;">
                                        \u2190 Back to tags
                                    </button>
                                    <p style="color:var(--text-dim);">
                                        Transfer <strong>${tagTransfer.selectedTag.count}</strong> memor${tagTransfer.selectedTag.count === 1 ? 'y' : 'ies'}
                                        tagged <span class="tag-chip" style="display:inline-flex;margin:0 4px;">${tagTransfer.selectedTag.tag}</span>
                                        from <strong>${tagTransfer.agentName}</strong> to:
                                    </p>
                                </div>
                                <div style="display:flex;flex-direction:column;gap:8px;">
                                    ${agents.filter(a => a.status !== 'removed' && a.agent_id !== tagTransfer.agentId).map(a => html`
                                        <button class="merge-target-btn" onClick=${() => handleTagTransfer(a.agent_id)} disabled=${transferring}>
                                            <span>${a.avatar || '\u{1F916}'}</span>
                                            <span>${a.name}</span>
                                            <span class="agent-role-badge ${a.role}" style="margin-left:auto;">${a.role}</span>
                                        </button>
                                    `)}
                                </div>
                                ${transferring && html`<p style="color:var(--primary);font-size:12px;margin-top:12px;">Transferring memories...</p>`}
                            `}
                        </div>
                    </div>
                </div>
            `}
            ${showWizard && html`<${AddAgentWizard} onClose=${() => setShowWizard(false)} onCreated=${() => { setShowWizard(false); loadAgents(); }} />`}
            ${showChatGPTWizard && html`<${RemoteAccessWizard} agents=${agents} target=${remoteWizardTarget} onClose=${() => setShowChatGPTWizard(false)} />`}
            ${showCursorPanel && html`<${CursorSetupPanel} agents=${agents} onClose=${() => setShowCursorPanel(false)} />`}
            ${showConnectTool && html`<${ConnectToolModal} agents=${agents}
                onOpenChatGPT=${(t) => { setRemoteWizardTarget(t || 'chatgpt'); setShowConnectTool(false); setShowChatGPTWizard(true); }}
                onClose=${() => setShowConnectTool(false)} />`}
            ${showJoinGuest && html`<${NetworkJoinGuestPanel} onClose=${() => setShowJoinGuest(false)} />`}
            ${showRemoveConfirm && html`<${RemoveConfirmModal} agent=${showRemoveConfirm} onConfirm=${() => handleRemove(showRemoveConfirm)} onCancel=${() => setShowRemoveConfirm(null)} />`}
            ${showRotateConfirm && html`
                <div class="wizard-overlay" onClick=${(e) => { if (e.target === e.currentTarget) setShowRotateConfirm(null); }}>
                    <div class="wizard-modal" style="max-width:440px;">
                        <div class="wizard-header"><h2>Rotate Agent Key</h2><button class="detail-close" onClick=${() => setShowRotateConfirm(null)}>×</button></div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:16px;">
                                This will generate a new Ed25519 identity key for <strong>${showRotateConfirm.name}</strong>.
                                All existing memories will be re-attributed to the new key. A chain redeployment will be triggered.
                            </p>
                            <div class="warning-banner">⚠ The agent will need a new bundle after rotation. The old key will be permanently retired.</div>
                        </div>
                        <div class="wizard-footer">
                            <button class="btn" onClick=${() => setShowRotateConfirm(null)}>Cancel</button>
                            <button class="btn btn-danger" disabled=${rotating} onClick=${() => handleRotateKey(showRotateConfirm)}>
                                ${rotating ? 'Rotating...' : 'Rotate Key'}
                            </button>
                        </div>
                    </div>
                </div>
            `}
        </div>
    `;
}

function AddAgentWizard({ onClose, onCreated }) {
    const [step, setStep] = useState(1);
    const [templates, setTemplates] = useState([]);
    const [creating, setCreating] = useState(false);
    const [createdAgent, setCreatedAgent] = useState(null);
    const [error, setError] = useState(null);

    // Step 1 state
    const [name, setName] = useState('');
    const [role, setRole] = useState('member');
    const [avatar, setAvatar] = useState('🤖');
    const [bio, setBio] = useState('');
    const [provider, setProvider] = useState('');
    const [template, setTemplate] = useState('');

    // Step 2 state
    const [clearance, setClearance] = useState(1);
    const [domainAccess, setDomainAccess] = useState({});
    const [allDomains, setAllDomains] = useState([]);

    // Step 3 state
    const [connectMethod, setConnectMethod] = useState('local');

    // Step 4 state — deploy progress
    const [deploying, setDeploying] = useState(false);
    const [deployStatus, setDeployStatus] = useState(null);
    const deployPollRef = useRef(null);
    const [pairingCode, setPairingCode] = useState(null);
    const [pairingExpiry, setPairingExpiry] = useState(null);

    useEffect(() => {
        fetchTemplates().then(data => {
            if (data.templates) setTemplates(data.templates);
        });
        fetchStats().then(data => {
            if (data?.by_domain) setAllDomains(Object.keys(data.by_domain).sort());
        }).catch(() => {});
    }, []);

    // Cleanup deploy poll on unmount
    useEffect(() => {
        return () => {
            if (deployPollRef.current) {
                clearInterval(deployPollRef.current);
                deployPollRef.current = null;
            }
        };
    }, []);

    const applyTemplate = (t) => {
        setTemplate(t.name);
        setRole(t.role);
        setBio(t.bio);
        setClearance(t.clearance);
        setAvatar(t.avatar);
    };

    const startDeployPoll = () => {
        if (deployPollRef.current) return;
        let sawRunning = false, ticks = 0;
        const stop = () => { clearInterval(deployPollRef.current); deployPollRef.current = null; setDeploying(false); };
        deployPollRef.current = setInterval(async () => {
            try {
                ticks++;
                const s = await fetchRedeployStatus();
                setDeployStatus(s);
                if (s && s.status === 'running') { sawRunning = true; return; }
                // Terminal (completed/failed/idle): only trust it once we've seen THIS
                // run reach 'running', so a stale verdict from a prior redeploy can't
                // flash instant success/failure at the start. Safety-stop after ~15s
                // in case the run never registers (e.g. trigger failed silently).
                if (sawRunning || ticks > 15 || !s) stop();
            } catch (e) { stop(); }
        }, 1000);
    };

    const handleCreate = async () => {
        setCreating(true);
        setError(null);
        try {
            const daArr = Object.entries(domainAccess)
                .filter(([_, v]) => v.read || v.write)
                .map(([domain, p]) => ({ domain, read: p.read, write: p.write }));
            const res = await createAgent({
                name, role, avatar, boot_bio: bio,
                clearance, domain_access: JSON.stringify(daArr),
                provider: provider || undefined,
            });
            if (res.error) {
                setError(res.error);
                setCreating(false);
                return;
            }
            setCreatedAgent(res);
            setStep(4);

            // Trigger redeployment
            setDeploying(true);
            const rdRes = await startRedeploy('add_agent', res.agent_id);
            if (rdRes.error) {
                setError(rdRes.error);
                setDeploying(false);
            } else if (rdRes.status === 'completed') {
                // Single-node (personal) network: applied instantly, no chain
                // redeploy — don't show the ~30s "Deploying…" progress screen.
                setDeployStatus(rdRes);
                setDeploying(false);
            } else {
                setDeployStatus(rdRes);
                startDeployPoll();
            }
        } catch (e) {
            setError(e.message);
        }
        setCreating(false);
    };

    const stepLabels = ['Identity', 'Permissions', 'Connect', 'Deploy'];

    return html`
        <div class="wizard-overlay" onClick=${(e) => { if (e.target === e.currentTarget) onClose(); }}>
            <div class="wizard-modal">
                <div class="wizard-header">
                    <h2>Add Agent</h2>
                    <button class="detail-close" onClick=${onClose}>×</button>
                </div>
                <div class="wizard-body">
                    <div class="wizard-stepper">
                        ${stepLabels.map((label, i) => html`
                            ${i > 0 && html`<div class="wizard-step-line"></div>`}
                            <div class="wizard-step ${step === i+1 ? 'active' : ''} ${step > i+1 ? 'completed' : ''}">
                                <span class="wizard-step-num">${step > i+1 ? '✓' : i+1}</span>
                                ${label}
                            </div>
                        `)}
                    </div>

                    ${error && html`<div class="import-error">${error}</div>`}

                    ${step === 1 && html`
                        <div class="wizard-field">
                            <label>Template <${HelpTip} text="Templates pre-fill role, bio, and clearance for common agent types. Choose Custom for manual configuration." /></label>
                            <select class="wizard-select" value=${template} onChange=${e => {
                                const t = templates.find(t => t.name === e.target.value);
                                if (t) applyTemplate(t);
                            }}>
                                <option value="">Select a template...</option>
                                ${templates.map(t => html`<option value=${t.name}>${t.name}</option>`)}
                            </select>
                        </div>
                        <div class="wizard-field">
                            <label>Name</label>
                            <input class="wizard-input" placeholder="Agent name" value=${name} onInput=${e => setName(e.target.value)} />
                        </div>
                        <div class="wizard-field">
                            <label>Avatar</label>
                            <div class="emoji-grid">
                                ${AGENT_EMOJIS.map(e => html`
                                    <button class="emoji-btn ${avatar === e ? 'selected' : ''}" onClick=${() => setAvatar(e)}>${e}</button>
                                `)}
                            </div>
                        </div>
                        <div class="wizard-field">
                            <label>Purpose <${HelpTip} text="Describe what this agent does. This is shown in the dashboard and returned to the AI during boot so it knows its role in the network. Think of it as a job description." /></label>
                            <textarea class="wizard-textarea" placeholder="What does this agent do? e.g. 'Coding assistant for the sage project — tracks architecture decisions, debugging insights, and release notes'" value=${bio} onInput=${e => setBio(e.target.value)} />
                        </div>
                        <div class="wizard-field">
                            <label>Provider <${HelpTip} text="Which AI tool will this agent connect from? Auto-detected on first connection if left as Auto-detect." /></label>
                            <select class="wizard-select" value=${provider} onChange=${e => setProvider(e.target.value)}>
                                <option value="">Auto-detect</option>
                                <option value="claude-code">Claude Code</option>
                                <option value="cursor">Cursor</option>
                                <option value="windsurf">Windsurf</option>
                                <option value="chatgpt">ChatGPT</option>
                                <option value="other">Other</option>
                            </select>
                        </div>
                    `}

                    ${step === 2 && html`
                        <div class="wizard-field">
                            <label>Role</label>
                            <div class="role-selector">
                                ${[
                                    { key: 'admin', name: 'Admin', desc: 'Full access to everything.' },
                                    { key: 'member', name: 'Member', desc: 'Read/write in allowed domains.' },
                                    { key: 'observer', name: 'Observer', desc: 'Read-only access.' },
                                ].map(r => html`
                                    <div class="role-card ${role === r.key ? 'selected ' + r.key : ''}"
                                        onClick=${() => setRole(r.key)}>
                                        <div class="role-card-name">${r.name}</div>
                                        <div class="role-card-desc">${r.desc}</div>
                                    </div>
                                `)}
                            </div>
                        </div>
                        <div class="wizard-field">
                            <label>Domain Access</label>
                            <${DomainAccessMatrix}
                                domains=${allDomains}
                                domainAccess=${domainAccess}
                                onChange=${setDomainAccess}
                                disabled=${role === 'admin'}
                            />
                        </div>
                        <div class="wizard-field">
                            <label>Clearance Level <${HelpTip} text="Determines what sensitivity level of memories this agent can access. Higher clearance = access to more classified knowledge." /></label>
                            <div class="clearance-row">
                                <input type="range" min="0" max="4" value=${clearance} onInput=${e => setClearance(parseInt(e.target.value))} style="flex:1;" />
                                <span class="clearance-label" style="color:${clearance >= 3 ? 'var(--danger)' : clearance >= 2 ? 'var(--warning)' : 'var(--text-dim)'};">
                                    ${CLEARANCE_LABELS[clearance]}
                                </span>
                            </div>
                        </div>
                    `}

                    ${step === 3 && html`
                        <div style="margin-bottom:16px;">
                            <p style="font-size:13px;color:var(--text-dim);margin-bottom:16px;">
                                Choose how the new agent will receive its configuration and keys.
                                <${HelpTip} text="Bundle: download a ZIP to copy manually. LAN: generate a pairing code — the new agent fetches config automatically over your local network." />
                            </p>
                            <div class="connect-cards">
                                <div class="connect-card ${connectMethod === 'local' ? 'selected' : ''}" onClick=${() => setConnectMethod('local')}>
                                    <div class="connect-card-icon">💻</div>
                                    <h4>Local Project</h4>
                                    <p>Install into a Claude Code project on this machine. One command.</p>
                                </div>
                                <div class="connect-card ${connectMethod === 'bundle' ? 'selected' : ''}" onClick=${() => setConnectMethod('bundle')}>
                                    <div class="connect-card-icon">📦</div>
                                    <h4>Download Bundle</h4>
                                    <p>Download a ZIP with keys and config. Copy to target machine manually.</p>
                                </div>
                                <div class="connect-card ${connectMethod === 'lan' ? 'selected' : ''}" onClick=${() => setConnectMethod('lan')}>
                                    <div class="connect-card-icon">📡</div>
                                    <h4>Easy Setup (LAN)</h4>
                                    <p>Generate a pairing code. New agent auto-fetches config over local network.</p>
                                </div>
                            </div>
                        </div>

                        <div class="summary-card" style="margin-top:16px;">
                            <div style="font-size:12px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.8px;margin-bottom:12px;">Summary</div>
                            <div class="summary-row"><span class="label">Name</span><span class="value">${name || '—'}</span></div>
                            <div class="summary-row"><span class="label">Role</span><span class="value" style="text-transform:capitalize;">${role}</span></div>
                            <div class="summary-row"><span class="label">Avatar</span><span class="value">${avatar}</span></div>
                            <div class="summary-row"><span class="label">Clearance</span><span class="value">${CLEARANCE_LABELS[clearance]}</span></div>
                            <div class="summary-row"><span class="label">Domains</span><span class="value">${role === 'admin' ? 'All (admin)' : (() => { const c = Object.values(domainAccess).filter(v => v.read || v.write).length; return c > 0 ? c + ' domain' + (c !== 1 ? 's' : '') : 'None'; })()}</span></div>
                            <div class="summary-row"><span class="label">Connect</span><span class="value">${connectMethod === 'local' ? 'Local Project' : connectMethod === 'bundle' ? 'Bundle Download' : 'LAN Pairing'}</span></div>
                        </div>

                        <div class="warning-banner">
                            ⚠ Adding an agent will briefly pause the chain (~30 seconds) for redeployment.
                        </div>
                    `}

                    ${step === 4 && html`
                        <div style="padding:12px 0;">
                            ${deploying || (deployStatus && deployStatus.status && deployStatus.status !== 'idle' && deployStatus.status !== 'completed' && deployStatus.status !== 'failed')
                                ? html`
                                    <div style="text-align:center;margin-bottom:16px;">
                                        <h3 style="font-size:18px;font-weight:700;color:var(--primary);margin-bottom:4px;">Deploying ${name}...</h3>
                                        <p style="font-size:13px;color:var(--text-dim);">Reconfiguring the network. This takes about 30 seconds.</p>
                                    </div>
                                    <${DeployProgress}
                                        currentPhase=${deployStatus && deployStatus.current_phase || ''}
                                        status=${deployStatus && deployStatus.status || 'running'}
                                        error=${deployStatus && deployStatus.error || ''}
                                    />
                                `
                                : deployStatus && deployStatus.status === 'failed'
                                    ? html`
                                        <div style="text-align:center;margin-bottom:16px;">
                                            <div style="font-size:48px;margin-bottom:12px;">✗</div>
                                            <h3 style="font-size:18px;font-weight:700;color:var(--danger);margin-bottom:4px;">Deployment Failed</h3>
                                            <p style="font-size:13px;color:var(--text-dim);margin-bottom:16px;">
                                                ${deployStatus.error || 'An error occurred during redeployment.'}
                                            </p>
                                        </div>
                                        <${DeployProgress}
                                            currentPhase=${deployStatus.current_phase || ''}
                                            status=${'failed'}
                                            error=${deployStatus.error || ''}
                                        />
                                    `
                                    : html`
                                        <div style="text-align:center;margin-bottom:16px;">
                                            <div style="font-size:48px;margin-bottom:12px;">✓</div>
                                            <h3 style="font-size:18px;font-weight:700;color:var(--accent);margin-bottom:4px;">Agent Deployed</h3>
                                            <p style="font-size:13px;color:var(--text-dim);margin-bottom:20px;">
                                                ${name} is live on the network. ${connectMethod === 'local' ? 'Run the install command in your project folder.' : connectMethod === 'bundle' ? 'Download the bundle to set up the agent.' : 'Use the pairing code on the target machine.'}
                                            </p>
                                        </div>
                                        ${deployStatus && deployStatus.status === 'completed' && html`
                                            <${DeployProgress}
                                                currentPhase=${'COMPLETED'}
                                                status=${'completed'}
                                            />
                                        `}
                                        <div style="margin-top:20px;text-align:center;">
                                            ${connectMethod === 'local' && createdAgent && createdAgent.claim_token && html`
                                                <div style="text-align:left;margin-bottom:16px;">
                                                    <p style="font-size:13px;color:var(--text-dim);margin-bottom:12px;">
                                                        Open a terminal in your project folder and run:
                                                    </p>
                                                    <div style="background:var(--bg-elevated);border:1px solid var(--border);border-radius:8px;padding:16px;font-family:monospace;font-size:14px;word-break:break-all;position:relative;">
                                                        <span style="color:var(--primary);">sage-gui mcp install</span> <span style="color:var(--accent);">--token ${createdAgent.claim_token}</span>
                                                        <button style="position:absolute;top:8px;right:8px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:11px;cursor:pointer;color:var(--text-dim);" onClick=${() => {
                                                            navigator.clipboard.writeText('sage-gui mcp install --token ' + createdAgent.claim_token);
                                                        }}>Copy</button>
                                                    </div>
                                                    <p style="font-size:11px;color:var(--text-muted);margin-top:8px;">
                                                        Token expires in 24 hours. One-time use — the key is delivered securely and the token is invalidated.
                                                    </p>
                                                    <p style="font-size:11px;color:var(--text-muted);margin-top:4px;">
                                                        After running, restart your Claude Code session. The agent will connect with the identity and permissions you just configured.
                                                    </p>
                                                </div>
                                            `}
                                            ${connectMethod === 'bundle' && createdAgent && html`
                                                <button class="btn btn-primary" style="padding:12px 28px;font-size:14px;" onClick=${async () => {
                                                    const ok = await downloadBundle(createdAgent.agent_id);
                                                    if (!ok) showToast('Bundle generation failed. Please try again.', 'error');
                                                }}>Download Bundle ZIP</button>
                                            `}
                                            ${connectMethod === 'lan' && html`
                                                ${pairingCode ? html`
                                                    <div class="pairing-code-display">
                                                        ${pairingCode}
                                                    </div>
                                                    <p style="font-size:12px;color:var(--text-muted);margin-top:8px;">
                                                        Valid for 15 minutes. Run <code style="background:var(--bg-elevated);padding:2px 6px;border-radius:4px;">sage-gui pair ${pairingCode}</code> on the new machine.
                                                    </p>
                                                ` : html`
                                                    <button class="btn btn-primary" style="padding:12px 28px;font-size:14px;" onClick=${async () => {
                                                        try {
                                                            const res = await createPairingCode(createdAgent.agent_id);
                                                            if (res.code) {
                                                                setPairingCode(res.code);
                                                                setPairingExpiry(res.expires_at);
                                                            }
                                                        } catch (e) { /* ignore */ }
                                                    }}>Generate Pairing Code</button>
                                                `}
                                            `}
                                        </div>
                                    `
                            }
                        </div>
                    `}
                </div>
                <div class="wizard-footer">
                    ${step < 4
                        ? html`
                            <button class="btn" onClick=${() => step > 1 ? setStep(step - 1) : onClose()}>
                                ${step === 1 ? 'Cancel' : 'Back'}
                            </button>
                            ${step < 3
                                ? html`<button class="btn btn-primary" onClick=${() => setStep(step + 1)} disabled=${step === 1 && !name}>Next</button>`
                                : html`<button class="btn btn-primary" onClick=${handleCreate} disabled=${creating || !name}>
                                    ${creating ? 'Creating...' : 'Create Agent'}
                                </button>`
                            }
                        `
                        : html`
                            <div></div>
                            <button class="btn btn-primary" onClick=${onCreated} disabled=${deploying}>
                                ${deploying ? 'Deploying...' : 'Done'}
                            </button>
                        `
                    }
                </div>
            </div>
        </div>
    `;
}

// RemoteAccessWizard — guided flow that gives SAGE a public URL via cloudflared
// so a tool on another computer (or a hosted chat like ChatGPT) can reach it.
// Steps 1-6 are generic tunnel setup (install → login → hostname → create →
// mint bearer); step 7 tailors the final instructions to `target`:
//   'chatgpt' — ChatGPT connector OAuth fields + "Open ChatGPT Connectors"
//   'tool'    — the public MCP URL + bearer, with per-client paste blocks
//               (Cursor/Cline url+headers, Windsurf/Claude Desktop mcp-remote)
//
// Local-first philosophy: SAGE never proxies through anyone's cloud. The user
// owns the cloudflared tunnel, the Cloudflare account, and the domain.
function RemoteAccessWizard({ agents, onClose, target }) {
    const forChatGPT = (target || 'chatgpt') === 'chatgpt';
    const [step, setStep] = useState(1);
    const [error, setError] = useState(null);

    // Step 2: install
    const [cloudflaredInstalled, setCloudflaredInstalled] = useState(null); // null | true | false
    const [cloudflaredVersion, setCloudflaredVersion] = useState('');
    const [installLog, setInstallLog] = useState('');
    const [installing, setInstalling] = useState(false);
    // Platform + per-OS hints come back from /check-cloudflared so the UI can
    // show Windows-specific guidance (winget install, post-wizard service
    // install) without baking platform detection into the frontend.
    const [platform, setPlatform] = useState('');
    const [installHint, setInstallHint] = useState('');
    const [autostartHint, setAutostartHint] = useState('');

    // Step 3: login
    const [loginURL, setLoginURL] = useState('');
    const [loginAuthenticated, setLoginAuthenticated] = useState(false);
    const loginPollRef = useRef(null);

    // Step 4: zone + subdomain
    const [zone, setZone] = useState('');
    const [subdomain, setSubdomain] = useState('sage');
    // Populated from /login-status when cert.pem is decoded successfully
    // and the Cloudflare API returns the user's zones. Empty list means we
    // fall back to a free-text input. Each zone is {id, name}.
    const [zoneOptions, setZoneOptions] = useState([]);

    // Step 5: tunnel create progress
    const [tunnelLog, setTunnelLog] = useState('');
    const [tunnelHostname, setTunnelHostname] = useState('');
    const [tunnelUUID, setTunnelUUID] = useState('');
    const [creatingTunnel, setCreatingTunnel] = useState(false);

    // Step 6: token mint
    const [agentChoice, setAgentChoice] = useState('');
    const [tokenName, setTokenName] = useState(forChatGPT ? 'chatgpt' : 'remote');
    const [mintedToken, setMintedToken] = useState(null);
    const [minting, setMinting] = useState(false);

    // Auto-check cloudflared on entry to step 2.
    useEffect(() => {
        if (step !== 2) return;
        wizardCheckCloudflared().then(d => {
            setCloudflaredInstalled(!!d.installed);
            setCloudflaredVersion(d.version || '');
            setPlatform(d.platform || '');
            setInstallHint(d.install_hint || '');
            setAutostartHint(d.autostart_hint || '');
        }).catch(() => setCloudflaredInstalled(false));
    }, [step]);

    // Auto-start polling for cert.pem on step 3.
    useEffect(() => {
        if (step !== 3 || loginAuthenticated) return;
        loginPollRef.current = setInterval(async () => {
            try {
                const s = await wizardLoginStatus();
                if (s.authenticated) {
                    setLoginAuthenticated(true);
                    if (Array.isArray(s.zones) && s.zones.length > 0) {
                        setZoneOptions(s.zones);
                        // If a zone hasn't been picked yet, pre-select the first
                        // — keeps the dropdown's "current value" matching what the
                        // user sees, and saves a click for single-zone accounts.
                        setZone(prev => prev || s.zones[0].name);
                    }
                    clearInterval(loginPollRef.current);
                    loginPollRef.current = null;
                }
            } catch (e) { /* ignore */ }
        }, 2000);
        return () => { if (loginPollRef.current) { clearInterval(loginPollRef.current); loginPollRef.current = null; } };
    }, [step, loginAuthenticated]);

    const startInstall = async () => {
        setInstalling(true);
        setInstallLog('');
        try {
            const res = await wizardInstallCloudflared();
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                // Decode each chunk ONCE, streaming, so a multi-byte UTF-8 char
                // straddling a chunk boundary is not corrupted (the old code
                // decoded every chunk twice through one stateful decoder).
                const text = decoder.decode(value, { stream: true });
                if (text) setInstallLog(prev => prev + text);
            }
            const tail = decoder.decode(); // flush any trailing partial sequence
            if (tail) setInstallLog(prev => prev + tail);
            // Re-check.
            const d = await wizardCheckCloudflared();
            setCloudflaredInstalled(!!d.installed);
            setCloudflaredVersion(d.version || '');
            setPlatform(d.platform || '');
            setInstallHint(d.install_hint || '');
            setAutostartHint(d.autostart_hint || '');
        } catch (e) { setError('install failed: ' + e.message); }
        setInstalling(false);
    };

    const startLogin = async () => {
        setError(null);
        try {
            const r = await wizardStartLogin();
            if (r.error) { setError(r.error); return; }
            setLoginURL(r.url || '');
        } catch (e) { setError('login failed: ' + e.message); }
    };

    const startCreateTunnel = async () => {
        setCreatingTunnel(true);
        setTunnelLog('');
        setError(null);
        try {
            const res = await wizardCreateTunnel(subdomain, zone);
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                const chunk = decoder.decode(value, { stream: true });
                buf += chunk;
                setTunnelLog(prev => prev + chunk);
            }
            // Parse final hostname/tunnel_uuid out of buffered output.
            const hostMatch = buf.match(/hostname:\s+(\S+)/);
            const uuidMatch = buf.match(/tunnel_uuid:\s+([0-9a-f-]+)/);
            if (hostMatch) setTunnelHostname(hostMatch[1]);
            if (uuidMatch) setTunnelUUID(uuidMatch[1]);
            const errMatch = buf.match(/^error:\s+(.+)$/m);
            if (errMatch && !hostMatch) setError(errMatch[1]);
            else if (hostMatch) setStep(6);
        } catch (e) { setError('tunnel create failed: ' + e.message); }
        setCreatingTunnel(false);
    };

    const startMintToken = async () => {
        setMinting(true);
        setError(null);
        try {
            const r = await wizardMintToken(agentChoice, tokenName);
            if (r.error) { setError(r.error); }
            else { setMintedToken(r); setStep(7); }
        } catch (e) { setError('mint token failed: ' + e.message); }
        setMinting(false);
    };

    const eligibleAgents = (agents || []).filter(a => a.status !== 'removed');

    return html`
        <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) onClose(); }}>
            <div class="wizard-modal" style="max-width:680px;max-height:85vh;display:flex;flex-direction:column;">
                <div class="wizard-header">
                    <h2>${forChatGPT ? 'Connect to ChatGPT' : 'Set up remote access'} — Step ${step} of 7</h2>
                    <button class="detail-close" onClick=${onClose}>×</button>
                </div>
                <div class="wizard-body" style="overflow-y:auto;flex:1;padding:20px;">
                    ${error && html`<div class="import-error" style="margin-bottom:12px;">${error}</div>`}

                    ${step === 1 && html`
                        <div style="line-height:1.55;color:var(--text);">
                            <h3 style="margin-top:0;color:var(--accent);">SAGE is local-first by design</h3>
                            ${forChatGPT
                                ? html`<p>ChatGPT lives at <code>chatgpt.com</code>. SAGE lives on <strong>your machine</strong>. To bridge them, you'll set up a tunnel from a domain you own to your localhost.</p>`
                                : html`<p>SAGE lives on <strong>your machine</strong>. To let a tool on another computer reach it, you'll give it a public URL — a tunnel from a domain you own to your localhost. The same URL works for a hosted chat like ChatGPT too.</p>`}
                            <p><strong>SAGE doesn't proxy through our cloud, ever.</strong> You own the tunnel. We just collapse the 9 terminal commands it normally takes into 6 clicks.</p>
                            <h4 style="color:var(--accent);">Prerequisites</h4>
                            <ul>
                                <li>A free <a href="https://dash.cloudflare.com/sign-up" target="_blank" rel="noopener" style="color:var(--accent);">Cloudflare account</a></li>
                                <li>A domain on Cloudflare DNS (a <code>.xyz</code> is ~$1/year if you don't already own one)</li>
                            </ul>
                            <div class="warning-banner" style="margin-top:16px;">
                                <strong>Don't want to own a domain?</strong> If the tool is on <em>this</em> computer, cancel and use the
                                <em>Connect an AI tool → On this computer</em> flow instead — those clients need no public URL at all.
                            </div>
                        </div>
                    `}

                    ${step === 2 && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;">cloudflared install check</h3>
                            ${cloudflaredInstalled === null && html`<p style="color:var(--text-dim);">Checking...</p>`}
                            ${cloudflaredInstalled === true && html`
                                <p style="color:var(--accent-green);">✓ cloudflared is installed.</p>
                                <pre style="background:var(--bg-elev);padding:8px;border-radius:4px;font-size:12px;color:var(--text-dim);">${cloudflaredVersion}</pre>
                                ${platform === 'windows' && autostartHint && html`
                                    <div class="warning-banner" style="margin-top:12px;">
                                        <strong>Windows autostart note.</strong> ${autostartHint}
                                    </div>
                                `}
                            `}
                            ${cloudflaredInstalled === false && html`
                                <p>
                                    cloudflared isn't on your PATH. We'll install it now via
                                    ${platform === 'darwin' ? ' Homebrew.' :
                                      platform === 'linux' ? ' the static binary into ~/.local/bin.' :
                                      platform === 'windows' ? ' winget (Cloudflare.cloudflared).' :
                                      " your platform's package manager."}
                                </p>
                                ${platform === 'windows' && installHint && html`
                                    <div class="warning-banner" style="margin-bottom:12px;">
                                        <strong>Windows users.</strong> ${installHint}
                                    </div>
                                `}
                                <button class="btn btn-primary" onClick=${startInstall} disabled=${installing}>
                                    ${installing ? 'Installing…' : 'Install cloudflared'}
                                </button>
                                ${installLog && html`<pre style="background:var(--bg-elev);padding:8px;border-radius:4px;font-size:11px;max-height:200px;overflow:auto;margin-top:12px;color:var(--text-dim);white-space:pre-wrap;">${installLog}</pre>`}
                                <p style="color:var(--text-muted);font-size:12px;margin-top:12px;">
                                    Or install manually from <a href="https://github.com/cloudflare/cloudflared/releases" target="_blank" rel="noopener" style="color:var(--accent);">github.com/cloudflare/cloudflared/releases</a> and re-run this step.
                                </p>
                            `}
                        </div>
                    `}

                    ${step === 3 && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;">Authorize cloudflared with your Cloudflare account</h3>
                            <p>This is a one-time browser login. cloudflared will print a URL — we'll open it in your browser. After you click "Authorize", a certificate file is dropped at <code>~/.cloudflared/cert.pem</code>.</p>
                            ${!loginURL && html`
                                <button class="btn btn-primary" onClick=${startLogin}>Open Cloudflare login</button>
                            `}
                            ${loginURL && !loginAuthenticated && html`
                                <div style="background:var(--bg-elev);padding:12px;border-radius:4px;margin:12px 0;">
                                    <div style="font-size:12px;color:var(--text-dim);margin-bottom:6px;">If your browser didn't open automatically:</div>
                                    <a href=${loginURL} target="_blank" rel="noopener" style="color:var(--accent);word-break:break-all;font-size:12px;">${loginURL}</a>
                                </div>
                                <div style="display:flex;align-items:center;gap:8px;color:var(--text-dim);">
                                    <span class="spinner" style="width:14px;height:14px;"></span>
                                    Waiting for cert.pem... (poll every 2s)
                                </div>
                            `}
                            ${loginAuthenticated && html`
                                <p style="color:var(--accent-green);">✓ Authenticated. cert.pem detected.</p>
                            `}
                        </div>
                    `}

                    ${step === 4 && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;">Pick your hostname</h3>
                            <p>This is the public URL your ${forChatGPT ? 'ChatGPT connector' : 'tool (or ChatGPT)'} will hit. SAGE will create a CNAME from
                                <strong>${subdomain || '<sub>'}.${zone || '<your-domain>'}</strong>
                                to your localhost via Cloudflare's tunnel.</p>
                            <div class="wizard-field">
                                <label>Subdomain</label>
                                <input class="wizard-input" value=${subdomain} onInput=${e => setSubdomain(e.target.value)} placeholder="sage" />
                            </div>
                            <div class="wizard-field">
                                <label>Domain (must be on your Cloudflare account)</label>
                                ${zoneOptions.length > 0 ? html`
                                    <select class="wizard-input" value=${zone} onChange=${e => setZone(e.target.value)}>
                                        ${zoneOptions.map(z => html`<option value=${z.name}>${z.name}</option>`)}
                                    </select>
                                    <div style="font-size:11px;color:var(--text-muted);margin-top:4px;">
                                        Showing ${zoneOptions.length} zone${zoneOptions.length === 1 ? '' : 's'} from your Cloudflare account.
                                        Need another? Add a domain at
                                        <a href="https://dash.cloudflare.com/?to=/:account/add-site" target="_blank" rel="noopener" style="color:var(--accent);">dash.cloudflare.com</a>.
                                    </div>
                                ` : html`
                                    <input class="wizard-input" value=${zone} onInput=${e => setZone(e.target.value)} placeholder="example.com" />
                                    <div style="font-size:11px;color:var(--text-muted);margin-top:4px;">
                                        Couldn't auto-list your zones — type the domain name manually. Don't have one yet? Add a domain at
                                        <a href="https://dash.cloudflare.com/?to=/:account/add-site" target="_blank" rel="noopener" style="color:var(--accent);">dash.cloudflare.com</a>.
                                    </div>
                                `}
                            </div>
                            <div style="background:var(--bg-elev);padding:10px;border-radius:4px;font-size:12px;color:var(--text);">
                                Public URL preview: <strong style="color:var(--accent);">https://${subdomain || 'sage'}.${zone || 'example.com'}</strong>
                            </div>
                        </div>
                    `}

                    ${step === 5 && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;">Create tunnel + configure autostart</h3>
                            <p>SAGE will now run the cloudflared dance for you:</p>
                            <ol style="color:var(--text-dim);font-size:13px;">
                                <li><code>cloudflared tunnel create sage</code></li>
                                <li><code>cloudflared tunnel route dns sage ${subdomain}.${zone}</code></li>
                                <li>Write <code>~/.cloudflared/config.yml</code> with path-restricted ingress (only <code>/v1/mcp/*</code>, <code>/oauth/*</code>, <code>/.well-known/oauth-authorization-server</code>, <code>/health</code> reach localhost; everything else is 404'd at Cloudflare's edge)</li>
                                <li>Install autostart (launchd plist on macOS, systemd user unit on Linux)</li>
                                <li>Verify <code>https://${subdomain}.${zone}/health</code> returns 200</li>
                            </ol>
                            ${!creatingTunnel && !tunnelHostname && html`
                                <button class="btn btn-primary" onClick=${startCreateTunnel}>Create tunnel</button>
                            `}
                            ${creatingTunnel && html`
                                <div style="display:flex;align-items:center;gap:8px;color:var(--accent);">
                                    <span class="spinner" style="width:14px;height:14px;"></span>
                                    Working...
                                </div>
                            `}
                            ${tunnelLog && html`<pre style="background:var(--bg-elev);padding:8px;border-radius:4px;font-size:11px;max-height:240px;overflow:auto;margin-top:12px;color:var(--text-dim);white-space:pre-wrap;">${tunnelLog}</pre>`}
                        </div>
                    `}

                    ${step === 6 && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;">Mint a bearer token</h3>
                            <p>The bearer is the credential the ${forChatGPT ? 'ChatGPT connector' : 'remote tool'} will use to talk to SAGE. Tokens are scoped to a specific agent identity — pick (or create) the agent it should run as.</p>
                            <div class="wizard-field">
                                <label>Agent</label>
                                <select class="wizard-select" value=${agentChoice} onInput=${e => setAgentChoice(e.target.value)}>
                                    <option value="">Pick an agent...</option>
                                    ${eligibleAgents.map(a => html`<option value=${a.agent_id}>${a.name} (${a.role}) — ${a.agent_id.slice(0,12)}…</option>`)}
                                </select>
                                <div style="font-size:11px;color:var(--text-muted);margin-top:4px;">
                                    No dedicated agent yet? Cancel and create one in the Network tab first, then come back. (Best practice: dedicate one agent per remote tool for clear audit trails.)
                                </div>
                            </div>
                            <div class="wizard-field">
                                <label>Token name</label>
                                <input class="wizard-input" value=${tokenName} onInput=${e => setTokenName(e.target.value)} placeholder=${forChatGPT ? 'chatgpt' : 'remote'} />
                            </div>
                            <button class="btn btn-primary" onClick=${startMintToken} disabled=${minting || !agentChoice}>
                                ${minting ? 'Minting…' : 'Mint bearer'}
                            </button>
                        </div>
                    `}

                    ${step === 7 && mintedToken && forChatGPT && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;color:var(--accent-green);">✓ Ready to paste into ChatGPT</h3>
                            <div class="warning-banner" style="margin-bottom:14px;">
                                Save the bearer token NOW — it's shown ONCE. ChatGPT will retrieve it via OAuth at consent time.
                            </div>
                            <${ChatGPTCopyField} label="App name" value="SAGE" />
                            <${ChatGPTCopyField} label="MCP Server URL" value=${`https://${tunnelHostname || (subdomain + '.' + zone)}/v1/mcp/sse`} />
                            <${ChatGPTCopyField} label="Authentication" value="OAuth" />
                            <${ChatGPTCopyField} label="Auth URL" value=${`https://${tunnelHostname || (subdomain + '.' + zone)}/oauth/authorize`} />
                            <${ChatGPTCopyField} label="Token URL" value=${`https://${tunnelHostname || (subdomain + '.' + zone)}/oauth/token`} />
                            <${ChatGPTCopyField} label="OAuth Client ID" value="chatgpt" />
                            <${ChatGPTCopyField} label="OAuth Client Secret" value="(leave empty)" />
                            <${ChatGPTCopyField} label="Token endpoint auth method" value="none" />
                            <${ChatGPTCopyField} label="Bearer token (save now!)" value=${mintedToken.token} sensitive=${true} />

                            <div style="margin-top:18px;display:flex;gap:8px;">
                                <a class="btn btn-primary" href="https://chatgpt.com/#settings/Connectors" target="_blank" rel="noopener">Open ChatGPT Connectors →</a>
                            </div>
                        </div>
                    `}

                    ${step === 7 && mintedToken && !forChatGPT && html`
                        <div style="line-height:1.55;">
                            <h3 style="margin-top:0;color:var(--accent-green);">✓ Your public URL is ready</h3>
                            <div class="warning-banner" style="margin-bottom:14px;">
                                Save the bearer token NOW — it's shown ONCE. It carries the agent's identity; treat it like a password.
                            </div>
                            <${ChatGPTCopyField} label="MCP Server URL" value=${`https://${tunnelHostname || (subdomain + '.' + zone)}/v1/mcp/sse`} />
                            <${ChatGPTCopyField} label="Bearer token (save now!)" value=${mintedToken.token} sensitive=${true} />
                            <p style="margin:16px 0 0;color:var(--text-dim);font-size:13px;">Paste the block that matches your tool on the other computer:</p>
                            <${RemotePasteBlocks} url=${`https://${tunnelHostname || (subdomain + '.' + zone)}/v1/mcp/sse`} token=${mintedToken.token} selfSigned=${false} />
                        </div>
                    `}
                </div>
                <div class="wizard-footer">
                    <button class="btn" onClick=${onClose}>${step === 7 ? 'Done' : 'Cancel'}</button>
                    ${step > 1 && step < 7 && html`<button class="btn" onClick=${() => setStep(step - 1)}>Back</button>`}
                    ${step === 1 && html`<button class="btn btn-primary" onClick=${() => setStep(2)}>Continue</button>`}
                    ${step === 2 && cloudflaredInstalled === true && html`<button class="btn btn-primary" onClick=${() => setStep(3)}>Continue</button>`}
                    ${step === 3 && loginAuthenticated && html`<button class="btn btn-primary" onClick=${() => setStep(4)}>Continue</button>`}
                    ${step === 4 && html`<button class="btn btn-primary" onClick=${() => setStep(5)} disabled=${!zone || !subdomain}>Continue</button>`}
                    ${step === 5 && tunnelHostname && html`<button class="btn btn-primary" onClick=${() => setStep(6)}>Continue</button>`}
                </div>
            </div>
        </div>
    `;
}

// ChatGPTCopyField — labeled value with one-click copy. Used in the final
// "paste into ChatGPT" card.
function ChatGPTCopyField({ label, value, sensitive }) {
    const [copied, setCopied] = useState(false);
    const [revealed, setRevealed] = useState(!sensitive);
    const onCopy = async () => {
        try {
            await navigator.clipboard.writeText(value);
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
        } catch (e) { /* clipboard blocked */ }
    };
    const display = sensitive && !revealed ? '•'.repeat(Math.min(value.length, 24)) : value;
    return html`
        <div style="display:grid;grid-template-columns:180px 1fr auto;gap:8px;align-items:center;margin-bottom:8px;">
            <div style="font-size:12px;color:var(--text-dim);">${label}</div>
            <code style="background:var(--bg-elev);padding:6px 8px;border-radius:3px;font-size:12px;word-break:break-all;">${display}</code>
            <div style="display:flex;gap:4px;">
                ${sensitive && html`<button class="btn" style="padding:4px 8px;font-size:11px;" onClick=${() => setRevealed(!revealed)}>${revealed ? 'Hide' : 'Show'}</button>`}
                <button class="btn" style="padding:4px 8px;font-size:11px;" onClick=${onCopy}>${copied ? 'Copied!' : 'Copy'}</button>
            </div>
        </div>
    `;
}

// CursorSetupPanel — informational panel for bearer-only clients (Cursor,
// Cline, Claude Desktop). These don't need a public URL/tunnel — they speak
// directly to localhost over the bearer auth scheme. The most local-first
// option: zero external surface area.
function CursorSetupPanel({ agents, onClose }) {
    const [agentChoice, setAgentChoice] = useState('');
    const [tokenName, setTokenName] = useState('cursor');
    const [mintedToken, setMintedToken] = useState(null);
    const [minting, setMinting] = useState(false);
    const [error, setError] = useState(null);
    const eligibleAgents = (agents || []).filter(a => a.status !== 'removed');

    const onMint = async () => {
        setMinting(true);
        setError(null);
        try {
            const r = await wizardMintToken(agentChoice, tokenName);
            if (r.error) setError(r.error);
            else setMintedToken(r);
        } catch (e) { setError('mint failed: ' + e.message); }
        setMinting(false);
    };

    return html`
        <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) onClose(); }}>
            <div class="wizard-modal" style="max-width:620px;max-height:85vh;display:flex;flex-direction:column;">
                <div class="wizard-header">
                    <h2>Connect to Cursor / Cline / Claude Desktop</h2>
                    <button class="detail-close" onClick=${onClose}>×</button>
                </div>
                <div class="wizard-body" style="overflow-y:auto;flex:1;padding:20px;line-height:1.55;">
                    <div class="summary-card" style="padding:14px;margin-bottom:14px;">
                        <strong>Easiest path:</strong> use <em>Connect an AI tool → On this computer</em> — SAGE writes a working
                        config for Cursor, Cline, or Claude Desktop automatically (a local stdio connection, no certificate to trust).
                        The bearer-over-HTTPS method below is a manual alternative for clients that specifically need a URL.
                    </div>
                    <p>These clients can also take a bearer token directly — <strong>no tunnel, no public URL, no Cloudflare account needed</strong>. They run on the same machine as SAGE and talk to <code>https://localhost:8443</code>.</p>
                    <p style="font-size:12px;color:var(--text-muted);">Heads up: <code>localhost:8443</code> uses SAGE's self-signed certificate. Stock Cursor/Cline reject that over a URL, so the stdio path above (or Claude Desktop, which prompts to trust it) is the reliable choice.</p>

                    ${error && html`<div class="import-error" style="margin-bottom:12px;">${error}</div>`}

                    ${!mintedToken && html`
                        <h3 style="margin-top:18px;">1. Mint a bearer</h3>
                        <div class="wizard-field">
                            <label>Agent</label>
                            <select class="wizard-select" value=${agentChoice} onInput=${e => setAgentChoice(e.target.value)}>
                                <option value="">Pick an agent...</option>
                                ${eligibleAgents.map(a => html`<option value=${a.agent_id}>${a.name} (${a.role}) — ${a.agent_id.slice(0,12)}…</option>`)}
                            </select>
                        </div>
                        <div class="wizard-field">
                            <label>Token name</label>
                            <input class="wizard-input" value=${tokenName} onInput=${e => setTokenName(e.target.value)} placeholder="cursor" />
                        </div>
                        <button class="btn btn-primary" onClick=${onMint} disabled=${minting || !agentChoice}>
                            ${minting ? 'Minting…' : 'Mint bearer'}
                        </button>
                    `}

                    ${mintedToken && html`
                        <h3 style="margin-top:18px;color:var(--accent-green);">✓ Token minted</h3>
                        <div class="warning-banner" style="margin-bottom:14px;">
                            Save the bearer NOW — it's shown ONCE. Paste it into your client's MCP config.
                        </div>
                        <${ChatGPTCopyField} label="MCP Server URL" value="https://localhost:8443/v1/mcp/sse" />
                        <${ChatGPTCopyField} label="Auth header" value=${`Authorization: Bearer ${mintedToken.token}`} sensitive=${true} />

                        <h3 style="margin-top:24px;">2. Drop it in your client's config</h3>
                        <p style="font-size:12px;color:var(--text-dim);margin-bottom:6px;"><strong>Cursor / Cline</strong> — add to your MCP servers config (typically <code>~/.cursor/mcp.json</code> or via Settings → MCP):</p>
                        <pre style="background:var(--bg-elev);padding:10px;border-radius:4px;font-size:11px;overflow:auto;color:var(--text);">{
  "mcpServers": {
    "sage": {
      "url": "https://localhost:8443/v1/mcp/sse",
      "headers": { "Authorization": "Bearer ${mintedToken.token}" }
    }
  }
}</pre>
                        <p style="font-size:12px;color:var(--text-dim);margin-top:14px;"><strong>Claude Desktop</strong> — Settings → Developer → Edit Config; add the same block. Restart Claude Desktop.</p>
                    `}
                </div>
                <div class="wizard-footer">
                    <button class="btn" onClick=${onClose}>${mintedToken ? 'Done' : 'Cancel'}</button>
                </div>
            </div>
        </div>
    `;
}

// SAME_MACHINE_PROVIDERS — the AI tools SAGE can wire up on THIS computer.
// folder=true tools keep their MCP config inside a project directory (so we
// need an absolute project path); folder=false tools have one app-wide config
// and ignore any path.
const SAME_MACHINE_PROVIDERS = [
    { key: 'claude-code', name: 'Claude Code', icon: '\u{1F916}', folder: true },
    { key: 'codex', name: 'Codex', icon: '⌨️', folder: true },
    { key: 'cursor', name: 'Cursor', icon: '\u{1F4BB}', folder: true },
    { key: 'windsurf', name: 'Windsurf', icon: '\u{1F30A}', folder: false },
    { key: 'claude-desktop', name: 'Claude Desktop', icon: '\u{1F5A5}️', folder: false },
];

// remoteMcpJson — the single source of truth for the MCP config a remote client
// pastes to reach SAGE. kind 'url' = HTTP+SSE clients (Cursor/Cline) that take a
// url + Authorization header; kind 'mcp-remote' = stdio-only clients (Windsurf,
// Claude Desktop) that bridge through the npx mcp-remote shim.
//
// For mcp-remote the header VALUE ("Bearer <token>") must NOT ride in an argv
// element — several MCP clients (Claude Desktop especially) split args on
// spaces, which mangles the token. mcp-remote supports ${VAR} substitution in
// --header, so we pass "Authorization:${AUTH_HEADER}" (no space after the colon)
// and put the value in env, where the space is safe. When selfSigned, env also
// carries NODE_TLS_REJECT_UNAUTHORIZED=0 so mcp-remote accepts the node's
// self-signed LAN cert (there is no equivalent escape for url clients).
function remoteMcpJson(kind, url, token, selfSigned) {
    if (kind === 'url') {
        return `{
  "mcpServers": {
    "sage": {
      "url": "${url}",
      "headers": { "Authorization": "Bearer ${token}" }
    }
  }
}`;
    }
    const env = [`"AUTH_HEADER": "Bearer ${token}"`];
    if (selfSigned) env.push(`"NODE_TLS_REJECT_UNAUTHORIZED": "0"`);
    return `{
  "mcpServers": {
    "sage": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "${url}", "--header", "Authorization:\${AUTH_HEADER}"],
      "env": { ${env.join(', ')} }
    }
  }
}`;
}

// RemotePasteBlocks — renders BOTH client families' paste blocks (used by the
// RemoteAccessWizard's tool step, where the specific tool wasn't carried
// through the tunnel handoff). url has a valid cert when it's the tunnel; pass
// selfSigned only for a self-signed LAN URL.
function RemotePasteBlocks({ url, token, selfSigned }) {
    const preStyle = 'background:var(--bg-elev);padding:10px;border-radius:4px;font-size:11px;overflow:auto;color:var(--text);';
    return html`
        <p style="font-size:12px;color:var(--text-dim);margin:14px 0 4px;"><strong>Cursor / Cline</strong> — url + bearer header (Cursor: <code>~/.cursor/mcp.json</code> or Settings → MCP):</p>
        <pre style=${preStyle}>${remoteMcpJson('url', url, token, selfSigned)}</pre>
        ${selfSigned && html`<div class="warning-banner" style="margin-top:8px;">Stock Cursor/Cline will reject a self-signed certificate — use the public tunnel URL for these.</div>`}
        <p style="font-size:12px;color:var(--text-dim);margin:16px 0 4px;"><strong>Windsurf / Claude Desktop</strong> — stdio via <code>mcp-remote</code> (needs Node.js on the other machine):</p>
        <pre style=${preStyle}>${remoteMcpJson('mcp-remote', url, token, selfSigned)}</pre>
    `;
}

// REMOTE_PROVIDERS — tools/chats running on ANOTHER computer that reach this
// node over the network (Flow 2). kind drives the paste block we generate:
//   'url'        — the client speaks HTTP+SSE with url+headers (Cursor, Cline)
//   'mcp-remote' — the client is stdio-only, so it bridges via the mcp-remote npx shim (Windsurf, Claude Desktop)
//   'oauth'      — a hosted chat that needs a public URL + OAuth (ChatGPT); routed to the dedicated tunnel wizard
const REMOTE_PROVIDERS = [
    { key: 'cursor', name: 'Cursor / Cline', icon: '\u{1F4BB}', kind: 'url' },
    { key: 'windsurf', name: 'Windsurf', icon: '\u{1F30A}', kind: 'mcp-remote' },
    { key: 'claude-desktop', name: 'Claude Desktop', icon: '\u{1F5A5}️', kind: 'mcp-remote' },
    { key: 'chatgpt', name: 'ChatGPT / hosted chat', icon: '\u{1F4AC}', kind: 'oauth' },
];

// RemoteConnectPanel — Flow 2 body. A tool on another computer can only reach
// this node via a public tunnel or a direct LAN bind, so we first ask the node
// how it's reachable (connectRemoteUrl), then walk the user through: pick tool
// → (bearer clients) mint a token + copy a paste block against the reachable
// URL, or (ChatGPT) hand off to the tunnel/OAuth wizard. When there's NO remote
// path we say so plainly and offer to set one up.
function RemoteConnectPanel({ agents, onOpenChatGPT }) {
    const [info, setInfo] = useState(null);
    const [loading, setLoading] = useState(true);
    const [loadErr, setLoadErr] = useState(null);
    const [tool, setTool] = useState('');
    const [base, setBase] = useState('');           // 'tunnel' | 'lan'
    const [lanIdx, setLanIdx] = useState(0);        // which lan_candidates entry the operator picked
    const [agentChoice, setAgentChoice] = useState('');
    const [tokenName, setTokenName] = useState('remote');
    const [minting, setMinting] = useState(false);
    const [minted, setMinted] = useState(null);
    const [mintErr, setMintErr] = useState(null);

    const eligibleAgents = (agents || []).filter(a => a.status !== 'removed');
    const selected = REMOTE_PROVIDERS.find(p => p.key === tool) || null;

    useEffect(() => {
        let alive = true;
        connectRemoteUrl()
            .then(d => { if (alive) { setInfo(d); setBase(d.has_tunnel ? 'tunnel' : (d.lan_exposed ? 'lan' : '')); } })
            .catch(e => { if (alive) setLoadErr(e.message || 'could not read node reachability'); })
            .finally(() => { if (alive) setLoading(false); });
        return () => { alive = false; };
    }, []);

    // Reset the per-tool wizard when switching tools.
    const pickTool = (key) => {
        setTool(key);
        setMinted(null);
        setMintErr(null);
        if (info) setBase(info.has_tunnel ? 'tunnel' : (info.lan_exposed ? 'lan' : ''));
    };

    const onMint = async () => {
        setMinting(true);
        setMintErr(null);
        try {
            const r = await wizardMintToken(agentChoice, tokenName || 'remote');
            if (r.error) setMintErr(r.error);
            else setMinted(r);
        } catch (e) { setMintErr('mint failed: ' + (e.message || e)); }
        setMinting(false);
    };

    const lanCands = (info && info.lan_candidates) || [];
    const lanCand = lanCands[lanIdx] || lanCands[0] || null;
    const lanUrl = lanCand ? `https://${lanCand.ip}:${(info && info.mcp_port) || 8443}/v1/mcp/sse` : null;
    const baseUrl = base === 'tunnel' ? (info && info.tunnel_mcp_url) : lanUrl;
    const isSelfSigned = base === 'lan' && !!(info && info.self_signed);

    // Cursor/Cline (url kind) can't be told to trust a self-signed cert, so a
    // LAN URL simply won't work for them. This gates the mint form (below) so
    // the dead-end is stated BEFORE the user mints a throwaway bearer.
    const urlSelfSignedDeadEnd = selected && selected.kind === 'url' && isSelfSigned;

    // Per-tool paste block, generated once a bearer is minted. url-kind never
    // reaches here while self-signed (urlSelfSignedDeadEnd gates minting).
    const renderPasteBlock = () => {
        const token = minted.token;
        if (selected.kind === 'url') {
            return html`
                <p style="font-size:12px;color:var(--text-dim);margin-bottom:6px;">
                    <strong>${selected.name}</strong> on the other computer — add this to its MCP servers config
                    (Cursor: <code>~/.cursor/mcp.json</code> or Settings → MCP; Cline: its MCP settings JSON):
                </p>
                <pre style="background:var(--bg-elev);padding:10px;border-radius:4px;font-size:11px;overflow:auto;color:var(--text);">${remoteMcpJson('url', baseUrl, token, false)}</pre>
                <p style="font-size:11px;color:var(--text-muted);margin-top:6px;">Save the file, then reload or restart ${selected.name} — SAGE shows up in its MCP servers list.</p>
            `;
        }
        // mcp-remote: stdio-only clients bridge through the npx shim.
        const where = selected.key === 'claude-desktop'
            ? 'Claude Desktop → Settings → Developer → Edit Config'
            : 'Windsurf → its MCP config (~/.codeium/windsurf/mcp_config.json)';
        return html`
            <p style="font-size:12px;color:var(--text-dim);margin-bottom:6px;">
                <strong>${selected.name}</strong> talks over stdio, so it bridges through <code>mcp-remote</code>
                (requires Node.js on the other machine). Add this in ${where}, then restart it:
            </p>
            <pre style="background:var(--bg-elev);padding:10px;border-radius:4px;font-size:11px;overflow:auto;color:var(--text);">${remoteMcpJson('mcp-remote', baseUrl, token, isSelfSigned)}</pre>
            ${isSelfSigned && html`<div class="warning-banner" style="margin-top:8px;">
                This LAN URL uses a self-signed certificate — the <code>NODE_TLS_REJECT_UNAUTHORIZED</code> line above tells
                mcp-remote to accept it. The public tunnel path has a real certificate and doesn't need it.
            </div>`}
        `;
    };

    if (loading) return html`<div style="text-align:center;padding:32px;color:var(--text-dim);">Checking how this node is reachable…</div>`;
    if (loadErr) return html`<div class="import-error">${loadErr}</div>`;

    return html`
        <h3 style="margin-top:0;">Which tool on the other computer?</h3>
        <div class="connect-cards" style="grid-template-columns:repeat(auto-fill,minmax(150px,1fr));margin-bottom:8px;">
            ${REMOTE_PROVIDERS.map(p => html`
                <div class="connect-card ${tool === p.key ? 'selected' : ''}" role="button" tabIndex="0"
                    onClick=${() => pickTool(p.key)}
                    onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pickTool(p.key); } }}>
                    <div class="connect-card-icon">${p.icon}</div>
                    <h4>${p.name}</h4>
                    <p>${p.kind === 'oauth' ? 'OAuth + public URL' : (p.kind === 'url' ? 'URL + bearer' : 'stdio via mcp-remote')}</p>
                </div>
            `)}
        </div>

        ${selected && selected.kind === 'oauth' && html`
            <div class="summary-card" style="padding:18px;margin-top:12px;">
                <p style="margin-top:0;">ChatGPT (and other hosted chats) need a <strong>public HTTPS URL and OAuth</strong> —
                    they can't reach a bare localhost or LAN address. The dedicated wizard sets up the Cloudflare tunnel,
                    mints the token, and gives you the exact fields to paste into ChatGPT's connector.</p>
                ${info.has_tunnel
                    ? html`<p style="color:var(--accent-green);margin:0 0 12px;">✓ A tunnel is already configured (<code>${info.tunnel_hostname}</code>) — the wizard will reuse it.</p>`
                    : html`<p style="color:var(--text-dim);margin:0 0 12px;">No tunnel yet — the wizard will walk you through creating one.</p>`}
                <button class="btn btn-primary" onClick=${() => onOpenChatGPT && onOpenChatGPT('chatgpt')}>Open the ChatGPT setup wizard →</button>
            </div>
        `}

        ${selected && selected.kind !== 'oauth' && !info.has_tunnel && !info.lan_exposed && html`
            <div class="summary-card" style="padding:18px;margin-top:12px;">
                <h4 style="margin-top:0;">This node isn't reachable from another computer yet</h4>
                <p style="color:var(--text-dim);">SAGE is listening on localhost only, so a tool on a different machine
                    has no way in. Give it a public URL — a stable HTTPS address that works from anywhere. The wizard sets up
                    the tunnel, then hands you the paste block for your tool.</p>
                <button class="btn btn-primary" onClick=${() => onOpenChatGPT && onOpenChatGPT('tool')}>Set up a public tunnel →</button>
            </div>
        `}

        ${selected && selected.kind !== 'oauth' && (info.has_tunnel || info.lan_exposed) && html`
            <div style="margin-top:12px;">
                ${(info.has_tunnel && info.lan_exposed) && html`
                    <div class="wizard-field">
                        <label>How will the other computer reach this node?</label>
                        <div style="display:flex;gap:16px;font-size:13px;margin-top:4px;">
                            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;">
                                <input type="radio" name="rc-base" checked=${base === 'tunnel'} onChange=${() => setBase('tunnel')} />
                                Public tunnel <span style="color:var(--text-muted);">(${info.tunnel_hostname})</span>
                            </label>
                            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;">
                                <input type="radio" name="rc-base" checked=${base === 'lan'} onChange=${() => setBase('lan')} />
                                ${lanCand && lanCand.is_private ? 'Local network' : 'Direct address'}
                            </label>
                        </div>
                    </div>
                `}

                ${base === 'lan' && lanCands.length > 1 && html`
                    <div class="wizard-field">
                        <label>Which address can the other computer reach? <span style="color:var(--text-muted);font-weight:normal;">(pick the network you share with it)</span></label>
                        <select class="wizard-select" value=${String(lanIdx)} onInput=${e => setLanIdx(parseInt(e.target.value, 10) || 0)}>
                            ${lanCands.map((c, i) => html`<option value=${String(i)}>${c.iface} — ${c.ip} ${c.is_private ? '(local network)' : '(direct/overlay)'}</option>`)}
                        </select>
                    </div>
                `}

                ${baseUrl && html`
                    <div style="font-size:12px;color:var(--text-muted);margin:6px 0 10px;">
                        Configured at <code>${baseUrl}</code>${base === 'tunnel'
                            ? ''
                            : (lanCand && lanCand.is_private
                                ? ` — should be reachable from devices on the same network (if your firewall allows inbound TCP :${(info && info.mcp_port) || 8443})`
                                : ' — reachable from devices that can route to this address')}.
                    </div>
                    ${base === 'tunnel' && html`
                        <div style="font-size:11px;color:var(--text-muted);margin:-4px 0 10px;">
                            The cloudflared tunnel must be running on this machine for the tool to connect. If it can't reach SAGE, confirm the tunnel is up.
                        </div>
                    `}
                    ${base === 'lan' && lanCand && !lanCand.is_private && html`
                        <div class="warning-banner" style="margin:0 0 10px;">
                            <strong>${lanCand.ip} is not a private LAN address.</strong> It may be reachable from the public internet — anyone with the bearer could connect, and mcp-remote is told to skip certificate checks. Prefer the public tunnel (a real certificate) unless you know this address is safe.
                        </div>
                    `}
                `}

                ${mintErr && html`<div class="import-error" style="margin-bottom:12px;">${mintErr}</div>`}

                ${urlSelfSignedDeadEnd
                    ? html`
                        <div class="warning-banner" style="margin-top:8px;">
                            <strong>${selected.name} can't use the local-network URL.</strong> It's served with a self-signed
                            certificate and stock Cursor/Cline reject that with no override.
                            ${info.has_tunnel
                                ? html` Use the public tunnel — it has a real certificate.`
                                : html` Set up a public tunnel (a real certificate) and this tool will connect cleanly.`}
                        </div>
                        ${info.has_tunnel
                            ? html`<button class="btn btn-primary" style="margin-top:10px;" onClick=${() => setBase('tunnel')}>Use the public tunnel instead →</button>`
                            : html`<button class="btn btn-primary" style="margin-top:10px;" onClick=${() => onOpenChatGPT && onOpenChatGPT('tool')}>Set up a public tunnel →</button>`}
                    `
                    : html`
                        ${!minted && html`
                            <h4 style="margin:14px 0 8px;">Mint a bearer for the other computer</h4>
                            ${eligibleAgents.length === 0
                                ? html`<div class="import-error" style="margin-bottom:8px;">No agents yet — create one in the Network tab first, then come back to mint a bearer for it.</div>`
                                : html`
                                    <div class="wizard-field">
                                        <label>Agent</label>
                                        <select class="wizard-select" value=${agentChoice} onInput=${e => setAgentChoice(e.target.value)}>
                                            <option value="">Pick an agent…</option>
                                            ${eligibleAgents.map(a => html`<option value=${a.agent_id}>${a.name} (${a.role}) — ${a.agent_id.slice(0,12)}…</option>`)}
                                        </select>
                                        <div style="font-size:11px;color:var(--text-muted);margin-top:4px;">No agent yet? Create one in the Network tab first. (Best practice: one dedicated agent per remote tool for clear audit trails.)</div>
                                    </div>
                                    <div class="wizard-field">
                                        <label>Token name</label>
                                        <input class="wizard-input" value=${tokenName} onInput=${e => setTokenName(e.target.value)} placeholder="remote" />
                                    </div>
                                    <button class="btn btn-primary" onClick=${onMint} disabled=${minting || !agentChoice}>
                                        ${minting ? 'Minting…' : 'Mint bearer'}
                                    </button>
                                `}
                        `}

                        ${minted && html`
                            <h4 style="margin:14px 0 8px;color:var(--accent-green);">✓ Token minted</h4>
                            <div class="warning-banner" style="margin-bottom:14px;">
                                Save the bearer NOW — it's shown ONCE. It carries the agent's identity; treat it like a password.
                            </div>
                            <${ChatGPTCopyField} label="MCP Server URL" value=${baseUrl} />
                            <${ChatGPTCopyField} label="Bearer token (save now!)" value=${minted.token} sensitive=${true} />
                            <div style="margin-top:16px;">${renderPasteBlock()}</div>
                        `}
                    `}
            </div>
        `}
    `;
}

// ConnectToolModal — Phase 5b entry point for wiring an AI tool to SAGE.
// Step 0 branches on WHERE the tool lives. Flow 1 (same machine) is fully
// implemented here: pick a provider, give a project folder for folder-scoped
// tools, hit Connect, and the node writes/merges the tool's MCP config, then we
// show a per-file result rail. Flows 2 and 3 are placeholders for later.
function ConnectToolModal({ onClose, agents, onOpenChatGPT }) {
    const [view, setView] = useState('branch'); // 'branch' | 'flow1' | 'flow2' | 'flow3'
    const [provider, setProvider] = useState('');
    const [projectPath, setProjectPath] = useState('');
    const [connecting, setConnecting] = useState(false);
    const [result, setResult] = useState(null); // full response JSON { ok, files, provider, error? }
    const [error, setError] = useState(null);    // thrown / network / 400 error message

    const selected = SAME_MACHINE_PROVIDERS.find(p => p.key === provider) || null;
    const needsPath = !!(selected && selected.folder);
    const canConnect = !!selected && (!needsPath || projectPath.trim()) && !connecting;

    const resetFlow1 = () => { setProvider(''); setProjectPath(''); setResult(null); setError(null); };
    const pickProvider = (key) => { setProvider(key); setResult(null); setError(null); };

    const doConnect = async () => {
        if (!selected) return;
        setConnecting(true);
        setResult(null);
        setError(null);
        try {
            const body = {};
            if (needsPath) body.path = projectPath.trim();
            const data = await connectProvider(selected.key, body);
            setResult(data);
        } catch (e) {
            setError(e.message || 'Connection failed');
        }
        setConnecting(false);
    };

    const badgeStyle = (action) => action === 'created'
        ? 'background:var(--accent-dim);color:var(--accent);'
        : 'background:var(--primary-dim);color:var(--primary);';

    return html`
        <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) onClose(); }}>
            <div class="wizard-modal" style="max-width:640px;max-height:85vh;display:flex;flex-direction:column;">
                <div class="wizard-header">
                    <h2>Connect an AI tool</h2>
                    <button class="detail-close" onClick=${onClose}>×</button>
                </div>
                <div class="wizard-body" style="overflow-y:auto;flex:1;padding:20px;line-height:1.55;">
                    ${view === 'branch' && html`
                        <h3 style="margin-top:0;">Is the tool on THIS computer, or another one?</h3>
                        <p style="color:var(--text-dim);">Pick where the AI tool you want to connect actually runs.</p>
                        <div class="connect-cards" style="grid-template-columns:1fr;margin-top:16px;">
                            <div class="connect-card" style="text-align:left;" role="button" tabIndex="0"
                                onClick=${() => { resetFlow1(); setView('flow1'); }}
                                onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); resetFlow1(); setView('flow1'); } }}>
                                <div class="connect-card-icon">\u{1F5A5}️</div>
                                <h4>On this computer</h4>
                                <p>Claude Code, Codex, Cursor, Windsurf, or Claude Desktop running right here. SAGE writes its config.</p>
                            </div>
                            <div class="connect-card" style="text-align:left;" role="button" tabIndex="0"
                                onClick=${() => setView('flow2')}
                                onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setView('flow2'); } }}>
                                <div class="connect-card-icon">\u{1F4AC}</div>
                                <h4>On another computer (AI chat/tool)</h4>
                                <p>A hosted chat or a tool on a different machine that reaches SAGE over the network.</p>
                            </div>
                            <div class="connect-card" style="text-align:left;" role="button" tabIndex="0"
                                onClick=${() => setView('flow3')}
                                onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setView('flow3'); } }}>
                                <div class="connect-card-icon">\u{1F5A7}</div>
                                <h4>On another computer on my network - add it to my SAGE</h4>
                                <p>Make another computer a node in your SAGE network. It runs its own SAGE and shares this node's memory.</p>
                            </div>
                        </div>
                    `}

                    ${view === 'flow2' && html`
                        <button class="btn" style="margin-bottom:16px;" onClick=${() => setView('branch')}>← Back</button>
                        <${RemoteConnectPanel} agents=${agents} onOpenChatGPT=${onOpenChatGPT} />
                    `}

                    ${view === 'flow3' && html`
                        <button class="btn" style="margin-bottom:16px;" onClick=${() => setView('branch')}>← Back</button>
                        <${NetworkJoinHostPanel} />
                    `}

                    ${view === 'flow1' && html`
                        <button class="btn" style="margin-bottom:16px;" onClick=${() => setView('branch')}>← Back</button>
                        <h3 style="margin-top:0;">Which tool on this computer?</h3>
                        <div class="connect-cards" style="grid-template-columns:repeat(auto-fill,minmax(150px,1fr));margin-bottom:4px;">
                            ${SAME_MACHINE_PROVIDERS.map(p => html`
                                <div class="connect-card ${provider === p.key ? 'selected' : ''}" role="button" tabIndex="0"
                                    onClick=${() => pickProvider(p.key)}
                                    onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pickProvider(p.key); } }}>
                                    <div class="connect-card-icon">${p.icon}</div>
                                    <h4>${p.name}</h4>
                                    <p>${p.folder ? 'Per-project config' : 'App-wide config'}</p>
                                </div>
                            `)}
                        </div>

                        ${needsPath && html`
                            <div class="wizard-field" style="margin-top:16px;">
                                <label>Project folder</label>
                                <input class="wizard-input" value=${projectPath}
                                    onInput=${e => setProjectPath(e.target.value)}
                                    placeholder="/Users/you/project" />
                                <div style="font-size:11px;color:var(--text-muted);margin-top:4px;">
                                    Absolute path to the project folder. There is no folder picker in this view - paste or type the full path.
                                </div>
                            </div>
                        `}

                        ${selected && html`
                            <div style="font-size:11px;color:var(--text-muted);margin:10px 0 4px;">
                                SAGE writes the config and the agent registers itself on first connect. You can manage its identity and permissions afterward here on the Agents page.
                            </div>
                        `}

                        ${selected && html`
                            <button class="btn btn-primary" style="width:100%;padding:12px;font-size:15px;margin-top:8px;"
                                onClick=${doConnect} disabled=${!canConnect}>
                                ${connecting ? 'Connecting...' : `Connect ${selected.name}`}
                            </button>
                        `}

                        ${error && html`<div class="import-error" style="margin-top:16px;">${error}</div>`}

                        ${result && html`
                            <div style="margin-top:16px;">
                                ${(result.files || []).map(f => html`
                                    <div style="display:flex;align-items:center;gap:10px;background:var(--bg-elevated);border-radius:6px;padding:10px 12px;margin-bottom:8px;">
                                        <span style="color:var(--accent);font-weight:700;font-size:15px;">✓</span>
                                        <code style="flex:1;font-size:12px;word-break:break-all;">${f.path}</code>
                                        <span style="font-size:11px;padding:2px 8px;border-radius:10px;${badgeStyle(f.action)}">${f.action}</span>
                                    </div>
                                `)}
                                ${result.ok === false && html`
                                    <div class="import-error" style="margin-top:4px;">${result.error || 'Connection failed'}</div>
                                `}
                                ${result.ok && html`
                                    <div class="warning-banner" style="margin-top:8px;">
                                        Restart ${selected ? selected.name : 'the tool'} to finish - it picks up its new SAGE connection on next launch.
                                    </div>
                                `}
                            </div>
                        `}
                    `}
                </div>
                <div class="wizard-footer">
                    <button class="btn" onClick=${onClose}>${result && result.ok ? 'Done' : 'Close'}</button>
                </div>
            </div>
        </div>
    `;
}

// NetworkJoinHostPanel — Flow 3 host side. Add another computer to your SAGE
// network as a non-validator peer. Ensures the node is in network mode (LAN
// P2P), lets the operator pick the LAN address, mints a pairing token, then
// number-compares + approves the guest.
function NetworkJoinHostPanel() {
    const [info, setInfo] = useState(null);      // { candidates, network_mode }
    const [loading, setLoading] = useState(true);
    const [err, setErr] = useState(null);
    const [switching, setSwitching] = useState(false);
    const [lanIdx, setLanIdx] = useState(0);
    const [session, setSession] = useState(null); // { token, session_id, lan_ip, expires_at }
    const [starting, setStarting] = useState(false);
    const [status, setStatus] = useState(null);   // { state, sas, guest_name, guest_node_id }
    const [approving, setApproving] = useState(false);
    const pollRef = useRef(null);

    useEffect(() => {
        joinHostInterfaces().then(setInfo).catch(e => setErr(e.message)).finally(() => setLoading(false));
        // On close, tear down any pairing immediately (the LAN listener) rather
        // than leaving it up until the TTL reaper.
        return () => {
            if (pollRef.current) clearInterval(pollRef.current);
            joinHostAbort().catch(() => {});
        };
    }, []);

    // Poll host status once a pairing is live.
    useEffect(() => {
        if (!session) return;
        pollRef.current = setInterval(async () => {
            try { setStatus(await joinHostStatus()); } catch (e) { /* keep last */ }
        }, 2000);
        return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } };
    }, [session]);

    const cands = (info && info.candidates) || [];
    const lanCand = cands[lanIdx] || cands[0] || null;

    const onSwitch = async () => {
        setSwitching(true); setErr(null);
        try { await enableNetworkMode(); } catch (e) { setErr(e.message); setSwitching(false); }
        // On success the node re-execs — this session drops; we show a restart notice.
    };

    const onStart = async () => {
        if (!lanCand) return;
        setStarting(true); setErr(null);
        try { setSession(await joinHostStart(lanCand.ip)); }
        catch (e) { setErr(e.message); }
        setStarting(false);
    };

    const onApprove = async () => {
        setApproving(true); setErr(null);
        try { await joinHostApprove(); setStatus(s => ({ ...(s || {}), state: 'approved' })); }
        catch (e) { setErr(e.message); }
        setApproving(false);
    };

    if (loading) return html`<div style="text-align:center;padding:32px;color:var(--text-dim);">Checking your network…</div>`;
    if (err && !info) return html`<div class="import-error">${err}</div>`;

    // Network-mode switch gate.
    if (info && !info.network_mode) {
        return html`
            <div class="summary-card" style="padding:18px;">
                <h4 style="margin-top:0;">Turn on network mode first</h4>
                <p style="color:var(--text-dim);">Right now SAGE only listens on this computer. To let another computer join, it needs to
                    listen on your local network. This is a <strong>one-time switch</strong> and a quick restart — your memories and chain are kept.</p>
                ${switching
                    ? html`<div class="warning-banner">SAGE is switching to network mode and restarting (about 10-20s). When it's back, unlock the vault and reopen <em>Add a computer</em>.</div>`
                    : html`<button class="btn btn-primary" onClick=${onSwitch}>Switch to network mode & restart</button>`}
                ${err && html`<div class="import-error" style="margin-top:12px;">${err}</div>`}
            </div>
        `;
    }

    // Not yet started: pick the LAN address.
    if (!session) {
        return html`
            <h3 style="margin-top:0;">Add a computer to your network</h3>
            <p style="color:var(--text-dim);">The other computer will run its own SAGE and share this node's memory. Pick the address it can reach you on:</p>
            ${cands.length === 0
                ? html`<div class="import-error">No local network address found. Make sure you're connected to a network.</div>`
                : html`
                    <div class="wizard-field">
                        <label>Your address on the network</label>
                        <select class="wizard-select" value=${String(lanIdx)} onInput=${e => setLanIdx(parseInt(e.target.value, 10) || 0)}>
                            ${cands.map((c, i) => html`<option value=${String(i)}>${c.iface} — ${c.ip} ${c.is_private ? '(local network)' : '(direct)'}</option>`)}
                        </select>
                    </div>
                    <button class="btn btn-primary" style="width:100%;padding:12px;" onClick=${onStart} disabled=${starting}>
                        ${starting ? 'Starting…' : 'Start'}
                    </button>
                `}
            ${err && html`<div class="import-error" style="margin-top:12px;">${err}</div>`}
        `;
    }

    // Pairing live.
    const st = (status && status.state) || 'waiting';
    return html`
        <h3 style="margin-top:0;">On the other computer</h3>
        <p style="color:var(--text-dim);font-size:13px;">Open SAGE there → <em>Join a network</em> and paste this code (or run <code>sage-gui pair &lt;code&gt;</code> in a terminal):</p>
        <${ChatGPTCopyField} label="Pairing code" value=${session.token} sensitive=${true} />

        ${st === 'waiting' && html`
            <div style="display:flex;align-items:center;gap:8px;color:var(--text-dim);margin-top:14px;">
                <span class="spinner" style="width:14px;height:14px;"></span> Waiting for the other computer to connect…
            </div>
        `}

        ${(st === 'connected') && html`
            <div class="summary-card" style="padding:16px;margin-top:14px;">
                <p style="margin:0 0 6px;">A computer wants to join${status.guest_name ? html` — <strong>${status.guest_name}</strong>` : ''}. Does the code shown on it match this one?</p>
                <div style="text-align:center;font-size:30px;letter-spacing:6px;font-weight:700;color:var(--accent);margin:10px 0;">${status.sas}</div>
                <button class="btn btn-primary" style="width:100%;" onClick=${onApprove} disabled=${approving}>
                    ${approving ? 'Approving…' : 'Yes, the codes match — approve'}
                </button>
                <div style="font-size:11px;color:var(--text-muted);margin-top:6px;">Only approve if the numbers are identical on both screens.</div>
            </div>
        `}

        ${(st === 'approved') && html`
            <div class="warning-banner" style="margin-top:14px;background:var(--accent-dim);color:var(--accent);">
                ✓ Approved. The other computer is joining and will restart to sync. You can close this.
            </div>
        `}
        ${err && html`<div class="import-error" style="margin-top:12px;">${err}</div>`}
    `;
}

// NetworkJoinGuestPanel — Flow 3 guest side. Make THIS node part of another
// SAGE network: paste the host's pairing code, compare the 6-digit code, wait
// for the host to approve, then restart to finish (the join is applied at boot).
function NetworkJoinGuestPanel({ onClose }) {
    const [token, setToken] = useState('');
    const [starting, setStarting] = useState(false);
    const [sas, setSas] = useState(null);
    const [status, setStatus] = useState(null); // { state, sas, chain_id, error }
    const [err, setErr] = useState(null);
    const [restarting, setRestarting] = useState(false);
    const pollRef = useRef(null);

    useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current); }, []);

    const onJoin = async () => {
        setStarting(true); setErr(null);
        try {
            const r = await joinGuestStart(token.trim());
            setSas(r.sas);
            pollRef.current = setInterval(async () => {
                try { setStatus(await joinGuestStatus()); } catch (e) { /* keep */ }
            }, 2000);
        } catch (e) { setErr(e.message); }
        setStarting(false);
    };

    const onRestart = async () => {
        setRestarting(true); setErr(null);
        try { await joinGuestRestart(); } catch (e) { setErr(e.message); setRestarting(false); }
    };

    const st = status && status.state;

    return html`
        <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) { joinGuestCancel().catch(() => {}); onClose(); } }}>
            <div class="wizard-modal" style="max-width:560px;">
                <div class="wizard-header">
                    <h2>Join a SAGE network</h2>
                    <button class="detail-close" onClick=${() => { joinGuestCancel().catch(() => {}); onClose(); }}>×</button>
                </div>
                <div class="wizard-body" style="padding:20px;line-height:1.55;">
                    ${!sas && html`
                        <p style="color:var(--text-dim);">This makes <strong>this computer</strong> part of another SAGE network — it keeps its own node but shares that network's memory.
                            On the host, open <em>Connect an AI tool → another computer on my network</em> to get a pairing code, then paste it here.</p>
                        <div class="warning-banner" style="margin-bottom:12px;">Heads up: joining resets this node's local chain state and restarts it. Your stored memories are kept.</div>
                        <div class="wizard-field">
                            <label>Pairing code from the host</label>
                            <textarea class="wizard-input" style="min-height:70px;font-family:monospace;font-size:11px;" value=${token} onInput=${e => setToken(e.target.value)} placeholder="paste the code shown on the other computer"></textarea>
                        </div>
                        <button class="btn btn-primary" onClick=${onJoin} disabled=${starting || !token.trim()}>
                            ${starting ? 'Connecting…' : 'Join'}
                        </button>
                    `}

                    ${sas && (!st || st === 'awaiting_approval' || st === 'connecting') && html`
                        <p style="margin-top:0;">Check this code matches the one on the host, then have them approve:</p>
                        <div style="text-align:center;font-size:34px;letter-spacing:8px;font-weight:700;color:var(--accent);margin:14px 0;">${sas}</div>
                        <div style="display:flex;align-items:center;gap:8px;color:var(--text-dim);justify-content:center;">
                            <span class="spinner" style="width:14px;height:14px;"></span> Waiting for the host to approve…
                        </div>
                    `}

                    ${st === 'ready' && html`
                        <div style="text-align:center;">
                            <div style="font-size:38px;margin-bottom:8px;">✓</div>
                            <h3 style="margin:0 0 8px;color:var(--accent-green);">Approved${status.chain_id ? html` — joining ${status.chain_id}` : ''}</h3>
                            <p style="color:var(--text-dim);">This node will restart to finish joining and start syncing the shared memory.</p>
                            <button class="btn btn-primary" onClick=${onRestart} disabled=${restarting}>
                                ${restarting ? 'Restarting…' : 'Restart & finish joining'}
                            </button>
                            <div style="font-size:11px;color:var(--text-muted);margin-top:8px;">Changed your mind? Close this window to cancel — nothing is applied until you restart.</div>
                        </div>
                    `}

                    ${st === 'error' && html`<div class="import-error" style="margin-top:12px;">${status.error || 'Join failed'}</div>`}
                    ${err && html`<div class="import-error" style="margin-top:12px;">${err}</div>`}
                </div>
            </div>
        </div>
    `;
}

function RemoveConfirmModal({ agent, onConfirm, onCancel }) {
    const [removing, setRemoving] = useState(false);
    const [deployStatus, setDeployStatus] = useState(null);
    const pollRef = useRef(null);

    useEffect(() => {
        return () => {
            if (pollRef.current) {
                clearInterval(pollRef.current);
                pollRef.current = null;
            }
        };
    }, []);

    const handleConfirm = async () => {
        setRemoving(true);
        onConfirm();
    };

    const isDeploying = removing;

    return html`
        <div class="confirm-overlay" onClick=${(e) => { if (e.target === e.currentTarget && !removing) onCancel(); }}>
            <div class="confirm-modal">
                ${!removing
                    ? html`
                        <h3>Remove ${agent.name}?</h3>
                        <p>
                            This marks the agent as removed. ${agent.memory_count > 0
                                ? html`<strong style="color:var(--warning);">Its ${agent.memory_count} memories are kept</strong> with their original attribution — nothing is deleted.`
                                : html`It has no memories.`}
                        </p>
                        <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">
                            On a single-node (personal) network this is instant. On a multi-node network the chain briefly reconfigures its validator set.
                        </div>
                        <div class="confirm-actions" style="margin-top:16px;">
                            <button class="btn" onClick=${onCancel}>Cancel</button>
                            <button class="btn btn-danger" onClick=${handleConfirm}>Remove Agent</button>
                        </div>
                    `
                    : html`
                        <h3>Removing ${agent.name}...</h3>
                        <p style="font-size:13px;color:var(--text-dim);margin-bottom:12px;">
                            Reconfiguring the network without this agent.
                        </p>
                        <div class="deploy-progress">
                            <div class="deploy-phase in-progress">
                                <div class="deploy-phase-icon"><span class="deploy-spinner"></span></div>
                                <span class="deploy-phase-label">Removing agent and redeploying...</span>
                            </div>
                        </div>
                    `
                }
            </div>
        </div>
    `;
}

// ============================================================================
// Pipeline Page — Agent-to-Agent Message Bus
// ============================================================================

function PipelinePage() {
    const [items, setItems] = useState([]);
    const [stats, setStats] = useState({});
    const [total, setTotal] = useState(0);
    const [filter, setFilter] = useState('');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [expanded, setExpanded] = useState(null);
    const [agentList, setAgentList] = useState([]);
    const [showCompose, setShowCompose] = useState(false);
    const [composeTo, setComposeTo] = useState('');
    const [composeIntent, setComposeIntent] = useState('');
    const [composeMsg, setComposeMsg] = useState('');
    const [sending, setSending] = useState(false);

    useEffect(() => { fetchAgents().then(d => setAgentList(d.agents || [])).catch(() => {}); }, []);

    async function doSendNote() {
        if (!composeTo || !composeMsg.trim()) return;
        setSending(true);
        try {
            const res = await sendPipelineNote(composeTo, composeMsg.trim(), composeIntent.trim() || 'note');
            if (res && res.error) { showToast('Could not send: ' + res.error, 'error'); }
            else {
                showToast('Note delivered to the agent inbox', 'success');
                setComposeMsg(''); setComposeIntent(''); setShowCompose(false);
                loadData();
            }
        } catch (e) { showToast('Could not send note: ' + (e.message || e), 'error'); }
        setSending(false);
    }

    const loadData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const [pipeData, statsData] = await Promise.all([
                fetchPipeline({ status: filter || undefined, limit: 100 }),
                fetchPipelineStats(),
            ]);
            setItems(pipeData.items || []);
            setStats(statsData.stats || {});
            setTotal(statsData.total || 0);
        } catch (e) {
            console.error('Pipeline load error:', e);
            setError(e && e.message ? e.message : 'Failed to load pipeline');
        }
        setLoading(false);
    }, [filter]);

    useEffect(() => { loadData(); }, [loadData]);

    // Auto-refresh every 10 seconds
    useEffect(() => {
        const interval = setInterval(loadData, 10000);
        return () => clearInterval(interval);
    }, [loadData]);

    const statusColor = (s) => {
        switch (s) {
            case 'pending': return '#f59e0b';
            case 'claimed': return '#3b82f6';
            case 'completed': return '#10b981';
            case 'expired': return '#6b7280';
            case 'failed': return '#ef4444';
            default: return '#64748b';
        }
    };

    const statusIcon = (s) => {
        switch (s) {
            case 'pending': return '\u23F3';
            case 'claimed': return '\u26A1';
            case 'completed': return '\u2713';
            case 'expired': return '\u23F0';
            case 'failed': return '\u2717';
            default: return '\u2022';
        }
    };

    const truncate = (s, n) => s && s.length > n ? s.slice(0, n) + '...' : s;

    return html`
        <div class="tasks-page" style="padding:24px;">
            <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap;">
                <h2 style="margin:0;font-size:20px;">Pipeline</h2>
                <span style="color:var(--text-muted);font-size:13px;">Agent message bus - what agents ask each other to do, and notes you send them.</span>
                <div style="flex:1"></div>
                ${agentList.length > 0 && html`<button class="btn btn-primary" onClick=${() => setShowCompose(v => !v)}>\u2709 Send note to agent</button>`}
                <button class="btn" onClick=${loadData}>\u21BB Refresh</button>
            </div>
            ${showCompose && html`
                <div style="background:var(--card-bg);border:1px solid var(--primary);border-radius:10px;padding:16px;margin-bottom:20px;">
                    <div style="font-weight:700;margin-bottom:4px;">Send a note to an agent</div>
                    <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">It lands in that agent's inbox (sage_inbox) and surfaces on its next turn - no need to impersonate another agent.</div>
                    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
                        <select class="filter-select" value=${composeTo} onChange=${e => setComposeTo(e.target.value)} style="min-width:180px;">
                            <option value="">Choose an agent...</option>
                            ${agentList.map(a => html`<option value=${a.agent_id}>${a.name}</option>`)}
                        </select>
                        <input class="filter-select" placeholder="intent (optional, e.g. review, fix)" value=${composeIntent} onInput=${e => setComposeIntent(e.target.value)} style="flex:1;min-width:160px;" />
                    </div>
                    <textarea placeholder="Your note or instruction for the agent..." value=${composeMsg} onInput=${e => setComposeMsg(e.target.value)}
                        style="width:100%;min-height:70px;padding:10px;background:var(--bg-secondary);color:var(--text);border:1px solid var(--border);border-radius:8px;font-family:inherit;font-size:13px;resize:vertical;box-sizing:border-box;"></textarea>
                    <div style="display:flex;gap:8px;margin-top:10px;justify-content:flex-end;">
                        <button class="btn btn-secondary" onClick=${() => setShowCompose(false)} disabled=${sending}>Cancel</button>
                        <button class="btn btn-primary" onClick=${doSendNote} disabled=${sending || !composeTo || !composeMsg.trim()}>${sending ? 'Sending...' : 'Send note'}</button>
                    </div>
                </div>
            `}

            <div style="display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap;align-items:center;">
                ${['pending', 'claimed', 'completed', 'expired', 'failed'].map(s => html`
                    <button key=${s}
                        style="display:inline-flex;align-items:center;gap:6px;padding:7px 16px;border-radius:8px;
                               border:1px solid ${filter === s ? statusColor(s) : 'var(--border)'};
                               background:${filter === s ? statusColor(s) + '20' : 'var(--card-bg)'};
                               color:${filter === s ? statusColor(s) : 'var(--text-muted)'};
                               cursor:pointer;font-size:13px;font-weight:500;transition:all 0.15s;"
                        onClick=${() => setFilter(filter === s ? '' : s)}>
                        <span>${statusIcon(s)}</span>
                        <span style="text-transform:capitalize">${s}</span>
                        <span style="font-weight:700;margin-left:2px;">${stats[s] || 0}</span>
                    </button>
                `)}
                <span style="margin-left:auto;color:var(--text-muted);font-size:12px;">
                    ${total} total
                </span>
            </div>

            ${error && html`
                <div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.4);color:#ef4444;padding:12px 16px;border-radius:8px;margin-bottom:16px;display:flex;align-items:center;gap:12px;font-size:13px;">
                    <span style="flex:1;">Couldn't load pipeline: ${error}</span>
                    <button class="btn" onClick=${loadData}>Retry</button>
                </div>
            `}
            ${loading && items.length === 0 && html`
                <div style="text-align:center;padding:60px;color:var(--text-muted);">Loading pipeline...</div>
            `}

            ${!loading && !error && items.length === 0 && html`
                <div style="text-align:center;padding:80px 20px;">
                    <div style="margin-bottom:16px;opacity:0.25;">${icons.pipeline}</div>
                    <p style="color:var(--text-muted);font-size:14px;margin:0 0 8px;">
                        No pipeline messages${filter ? html` with status <strong>"${filter}"</strong>` : ''}.
                    </p>
                    <p style="color:var(--text-muted);font-size:12px;margin:0;max-width:500px;margin:0 auto;">
                        Use <code style="background:var(--code-bg);padding:2px 6px;border-radius:4px;font-size:11px;">sage_pipe(to="perplexity", intent="research", payload="...")</code> from any connected agent to route work between AI providers.
                    </p>
                </div>
            `}

            ${items.length > 0 && html`
                <div style="display:flex;flex-direction:column;gap:10px;">
                    ${items.map(item => {
                        const isExpanded = expanded === item.pipe_id;
                        const from = item.from_name || item.from_provider || (item.from_agent ? item.from_agent.slice(0, 12) + '...' : 'unknown');
                        const to = item.to_name || item.to_provider || (item.to_agent ? item.to_agent.slice(0, 12) + '...' : 'any');
                        return html`
                            <div key=${item.pipe_id}
                                style="background:var(--card-bg);border:1px solid ${isExpanded ? 'var(--accent)' : 'var(--border)'};
                                       border-radius:10px;padding:14px 18px;cursor:pointer;
                                       transition:border-color 0.15s,box-shadow 0.15s;
                                       ${isExpanded ? 'box-shadow:0 0 0 1px var(--accent);' : ''}"
                                onClick=${() => setExpanded(isExpanded ? null : item.pipe_id)}>

                                <div style="display:flex;align-items:center;gap:12px;">
                                    <span style="color:${statusColor(item.status)};font-size:18px;min-width:24px;text-align:center;">
                                        ${statusIcon(item.status)}
                                    </span>
                                    <div style="flex:1;min-width:0;">
                                        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
                                            <span style="font-weight:600;font-size:14px;color:var(--text);">${from}</span>
                                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--text-muted)" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
                                            <span style="font-weight:600;font-size:14px;color:var(--text);">${to}</span>
                                            ${item.intent && html`
                                                <span style="background:var(--accent-bg,rgba(59,130,246,0.1));color:var(--accent,#3b82f6);
                                                             padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600;">
                                                    ${item.intent}
                                                </span>
                                            `}
                                        </div>
                                        <div style="font-size:12px;color:var(--text-muted);margin-top:4px;display:flex;gap:8px;align-items:center;">
                                            <code style="font-size:11px;opacity:0.7;">${item.pipe_id.slice(0, 24)}...</code>
                                            <span>\u00B7</span>
                                            <span>${timeAgo(item.created_at)}</span>
                                            ${item.completed_at ? html`<span>\u00B7</span><span>completed ${timeAgo(item.completed_at)}</span>` : ''}
                                        </div>
                                    </div>
                                    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px;">
                                        <span style="color:${statusColor(item.status)};font-size:11px;font-weight:700;
                                                     text-transform:uppercase;letter-spacing:0.5px;">
                                            ${item.status}
                                        </span>
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--text-muted)" stroke-width="2"
                                             style="transform:rotate(${isExpanded ? '180' : '0'}deg);transition:transform 0.2s;opacity:0.5;">
                                            <path d="M6 9l6 6 6-6"/>
                                        </svg>
                                    </div>
                                </div>

                                ${isExpanded && html`
                                    <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border);">
                                        <div style="margin-bottom:12px;">
                                            <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Payload</div>
                                            <pre style="background:var(--code-bg);padding:12px 14px;border-radius:8px;
                                                        font-size:12px;line-height:1.5;white-space:pre-wrap;word-break:break-word;
                                                        max-height:200px;overflow-y:auto;margin:0;color:var(--text);
                                                        border:1px solid var(--border);">${item.payload || '(no payload)'}</pre>
                                        </div>
                                        ${item.result && html`
                                            <div style="margin-bottom:12px;">
                                                <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Result</div>
                                                <pre style="background:var(--code-bg);padding:12px 14px;border-radius:8px;
                                                            font-size:12px;line-height:1.5;white-space:pre-wrap;word-break:break-word;
                                                            max-height:200px;overflow-y:auto;margin:0;color:var(--text);
                                                            border:1px solid var(--border);">${item.result}</pre>
                                            </div>
                                        `}
                                        <div style="display:flex;gap:20px;font-size:11px;color:var(--text-muted);flex-wrap:wrap;padding-top:4px;">
                                            <span><strong>Pipe ID:</strong> <code style="font-size:10px;">${item.pipe_id}</code></span>
                                            <span><strong>From:</strong> <code style="font-size:10px;">${item.from_agent ? item.from_agent.slice(0, 16) + '...' : 'n/a'}</code></span>
                                            <span><strong>To:</strong> <code style="font-size:10px;">${item.to_agent ? item.to_agent.slice(0, 16) + '...' : item.to_provider || 'any'}</code></span>
                                            <span><strong>Expires:</strong> ${new Date(item.expires_at).toLocaleString()}</span>
                                            ${item.journal_id ? html`<span><strong>Journal:</strong> <code style="font-size:10px;">${item.journal_id.slice(0, 16)}...</code></span>` : ''}
                                        </div>
                                    </div>
                                `}
                            </div>
                        `;
                    })}
                </div>
            `}
        </div>
    `;
}

// ============================================================================
// Overview - top-level node control board. Five stacked section cards (Chain
// health, Quorum and nodes, Agents, Federation, Embeddings) plus an overall
// node-status banner. Polls every 3s; each data source is stored independently
// so one failing feed never blanks the board (last-good values are kept and the
// affected section shows a subtle "stale" note). Terminology is locked:
// "Agents" = this node's own agents; "Federation" = other nodes.
// ============================================================================
function OverviewPage({ sse }) {
    const [health, setHealth] = useState(null);
    const [stats, setStats] = useState(null);
    const [agents, setAgents] = useState([]);
    const [validators, setValidators] = useState(null);
    const [fed, setFed] = useState(null);
    const [failed, setFailed] = useState({});
    const [blockRate, setBlockRate] = useState(null);
    const [, setTick] = useState(0);
    const prevBlockRef = useRef(null);          // { height, timeMs } across polls
    const uptimeRef = useRef({ base: 0, at: 0 });
    const fedEverRef = useRef(false);           // was federation ever reachable? a node with
                                                // federation disabled returns 501 forever, which is
                                                // "not federated", not a "stale" fetch error.

    // Poll every 3s. allSettled so a single failed source keeps its last-good
    // value while the others refresh; each result is stored independently.
    useEffect(() => {
        let alive = true;
        const load = async () => {
            const [rHealth, rStats, rAgents, rValidators, rFed] = await Promise.allSettled([
                fetchHealth(), fetchStats(), fetchAgents(), fetchValidators(), fedConnections(),
            ]);
            if (!alive) return;
            if (rHealth.status === 'fulfilled' && rHealth.value) {
                const hv = rHealth.value;
                setHealth(hv);
                uptimeRef.current = { base: parseUptimeSec(hv.uptime || ''), at: Date.now() };
                const ch = hv.chain;
                if (ch) {
                    const hgt = Number(ch.block_height || 0);
                    const t = ch.block_time ? new Date(ch.block_time).getTime() : null;
                    const prev = prevBlockRef.current;
                    if (prev && t && prev.timeMs && hgt > prev.height) {
                        const dt = (t - prev.timeMs) / 1000;
                        const dh = hgt - prev.height;
                        if (dt > 0 && dh > 0) setBlockRate(dt / dh);
                    }
                    prevBlockRef.current = { height: hgt, timeMs: t };
                }
            }
            if (rStats.status === 'fulfilled') setStats(rStats.value);
            if (rAgents.status === 'fulfilled') setAgents(rAgents.value?.agents || []);
            if (rValidators.status === 'fulfilled') setValidators(rValidators.value);
            if (rFed.status === 'fulfilled') { setFed(rFed.value); fedEverRef.current = true; }
            setFailed({
                health: rHealth.status !== 'fulfilled',
                stats: rStats.status !== 'fulfilled',
                agents: rAgents.status !== 'fulfilled',
                validators: rValidators.status !== 'fulfilled',
                fed: rFed.status !== 'fulfilled',
            });
        };
        load();
        const iv = setInterval(load, 3000);
        return () => { alive = false; clearInterval(iv); };
    }, []);

    // 1s tick so block age, block-rate idle-ness and uptime advance live.
    useEffect(() => {
        const iv = setInterval(() => setTick(t => t + 1), 1000);
        return () => clearInterval(iv);
    }, []);

    // ---- tone tokens + small render helpers ----
    const TONE = {
        healthy:  { bg: 'rgba(16,185,129,0.15)', fg: '#10b981' },
        degraded: { bg: 'rgba(245,158,11,0.15)', fg: '#f59e0b' },
        limited:  { bg: 'rgba(245,158,11,0.15)', fg: '#f59e0b' }, // amber like degraded, but a capability note, not a node fault
        danger:   { bg: 'rgba(239,68,68,0.15)',  fg: '#ef4444' },
        neutral:  { bg: 'rgba(107,114,128,0.15)', fg: '#9ca3af' },
    };
    const pill = (tone, text) => {
        const t = TONE[tone] || TONE.neutral;
        return html`<span style=${`display:inline-flex;align-items:center;gap:6px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;padding:3px 10px;border-radius:999px;background:${t.bg};color:${t.fg};`}>
            <span style=${`width:7px;height:7px;border-radius:50%;background:${t.fg};`}></span>${text}</span>`;
    };
    const tile = (value, label, opts = {}) => {
        const s = [];
        if (opts.color) s.push(`color:${opts.color}`);
        if (opts.small) s.push('font-size:14px;word-break:break-word');
        return html`<div class="chain-stat-card" title=${opts.title || ''}>
            <div class="chain-stat-value" style=${s.join(';')}>${value}</div>
            <div class="chain-stat-label">${label}</div>
            ${opts.sub ? html`<div style="font-size:10px;color:var(--text-muted);margin-top:4px">${opts.sub}</div>` : ''}
        </div>`;
    };
    const deferredTile = (label, help) => html`<div class="chain-stat-card" title="Coming in a later phase.">
        <div class="chain-stat-value" style="color:var(--text-muted)">--</div>
        <div class="chain-stat-label" style="display:inline-flex;align-items:center;gap:4px;justify-content:center;">${label} <${HelpTip} text=${help} /></div>
    </div>`;
    const sectionCard = (title, help, pillNode, stale, body) => html`<div class="settings-section">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
            <h3 style="font-size:14px;font-weight:600;margin:0;color:var(--text);">${title}</h3>
            <${HelpTip} text=${help} />
            <div style="flex:1"></div>
            ${stale ? html`<span style="font-size:10px;color:var(--text-muted);font-style:italic;margin-right:4px;">stale</span>` : ''}
            ${pillNode}
        </div>
        ${body}
    </div>`;
    const fmtAge = (ms) => {
        if (ms == null) return '--';
        const s = Math.floor(ms / 1000);
        if (s < 60) return s + 's';
        if (s < 3600) return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
        return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
    };

    // ---- section 1: chain health ----
    const CHAIN_IDLE_AFTER_MS = 30000;
    const chain = health?.chain || null;
    const blockElapsed = chain?.block_time ? Date.now() - new Date(chain.block_time).getTime() : null;
    const chainIdle = blockElapsed !== null && blockElapsed > CHAIN_IDLE_AFTER_MS;
    const ageS = blockElapsed !== null ? blockElapsed / 1000 : null;
    const appVer = chain?.app_version || '';
    const appVerNum = parseInt(appVer, 10);
    // "0"/empty/unparseable -> neutral: a fresh or un-upgraded chain may not
    // report a consensus app version yet, so do not cry "behind" (amber) on it.
    // Only a real version below 15 is genuinely behind.
    const appVerTone = appVer === '15' ? 'healthy' : (appVerNum > 0 && appVerNum < 15 ? 'degraded' : 'neutral');
    const appVerShown = (appVer && appVer !== '0') ? ('v' + appVer) : '--';
    const mempoolTxs = (chain && chain.mempool_txs != null) ? Number(chain.mempool_txs) : null;
    const mempoolHot = mempoolTxs != null && !isNaN(mempoolTxs) && mempoolTxs > 50;
    let uptimeDisplay = '--';
    if (health && uptimeRef.current.at) {
        uptimeDisplay = formatUptime(uptimeRef.current.base + Math.floor((Date.now() - uptimeRef.current.at) / 1000));
    }
    let chainTone = 'neutral';
    if (chain) {
        // A BFT chain does not mint empty blocks, so a rising block age on an
        // idle chain is normal, not a stall - never flash amber on age alone
        // (that produced a spurious 10-30s amber window before the idle
        // threshold kicked in). Degraded only when the node is behind, the
        // mempool is backed up, or there is pending work not being minted.
        const stalled = mempoolTxs != null && mempoolTxs > 0 && ageS !== null && ageS > CHAIN_IDLE_AFTER_MS / 1000;
        if (chain.catching_up) chainTone = 'degraded';
        else if (mempoolHot) chainTone = 'degraded';
        else if (stalled) chainTone = 'degraded';
        else chainTone = 'healthy';
    }
    const chainTiles = [
        tile(chain ? Number(chain.block_height || 0).toLocaleString() : '--', 'Block height', { color: '#10b981', title: 'Total blocks committed to the chain.' }),
        tile(fmtAge(blockElapsed), 'Last block age', { title: 'Time since the last committed block.', sub: chainIdle ? 'idle - not a stall' : '' }),
        tile(chain ? (chain.catching_up ? 'Catching up' : 'In sync') : '--', 'Sync state', { small: true, color: chain ? (chain.catching_up ? '#f59e0b' : '#10b981') : undefined }),
        tile(appVerShown, 'App version', { small: true, color: appVerTone === 'healthy' ? '#10b981' : (appVerTone === 'degraded' ? '#f59e0b' : undefined), title: 'CometBFT app protocol version. Green when current (15).' }),
        tile(chainIdle ? 'Idle' : (blockRate ? blockRate.toFixed(1) + 's' : '--'), 'Block rate', { small: chainIdle, title: 'Seconds per block, derived client-side from height deltas.' }),
        tile(uptimeDisplay, 'Node uptime', { small: true, title: 'Time since this node process started.' }),
        tile(chain && chain.mempool_txs != null ? chain.mempool_txs : '--', 'Pending transactions', { color: mempoolHot ? '#f59e0b' : undefined, title: 'Unconfirmed transactions waiting in the mempool. Amber above 50 signals a backlog.' }),
        tile(chain?.app_hash ? chain.app_hash.slice(0, 10) : '--', 'State fingerprint', { small: true, title: chain?.app_hash || 'Short prefix of the app hash (consensus state root).' }),
    ];

    // ---- section 2: quorum and nodes ----
    const peerCount = Number(chain?.peers || 0);
    const vCount = validators?.count;
    let quorumTone = 'neutral', quorumLabel = 'Single node';
    if (validators) {
        if (validators.error) { quorumTone = 'degraded'; quorumLabel = 'RPC error'; }
        else if ((vCount || 0) <= 1 && peerCount === 0) { quorumTone = 'neutral'; quorumLabel = 'Single node'; }
        else { quorumTone = 'healthy'; quorumLabel = 'Quorum'; }
    }
    const quorumTiles = [
        tile(vCount != null ? vCount : '--', 'Validators', { title: 'Validators in the consensus set.' }),
        tile(validators && validators.total_voting_power != null ? validators.total_voting_power : '--', 'Total voting power', { small: true, title: 'Sum of every validator voting power.' }),
        tile(chain ? peerCount : '--', 'Connected peers', { title: 'Connected SAGE nodes (0 = solo).' }),
    ];

    // ---- section 3: agents (this node's own) ----
    const now = Date.now();
    const agentAge = (a) => a.last_seen ? (now - new Date(a.last_seen).getTime()) : Infinity;
    const online = agents.filter(a => agentAge(a) < 5 * 60 * 1000).length;
    const offline = agents.filter(a => agentAge(a) > 60 * 60 * 1000).length;
    const memSum = agents.reduce((s, a) => s + (a.memory_count || 0), 0);
    const storedMem = (stats && stats.total_memories != null) ? stats.total_memories : (agents.length ? memSum : null);
    const rankedAgents = agents.slice().sort((a, b) =>
        (b.memory_count || 0) - (a.memory_count || 0) ||
        (new Date(b.last_seen || 0).getTime() - new Date(a.last_seen || 0).getTime()));
    const mostActive = rankedAgents[0] || null;
    const topAgents = rankedAgents.slice(0, 5);
    const dotColor = (a) => { const ms = agentAge(a); return ms < 5 * 60 * 1000 ? '#10b981' : (ms < 60 * 60 * 1000 ? '#f59e0b' : '#6b7280'); };
    let agentTone = 'neutral';
    if (agents.length) agentTone = online > 0 ? 'healthy' : 'neutral';
    const agentTiles = [
        tile(agents.length ? online : '--', 'Agents online', { color: '#10b981', title: 'Agents seen in the last 5 minutes.' }),
        tile(agents.length ? offline : '--', 'Agents offline', { color: '#6b7280', title: 'Agents not seen for over 60 minutes.' }),
        tile(storedMem != null ? Number(storedMem).toLocaleString() : '--', 'Stored memories', { title: 'Total memories stored on this node.' }),
        tile(mostActive ? (mostActive.name || (mostActive.agent_id || '').slice(0, 8)) : '--', 'Most active agent', { small: true, sub: mostActive ? (mostActive.memory_count || 0) + ' memories' : '', title: 'Agent holding the most memories.' }),
    ];

    // ---- section 4: federation (other nodes) ----
    const connections = (fed && fed.connections) || [];
    const activeCount = connections.filter(c => c.status === 'active' && !c.expired).length;
    let fedTone = 'neutral';
    if (fed && connections.length) fedTone = activeCount > 0 ? 'healthy' : 'neutral';
    const connTone = (c) => c.expired ? 'degraded' : (c.status === 'active' ? 'healthy' : (c.status === 'revoked' ? 'danger' : 'neutral'));
    const fedTiles = [
        tile(fed ? connections.length : '--', 'Connected nodes', { title: 'Other SAGE nodes federated with this one.' }),
        tile(fed ? activeCount : '--', 'Active agreements', { title: 'Cross-node agreements that are active and unexpired.' }),
        deferredTile('Nodes reachable', 'Live reachability probing of federated nodes is coming in a later phase.'),
        deferredTile('Last exchange', 'Tracking the last cross-node exchange time is coming in a later phase.'),
    ];

    // ---- section 5: embeddings ----
    const emb = describeEmbedder(health);
    const rr = health?.embedder?.reranker;
    const rrOn = !!(rr && rr.enabled);
    const rrText = rrOn ? `On - ${rr.model || 'reranker'}` : 'Off';
    // Distinguish a reduced-capability fallback (hash/none: no semantic search,
    // but the node itself is fine) from a genuinely broken embedder (a
    // configured provider that is offline). Only the latter is a node fault.
    const embFallback = !emb.provider || emb.provider === 'hash' || emb.provider === 'none' || emb.provider === 'unknown';
    let embTone;
    if (!health) embTone = 'neutral';
    else if (embFallback) embTone = 'limited';
    else embTone = emb.online ? 'healthy' : 'degraded';
    const embTiles = [
        tile(emb.displayName || '--', 'Provider', { small: true, title: emb.label || '' }),
        tile(emb.online ? 'Online' : 'Offline', 'Runtime state', { small: true, color: emb.online ? '#10b981' : '#ef4444' }),
        tile(health?.embedder?.model || '--', 'Model', { small: true }),
        tile(health?.embedder?.dimension || '--', 'Vector dimensions', { title: 'Embedding vector width.' }),
        tile(rrText, 'Reranker', { small: true, color: rrOn ? '#10b981' : '#6b7280', title: (rr && rr.url) ? rr.url : 'Cross-encoder reranker over recall candidates.' }),
    ];

    // ---- overall node banner (worst section wins) ----
    const cannotReach = failed.health && !health;
    const loading = health === null && !failed.health;
    // Only CORE node health escalates the top banner: chain (behind/stalled),
    // quorum (RPC error), or a CONFIGURED embedder that is offline. The hash
    // fallback (embTone 'limited') is a capability note, not a node fault - if
    // it escalated, every default/no-Ollama install would sit permanently amber
    // and operators would learn to ignore the banner (alarm fatigue).
    const coreDegraded = chainTone === 'degraded' || quorumTone === 'degraded' || embTone === 'degraded';
    const nodeTone = cannotReach ? 'danger' : (loading ? 'neutral' : (coreDegraded ? 'degraded' : 'healthy'));
    const nodeLabel = cannotReach ? 'Cannot reach node' : (loading ? 'Checking node' : (coreDegraded ? 'Node degraded' : 'Node healthy'));
    const nodePillText = cannotReach ? 'offline' : (loading ? 'checking' : (coreDegraded ? 'degraded' : 'healthy'));

    return html`<div class="settings-page">
        <div class="settings-section" style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
            <div style="display:flex;flex-direction:column;gap:6px;min-width:0;">
                <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
                    <span style="font-size:18px;font-weight:700;color:var(--text);">${nodeLabel}</span>
                    ${pill(nodeTone, nodePillText)}
                </div>
                <div style="font-size:12px;color:var(--text-muted);">${chain?.chain_id || '--'} · ${chain?.moniker || 'this node'}</div>
            </div>
            <div style="flex:1"></div>
            <div style="display:flex;gap:24px;flex-wrap:wrap;">
                <div style="text-align:right;">
                    <div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text-muted);">SAGE</div>
                    <div style="font-size:15px;font-weight:600;color:var(--text);">${SAGE_VERSION}</div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text-muted);">Uptime</div>
                    <div style="font-size:15px;font-weight:600;color:var(--text);font-variant-numeric:tabular-nums;">${uptimeDisplay}</div>
                </div>
            </div>
        </div>

        ${sectionCard('Chain health',
            'BFT consensus status. Blocks are minted about every 5 seconds while there is activity; an idle chain stops minting empty blocks, so a long gap is normal, not a stall.',
            pill(chainTone, chainTone === 'degraded' ? 'degraded' : (chainTone === 'healthy' ? 'healthy' : 'no data')),
            failed.health,
            html`<div class="chain-stats-grid">${chainTiles}</div>`)}

        ${sectionCard('Quorum and nodes',
            'Validators forming this node consensus set and the peers it is connected to. A solo node with no peers is a normal single-node deployment, not a fault.',
            pill(quorumTone, quorumLabel),
            failed.validators,
            html`<div class="chain-stats-grid">${quorumTiles}</div>`)}

        ${sectionCard('Agents',
            'This node own agents - the identities that read and write its memories. Online means seen in the last 5 minutes.',
            pill(agentTone, agents.length ? `${online} online` : 'no agents'),
            failed.agents,
            html`<div class="chain-stats-grid">${agentTiles}</div>
                ${topAgents.length ? html`<div style="margin-top:12px;">${topAgents.map(a => html`<div key=${a.agent_id} style="display:flex;align-items:center;gap:10px;padding:8px 4px;border-top:1px solid var(--border);font-size:13px;">
                    <span style=${`width:8px;height:8px;border-radius:50%;flex:none;background:${dotColor(a)};`}></span>
                    <span style="font-weight:600;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${a.name || (a.agent_id || '').slice(0, 8)}</span>
                    <span style="color:var(--text-muted);">${(a.memory_count || 0).toLocaleString()} mem</span>
                    <span style="color:var(--text-muted);min-width:64px;text-align:right;">${a.last_seen ? timeAgo(a.last_seen) : 'never'}</span>
                </div>`)}</div>` : html`<div style="margin-top:12px;color:var(--text-muted);font-size:13px;">No agents registered on this node yet.</div>`}`)}

        ${sectionCard('Federation',
            'Other SAGE nodes this one is connected to. Federation links two separate brains; it is not the same as adding an agent to this node.',
            pill(fedTone, connections.length ? `${activeCount} active` : 'no links'),
            failed.fed && fedEverRef.current,
            html`<div class="chain-stats-grid">${fedTiles}</div>
                ${connections.length ? html`<div style="margin-top:12px;">${connections.map(c => html`<div key=${c.remote_chain_id} style="display:flex;align-items:center;gap:10px;padding:8px 4px;border-top:1px solid var(--border);font-size:13px;">
                    <span style="font-weight:600;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${c.remote_chain_id}</span>
                    ${pill(connTone(c), c.expired ? 'expired' : c.status)}
                </div>`)}</div>` : html`<div style="margin-top:12px;color:var(--text-muted);font-size:13px;">No federated nodes yet.</div>`}`)}

        ${sectionCard('Embeddings',
            'The embedding provider that turns memories into vectors for semantic recall, and the optional reranker that reorders recall candidates.',
            pill(embTone, embTone === 'healthy' ? 'healthy' : (embTone === 'degraded' ? 'offline' : (embTone === 'limited' ? 'limited' : 'checking'))),
            failed.health,
            html`<div class="chain-stats-grid">${embTiles}</div>`)}
    </div>`;
}

// ============================================================================
// Root App
// ============================================================================

function App() {
    const [authState, setAuthState] = useState('loading'); // loading | login | ready
    const [isEncrypted, setIsEncrypted] = useState(false);
    const [page, setPage] = useState('brain');
    // Brain view mode: 'mri' (3D MRI, the DEFAULT) | '2d' (canvas). Persisted so
    // a user who prefers 2D keeps it across reloads; new users land on the MRI.
    const [brainMode, setBrainMode] = useState(() => {
        try { return localStorage.getItem('sage-brain-mode') || 'mri'; } catch (e) { return 'mri'; }
    });
    const changeBrainMode = (mode) => {
        setBrainMode(mode);
        try { localStorage.setItem('sage-brain-mode', mode); } catch (e) {}
    };
    const [selectedMemory, setSelectedMemory] = useState(null);
    const [sseConnected, setSseConnected] = useState(false);
    const [timelineFilter, setTimelineFilter] = useState([]); // [{from, to}, ...]
    const [showHelp, setShowHelp] = useState(false);
    const [helpSection, setHelpSection] = useState(null);
    const openHelp = (section) => { setHelpSection(section || null); setShowHelp(true); };
    window.__sageOpenHelp = openHelp;
    const [tooltipsEnabled, setTooltipsEnabled] = useState(() => {
        try { return localStorage.getItem('sage-tooltips') === '1'; } catch (e) { return false; }
    });
    const sseRef = useRef(null);
    const [textSize, setTextSize] = useState(() => {
        try { return localStorage.getItem('sage-text-size') || 'medium'; } catch (e) { return 'medium'; }
    });
    const changeTextSize = (size) => {
        setTextSize(size);
        try { localStorage.setItem('sage-text-size', size); } catch (e) {}
    };

    // Expose lock function for SynapticLedger (called after enabling encryption)
    window.__sageLock = async () => {
        await lockSession();
        setIsEncrypted(true);
        setAuthState('login');
    };

    // Expose tooltip toggle for SettingsPage
    window.__sageTooltips = { enabled: tooltipsEnabled, toggle: () => {
        setTooltipsEnabled(v => {
            const next = !v;
            try { localStorage.setItem('sage-tooltips', next ? '1' : '0'); } catch (e) {}
            return next;
        });
    }};

    // sanitiseNextPath returns a same-origin relative path or null. It
    // exists to close go/js/client-side-unvalidated-url-redirection: the
    // ?next= query param is attacker-controllable, so we require the
    // value to (a) start with "/", (b) NOT be protocol-relative ("//"
    // or "/\\") and (c) match a tight allowlist of known SAGE path
    // prefixes. This blocks open redirect to "//evil.com",
    // "/\\evil.com" and any "javascript:" payload.
    function sanitiseNextPath(raw) {
        if (!raw || typeof raw !== 'string') return null;
        if (!raw.startsWith('/')) return null;
        if (raw.startsWith('//') || raw.startsWith('/\\')) return null;
        const allowedPrefixes = ['/oauth/', '/ui/', '/v1/'];
        if (!allowedPrefixes.some(p => raw === p.slice(0, -1) || raw.startsWith(p))) return null;
        return raw;
    }

    // Check auth on mount
    useEffect(() => {
        checkAuth().then(res => {
            setIsEncrypted(!!res.auth_required);
            // OAuth wizard hop: if ?next=<safe-path> is present, the user came
            // through the /oauth/authorize → /ui/?next=… redirect. Once we know
            // auth is good, bounce straight to that path so the consent screen
            // renders. Without this, an already-authed user lands on the
            // dashboard and the OAuth flow stalls. Same-origin paths only.
            if (!res.auth_required || res.authenticated) {
                const nextRaw = new URLSearchParams(window.location.search).get('next');
                const nextSafe = sanitiseNextPath(nextRaw);
                if (nextSafe) {
                    window.location.replace(nextSafe);
                    return;
                }
                setAuthState('ready');
            } else {
                setAuthState('login');
            }
        }).catch(() => setAuthState('ready')); // if auth check fails, assume no auth
    }, []);

    // First-run onboarding: auto-open the Getting Started guide exactly once.
    // The 'sage-help-dismissed' flag is set the moment we open it, so it never
    // reappears on refresh or later visits (the overlay's own "Don't show again"
    // checkbox still governs manual opens).
    useEffect(() => {
        if (authState !== 'ready') return;
        let dismissed = false;
        try { dismissed = localStorage.getItem('sage-help-dismissed') === '1'; } catch (e) {}
        if (dismissed) return;
        try { localStorage.setItem('sage-help-dismissed', '1'); } catch (e) {}
        openHelp('getting-started');
    }, [authState]);

    // Auto-lock after 30 minutes of inactivity when encrypted.
    useEffect(() => {
        if (!isEncrypted || authState !== 'ready') return;
        const AUTO_LOCK_MS = 30 * 60 * 1000; // 30 minutes
        let timer = setTimeout(() => {
            lockSession().then(() => setAuthState('login'));
        }, AUTO_LOCK_MS);
        const resetTimer = () => {
            clearTimeout(timer);
            timer = setTimeout(() => {
                lockSession().then(() => setAuthState('login'));
            }, AUTO_LOCK_MS);
        };
        const events = ['mousedown', 'keydown', 'scroll', 'touchstart'];
        events.forEach(e => window.addEventListener(e, resetTimer, { passive: true }));
        return () => {
            clearTimeout(timer);
            events.forEach(e => window.removeEventListener(e, resetTimer));
        };
    }, [isEncrypted, authState]);

    useEffect(() => {
        if (authState !== 'ready') return;

        const sse = new SSEClient();
        sse.connect();
        sseRef.current = sse;
        sse.on('connection', (data) => setSseConnected(data.connected));

        // Hash routing
        function onHash() {
            const hash = window.location.hash.slice(1) || '/';
            if (hash === '/overview') setPage('overview');
            else if (hash === '/search') setPage('search');
            else if (hash === '/tasks') setPage('tasks');
            else if (hash === '/settings') setPage('settings');
            else if (hash === '/import') setPage('import');
            else if (hash === '/network') setPage('network');
            else if (hash === '/pipeline') setPage('pipeline');
            else if (hash === '/federation') setPage('federation');
            else setPage('brain');
        }
        window.addEventListener('hashchange', onHash);
        onHash();

        return () => {
            sse.disconnect();
            window.removeEventListener('hashchange', onHash);
        };
    }, [authState]);

    // Show loading spinner
    if (authState === 'loading') {
        return html`<div class="login-screen"><div class="login-card" style="text-align:center;"><p style="color:var(--text-muted, #6b7280);">Loading...</p></div></div>`;
    }

    // Show login screen. If ?next=<path> is set in the URL, redirect to that
    // path after successful login instead of dropping into the dashboard.
    // This is the OAuth wizard flow: /oauth/authorize → /ui/?next=<authorize>
    // → user logs in → bounce to /oauth/authorize → user clicks Authorize →
    // ChatGPT receives the code. Without this hop the user lands on the
    // dashboard with a stale ?next= in the URL bar and the OAuth flow stalls.
    // Same-origin paths only — accept /oauth/* and /ui/* prefixes; reject
    // protocol-relative or absolute URLs to prevent open-redirect.
    if (authState === 'login') {
        const nextRaw = new URLSearchParams(window.location.search).get('next');
        const nextSafe = sanitiseNextPath(nextRaw);
        const onSuccess = () => {
            if (nextSafe) {
                window.location.replace(nextSafe);
            } else {
                setAuthState('ready');
            }
        };
        return html`<${LoginScreen} onSuccess=${onSuccess} nextHint=${nextSafe} />`;
    }

    function navigate(p) {
        window.location.hash = p === 'brain' ? '/' : '/' + p;
    }

    const onSelectMemory = useCallback((node) => {
        setSelectedMemory(node);
    }, []);

    return html`<${TooltipsContext.Provider} value=${tooltipsEnabled}>
        <${ToastContainer} />
        <div class="sidebar">
            <div class="sidebar-logo">S</div>
            <button class="sidebar-btn ${page === 'overview' ? 'active' : ''}" onClick=${() => navigate('overview')} title="Overview - node control board" aria-label="Overview - node control board">
                ${icons.overview}
            </button>
            <button class="sidebar-btn ${page === 'brain' ? 'active' : ''}" onClick=${() => navigate('brain')} title="Cerebrum - your brain home" aria-label="Cerebrum - your brain home">
                ${icons.brain}
            </button>
            <button class="sidebar-btn ${page === 'search' ? 'active' : ''}" onClick=${() => navigate('search')} title="Search - find memories" aria-label="Search - find memories">
                ${icons.search}
            </button>
            <button class="sidebar-btn ${page === 'tasks' ? 'active' : ''}" onClick=${() => navigate('tasks')} title="Tasks - governed task board" aria-label="Tasks - governed task board">
                ${icons.tasks}
            </button>
            <button class="sidebar-btn ${page === 'import' ? 'active' : ''}" onClick=${() => navigate('import')} title="Import - ingest conversation exports" aria-label="Import - ingest conversation exports">
                ${icons.import}
            </button>
            <button class="sidebar-btn ${page === 'network' ? 'active' : ''}" onClick=${() => navigate('network')} title="Agents - your agents, RBAC and validator quorum" aria-label="Agents - your agents, RBAC and validator quorum">
                ${icons.network}
            </button>
            <button class="sidebar-btn ${page === 'pipeline' ? 'active' : ''}" onClick=${() => navigate('pipeline')} title="Pipeline - agent-to-agent message bus" aria-label="Pipeline - agent-to-agent message bus">
                ${icons.pipeline}
            </button>
            <button class="sidebar-btn ${page === 'federation' ? 'active' : ''}" onClick=${() => navigate('federation')} title="Federation - connect your whole node to another SAGE" aria-label="Federation - connect your whole node to another SAGE">
                ${icons.federation}
            </button>
            <button class="sidebar-btn ${page === 'settings' ? 'active' : ''}" onClick=${() => navigate('settings')} title="Settings - node configuration" aria-label="Settings - node configuration">
                ${icons.settings}
            </button>
            <div style="flex:1;"></div>
            <button class="sidebar-btn" onClick=${() => openHelp(null)} title="Help - open the CEREBRUM guide" aria-label="Help - open the CEREBRUM guide">
                ${icons.help}
            </button>
        </div>
        <div class="main-content zoom-${textSize}">
            <div class="top-bar">
                <h1 aria-label=${`CEREBRUM - ${PAGE_LABELS[page] || 'Your SAGE Brain'}`}>CEREBRUM <span class="sage-version" title="SAGE release">${SAGE_VERSION}</span> <span class="topbar-sep" aria-hidden="true">/</span> <span class="topbar-page">${PAGE_LABELS[page] || 'Your SAGE Brain'}</span></h1>
                <div class="spacer"></div>
                ${isEncrypted && html`
                    <button class="lock-btn" title="Lock CEREBRUM" onClick=${async () => {
                        await lockSession();
                        setAuthState('login');
                    }}>
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="16" height="16">
                            <rect x="3" y="11" width="18" height="11" rx="2"/>
                            <path d="M7 11V7a5 5 0 0110 0v4"/>
                        </svg>
                    </button>
                `}
                <div class="text-size-toggle" title="Text size">
                    <button class="text-size-btn sz-s ${textSize === 'small' ? 'active' : ''}" onClick=${() => changeTextSize('small')}>A</button>
                    <button class="text-size-btn sz-m ${textSize === 'medium' ? 'active' : ''}" onClick=${() => changeTextSize('medium')}>A</button>
                    <button class="text-size-btn sz-l ${textSize === 'large' ? 'active' : ''}" onClick=${() => changeTextSize('large')}>A</button>
                </div>
                <div class="connection-badge">
                    <div class="connection-dot ${sseConnected ? '' : 'disconnected'}"></div>
                    ${sseConnected ? 'Live' : 'Connecting...'}
                    <${HelpTip} text="Real-time connection to your SAGE node via Server-Sent Events. When live, new memories appear automatically." align="right" />
                </div>
            </div>
            <${HealthBar} />
            <${ChainActivityLog} sse=${sseRef.current} />

            ${page === 'overview' && html`<${OverviewPage} sse=${sseRef.current} />`}
            ${page === 'brain' && html`
                <div class="brain-mode-toggle">
                    <button class=${brainMode === 'mri' ? 'active' : ''} onClick=${() => changeBrainMode('mri')} title="3D MRI brain view">⬡ MRI</button>
                    <button class=${brainMode === '2d' ? 'active' : ''} onClick=${() => changeBrainMode('2d')}>2D</button>
                </div>
                ${brainMode === '2d'
                    ? html`
                        <${BrainView} sse=${sseRef.current} onSelectMemory=${onSelectMemory} timelineFilter=${timelineFilter} />
                        <${TimelineBar} selectedRanges=${timelineFilter} onSelectRange=${setTimelineFilter} />
                    `
                    : html`<${MriView} sse=${sseRef.current} />`}
            `}
            ${page === 'search' && html`<${SearchPage} onSelectMemory=${onSelectMemory} />`}
            ${page === 'tasks' && html`<${TasksPage} sse=${sseRef.current} />`}
            ${page === 'import' && html`<${ImportPage} sse=${sseRef.current} />`}
            ${page === 'network' && html`<${NetworkPage} sse=${sseRef.current} />`}
            ${page === 'pipeline' && html`<${PipelinePage} />`}
            ${page === 'federation' && html`<${FederationPage} />`}
            ${page === 'settings' && html`<${SettingsPage} />`}

            <${MemoryDetail}
                memory=${selectedMemory}
                onClose=${() => setSelectedMemory(null)}
                onDelete=${() => setSelectedMemory(null)}
                onNavigate=${(p) => { setPage(p); window.location.hash = '#/' + p; }}
            />
        </div>
        ${showHelp && html`<${HelpOverlay} onClose=${() => setShowHelp(false)} initialSection=${helpSection} />`}
    </${TooltipsContext.Provider}>`;
}

// ============================================================================
// v11 federation JOIN ceremony - the guided guest/host wizards.
//
// Load-bearing safety rule (repeated in copy everywhere): federation-join ADDS a
// connection and DELETES NOTHING. It is code-path-distinct from quorum-join
// (which wipes node state). The word "delete" appears only inside negations.
//
// Honest crypto copy: the peer-authentication anchor is HUMAN - the in-person /
// on-camera QR scan (default) or the spoken-code compare (fallback). The 6-digit
// codes prove co-possession + 2-of-2 consent. Never "two cryptographic proofs";
// never surface SPKI / mTLS / TOTP secret in the happy path.
// ============================================================================

// FedGreenRail - the persistent "nothing is deleted" reassurance rail. Present
// on every ceremony screen (accent-green, NOT the amber warning banner).
function FedGreenRail() {
    return html`<div class="fed-green-rail">
        <span class="fed-green-dot">✓</span>
        Nothing is deleted. This only adds a connection to another network. Your memories are never touched.
    </div>`;
}

// TwoOfTwoMeter - the ●○ / ●● 2-of-2 consent meter. n = 0..2.
function TwoOfTwoMeter({ n, labelA = 'You', labelB = 'Them' }) {
    return html`<div class="fed-2of2" role="status" aria-label=${`${n} of 2 confirmed`}>
        <span class="fed-dot ${n >= 1 ? 'on' : ''}" title=${labelA}></span>
        <span class="fed-dot ${n >= 2 ? 'on' : ''}" title=${labelB}></span>
        <span class="fed-2of2-text">${n} of 2 confirmed</span>
    </div>`;
}

// BigCode - a large, widely-spaced 6-digit code for reading aloud.
function BigCode({ code }) {
    const digits = String(code || '').split('');
    return html`<div class="fed-bigcode" aria-label=${`code ${digits.join(' ')}`}>
        ${digits.map((d, i) => html`<span class="fed-bigcode-d" key=${i}>${d}</span>`)}
    </div>`;
}

// FedQr - renders a scannable QR from a string via the vendored encoder, with a
// copyable text fallback so the flow never dead-ends if rendering fails.
function FedQr({ text, size = 220, caption }) {
    const ref = useRef(null);
    const [copied, setCopied] = useState(false);
    const [rendered, setRendered] = useState(false);
    useEffect(() => {
        if (!ref.current || !text) return;
        ref.current.innerHTML = '';
        try {
            if (window.QRCode) {
                // eslint-disable-next-line no-new
                new window.QRCode(ref.current, {
                    text, width: size, height: size,
                    colorDark: '#0b1220', colorLight: '#ffffff',
                    correctLevel: window.QRCode.CorrectLevel ? window.QRCode.CorrectLevel.L : 1,
                });
                setRendered(true);
            }
        } catch (e) { setRendered(false); }
    }, [text, size]);
    const copy = async () => {
        try { await navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); } catch (e) {}
    };
    return html`<div class="fed-qr">
        <div class="fed-qr-canvas" ref=${ref}></div>
        ${!rendered && html`<div class="fed-qr-fallback muted">QR unavailable - share the code below instead.</div>`}
        ${caption && html`<div class="fed-qr-caption muted">${caption}</div>`}
        <button class="btn fed-copy-btn" onClick=${copy}>${copied ? '✓ Copied' : 'Copy code instead'}</button>
    </div>`;
}

// FedScanInput - get a connection code from the peer: live camera scan (Chrome's
// native BarcodeDetector, jsQR fallback) OR paste. Both feed the same handler.
// A camera-denied path silently offers paste (never a weaker default).
function FedScanInput({ onValue, busy }) {
    const videoRef = useRef(null);
    const streamRef = useRef(null);
    const rafRef = useRef(null);
    const activeRef = useRef(false); // true only while the camera view is live
    const [mode, setMode] = useState('choose'); // choose | camera | paste
    const [pasted, setPasted] = useState('');
    const [camErr, setCamErr] = useState('');

    const stop = () => {
        activeRef.current = false;
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        if (streamRef.current) { streamRef.current.getTracks().forEach(t => t.stop()); streamRef.current = null; }
    };
    useEffect(() => () => stop(), []);

    const startCamera = async () => {
        setCamErr(''); setMode('camera'); activeRef.current = true;
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } });
            // If the user cancelled / navigated away WHILE the permission prompt
            // was open, stop() already ran (activeRef=false) but this stream was
            // granted afterwards - release it now so the camera light goes off.
            if (!activeRef.current) { stream.getTracks().forEach(t => t.stop()); return; }
            streamRef.current = stream;
            const v = videoRef.current;
            if (!v) { stream.getTracks().forEach(t => t.stop()); return; }
            v.srcObject = stream; await v.play();
            let detector = null;
            if ('BarcodeDetector' in window) {
                try { detector = new window.BarcodeDetector({ formats: ['qr_code'] }); } catch (e) { detector = null; }
            }
            const canvas = document.createElement('canvas');
            const scanLoop = async () => {
                if (!streamRef.current || !videoRef.current) return;
                try {
                    let value = null;
                    if (detector) {
                        const codes = await detector.detect(videoRef.current);
                        if (codes && codes.length) value = codes[0].rawValue;
                    } else if (window.jsQR && v.videoWidth) {
                        canvas.width = v.videoWidth; canvas.height = v.videoHeight;
                        const ctx = canvas.getContext('2d');
                        ctx.drawImage(v, 0, 0, canvas.width, canvas.height);
                        const img = ctx.getImageData(0, 0, canvas.width, canvas.height);
                        const r = window.jsQR(img.data, img.width, img.height);
                        if (r) value = r.data;
                    }
                    if (value) { stop(); onValue(value); return; }
                } catch (e) {}
                rafRef.current = requestAnimationFrame(scanLoop);
            };
            rafRef.current = requestAnimationFrame(scanLoop);
        } catch (e) {
            setCamErr('Camera unavailable - paste the code instead.');
            setMode('paste');
        }
    };

    if (mode === 'camera') {
        return html`<div class="fed-scan">
            <video class="fed-scan-video" ref=${videoRef} playsinline muted></video>
            <div class="fed-scan-hint muted">Point your camera at their code.</div>
            <div class="fed-scan-actions">
                <button class="btn" onClick=${() => { stop(); setMode('paste'); }}>Paste it instead</button>
                <button class="btn" onClick=${() => { stop(); setMode('choose'); }}>Cancel</button>
            </div>
        </div>`;
    }
    if (mode === 'paste') {
        const v = pasted.trim();
        return html`<div class="fed-scan">
            ${camErr && html`<div class="muted">${camErr}</div>`}
            <textarea class="fed-paste" rows="3" placeholder="Paste the connection code (starts with otpauth://…)"
                value=${pasted} onInput=${e => setPasted(e.target.value)}></textarea>
            <div class="fed-scan-actions">
                <button class="btn" onClick=${() => setMode('choose')}>Back</button>
                <button class="btn btn-primary" disabled=${!v || busy} onClick=${() => onValue(v)}>Use this code</button>
            </div>
        </div>`;
    }
    return html`<div class="fed-scan-choose">
        <button class="btn btn-primary" disabled=${busy} onClick=${startCamera}>📷 Scan with camera</button>
        <button class="btn" disabled=${busy} onClick=${() => setMode('paste')}>Paste the code</button>
    </div>`;
}

// FedCodeCompare - THE TRUST MOMENT. Structurally unskippable (RT-1/RT-11): no
// pre-highlighted "Yes", no auto-advance, no timeout-confirm. Decisive consent
// requires TYPING the code you heard; the confirm button stays inert until the
// typed value matches what this node computed. "No - stop" is first-class danger.
function FedCodeCompare({ title, instruction, expectedCode, scope, scopeHeading, confirmLabel, onConfirm, onReject, onBack, busy, tier4 }) {
    const [typed, setTyped] = useState('');
    const norm = typed.replace(/\D/g, '');
    const match = norm.length === 6 && expectedCode && norm === String(expectedCode);
    return html`<div class="fed-compare">
        <${FedGreenRail} />
        <h3>${title}</h3>
        <p class="fed-compare-instr">${instruction}</p>
        ${tier4 && html`<div class="fed-tier4-note">No camera or shared screen? This spoken-code compare is your ONLY safety check - say it out loud on a call you placed to someone you trust, and only continue if it matches exactly.</div>`}
        ${scope && html`<${FedScopeSummary} scope=${scope} heading=${scopeHeading || 'They will be able to reach:'} />`}
        <label class="fed-compare-label">Type the code they read you</label>
        <input class="fed-compare-input" inputmode="numeric" autocomplete="off" maxlength="7"
            placeholder="• • • • • •" value=${typed} onInput=${e => setTyped(e.target.value)} />
        ${norm.length === 6 && !match && html`<div class="fed-compare-bad">That doesn't match what's on your screen. Do NOT continue - hang up and call them back on a number you trust.</div>`}
        <div class="fed-compare-actions">
            <button class="btn btn-danger" disabled=${busy} onClick=${onReject}>No - stop</button>
            <button class="btn ${match ? 'btn-confirm-live' : ''}" disabled=${!match || busy} onClick=${() => onConfirm(norm)}>
                ${busy ? 'Working…' : (confirmLabel || 'Yes, they match')}
            </button>
        </div>
        ${onBack && html`<button class="btn fed-compare-back" disabled=${busy} onClick=${onBack}>← Change what I share first</button>`}
    </div>`;
}

// FedScopeSummary - plain-words rendering of a cross_fed scope (RT-9).
function FedScopeSummary({ scope, heading }) {
    if (!scope) return null;
    const doms = (scope.allowed_domains && scope.allowed_domains.length) ? scope.allowed_domains : [];
    const all = doms.some(d => d === '*');
    const clr = CLEARANCE_LABELS[scope.max_clearance || 0] || 'Public';
    return html`<div class="fed-scope">
        <div class="fed-scope-h">${heading || 'What is shared:'}</div>
        <div class="fed-scope-line">${all ? 'All domains' : (doms.length ? doms.join(', ') : 'Nothing (no domains shared)')}</div>
        <div class="fed-scope-line muted">up to ${clr} level</div>
    </div>`;
}

// FedShareForm - configure a cross_fed grant (what the OTHER side may read).
function FedShareForm({ value, onChange }) {
    const v = value || { max_clearance: 0, allowed_domains: [], mode: 'exchange', direction: 'both' };
    const [domInput, setDomInput] = useState((v.allowed_domains || []).join(', '));
    const applyDomains = (s) => {
        const list = s.split(',').map(x => x.trim()).filter(Boolean);
        onChange({ ...v, allowed_domains: list });
    };
    return html`<div class="fed-share">
        <label class="fed-share-label">Domains they can read (comma-separated, or * for everything)</label>
        <input class="fed-share-input" placeholder="e.g. family, recipes   (blank = nothing)"
            value=${domInput} onInput=${e => { setDomInput(e.target.value); applyDomains(e.target.value); }} />
        <label class="fed-share-label">Highest level they can read</label>
        <input type="range" min="0" max="4" value=${v.max_clearance || 0}
            onInput=${e => onChange({ ...v, max_clearance: parseInt(e.target.value, 10) })} />
        <div class="fed-share-clr ${(v.max_clearance || 0) >= 3 ? 'danger' : (v.max_clearance || 0) >= 2 ? 'warn' : ''}">
            ${CLEARANCE_LABELS[v.max_clearance || 0]}
        </div>
    </div>`;
}

// FedChannelGate - the physical-object question (redteam #2). Distinguishes a
// code held as a PHYSICAL object from a transmitted image. A shared screen /
// forwarded image / pure phone call routes to the spoken-code (Tier-4) path,
// never presented as equal to in-person. Also collects the guest's own reachable
// address (where the host will connect back).
function FedChannelGate({ endpoint, onEndpoint, onChoose }) {
    return html`<div class="fed-step">
        <h2>Connect to another network</h2>
        <${FedGreenRail} />
        <div class="fed-field">
            <label>Your network address (how they'll reach you)</label>
            <input class="fed-share-input" value=${endpoint} onInput=${e => onEndpoint(e.target.value)}
                placeholder="https://192.168.1.20:8444" />
            <div class="muted">Usually https://[your computer's address]:8444 on your network.</div>
        </div>
        <div class="fed-gate-q">Are you looking at their code as a physical thing they're holding - in the same room, or held up to the camera on a call YOU placed to someone you trust?</div>
        <div class="fed-gate-choices">
            <button class="btn btn-primary" onClick=${() => onChoose(false)}>Yes - same room, or their phone on my camera</button>
            <button class="btn" onClick=${() => onChoose(true)}>We're on a call, I'd see a shared screen or an image</button>
            <button class="btn" onClick=${() => onChoose(true)}>We're just on the phone / no camera</button>
        </div>
        <div class="muted fed-gate-note">A shared screen or a forwarded image can be faked. For those we use the spoken-code method - it's built for calls.</div>
    </div>`;
}

// GuestJoinWizard - "Join a network". Maps to guest STEP 1-4.
function GuestJoinWizard({ onExit }) {
    const [step, setStep] = useState('channel');
    const [endpoint, setEndpoint] = useState(`https://${location.hostname}:8444`);
    const [tier4, setTier4] = useState(false);
    const [scan, setScan] = useState(null);       // {session_id, host_chain, host_endpoint, host_pin, return_uri}
    const [scopeG, setScopeG] = useState({ max_clearance: 0, allowed_domains: [], mode: 'exchange', direction: 'both' });
    const [codes, setCodes] = useState(null);      // {code_g, code_h, confirm_step}
    const [hostScope, setHostScope] = useState(null);
    const [busy, setBusy] = useState(false);
    const [err, setErr] = useState('');

    const fail = (e) => { setErr(String(e.message || e)); setBusy(false); };

    const doScan = async (uri) => {
        setBusy(true); setErr('');
        try { const r = await fedGuestScan(uri, endpoint); setScan(r); setStep('return'); }
        catch (e) { fail(e); }
        setBusy(false);
    };
    const doRequest = async () => {
        setBusy(true); setErr('');
        try {
            const r = await fedGuestRequest({ session_id: scan.session_id, endpoint, ...scopeG });
            setCodes(r); setStep('yourcode');
        } catch (e) { fail(e); }
        setBusy(false);
    };
    const doConfirm = async () => {
        setBusy(true); setErr('');
        try {
            await fedGuestConfirm({ session_id: scan.session_id, endpoint, host_scope: hostScope || {} });
            setStep('done');
        } catch (e) { fail(e); }
        setBusy(false);
    };

    // Poll host status while showing our code (waiting for host approval #1).
    useEffect(() => {
        if (step !== 'yourcode' || !scan) return;
        let live = true;
        const tick = async () => {
            try {
                const s = await fedGuestStatus(scan.session_id);
                if (!live) return;
                if (s.aborted) { setErr('They stopped the connection.'); setStep('channel'); return; }
                if (s.host_approved) { setHostScope(s.host_scope || {}); setStep('theircode'); }
            } catch (e) { /* transient; keep polling */ }
        };
        const id = setInterval(tick, 2000); tick();
        return () => { live = false; clearInterval(id); };
    }, [step, scan]);

    return html`<div class="fed-wizard">
        <div class="fed-wizard-head">
            <button class="btn fed-back" onClick=${onExit}>← Back</button>
            <span class="fed-wizard-title">Join a network</span>
        </div>
        ${err && html`<div class="fed-err">${err}</div>`}

        ${step === 'channel' && html`<${FedChannelGate} endpoint=${endpoint} onEndpoint=${setEndpoint}
            onChoose=${(t) => { setTier4(t); setStep('scan'); }} />`}

        ${step === 'scan' && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Point your camera at their code</h3>
            ${tier4 && html`<div class="fed-tier4-note">No camera / shared screen: paste the code they send, then we'll double-check with a spoken code you compare out loud.</div>`}
            <${FedScanInput} onValue=${doScan} busy=${busy} />
        </div>`}

        ${step === 'return' && scan && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Now show them YOUR code</h3>
            <p class="muted">Connecting to <strong>${scan.host_chain}</strong>. Hold this up to their camera, or send it for them to paste.</p>
            <${FedQr} text=${scan.return_uri} caption="Their SAGE scans this to check it's really you." />
            <div class="fed-step-actions">
                <button class="btn btn-primary" onClick=${() => setStep('share')}>They've got my code - next</button>
            </div>
        </div>`}

        ${step === 'share' && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>What will you let them read?</h3>
            <p class="muted">Leave it blank to share nothing. You can change this later.</p>
            <${FedShareForm} value=${scopeG} onChange=${setScopeG} />
            <div class="fed-step-actions">
                <button class="btn btn-primary" disabled=${busy} onClick=${doRequest}>${busy ? 'Working…' : 'Continue'}</button>
            </div>
        </div>`}

        ${step === 'yourcode' && codes && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Read this to them out loud</h3>
            <${BigCode} code=${codes.code_g} />
            <p class="fed-read-instr">Call them and read this code. It proves you're really connected to each other - not someone in the middle.</p>
            <div class="fed-waiting"><span class="fed-spinner"></span> Waiting for them to check your code…</div>
            <${TwoOfTwoMeter} n=${0} />
        </div>`}

        ${step === 'theircode' && codes && html`<${FedCodeCompare}
            title="Check the code they read you"
            instruction="They'll read you a code. Type exactly what you hear."
            expectedCode=${codes.code_h}
            scope=${hostScope}
            scopeHeading="You will be able to reach:"
            tier4=${tier4}
            confirmLabel="Yes - connect"
            busy=${busy}
            onConfirm=${doConfirm}
            onReject=${() => setStep('aborted')} />`}

        ${step === 'done' && html`<div class="fed-step fed-done">
            <div class="fed-done-check">✓</div>
            <h2>You're connected to ${scan && scan.host_chain}</h2>
            <${TwoOfTwoMeter} n=${2} />
            <p class="muted">Nothing was deleted. This added a connection you can turn off any time.</p>
            <button class="btn btn-primary" onClick=${onExit}>Done</button>
        </div>`}

        ${step === 'aborted' && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Stopped - the codes didn't match</h3>
            <p class="fed-compare-bad">Nothing was shared and nothing was changed on your brain. Hang up and call them back on a number you trust, then start over.</p>
            <div class="fed-step-actions"><button class="btn btn-primary" onClick=${onExit}>Back to Networks</button></div>
        </div>`}
    </div>`;
}

// HostJoinWizard - "Let someone join". Maps to host H1-H7.
function HostJoinWizard({ onExit }) {
    const [step, setStep] = useState('create');
    const [endpoint, setEndpoint] = useState(`https://${location.hostname}:8444`);
    const [session, setSession] = useState(null);   // {session_id, otpauth_uri, host_pin}
    const [view, setView] = useState(null);          // host status view
    const [grant, setGrant] = useState({ max_clearance: 0, allowed_domains: [], mode: 'exchange', direction: 'both' });
    const [busy, setBusy] = useState(false);
    const [err, setErr] = useState('');
    const fail = (e) => { setErr(String(e.message || e)); setBusy(false); };

    const doCreate = async () => {
        setBusy(true); setErr('');
        try { const r = await fedHostCreate(endpoint); setSession(r); setStep('showqr'); }
        catch (e) { fail(e); }
        setBusy(false);
    };
    const doScanReturn = async (uri) => {
        setBusy(true); setErr('');
        try { await fedHostScanReturn(session.session_id, uri); setStep('waiting'); }
        catch (e) { fail(e); }
        setBusy(false);
    };
    const doApprove = async (typedCode) => {
        setBusy(true); setErr('');
        try { await fedHostApprove(session.session_id, { typed_code: typedCode, ...grant }); setStep('readback'); }
        catch (e) { fail(e); }
        setBusy(false);
    };
    // Cancel from a non-decision screen: burn the session and leave.
    const abort = async () => {
        try { if (session) await fedHostAbort(session.session_id); } catch (e) {}
        onExit();
    };
    // Reject at the trust moment: burn the session but STAY mounted so the
    // "Do NOT approve" safety message is actually seen.
    const rejectStop = async () => {
        try { if (session) await fedHostAbort(session.session_id); } catch (e) {}
        setStep('aborted');
    };

    // Poll host session status during waiting / readback.
    useEffect(() => {
        if (!session || (step !== 'waiting' && step !== 'readback')) return;
        let live = true;
        const tick = async () => {
            try {
                const v = await fedHostStatus(session.session_id);
                if (!live) return;
                setView(v);
                if (step === 'waiting' && v.guest_chain) setStep('review');
                if (step === 'readback' && v.active) setStep('connected');
            } catch (e) {}
        };
        const id = setInterval(tick, 2000); tick();
        return () => { live = false; clearInterval(id); };
    }, [step, session]);

    return html`<div class="fed-wizard">
        <div class="fed-wizard-head">
            <button class="btn fed-back" onClick=${step === 'create' ? onExit : abort}>← ${step === 'create' ? 'Back' : 'Cancel'}</button>
            <span class="fed-wizard-title">Let someone join</span>
        </div>
        ${err && html`<div class="fed-err">${err}</div>`}

        ${step === 'create' && html`<div class="fed-step">
            <h2>Let someone join your network</h2>
            <${FedGreenRail} />
            <div class="fed-field">
                <label>Your network address (how they'll reach you)</label>
                <input class="fed-share-input" value=${endpoint} onInput=${e => setEndpoint(e.target.value)} placeholder="https://192.168.1.10:8444" />
            </div>
            <button class="btn btn-primary" disabled=${busy} onClick=${doCreate}>${busy ? 'Working…' : 'Show my connection code'}</button>
        </div>`}

        ${step === 'showqr' && session && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Have them scan this</h3>
            <${FedQr} text=${session.otpauth_uri} caption="Their SAGE app - or Google Authenticator - points at this." />
            <p class="muted">Best done in the same room, or held up to a video call you trust.</p>
            <h3>Then scan their code back</h3>
            <${FedScanInput} onValue=${doScanReturn} busy=${busy} />
        </div>`}

        ${step === 'waiting' && html`<div class="fed-step">
            <${FedGreenRail} />
            <div class="fed-waiting"><span class="fed-spinner"></span> Waiting for their request…</div>
        </div>`}

        ${step === 'review' && view && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Someone using the name "${view.guest_chain}" wants to connect</h3>
            ${view.guest_scope && html`<${FedScopeSummary} scope=${view.guest_scope} heading="They'll let YOU reach:" />`}
            <h4>What can they read on YOUR network?</h4>
            <p class="muted">Conservative by default. Blank shares nothing.</p>
            <${FedShareForm} value=${grant} onChange=${setGrant} />
            <div class="fed-step-actions">
                <button class="btn btn-danger" onClick=${abort}>Ignore</button>
                <button class="btn btn-primary" onClick=${() => setStep('compare')}>Next: check their code</button>
            </div>
        </div>`}

        ${step === 'compare' && view && html`<${FedCodeCompare}
            title="Check their code"
            instruction="They'll read you a code. Type exactly what you hear. This is the moment that proves it's really them."
            expectedCode=${view.code_g}
            scope=${grant}
            confirmLabel="Yes, they match - approve"
            busy=${busy}
            onConfirm=${doApprove}
            onBack=${() => setStep('review')}
            onReject=${rejectStop} />`}

        ${step === 'readback' && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Read this code back to them</h3>
            ${view && view.code_h ? html`<${BigCode} code=${view.code_h} />` : html`<div class="fed-waiting"><span class="fed-spinner"></span> Preparing…</div>`}
            <p class="fed-read-instr">Read ${view && view.guest_chain ? view.guest_chain : 'them'} this code so they can confirm it. Say it out loud - don't paste it.</p>
            <div class="fed-waiting"><span class="fed-spinner"></span> You approved. Waiting for them (2 of 2)…</div>
            <${TwoOfTwoMeter} n=${1} />
        </div>`}

        ${step === 'connected' && html`<div class="fed-step fed-done">
            <div class="fed-done-check">✓</div>
            <h2>Connected to ${view && view.guest_chain}</h2>
            <${TwoOfTwoMeter} n=${2} />
            <p class="muted">Nothing was deleted. You can turn this connection off any time.</p>
            <button class="btn btn-primary" onClick=${onExit}>Done</button>
        </div>`}

        ${step === 'aborted' && html`<div class="fed-step">
            <${FedGreenRail} />
            <h3>Stopped - the codes didn't match</h3>
            <p class="fed-compare-bad">Do NOT approve. Nothing was shared and nothing was changed on your brain. Hang up and call them back on a number you trust, then start over.</p>
            <div class="fed-step-actions"><button class="btn btn-primary" onClick=${onExit}>Back to Networks</button></div>
        </div>`}
    </div>`;
}

// FederationPage - the 8th sidebar section landing: role fork + connections list.
function FederationPage() {
    const [mode, setMode] = useState('landing'); // landing | guest | host
    const [conns, setConns] = useState(null);
    const [localChain, setLocalChain] = useState('');
    const [err, setErr] = useState('');

    const load = async () => {
        try { const r = await fedConnections(); setConns(r.connections || []); setLocalChain(r.local_chain_id || ''); setErr(''); }
        catch (e) { setErr(String(e.message || e)); setConns([]); }
    };
    useEffect(() => { if (mode === 'landing') load(); }, [mode]);

    const revoke = async (chain) => {
        if (!confirm(`Turn off the connection to ${chain}? This does NOT erase anything - it just stops the two networks from reaching each other.`)) return;
        try { await fedRevoke(chain); showToast(`Disconnected from ${chain}`, 'success'); load(); }
        catch (e) { showToast(String(e.message || e), 'error'); }
    };

    if (mode === 'guest') return html`<div class="page fed-page"><${GuestJoinWizard} onExit=${() => setMode('landing')} /></div>`;
    if (mode === 'host') return html`<div class="page fed-page"><${HostJoinWizard} onExit=${() => setMode('landing')} /></div>`;

    return html`<div class="page fed-page">
        <div class="fed-landing">
            <h1>Federation</h1>
            <p class="fed-landing-sub muted">Connect your <strong>whole SAGE</strong> to <strong>another SAGE</strong> - across the internet or your network. This is not the same as adding an agent to your own SAGE (do that under <strong>Agents</strong>), or the validator quorum your own agents form (also under Agents). Federation links two separate brains.</p>
            <${FedGreenRail} />
            ${err && html`<div class="fed-err">Couldn't load connections: ${err}</div>`}
            <div class="fed-roles">
                <button class="fed-role-card" onClick=${() => setMode('guest')}>
                    <div class="fed-role-glyph">${icons.federation}</div>
                    <div class="fed-role-title">Join someone's network</div>
                    <div class="fed-role-desc">They'll show you a code to scan.</div>
                </button>
                <button class="fed-role-card" onClick=${() => setMode('host')}>
                    <div class="fed-role-glyph">${icons.federation}</div>
                    <div class="fed-role-title">Let someone join mine</div>
                    <div class="fed-role-desc">You'll show a code for them to scan.</div>
                </button>
            </div>

            <div class="fed-conns">
                <h3>Your connections <${HelpTip} text="Each row is a treaty with another SAGE. The dot is green when the connection is active and unexpired. The domains listed are the only knowledge shared across the link - everything else stays private." /></h3>
                ${conns === null && html`<div class="muted">Loading…</div>`}
                ${conns && conns.length === 0 && html`
                    <${EmptyState} icon="federation"
                        headline="No connections yet"
                        hint="Link your whole SAGE to another SAGE to share memories across networks. Join someone's network with a code they share, or host one and hand out a code."
                        actionLabel="Join a network"
                        onAction=${() => setMode('guest')} />
                    ${localChain && html`<div class="muted" style="text-align:center;margin-top:4px;">Your network id: <code>${localChain}</code></div>`}
                `}
                ${conns && conns.map(c => html`<div class="fed-conn-row" key=${c.remote_chain_id}>
                    <div class="fed-conn-main">
                        <span class="fed-conn-status ${c.status === 'active' && !c.expired ? 'on' : 'off'}"></span>
                        <span class="fed-conn-name">${c.remote_chain_id}</span>
                        <span class="fed-conn-meta muted">${c.expired ? 'expired' : c.status} · ${(c.allowed_domains || []).join(', ') || 'no domains'}</span>
                    </div>
                    ${c.status === 'active' && html`<button class="btn btn-danger fed-conn-off" onClick=${() => revoke(c.remote_chain_id)}>Turn off</button>`}
                </div>`)}
            </div>
        </div>
    </div>`;
}

// PAGE_LABELS - human-readable page names for the top-bar breadcrumb and the
// h1 aria-label, so the header orients the user instead of always reading
// "CEREBRUM". Keyed by the same page ids used by navigate()/the sidebar.
const PAGE_LABELS = {
    overview: 'Overview',
    brain: 'Home',
    search: 'Search',
    tasks: 'Tasks',
    import: 'Import',
    network: 'Agents',
    pipeline: 'Pipeline',
    federation: 'Federation',
    settings: 'Settings',
};

// EmptyState - shared, friendly zero-state used across empty screens (Search,
// Tasks, Federation, and the zero-memory 2D/3D brain views). Icon + headline +
// one-line explainer + optional primary action, so no dead-end is ever a blank
// void. `icon` is a key into the shared `icons` set (falls back to a raw node).
function EmptyState({ icon, headline, hint, actionLabel, onAction, compact }) {
    return html`<div class="empty-state ${compact ? 'empty-state-compact' : ''}">
        ${icon && html`<div class="empty-state-icon">${icons[icon] || icon}</div>`}
        <div class="empty-state-headline">${headline}</div>
        ${hint && html`<div class="empty-state-hint">${hint}</div>`}
        ${actionLabel && onAction && html`
            <button class="btn empty-state-action" onClick=${onAction}>${actionLabel}</button>
        `}
    </div>`;
}

render(html`<${App} />`, document.getElementById('app'));
