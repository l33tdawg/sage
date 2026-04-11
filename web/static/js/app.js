// CEREBRUM — Your SAGE Brain
import { SSEClient } from './sse.js';
import { fetchStats, fetchGraph, fetchMemories, deleteMemory, fetchHealth, checkAuth, login, recoverVault, lockSession, importMemories, importPreview, importConfirm, fetchCleanupSettings, saveCleanupSettings, runCleanup, fetchAgents, fetchAgent, createAgent, updateAgent, removeAgent, downloadBundle, fetchTemplates, fetchRedeployStatus, startRedeploy, createPairingCode, rotateAgentKey, fetchBootInstructions, saveBootInstructions, fetchLedgerStatus, enableLedger, changeLedgerPassphrase, disableLedger, fetchTags, fetchMemoryTags, setMemoryTags, fetchAutostart, setAutostart, checkForUpdate, applyUpdate, restartServer, fetchTasks, updateTaskStatus, createTask, fetchUnregisteredAgents, mergeAgent, fetchRecallSettings, saveRecallSettings, fetchAgentTags, transferTag, transferDomain, bulkUpdateMemories, fetchMemoryMode, saveMemoryMode, fetchPipeline, fetchPipelineStats, fetchGovProposals, fetchGovProposalDetail, submitGovProposal, submitGovVote } from './api.js';

const { h, render, createContext } = preact;
const { useState, useEffect, useRef, useCallback, useContext } = preactHooks;
const html = window.html;

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
            // retry on next interval
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
        <div class="domain-bar">
            <${HelpTip} text="Click a domain to filter the graph. Click again to show all." />
            ${domains.map(d => html`
                <button class="domain-pill ${filterDomains.has(d) ? 'active' : ''}"
                        style="color: ${getDomainColor(d)}; ${filterDomains.has(d) ? `background: ${getDomainColor(d)}20` : ''}"
                        onClick=${() => toggleDomain(d)}>
                    ${d}
                </button>
            `)}
        </div>
        <div class="brain-container">
            <canvas ref=${canvasRef} class="brain-canvas"></canvas>

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
                            <button class="detail-close" onClick=${() => setDomainTransfer(null)}>x</button>
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
                            <button class="detail-close" onClick=${() => { setBulkAction(null); setBulkInput(''); }}>x</button>
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
                            <button class="detail-close" onClick=${() => setBulkAction(null)}>x</button>
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
        <div class="toast-container">
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

    // Keep last memory data for closing animation
    useEffect(() => {
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

    // Use displayMemory for rendering (keeps last data during close animation)
    const m = displayMemory;
    const conf = m.confidence;
    const color = getDomainColor(m.domain);

    return html`
        <div class="detail-overlay ${visible ? 'open' : ''}" onTransitionEnd=${handleTransitionEnd}>
            <div class="detail-header">
                <h2>Memory Detail</h2>
                <button class="detail-close" onClick=${onClose}>x</button>
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
                        <span class="domain-badge" style="background: ${color}20; color: ${color};">${m.domain}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Type</label>
                        <span class="value">${m.memory_type || m.memoryType || 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Status</label>
                        <span class="value">${m.status}</span>
                    </div>
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

function TasksPage() {
    const [tasks, setTasks] = useState([]);
    const [loading, setLoading] = useState(true);
    const [domainFilter, setDomainFilter] = useState('');
    const [domains, setDomains] = useState([]);
    const [dragging, setDragging] = useState(null);
    const [dragOver, setDragOver] = useState(null);
    const [showAddForm, setShowAddForm] = useState(false);
    const [newContent, setNewContent] = useState('');
    const [newDomain, setNewDomain] = useState('');
    const [adding, setAdding] = useState(false);
    const [showOldDone, setShowOldDone] = useState(false);

    useEffect(() => { loadTasks(); }, []);

    async function loadTasks() {
        setLoading(true);
        try {
            const data = await fetchTasks({ all: true, limit: 200 });
            const items = data.tasks || [];
            setTasks(items);
            const ds = [...new Set(items.map(t => t.domain_tag).filter(Boolean))].sort();
            setDomains(ds);
        } catch (e) { setTasks([]); }
        setLoading(false);
    }

    async function moveTask(taskId, newStatus) {
        // Optimistic update
        setTasks(prev => prev.map(t => t.memory_id === taskId ? { ...t, task_status: newStatus } : t));
        try {
            await updateTaskStatus(taskId, newStatus);
        } catch (e) {
            loadTasks(); // revert on error
        }
    }

    async function handleAddTask(e) {
        e.preventDefault();
        if (!newContent.trim()) return;
        setAdding(true);
        try {
            await createTask(newContent.trim(), newDomain.trim() || 'general');
            setNewContent('');
            setNewDomain('');
            setShowAddForm(false);
            loadTasks();
        } catch (err) {
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
        const taskId = e.dataTransfer.getData('text/plain');
        if (taskId && colKey) {
            moveTask(taskId, colKey);
        }
        setDragging(null);
        setDragOver(null);
    }

    const filtered = (domainFilter ? tasks.filter(t => t.domain_tag === domainFilter) : tasks).filter(isRecentDone);
    const hiddenDoneCount = tasks.filter(t => (t.task_status === 'done' || t.task_status === 'dropped') && !isRecentDone(t)).length;

    return html`
        <div class="tasks-page">
            <div class="tasks-header">
                <h2 class="tasks-title">Task Board</h2>
                <div class="tasks-filters">
                    <select class="filter-select" value=${domainFilter} onChange=${e => setDomainFilter(e.target.value)}>
                        <option value="">All domains</option>
                        ${domains.map(d => html`<option value=${d}>${d}</option>`)}
                    </select>
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
                                         onDragStart=${e => handleDragStart(e, task)}>
                                        <div class="kanban-card-content">${task.content.replace(/^\[TASK\]\s*/i, '')}</div>
                                        <div class="kanban-card-meta">
                                            <span class="domain-badge" style="background:${getDomainColor(task.domain_tag)}20;color:${getDomainColor(task.domain_tag)};font-size:10px;padding:2px 6px;">
                                                ${task.domain_tag}
                                            </span>
                                            <span style="font-size:11px;color:var(--text-muted);">${timeAgo(task.created_at)}</span>
                                        </div>
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
            </div>
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
    const [agentFilter, setAgentFilter] = useState('');
    const [agents, setAgents] = useState([]);
    const [domainFilter, setDomainFilter] = useState('');
    const [domains, setDomains] = useState([]);
    const [tagFilter, setTagFilter] = useState('');
    const [allTags, setAllTags] = useState([]);

    useEffect(() => {
        loadMemories();
        fetchAgents().then(data => setAgents(data.agents || [])).catch(() => {});
        fetchStats().then(data => { if (data.by_domain) setDomains(Object.keys(data.by_domain).sort()); }).catch(() => {});
        fetchTags().then(data => setAllTags(data.tags || [])).catch(() => {});
    }, []);

    async function loadMemories(search, agent, domain, tag) {
        setLoading(true);
        try {
            const params = { limit: 100, sort: 'newest' };
            if (agent) params.agent = agent;
            if (domain) params.domain = domain;
            if (tag) params.tag = tag;
            const data = await fetchMemories(params);
            let memories = data.memories || [];
            if (search) {
                const q = search.toLowerCase();
                memories = memories.filter(m =>
                    (m.content && m.content.toLowerCase().includes(q)) ||
                    (m.domain_tag && m.domain_tag.toLowerCase().includes(q))
                );
            }
            setResults(memories);
            setTotal(data.total || memories.length);
        } catch (err) {
            setResults([]);
        }
        setLoading(false);
    }

    function handleSearch(e) {
        const v = e.target.value;
        setQuery(v);
        loadMemories(v, agentFilter, domainFilter, tagFilter);
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

    return html`
        <div class="search-page">
            <input class="search-page-input" type="text" placeholder="Search memories..."
                   value=${query} onInput=${handleSearch} />
            <div class="search-filters">
                <${HelpTip} text="Search across all committed memories by content, domain, or tags. Results are ranked by relevance." />
                <${PageHelp} section="search" label="Search & Import guide" />
                <select class="filter-select" value=${domainFilter} onChange=${handleDomainFilter}>
                    <option value="">All domains</option>
                    ${domains.map(d => html`<option value=${d}>${d}</option>`)}
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
                <span style="font-size: 12px; color: var(--text-muted); align-self: center;">${total} memories</span>
            </div>
            <div class="memory-list">
                ${results.map(m => html`
                    <div class="memory-card" onClick=${() => onSelectMemory({
                        id: m.memory_id,
                        content: m.content,
                        domain: m.domain_tag,
                        confidence: m.confidence_score,
                        status: m.status,
                        memory_type: m.memory_type,
                        created_at: m.created_at,
                        agent: m.submitting_agent,
                    })}>
                        <div class="memory-card-header">
                            <span class="domain-badge" style="background: ${getDomainColor(m.domain_tag)}20; color: ${getDomainColor(m.domain_tag)};">
                                ${m.domain_tag}
                            </span>
                            <span style="font-size: 12px; font-weight: 600; color: ${confidenceColor(m.confidence_score)};">
                                ${(m.confidence_score * 100).toFixed(0)}%
                            </span>
                        </div>
                        <div class="memory-card-content">${m.content || 'No content'}</div>
                        <div class="memory-card-footer">
                            <span>${m.memory_type} | ${m.status}</span>
                            <span>${m.created_at ? timeAgo(m.created_at) : ''}</span>
                        </div>
                    </div>
                `)}
                ${results.length === 0 && !loading && html`
                    <div style="text-align: center; color: var(--text-muted); padding: 40px;">
                        ${query ? 'No memories match your search.' : 'No memories yet.'}
                    </div>
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

            ${!updateInfo?.update_available && updateInfo?.latest_version && !error && !installed && html`
                <div class="update-current">You're up to date.</div>
            `}

            ${updateInfo?.update_available && !installed && !updating && html`
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

                ${updateInfo?.update_available && !installed && !updating && html`
                    <button class="btn btn-primary" onClick=${doUpdate}>
                        Update Now
                    </button>
                `}

                ${updating && html`
                    <button class="btn btn-primary" disabled>
                        <span class="spinner"></span> Updating...
                    </button>
                `}

                ${installed && !restarting && html`
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

function SettingsPage() {
    const [settingsTab, setSettingsTab] = useState('overview');
    const [stats, setStats] = useState(null);
    const [health, setHealth] = useState(null);
    const [updateAvailable, setUpdateAvailable] = useState(false);

    // Fetch health with live polling every 3s
    useEffect(() => {
        const poll = () => {
            fetchHealth().then(h => {
                setHealth(h);
            }).catch(() => {});
            fetchStats().then(setStats).catch(() => {});
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

    // Countdown ticker — force re-render every 100ms for smooth display
    const [, setTick] = useState(0);
    useEffect(() => {
        const iv = setInterval(() => setTick(t => t + 1), 100);
        return () => clearInterval(iv);
    }, []);

    const ver = health?.version || 'dev';
    const encrypted = health?.encrypted || false;
    const chain = health?.chain || null;
    const ollama = health?.ollama || 'unknown';
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

    // Format countdown — compute from block_time relative to now
    const getCountdown = () => {
        if (!chain?.block_time) return null;
        const lastBlock = new Date(chain.block_time).getTime();
        const blockInterval = 5000;
        const elapsed = Date.now() - lastBlock;
        const remaining = blockInterval - (elapsed % blockInterval);
        return remaining;
    };
    const liveCountdown = getCountdown();
    const countdownDisplay = liveCountdown !== null ? (liveCountdown / 1000).toFixed(1) + 's' : '--';
    const countdownPct = liveCountdown !== null ? Math.min(100, (liveCountdown / 5000) * 100) : 0;

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

    const tabs = [
        { id: 'overview', label: 'Overview', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M4 4h3v3H4zM9 4h3v3H9zM4 9h3v3H4zM9 9h3v3H9z" fill="currentColor" opacity="0.8"/><path d="M2 2h12v12H2z" stroke="currentColor" fill="none" stroke-width="1.5" rx="2"/></svg>` },
        { id: 'security', label: 'Security', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M8 1L2 4v4c0 4 3 6 6 7 3-1 6-3 6-7V4L8 1z" stroke="currentColor" fill="none" stroke-width="1.5"/></svg>` },
        { id: 'data', label: 'Data', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M2 4h12v8H2z" stroke="currentColor" fill="none" stroke-width="1.5" rx="1"/><path d="M5 1v3M11 1v3M2 8h12" stroke="currentColor" stroke-width="1.5"/></svg>` },
        { id: 'config', label: 'Configuration', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><circle cx="8" cy="8" r="3" stroke="currentColor" fill="none" stroke-width="1.5"/><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3 3l1.5 1.5M11.5 11.5L13 13M13 3l-1.5 1.5M4.5 11.5L3 13" stroke="currentColor" stroke-width="1.5"/></svg>` },
        { id: 'update', label: 'Update', icon: html`<svg width="14" height="14" viewBox="0 0 16 16"><path d="M8 2v8M5 7l3 3 3-3" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/><path d="M3 12h10" stroke="currentColor" fill="none" stroke-width="1.5" stroke-linecap="round"/></svg>` },
    ];

    return html`
        <div class="settings-page">
            <div class="settings-tabs">
                ${tabs.map(t => html`
                    <button class="settings-tab ${settingsTab === t.id ? 'active' : ''}"
                            onClick=${() => setSettingsTab(t.id)}>
                        ${t.icon}
                        <span>${t.label}</span>
                        ${t.id === 'update' && updateAvailable ? html`<span class="update-badge" title="Update available"></span>` : ''}
                    </button>
                `)}
                <${PageHelp} section="settings" label="Settings guide" />
            </div>

            ${settingsTab === 'overview' && html`
                <div class="settings-tab-content">
                    <!-- Chain Health -->
                    <div class="settings-section chain-health-section">
                        <h3>
                            Chain Health <${HelpTip} text="Your BFT consensus chain status. Blocks are produced every ~5 seconds. All validators must agree on memory operations." />
                        </h3>
                        ${chain ? html`
                            <div class="chain-stats-grid">
                                <div class="chain-stat-card" title="Total number of blocks committed to the chain.">
                                    <div class="chain-stat-value block-height">${Number(chain.block_height || 0).toLocaleString()}</div>
                                    <div class="chain-stat-label">Block Height</div>
                                </div>
                                <div class="chain-stat-card" title="Countdown to the next block (~5s intervals).">
                                    <div class="chain-stat-value countdown-value">${countdownDisplay}</div>
                                    <div class="chain-stat-label">Next Block</div>
                                    <div class="countdown-bar"><div class="countdown-fill" style="width: ${countdownPct}%"></div></div>
                                </div>
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
                                <div class="settings-row"><span class="label">Last Block</span><span class="value">${chain.block_time ? new Date(chain.block_time).toLocaleTimeString() : '--'}</span></div>
                            </div>
                        ` : html`<div class="chain-offline">${statusDot(false)} <span>Chain unavailable — CometBFT not running</span></div>`}
                    </div>

                    <div class="settings-grid">
                        <!-- System Status -->
                        <div class="settings-section">
                            <h3>System Status</h3>
                            <div class="settings-row"><span class="label">${statusDot(true)} SAGE</span><span class="value" style="color:#10b981">Running</span></div>
                            <div class="settings-row"><span class="label">${statusDot(ollama === 'running')} Ollama</span><span class="value" style="color: ${ollama === 'running' ? '#10b981' : '#6b7280'}">${ollama === 'running' ? 'Connected' : 'Offline'}</span></div>
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

                        <!-- Connected Peers -->
                        <div class="settings-section">
                            <h3>
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px">
                                    <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/>
                                    <path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>
                                </svg>
                                Connected Peers
                            </h3>
                            ${peers.length > 0 ? peers.map(p => html`
                                <div class="peer-card">
                                    <div class="peer-header">
                                        <span class="status-dot active"></span>
                                        <span class="peer-moniker">${p.moniker || 'unknown'}</span>
                                        <span class="peer-badge">${p.outbound ? 'outbound' : 'inbound'}</span>
                                    </div>
                                    <div class="peer-meta">
                                        <span class="peer-meta-label">IP</span><span class="peer-meta-value">${p.remote_ip}</span>
                                        <span class="peer-meta-label">Connected</span><span class="peer-meta-value">${formatDuration(p.duration)}</span>
                                        <span class="peer-meta-label">Sent</span><span class="peer-meta-value">${formatBytes(p.bytes_sent)}</span>
                                        <span class="peer-meta-label">Received</span><span class="peer-meta-value">${formatBytes(p.bytes_recv)}</span>
                                        <span class="peer-meta-label">Node ID</span><span class="peer-meta-value">${p.id}...</span>
                                    </div>
                                </div>
                            `) : html`
                                <div class="peer-empty">No peers connected — running in Personal mode.
                                    <div style="margin-top:8px;font-size:11px;color:var(--text-muted)">Connect other SAGE nodes via quorum mode to see peers here.</div>
                                </div>
                            `}
                        </div>
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

            ${settingsTab === 'data' && html`
                <div class="settings-tab-content">
                    <div class="settings-section">
                        <h3>Export & Backup</h3>
                        <p style="color:var(--text-muted);font-size:0.85rem;margin-bottom:16px;">
                            Download a full backup of your memories in JSONL format. This file can be re-imported on a new machine via the Import page to restore your brain.
                        </p>
                        <div class="settings-row">
                            <span class="label">Export all memories (JSONL — re-importable)</span>
                            <button class="btn" onClick=${() => {
                                window.open('/v1/dashboard/export', '_blank');
                            }}>Download Backup</button>
                        </div>
                    </div>

                    <div class="settings-section" style="margin-top:16px">
                        <h3>Restore</h3>
                        <p style="color:var(--text-muted);font-size:0.85rem;margin-bottom:16px;">
                            To restore from a backup, go to the <strong>Import</strong> page (sidebar) and upload your <code>.jsonl</code> backup file. All domains, types, and metadata will be preserved.
                        </p>
                    </div>
                </div>
            `}

            ${settingsTab === 'config' && html`
                <div class="settings-tab-content">
                    ${html`<${BootInstructions} />`}

                    <div class="settings-section" style="margin-top:16px">
                        ${html`<${MemoryMode} />`}
                    </div>

                    <div class="settings-section" style="margin-top:16px">
                        ${html`<${RecallSettings} />`}
                    </div>

                    <div class="settings-section" style="margin-top:16px">
                        ${html`<${CleanupSettings} />`}
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

            ${settingsTab === 'update' && html`
                <div class="settings-tab-content">
                    ${html`<${SoftwareUpdate} />`}

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
            if (data.phase === 'complete') {
                setProgress(null);
            } else {
                setProgress(data);
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

    const ollamaOk = health.ollama === 'running';
    const totalMem = health.memories?.total_memories || 0;
    const domains = health.memories?.by_domain ? Object.keys(health.memories.by_domain).length : 0;

    return html`
        <div class="health-bar">
            <div class="health-item">
                <div class="health-dot ${ollamaOk ? 'ok' : 'err'}"></div>
                <span>Ollama ${ollamaOk ? 'connected' : 'offline'}</span>
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
                        <div class="guide-detail-desc">Type any keyword or phrase to search across all memory content. Results are sorted by relevance with domain badges and confidence scores shown inline.</div>
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
                        <div class="guide-detail-desc">Click Remove in the action bar. This triggers a chain redeployment — the chain briefly pauses (~30 seconds) while the validator set is updated. Memories from the removed agent are preserved. You cannot remove the last admin.</div>
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
                    <button class="detail-close" onClick=${handleDismiss}>x</button>
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

function LoginScreen({ onSuccess }) {
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

const CLEARANCE_LABELS = ['Guest', 'Internal', 'Confidential', 'Secret', 'Top Secret'];
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

    // Governance state
    const [govProposals, setGovProposals] = useState([]);
    const [activeProposal, setActiveProposal] = useState(null);
    const [govHistoryOpen, setGovHistoryOpen] = useState(false);
    const [showGovModal, setShowGovModal] = useState(false);
    const [govSubmitting, setGovSubmitting] = useState(false);
    const [govVoting, setGovVoting] = useState(false);
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
                    fetchGovProposalDetail(activeProposal.id),
                    fetchHealth()
                ]);
                if (detail) {
                    setActiveProposal(detail.proposal || detail);
                    // If proposal is no longer voting, refresh the full list
                    const p = detail.proposal || detail;
                    if (p.status !== 'voting') {
                        loadGovProposals();
                    }
                }
                if (health?.chain?.block_height) setCurrentHeight(Number(health.chain.block_height));
            } catch (e) { /* ignore polling errors */ }
        }, 3000);
        return () => { if (govPollRef.current) { clearInterval(govPollRef.current); govPollRef.current = null; } };
    }, [activeProposal?.id, loadGovProposals]);

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
        redeployPollRef.current = setInterval(async () => {
            try {
                const s = await fetchRedeployStatus();
                setRedeployStatus(s);
                if (!s || s.status === 'idle' || s.status === 'completed' || s.status === 'failed') {
                    clearInterval(redeployPollRef.current);
                    redeployPollRef.current = null;
                    if (s?.status === 'completed') loadAgents();
                }
            } catch (e) { clearInterval(redeployPollRef.current); redeployPollRef.current = null; }
        }, 1000);
    }, [loadAgents]);

    useEffect(() => {
        fetchRedeployStatus().then(s => {
            if (s?.status && s.status !== 'idle') { setRedeployStatus(s); startRedeployPoll(); }
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
        await updateAgent(agentId, { role: editRole, clearance: editClearance, domain_access: JSON.stringify(arr), visible_agents: editVisibleAgents });
        loadAgents();
        setAccessDirty(false);
        setAccessSaved(true);
        setTimeout(() => setAccessSaved(false), 2000);
    }, [editRole, editClearance, editDomainAccess, loadAgents]);

    const handleOverviewSave = useCallback(async (agentId) => {
        try {
            const res = await updateAgent(agentId, { name: editName, boot_bio: editBio });
            if (res.error) { showToast(res.error, 'error'); return; }
            if (res.on_chain_warning) {
                showToast('Saved locally but on-chain sync failed — will auto-heal on next agent boot. (' + res.on_chain_warning + ')', 'warning', 8000);
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
            else { setRedeployStatus(rdRes); startRedeployPoll(); }
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
            else { setRedeployStatus(rdRes); startRedeployPoll(); }
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
            else { showToast(`Vote submitted: ${decision}`, 'success'); }
            loadGovProposals();
        } catch (e) { showToast('Vote failed: ' + e.message, 'error'); }
        setGovVoting(false);
    }, [loadGovProposals]);

    const handleGovSubmit = useCallback(async () => {
        setGovSubmitting(true);
        try {
            const proposal = {
                operation: govNewOp,
                target_agent_id: govNewTarget,
                reason: govNewReason,
            };
            if (govNewOp === 'add_validator' || govNewOp === 'update_power') {
                proposal.power = parseInt(govNewPower, 10) || 10;
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

    const isRedeploying = redeployStatus?.status && !['idle','completed','failed'].includes(redeployStatus.status);

    return html`
        <div class="network-page fade-in">
            ${isRedeploying && html`<div class="redeploy-banner"><span class="deploy-spinner"></span> Network reconfiguration in progress... Phase: ${(DEPLOY_PHASES.find(p => p.key === redeployStatus.current_phase) || {}).label || redeployStatus.current_phase}</div>`}
            <div class="network-header">
                <div><h2>Network <${HelpTip} text="Manage agents on your SAGE chain. Each agent is a separate node that participates in BFT consensus. Click any agent to expand its details and permissions." /><${PageHelp} section="network" label="Network & Agents guide" /></h2><div class="network-header-sub">${agents.length} agent${agents.length !== 1 ? 's' : ''} on this network</div></div>
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
                            Proposed by: ${resolveAgentName(activeProposal.proposer_id)}${activeProposal.proposed_height ? ` \u00b7 Block #${activeProposal.proposed_height}` : ''}
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
                            ${activeProposal.my_vote ? html`
                                <div class="gov-voted-badge">
                                    You voted: <strong>${activeProposal.my_vote}</strong>
                                </div>
                            ` : html`
                                <button class="gov-vote-btn accept" onClick=${() => handleGovVote(activeProposal.id, 'accept')} disabled=${govVoting}>Accept</button>
                                <button class="gov-vote-btn reject" onClick=${() => handleGovVote(activeProposal.id, 'reject')} disabled=${govVoting}>Reject</button>
                                <button class="gov-vote-btn abstain" onClick=${() => handleGovVote(activeProposal.id, 'abstain')} disabled=${govVoting}>Abstain</button>
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
                                <div class="gov-history-card" key=${p.id}>
                                    <div class="gov-history-info">
                                        <div class="gov-history-op">${govOpLabel(p.operation)}: ${resolveAgentName(p.target_agent_id)}</div>
                                        <div class="gov-history-detail">
                                            by ${resolveAgentName(p.proposer_id)}${p.proposed_height ? ` \u00b7 Block #${p.proposed_height}` : ''}
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
                            <button class="detail-close" onClick=${() => setShowGovModal(false)}>x</button>
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
                            <button class="btn btn-primary" onClick=${handleGovSubmit} disabled=${govSubmitting || !govNewTarget}>
                                ${govSubmitting ? 'Submitting...' : 'Submit Proposal'}
                            </button>
                        </div>
                    </div>
                </div>
            `}

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
                                            <div class="access-section-title">Role <${HelpTip} text="Admins have full access to all domains and can manage the network. Members read/write in allowed domains only. Observers are read-only." /></div>
                                            <div class="role-selector" onClick=${e => e.stopPropagation()}>
                                                ${[
                                                    { key: 'admin', name: 'Admin', desc: 'Full access. Can manage agents and network settings.' },
                                                    { key: 'member', name: 'Member', desc: 'Read/write within allowed domains. Cannot manage agents.' },
                                                    { key: 'observer', name: 'Observer', desc: 'Read-only. Can view memories but cannot submit.' },
                                                ].map(r => html`
                                                    <div class="role-card ${editRole === r.key ? 'selected ' + r.key : ''}"
                                                        onClick=${() => { setEditRole(r.key); setAccessDirty(true); }}>
                                                        <div class="role-card-name">${r.name}</div>
                                                        <div class="role-card-desc">${r.desc}</div>
                                                    </div>
                                                `)}
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
                    <h3 style="color:var(--text-muted);font-size:14px;font-weight:500;margin-bottom:12px;display:flex;align-items:center;gap:8px;">
                        <span style="color:var(--warning, #f5a623);">?</span> Unregistered Agents
                        <${HelpTip} text="These agents have memories in your SAGE database but aren't registered in the dashboard. They were likely created by per-project Claude Code sessions. You can merge their memories into a registered agent." />
                    </h3>
                    <div class="unregistered-list">
                        ${unregistered.map(u => html`
                            <div class="unregistered-card" key=${u.agent_id}>
                                <div class="unregistered-info">
                                    <span class="mono" style="font-size:12px;color:var(--text-dim);">${u.short_id}</span>
                                    <span style="font-size:12px;color:var(--text-muted);">${u.memory_count} memor${u.memory_count === 1 ? 'y' : 'ies'}</span>
                                </div>
                                <button class="merge-btn" onClick=${() => setMergeTarget({ source: u.agent_id, sourceShort: u.short_id, memoryCount: u.memory_count })}>
                                    Merge into...
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
                            <h2>Merge Agent Memories</h2>
                            <button class="detail-close" onClick=${() => setMergeTarget(null)}>x</button>
                        </div>
                        <div class="wizard-body" style="padding:20px;">
                            <p style="color:var(--text-dim);margin-bottom:16px;">
                                Reassign <strong>${mergeTarget.memoryCount}</strong> memor${mergeTarget.memoryCount === 1 ? 'y' : 'ies'} from
                                <code style="font-size:11px;">${mergeTarget.sourceShort}</code> to a registered agent.
                            </p>
                            <p style="color:var(--text-muted);font-size:12px;margin-bottom:16px;">
                                This operation goes through BFT consensus on-chain. The memories will be re-attributed on the next block.
                            </p>
                            <div style="display:flex;flex-direction:column;gap:8px;">
                                ${agents.filter(a => a.status !== 'removed').map(a => html`
                                    <button class="merge-target-btn" onClick=${() => handleMerge(mergeTarget.source, a.agent_id)} disabled=${merging}>
                                        <span>${a.avatar || '\u{1F916}'}</span>
                                        <span>${a.name}</span>
                                        <span class="agent-role-badge ${a.role}" style="margin-left:auto;">${a.role}</span>
                                    </button>
                                `)}
                            </div>
                            ${merging && html`<p style="color:var(--primary);font-size:12px;margin-top:12px;">Submitting to blockchain consensus...</p>`}
                        </div>
                    </div>
                </div>
            `}
            ${tagTransfer && html`
                <div class="wizard-overlay" onClick=${e => { if (e.target === e.currentTarget) setTagTransfer(null); }}>
                    <div class="wizard-modal" style="max-width:520px;">
                        <div class="wizard-header">
                            <h2>${tagTransfer.step === 'tags' ? 'Transfer Memories by Tag' : 'Select Target Agent'}</h2>
                            <button class="detail-close" onClick=${() => setTagTransfer(null)}>x</button>
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
            ${showRemoveConfirm && html`<${RemoveConfirmModal} agent=${showRemoveConfirm} onConfirm=${() => handleRemove(showRemoveConfirm)} onCancel=${() => setShowRemoveConfirm(null)} />`}
            ${showRotateConfirm && html`
                <div class="wizard-overlay" onClick=${(e) => { if (e.target === e.currentTarget) setShowRotateConfirm(null); }}>
                    <div class="wizard-modal" style="max-width:440px;">
                        <div class="wizard-header"><h2>Rotate Agent Key</h2><button class="detail-close" onClick=${() => setShowRotateConfirm(null)}>x</button></div>
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
        deployPollRef.current = setInterval(async () => {
            try {
                const s = await fetchRedeployStatus();
                setDeployStatus(s);
                if (!s || s.status === 'idle' || s.status === 'completed' || s.status === 'failed') {
                    clearInterval(deployPollRef.current);
                    deployPollRef.current = null;
                    setDeploying(false);
                }
            } catch (e) {
                clearInterval(deployPollRef.current);
                deployPollRef.current = null;
                setDeploying(false);
            }
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
                    <button class="detail-close" onClick=${onClose}>x</button>
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
                            This will mark the agent as removed and trigger a chain redeployment.
                            ${agent.memory_count > 0 ? html`<br/><br/><strong style="color:var(--warning);">This agent has ${agent.memory_count} memories.</strong> Memories will be preserved with original attribution.` : ''}
                        </p>
                        <div class="warning-banner">
                            ⚠ Chain will be briefly paused during redeployment.
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
    const [expanded, setExpanded] = useState(null);

    const loadData = useCallback(async () => {
        setLoading(true);
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
            <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">
                <h2 style="margin:0;font-size:20px;">Pipeline</h2>
                <span style="color:var(--text-muted);font-size:13px;">Agent-to-Agent Message Bus</span>
                <div style="flex:1"></div>
                <button class="btn" onClick=${loadData}>\u21BB Refresh</button>
            </div>

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

            ${loading && items.length === 0 && html`
                <div style="text-align:center;padding:60px;color:var(--text-muted);">Loading pipeline...</div>
            `}

            ${!loading && items.length === 0 && html`
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
// Root App
// ============================================================================

function App() {
    const [authState, setAuthState] = useState('loading'); // loading | login | ready
    const [isEncrypted, setIsEncrypted] = useState(false);
    const [page, setPage] = useState('brain');
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

    // Check auth on mount
    useEffect(() => {
        checkAuth().then(res => {
            setIsEncrypted(!!res.auth_required);
            if (!res.auth_required || res.authenticated) {
                setAuthState('ready');
            } else {
                setAuthState('login');
            }
        }).catch(() => setAuthState('ready')); // if auth check fails, assume no auth
    }, []);

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
            if (hash === '/search') setPage('search');
            else if (hash === '/tasks') setPage('tasks');
            else if (hash === '/settings') setPage('settings');
            else if (hash === '/import') setPage('import');
            else if (hash === '/network') setPage('network');
            else if (hash === '/pipeline') setPage('pipeline');
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

    // Show login screen
    if (authState === 'login') {
        return html`<${LoginScreen} onSuccess=${() => setAuthState('ready')} />`;
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
            <button class="sidebar-btn ${page === 'brain' ? 'active' : ''}" onClick=${() => navigate('brain')} title="Cerebrum">
                ${icons.brain}
            </button>
            <button class="sidebar-btn ${page === 'search' ? 'active' : ''}" onClick=${() => navigate('search')} title="Search">
                ${icons.search}
            </button>
            <button class="sidebar-btn ${page === 'tasks' ? 'active' : ''}" onClick=${() => navigate('tasks')} title="Tasks">
                ${icons.tasks}
            </button>
            <button class="sidebar-btn ${page === 'import' ? 'active' : ''}" onClick=${() => navigate('import')} title="Import">
                ${icons.import}
            </button>
            <button class="sidebar-btn ${page === 'network' ? 'active' : ''}" onClick=${() => navigate('network')} title="Network">
                ${icons.network}
            </button>
            <button class="sidebar-btn ${page === 'pipeline' ? 'active' : ''}" onClick=${() => navigate('pipeline')} title="Pipeline">
                ${icons.pipeline}
            </button>
            <button class="sidebar-btn ${page === 'settings' ? 'active' : ''}" onClick=${() => navigate('settings')} title="Settings">
                ${icons.settings}
            </button>
            <div style="flex:1;"></div>
            <button class="sidebar-btn" onClick=${() => openHelp(null)} title="Help">
                ${icons.help}
            </button>
        </div>
        <div class="main-content zoom-${textSize}">
            <div class="top-bar">
                <h1>CEREBRUM <span style="font-size:12px;font-weight:400;color:var(--text-muted);margin-left:6px">Your SAGE Brain</span></h1>
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

            ${page === 'brain' && html`
                <${BrainView} sse=${sseRef.current} onSelectMemory=${onSelectMemory} timelineFilter=${timelineFilter} />
                <${TimelineBar} selectedRanges=${timelineFilter} onSelectRange=${setTimelineFilter} />
            `}
            ${page === 'search' && html`<${SearchPage} onSelectMemory=${onSelectMemory} />`}
            ${page === 'tasks' && html`<${TasksPage} />`}
            ${page === 'import' && html`<${ImportPage} sse=${sseRef.current} />`}
            ${page === 'network' && html`<${NetworkPage} sse=${sseRef.current} />`}
            ${page === 'pipeline' && html`<${PipelinePage} />`}
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

// Mount
render(html`<${App} />`, document.getElementById('app'));
