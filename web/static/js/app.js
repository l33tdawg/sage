// SAGE Brain Dashboard — Root Application
import { SSEClient } from './sse.js';
import { fetchStats, fetchGraph, fetchMemories, deleteMemory, fetchHealth, checkAuth, login, importMemories } from './api.js';

const { h, render } = preact;
const { useState, useEffect, useRef, useCallback } = preactHooks;
const html = window.html;

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
    brain: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a7 7 0 0 0-7 7c0 2.38 1.19 4.47 3 5.74V17a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2v-2.26c1.81-1.27 3-3.36 3-5.74a7 7 0 0 0-7-7z"/><line x1="10" y1="22" x2="14" y2="22"/><line x1="9" y1="17" x2="15" y2="17"/></svg>`,
    search: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
    settings: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.32 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>`,
    import: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>`,
};

// ============================================================================
// Brain Visualization (Canvas)
// ============================================================================

function BrainView({ sse, onSelectMemory }) {
    const canvasRef = useRef(null);
    const stateRef = useRef({
        nodes: [], edges: [], simulation: null,
        camera: { x: 0, y: 0, zoom: 1 },
        mouse: { x: 0, y: 0, dragging: false, dragStart: null, hoveredNode: null },
        filterDomains: new Set(),
        searchFilter: '',
        animTime: 0,
        pulseNodes: new Map(),
    });
    const [stats, setStats] = useState(null);
    const [domains, setDomains] = useState([]);
    const [filterDomains, setFilterDomains] = useState(new Set());
    const [searchText, setSearchText] = useState('');
    const [tooltip, setTooltip] = useState(null);
    const [sseConnected, setSseConnected] = useState(false);

    // Load graph data
    useEffect(() => {
        loadData();
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
            setTimeout(loadData, 500);
        });
        return () => { unsub1(); unsub2(); unsub3(); unsub4(); };
    }, [sse]);

    // Canvas rendering loop
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let animFrame;

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
            const s = stateRef.current;
            const W = canvas.width / devicePixelRatio;
            const H = canvas.height / devicePixelRatio;
            const now = performance.now();
            s.animTime = now;

            // Update filter state from React
            s.filterDomains = filterDomains;
            s.searchFilter = searchText.toLowerCase();

            // Force simulation
            simulateForces(s, W, H);

            // Clear
            ctx.save();
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            ctx.scale(devicePixelRatio, devicePixelRatio);
            ctx.clearRect(0, 0, W, H);

            // Camera transform
            const cam = s.camera;
            ctx.translate(W / 2 + cam.x, H / 2 + cam.y);
            ctx.scale(cam.zoom, cam.zoom);

            // Determine visible nodes
            const visibleNodes = s.nodes.filter(n => {
                if (s.filterDomains.size > 0 && !s.filterDomains.has(n.domain)) return false;
                return true;
            });
            const visibleIds = new Set(visibleNodes.map(n => n.id));

            // Determine search-matching nodes
            const searchMatch = s.searchFilter
                ? new Set(visibleNodes.filter(n =>
                    (n.content && n.content.toLowerCase().includes(s.searchFilter)) ||
                    (n.domain && n.domain.toLowerCase().includes(s.searchFilter))
                ).map(n => n.id))
                : null;

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

                const pulse = s.pulseNodes.get(n.id);
                let extraGlow = 0;
                let fadeOut = 1;

                if (pulse) {
                    const elapsed = now - pulse.start;
                    if (elapsed > 3000) {
                        s.pulseNodes.delete(n.id);
                    } else if (pulse.type === 'remember') {
                        extraGlow = Math.max(0, 1 - elapsed / 1500) * 20;
                    } else if (pulse.type === 'recall') {
                        extraGlow = Math.max(0, 1 - elapsed / 2000) * 15;
                    } else if (pulse.type === 'forget') {
                        fadeOut = Math.max(0, 1 - elapsed / 2000);
                    }
                }

                // Organic drift
                const drift = Math.sin(now / 2000 + n.x * 0.01) * 0.3;

                const drawX = n.x;
                const drawY = n.y + drift;
                const r = n.radius;

                ctx.globalAlpha = dim ? 0.15 * fadeOut : fadeOut;

                // Glow
                const glowSize = r + 8 + extraGlow + Math.sin(now / 1000 + n.x) * 2;
                const glow = ctx.createRadialGradient(drawX, drawY, r * 0.3, drawX, drawY, glowSize);
                glow.addColorStop(0, n.color + '40');
                glow.addColorStop(1, n.color + '00');
                ctx.fillStyle = glow;
                ctx.beginPath();
                ctx.arc(drawX, drawY, glowSize, 0, Math.PI * 2);
                ctx.fill();

                // Node body
                ctx.beginPath();
                ctx.arc(drawX, drawY, r, 0, Math.PI * 2);
                ctx.fillStyle = n.color;
                ctx.globalAlpha = (dim ? 0.2 : 0.85) * fadeOut;
                ctx.fill();

                // Inner highlight
                ctx.beginPath();
                ctx.arc(drawX - r * 0.2, drawY - r * 0.2, r * 0.4, 0, Math.PI * 2);
                ctx.fillStyle = 'rgba(255,255,255,0.25)';
                ctx.globalAlpha = (dim ? 0.1 : 0.5) * fadeOut;
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

                ctx.globalAlpha = 1;
            }

            // Hover highlight
            if (s.mouse.hoveredNode && visibleIds.has(s.mouse.hoveredNode.id)) {
                const n = s.mouse.hoveredNode;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 4, 0, Math.PI * 2);
                ctx.strokeStyle = '#ffffff';
                ctx.globalAlpha = 0.5;
                ctx.lineWidth = 1.5;
                ctx.stroke();
                ctx.globalAlpha = 1;
            }

            ctx.restore();
            animFrame = requestAnimationFrame(tick);
        }

        animFrame = requestAnimationFrame(tick);
        return () => {
            cancelAnimationFrame(animFrame);
            window.removeEventListener('resize', resize);
        };
    }, [filterDomains, searchText]);

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
            for (let i = s.nodes.length - 1; i >= 0; i--) {
                const n = s.nodes[i];
                const dx = n.x - wx;
                const dy = n.y - wy;
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

        function onMouseUp(e) {
            const s = stateRef.current;
            const wasDragging = s.mouse.dragging;
            s.mouse.dragging = false;

            if (wasDragging && s.mouse.dragStart) {
                const dx = e.clientX - s.mouse.dragStart.x;
                const dy = e.clientY - s.mouse.dragStart.y;
                if (Math.abs(dx) < 4 && Math.abs(dy) < 4 && s.mouse.hoveredNode) {
                    onSelectMemory(s.mouse.hoveredNode);
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

    return html`
        <div class="brain-container">
            <canvas ref=${canvasRef} class="brain-canvas"></canvas>

            <div class="domain-bar">
                ${domains.map(d => html`
                    <button class="domain-pill ${filterDomains.has(d) ? 'active' : ''}"
                            style="color: ${getDomainColor(d)}; ${filterDomains.has(d) ? `background: ${getDomainColor(d)}20` : ''}"
                            onClick=${() => toggleDomain(d)}>
                        ${d}
                    </button>
                `)}
            </div>

            <div class="search-overlay">
                <input class="search-input" type="text" placeholder="Filter memories..."
                       value=${searchText} onInput=${e => setSearchText(e.target.value)} />
            </div>

            ${stats && html`
                <div class="stats-panel">
                    <h3>Memory Stats</h3>
                    <div class="stat-row">
                        <span class="stat-label">Total</span>
                        <span class="stat-value">${stats.total_memories || 0}</span>
                    </div>
                    ${stats.by_domain && Object.entries(stats.by_domain).map(([d, c]) => html`
                        <div class="stat-bar-container">
                            <span style="color: ${getDomainColor(d)}; font-size: 11px; min-width: 80px; text-transform: uppercase; letter-spacing: 0.5px;">${d}</span>
                            <div class="stat-bar">
                                <div class="stat-bar-fill" style="width: ${stats.total_memories ? (c / stats.total_memories * 100) : 0}%; background: ${getDomainColor(d)};"></div>
                            </div>
                            <span class="stat-bar-label">${c}</span>
                        </div>
                    `)}
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
                    <div class="tooltip-domain" style="color: ${getDomainColor(tooltip.node.domain)}">${tooltip.node.domain}</div>
                    <div class="tooltip-content">${tooltip.node.content ? tooltip.node.content.slice(0, 120) : 'No content'}${tooltip.node.content && tooltip.node.content.length > 120 ? '...' : ''}</div>
                    <div class="tooltip-meta">${tooltip.node.memory_type || tooltip.node.memoryType} | conf: ${(tooltip.node.confidence * 100).toFixed(0)}% | ${timeAgo(tooltip.node.created_at || tooltip.node.createdAt)}</div>
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

    const dt = 0.3;
    const repulsion = 800;
    const attraction = 0.005;
    const centerGravity = 0.002;
    const damping = 0.92;

    // Build node index
    const nodeIdx = {};
    for (let i = 0; i < nodes.length; i++) nodeIdx[nodes[i].id] = i;

    // Repulsion (Barnes-Hut approximation: only check nearby)
    for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
            const a = nodes[i], b = nodes[j];
            let dx = b.x - a.x;
            let dy = b.y - a.y;
            let dist = Math.sqrt(dx * dx + dy * dy) || 1;
            if (dist > 300) continue; // Skip far nodes
            const force = repulsion / (dist * dist);
            const fx = (dx / dist) * force;
            const fy = (dy / dist) * force;
            a.vx -= fx * dt;
            a.vy -= fy * dt;
            b.vx += fx * dt;
            b.vy += fy * dt;
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

    // Center gravity + damping + integration
    for (const n of nodes) {
        n.vx -= n.x * centerGravity;
        n.vy -= n.y * centerGravity;
        n.vx *= damping;
        n.vy *= damping;
        n.x += n.vx * dt;
        n.y += n.vy * dt;
    }
}

// ============================================================================
// Memory Detail Panel
// ============================================================================

function MemoryDetail({ memory, onClose, onDelete }) {
    const [confirming, setConfirming] = useState(false);

    if (!memory) return null;

    async function handleDelete() {
        if (!confirming) { setConfirming(true); return; }
        await deleteMemory(memory.id);
        if (onDelete) onDelete(memory.id);
        onClose();
    }

    const conf = memory.confidence;
    const color = getDomainColor(memory.domain);

    return html`
        <div class="detail-overlay open fade-in">
            <div class="detail-header">
                <h2>Memory Detail</h2>
                <button class="detail-close" onClick=${onClose}>x</button>
            </div>
            <div class="detail-body">
                <div class="detail-section">
                    <label>Content</label>
                    <div class="detail-content">${memory.content || 'No content available'}</div>
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
                        <span class="domain-badge" style="background: ${color}20; color: ${color};">${memory.domain}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Type</label>
                        <span class="value">${memory.memory_type || memory.memoryType || 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Status</label>
                        <span class="value">${memory.status}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Created</label>
                        <span class="value">${memory.created_at ? timeAgo(memory.created_at) : 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Agent</label>
                        <span class="value" style="font-size: 11px; word-break: break-all;">${memory.agent || memory.submitting_agent || 'unknown'}</span>
                    </div>
                    <div class="detail-meta-item">
                        <label>Memory ID</label>
                        <span class="value" style="font-size: 10px; word-break: break-all;">${memory.id || memory.memory_id}</span>
                    </div>
                    ${memory.content_hash && html`
                        <div class="detail-meta-item">
                            <label>Content Hash</label>
                            <span class="value" style="font-size: 10px; word-break: break-all; font-family: var(--font-mono, monospace);">${typeof memory.content_hash === 'string' ? memory.content_hash : btoa(String.fromCharCode(...new Uint8Array(memory.content_hash)))}</span>
                        </div>
                    `}
                    ${memory.committed_at && html`
                        <div class="detail-meta-item">
                            <label>Committed</label>
                            <span class="value">${timeAgo(memory.committed_at)}</span>
                        </div>
                    `}
                    ${memory.provider && html`
                        <div class="detail-meta-item">
                            <label>Provider</label>
                            <span class="value">${memory.provider}</span>
                        </div>
                    `}
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
// Search Page
// ============================================================================

function SearchPage({ onSelectMemory }) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        loadMemories();
    }, []);

    async function loadMemories(search) {
        setLoading(true);
        try {
            const data = await fetchMemories({ limit: 100, sort: 'newest' });
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
        loadMemories(v);
    }

    return html`
        <div class="search-page">
            <input class="search-page-input" type="text" placeholder="Search memories..."
                   value=${query} onInput=${handleSearch} />
            <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 12px;">${total} memories</div>
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
// Settings Page
// ============================================================================

function SettingsPage() {
    const [stats, setStats] = useState(null);
    const [health, setHealth] = useState(null);

    useEffect(() => {
        fetchStats().then(setStats).catch(() => {});
        fetchHealth().then(setHealth).catch(() => {});
    }, []);

    const ver = health?.version || 'dev';
    const encrypted = health?.encrypted || false;

    return html`
        <div class="settings-page">
            <div class="settings-section">
                <h3>SAGE Instance</h3>
                <div class="settings-row">
                    <span class="label">Version</span>
                    <span class="value">${ver}</span>
                </div>
                <div class="settings-row">
                    <span class="label">Encryption</span>
                    <span class="value" style="color: ${encrypted ? 'var(--success, #10b981)' : 'var(--text-muted, #6b7280)'}">
                        ${encrypted ? 'AES-256-GCM (active)' : 'Off'}
                    </span>
                </div>
                <div class="settings-row">
                    <span class="label">Dashboard API</span>
                    <span class="value">${window.location.origin}</span>
                </div>
            </div>

            ${stats && html`
                <div class="settings-section">
                    <h3>Memory Statistics</h3>
                    <div class="settings-row">
                        <span class="label">Total Memories</span>
                        <span class="value">${stats.total_memories || 0}</span>
                    </div>
                    ${stats.by_status && Object.entries(stats.by_status).map(([s, c]) => html`
                        <div class="settings-row">
                            <span class="label">${s}</span>
                            <span class="value">${c}</span>
                        </div>
                    `)}
                    ${stats.db_size_bytes != null && html`
                        <div class="settings-row">
                            <span class="label">DB Size</span>
                            <span class="value">${(stats.db_size_bytes / 1024 / 1024).toFixed(1)} MB</span>
                        </div>
                    `}
                </div>
            `}

            <div class="settings-section">
                <h3>Export</h3>
                <div class="settings-row">
                    <span class="label">Export all memories</span>
                    <button class="btn" onClick=${() => {
                        fetchMemories({ limit: 10000 }).then(data => {
                            const blob = new Blob([JSON.stringify(data.memories, null, 2)], { type: 'application/json' });
                            const url = URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.href = url;
                            a.download = 'sage-memories-export.json';
                            a.click();
                            URL.revokeObjectURL(url);
                        });
                    }}>Download JSON</button>
                </div>
            </div>

            <div class="settings-section">
                <h3>About SAGE</h3>
                <div class="settings-row">
                    <span class="label">Full Name</span>
                    <span class="value">(Sovereign) Agent Governed Experience</span>
                </div>
                <div class="settings-row">
                    <span class="label">Author</span>
                    <span class="value">Dhillon Andrew Kannabhiran</span>
                </div>
                <div class="settings-row">
                    <span class="label">License</span>
                    <span class="value">Apache 2.0</span>
                </div>
                <div class="settings-row">
                    <span class="label">GitHub</span>
                    <span class="value"><a href="https://github.com/l33tdawg/sage" target="_blank" style="color:var(--accent)">l33tdawg/sage</a></span>
                </div>
                <div class="settings-row">
                    <span class="label">Website</span>
                    <span class="value"><a href="https://l33tdawg.github.io/sage/" target="_blank" style="color:var(--accent)">l33tdawg.github.io/sage</a></span>
                </div>
                <div class="settings-row">
                    <span class="label">Architecture</span>
                    <span class="value">CometBFT v0.38 + SQLite + Ed25519</span>
                </div>
                <div class="settings-row">
                    <span class="label">Connect Guide</span>
                    <span class="value"><a href="https://l33tdawg.github.io/sage/connect.html" target="_blank" style="color:var(--accent)">How to connect your AI</a></span>
                </div>
            </div>
        </div>
    `;
}

// ============================================================================
// Import Page
// ============================================================================

function ImportPage() {
    const [selectedFile, setSelectedFile] = useState(null);
    const [dragging, setDragging] = useState(false);
    const [importing, setImporting] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const fileInputRef = useRef(null);

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
        if (file && (file.name.endsWith('.json') || file.name.endsWith('.zip'))) {
            setSelectedFile(file);
            setResult(null);
            setError(null);
        } else {
            setError('Please drop a .json or .zip file.');
        }
    }

    function handleFileSelect(e) {
        const file = e.target.files[0];
        if (file) {
            setSelectedFile(file);
            setResult(null);
            setError(null);
        }
    }

    async function handleImport() {
        if (!selectedFile || importing) return;
        setImporting(true);
        setError(null);
        setResult(null);
        try {
            const res = await importMemories(selectedFile);
            if (res.error) {
                setError(res.error);
            } else {
                setResult(res);
            }
        } catch (err) {
            setError(err.message || 'Import failed. Please try again.');
        } finally {
            setImporting(false);
        }
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
            </div>

            <div class="drop-zone ${dragging ? 'drop-zone-active' : ''} ${selectedFile ? 'drop-zone-has-file' : ''}"
                 onDragOver=${handleDragOver}
                 onDragLeave=${handleDragLeave}
                 onDrop=${handleDrop}
                 onClick=${() => fileInputRef.current && fileInputRef.current.click()}>
                <input type="file" ref=${fileInputRef} accept=".json,.zip"
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
                    <span class="drop-zone-hint">Accepts .zip and .json files</span>
                `}
            </div>

            <div class="import-actions">
                <button class="btn import-btn ${importing ? 'importing' : ''}"
                        disabled=${!selectedFile || importing}
                        onClick=${handleImport}>
                    ${importing ? html`
                        <span class="import-spinner"></span> Importing...
                    ` : 'Import Memories'}
                </button>
            </div>

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

function TimelineBar() {
    const [buckets, setBuckets] = useState([]);

    useEffect(() => {
        import('./api.js').then(({ fetchTimeline }) => {
            fetchTimeline({ bucket: 'hour' }).then(data => {
                setBuckets(data.buckets || []);
            }).catch(() => {});
        });
    }, []);

    const maxCount = Math.max(1, ...buckets.map(b => b.count));

    return html`
        <div class="timeline-bar">
            <span class="timeline-label">24h</span>
            <div class="timeline-track">
                ${buckets.map((b, i) => html`
                    <div class="timeline-bucket-bar"
                         style="left: ${(i / Math.max(1, buckets.length)) * 100}%;
                                width: ${100 / Math.max(1, buckets.length)}%;
                                height: ${(b.count / maxCount) * 100}%;"
                         title="${b.period}: ${b.count} memories">
                    </div>
                `)}
            </div>
            <span class="timeline-label" style="text-align: right;">Now</span>
        </div>
    `;
}

// ============================================================================
// Health Status Bar
// ============================================================================

function HealthBar() {
    const [health, setHealth] = useState(null);

    useEffect(() => {
        loadHealth();
        const interval = setInterval(loadHealth, 15000);
        return () => clearInterval(interval);
    }, []);

    async function loadHealth() {
        try {
            const data = await fetchHealth();
            setHealth(data);
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
                <span class="health-num">${totalMem}</span> memories
            </div>
            <div class="health-sep"></div>
            <div class="health-item">
                <span class="health-num">${domains}</span> domains
            </div>
            <div class="health-sep"></div>
            <div class="health-item">
                <span style="color: var(--text-muted)">uptime</span> ${health.uptime ? health.uptime.split('.')[0] : '—'}
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
                <p class="login-subtitle">Enter your vault passphrase to unlock the Brain Dashboard.</p>
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
            </div>
        </div>
    `;
}

// ============================================================================
// Root App
// ============================================================================

function App() {
    const [authState, setAuthState] = useState('loading'); // loading | login | ready
    const [page, setPage] = useState('brain');
    const [selectedMemory, setSelectedMemory] = useState(null);
    const [sseConnected, setSseConnected] = useState(false);
    const sseRef = useRef(null);

    // Check auth on mount
    useEffect(() => {
        checkAuth().then(res => {
            if (!res.auth_required || res.authenticated) {
                setAuthState('ready');
            } else {
                setAuthState('login');
            }
        }).catch(() => setAuthState('ready')); // if auth check fails, assume no auth
    }, []);

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
            else if (hash === '/settings') setPage('settings');
            else if (hash === '/import') setPage('import');
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

    return html`
        <div class="sidebar">
            <div class="sidebar-logo">S</div>
            <button class="sidebar-btn ${page === 'brain' ? 'active' : ''}" onClick=${() => navigate('brain')} title="Brain">
                ${icons.brain}
            </button>
            <button class="sidebar-btn ${page === 'search' ? 'active' : ''}" onClick=${() => navigate('search')} title="Search">
                ${icons.search}
            </button>
            <button class="sidebar-btn ${page === 'import' ? 'active' : ''}" onClick=${() => navigate('import')} title="Import">
                ${icons.import}
            </button>
            <button class="sidebar-btn ${page === 'settings' ? 'active' : ''}" onClick=${() => navigate('settings')} title="Settings">
                ${icons.settings}
            </button>
        </div>
        <div class="main-content">
            <div class="top-bar">
                <h1>SAGE Brain</h1>
                <div class="spacer"></div>
                <div class="connection-badge">
                    <div class="connection-dot ${sseConnected ? '' : 'disconnected'}"></div>
                    ${sseConnected ? 'Live' : 'Connecting...'}
                </div>
            </div>
            <${HealthBar} />

            ${page === 'brain' && html`
                <${BrainView} sse=${sseRef.current} onSelectMemory=${onSelectMemory} />
                <${TimelineBar} />
            `}
            ${page === 'search' && html`<${SearchPage} onSelectMemory=${onSelectMemory} />`}
            ${page === 'import' && html`<${ImportPage} />`}
            ${page === 'settings' && html`<${SettingsPage} />`}

            <${MemoryDetail}
                memory=${selectedMemory}
                onClose=${() => setSelectedMemory(null)}
                onDelete=${() => setSelectedMemory(null)}
            />
        </div>
    `;
}

// Mount
render(html`<${App} />`, document.getElementById('app'));
