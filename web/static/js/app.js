// CEREBRUM — Your SAGE Brain
import { SSEClient } from './sse.js';
import { fetchStats, fetchGraph, fetchMemories, deleteMemory, fetchHealth, checkAuth, login, importMemories, fetchCleanupSettings, saveCleanupSettings, runCleanup, fetchAgents, fetchAgent, createAgent, updateAgent, removeAgent, downloadBundle, fetchTemplates, fetchRedeployStatus, startRedeploy, createPairingCode, rotateAgentKey, fetchBootInstructions, saveBootInstructions, fetchLedgerStatus, enableLedger, changeLedgerPassphrase, disableLedger, checkForUpdate, applyUpdate, restartServer } from './api.js';

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
    const [searchOpen, setSearchOpen] = useState(false);
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
                if (s.searchFilter) {
                    const q = s.searchFilter;
                    const match = (n.content && n.content.toLowerCase().includes(q)) ||
                        (n.domain && n.domain.toLowerCase().includes(q)) ||
                        (n.memory_type && n.memory_type.toLowerCase().includes(q)) ||
                        (n.agent && n.agent.toLowerCase().includes(q));
                    if (!match) return false;
                }
                return true;
            });
            const visibleIds = new Set(visibleNodes.map(n => n.id));
            const searchMatch = null;

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

            <div class="nav-pad">
                <button class="nav-btn nav-up" onClick=${() => { stateRef.current.camera.y += 60; }} title="Pan up">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M6 2L1 8h10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-left" onClick=${() => { stateRef.current.camera.x += 60; }} title="Pan left">
                    <svg width="12" height="12" viewBox="0 0 12 12"><path d="M2 6l6-5v10z" fill="currentColor"/></svg>
                </button>
                <button class="nav-btn nav-center" onClick=${() => { stateRef.current.camera.zoom = 1; stateRef.current.camera.x = 0; stateRef.current.camera.y = 0; }} title="Reset view">
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
                    <div class="tooltip-domain" style="color: ${getDomainColor(tooltip.node.domain)}; background: ${getDomainColor(tooltip.node.domain)}20;">${tooltip.node.domain || 'unknown'}</div>
                    <div class="tooltip-content">${tooltip.node.content ? tooltip.node.content.slice(0, 120) : 'No content'}${tooltip.node.content && tooltip.node.content.length > 120 ? '...' : ''}</div>
                    <div class="tooltip-meta">
                        <span class="tooltip-meta-item">${tooltip.node.memory_type || tooltip.node.memoryType || 'memory'}</span>
                        <span class="tooltip-meta-sep"></span>
                        <span class="tooltip-meta-item" style="color: ${confidenceColor(tooltip.node.confidence)};">${(tooltip.node.confidence * 100).toFixed(0)}%</span>
                        <span class="tooltip-meta-sep"></span>
                        <span class="tooltip-meta-item">${timeAgo(tooltip.node.created_at || tooltip.node.createdAt)}</span>
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

function MemoryDetail({ memory, onClose, onDelete, onNavigate }) {
    const [confirming, setConfirming] = useState(false);
    const [agentInfo, setAgentInfo] = useState(null);

    const agentId = memory?.agent || memory?.submitting_agent;
    useEffect(() => {
        if (!agentId) return;
        fetchAgents().then(data => {
            const agents = data.agents || [];
            const match = agents.find(a => a.agent_id === agentId);
            if (match) setAgentInfo(match);
        }).catch(() => {});
    }, [agentId]);

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
                            <span class="value" style="font-size: 11px; word-break: break-all;">${memory.agent || memory.submitting_agent || 'unknown'}</span>
                        `}
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
    const [agentFilter, setAgentFilter] = useState('');
    const [agents, setAgents] = useState([]);
    const [domainFilter, setDomainFilter] = useState('');
    const [domains, setDomains] = useState([]);

    useEffect(() => {
        loadMemories();
        fetchAgents().then(data => setAgents(data.agents || [])).catch(() => {});
        fetchStats().then(data => { if (data.by_domain) setDomains(Object.keys(data.by_domain).sort()); }).catch(() => {});
    }, []);

    async function loadMemories(search, agent, domain) {
        setLoading(true);
        try {
            const params = { limit: 100, sort: 'newest' };
            if (agent) params.agent = agent;
            if (domain) params.domain = domain;
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
        loadMemories(v, agentFilter, domainFilter);
    }

    function handleAgentFilter(e) {
        const v = e.target.value;
        setAgentFilter(v);
        loadMemories(query, v, domainFilter);
    }

    function handleDomainFilter(e) {
        const v = e.target.value;
        setDomainFilter(v);
        loadMemories(query, agentFilter, v);
    }

    return html`
        <div class="search-page">
            <input class="search-page-input" type="text" placeholder="Search memories..."
                   value=${query} onInput=${handleSearch} />
            <div class="search-filters">
                <${HelpTip} text="Search across all committed memories by content, domain, or tags. Results are ranked by relevance." />
                <select class="filter-select" value=${domainFilter} onChange=${handleDomainFilter}>
                    <option value="">All domains</option>
                    ${domains.map(d => html`<option value=${d}>${d}</option>`)}
                </select>
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
                    <button class="btn" onClick=${() => { setRecoveryKey(null); setView('status'); }}>
                        I've saved it
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
                Synaptic Ledger <${HelpTip} text="Encrypts all memories at rest with AES-256-GCM. You'll need to enter your passphrase each session. If you lose it, your data cannot be recovered." />
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
    const [downloading, setDownloading] = useState(false);
    const [progress, setProgress] = useState('');
    const [error, setError] = useState(null);
    const [installed, setInstalled] = useState(false);
    const [restarting, setRestarting] = useState(false);

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

    const doUpdate = async () => {
        if (!updateInfo?.download_url) return;
        setDownloading(true);
        setProgress('Downloading...');
        setError(null);
        try {
            const res = await applyUpdate(updateInfo.download_url);
            if (res.ok) {
                setInstalled(true);
                setProgress('Update installed!');
            } else {
                setError(res.error || 'Update failed');
                setProgress('');
            }
        } catch (e) {
            setError('Update failed: ' + e.message);
            setProgress('');
        }
        setDownloading(false);
    };

    const doRestart = async () => {
        setRestarting(true);
        try {
            await restartServer();
            // Server will restart — wait then reload
            setTimeout(() => {
                const poll = setInterval(() => {
                    fetch('/health').then(r => {
                        if (r.ok) { clearInterval(poll); window.location.reload(); }
                    }).catch(() => {});
                }, 1000);
            }, 2000);
        } catch (e) {
            // Expected — server is restarting
            setTimeout(() => {
                const poll = setInterval(() => {
                    fetch('/health').then(r => {
                        if (r.ok) { clearInterval(poll); window.location.reload(); }
                    }).catch(() => {});
                }, 1000);
            }, 2000);
        }
    };

    const formatSize = (bytes) => {
        if (!bytes) return '';
        if (bytes < 1048576) return (bytes / 1024).toFixed(0) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
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

            ${!updateInfo?.update_available && updateInfo?.latest_version && !error && html`
                <div class="update-current">You're up to date.</div>
            `}

            ${updateInfo?.update_available && !installed && html`
                <div class="update-available">
                    <div class="update-release-name">${updateInfo.release_name || 'New version available'}</div>
                    ${updateInfo.download_size ? html`<span class="update-size">${formatSize(updateInfo.download_size)}</span>` : null}
                    ${updateInfo.release_url && html`
                        <a class="update-notes-link" href="${updateInfo.release_url}" target="_blank" rel="noopener">Release notes →</a>
                    `}
                </div>
            `}

            ${progress && html`<div class="update-progress">${progress}</div>`}

            <div class="update-actions">
                ${!installed && !downloading && html`
                    <button class="btn btn-secondary" onClick=${doCheck} disabled=${checking}>
                        ${checking ? 'Checking...' : 'Check for Updates'}
                    </button>
                `}

                ${updateInfo?.update_available && !installed && !downloading && html`
                    <button class="btn btn-primary" onClick=${doUpdate}>
                        Update Now
                    </button>
                `}

                ${downloading && html`
                    <button class="btn btn-primary" disabled>
                        <span class="spinner"></span> Downloading...
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

    const handleCleanup = async () => {
        if (!confirm('This will permanently deprecate stale memories. Continue?')) return;
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
                        onChange=${(e) => updateField('enabled', e.target.checked)} />
                    <span class="toggle-slider"></span>
                </label>
            </div>

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
    const uptime = health?.uptime || '--';

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
                    </button>
                `)}
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
                            <div class="settings-row"><span class="label">${statusDot(encrypted)} Synaptic Ledger</span><span class="value" style="color: ${encrypted ? '#10b981' : '#6b7280'}">${encrypted ? 'AES-256-GCM' : 'Off'}</span></div>
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

                    <div class="settings-section" style="margin-top:16px">
                        <h3>Export & Data</h3>
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
                </div>
            `}

            ${settingsTab === 'config' && html`
                <div class="settings-tab-content">
                    ${html`<${BootInstructions} />`}

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

    function formatPeriod(period) {
        try {
            const d = new Date(period);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        } catch (e) {
            return period;
        }
    }

    return html`
        <div class="timeline-bar">
            <span class="timeline-label">24h</span>
            <div class="timeline-track">
                ${buckets.map((b, i) => {
                    const pct = (b.count / maxCount) * 100;
                    return html`
                        <div class="timeline-bucket-bar"
                             style="left: ${(i / Math.max(1, buckets.length)) * 100}%;
                                    width: ${100 / Math.max(1, buckets.length)}%;
                                    height: ${Math.max(pct, 4)}%;">
                            <div class="timeline-tooltip">
                                <span class="timeline-tooltip-count">${b.count}</span> memor${b.count === 1 ? 'y' : 'ies'}
                                <br/>
                                <span class="timeline-tooltip-time">${formatPeriod(b.period)}</span>
                            </div>
                        </div>
                    `;
                })}
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
                <span class="health-num">${totalMem}</span> memories <${HelpTip} text="Total committed memories across all domains and agents." />
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
// Help Overlay
// ============================================================================

function HelpOverlay({ onClose }) {
    const [dontShow, setDontShow] = useState(false);
    const [openSection, setOpenSection] = useState(null);

    function handleDismiss() {
        if (dontShow) {
            try { localStorage.setItem('sage-help-dismissed', '1'); } catch (e) {}
        }
        onClose();
    }

    const toggleSection = (key) => setOpenSection(openSection === key ? null : key);

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
                        <div class="guide-detail-desc">Scroll to zoom in/out. Click and drag to pan. Use the navigation pad in the corner for precise movement. Click any bubble to open its detail panel with full content, metadata, and actions.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Domain filter</div>
                        <div class="guide-detail-desc">Click the colored domain pills at the top to filter. Only bubbles from selected domains will appear. Click again to remove the filter.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Timeline bar</div>
                        <div class="guide-detail-desc">The bar at the bottom shows memory activity over the last 24 hours. Hover over segments to see how many memories were created in each time window.</div>
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
                <p>The Network page manages all agents participating in your SAGE consensus chain. Each agent is a separate node (a different machine, assistant, or identity) that shares the same memory network.</p>
                <div class="guide-detail-grid">
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Agent list</div>
                        <div class="guide-detail-desc">Each agent appears as a row showing its name, role badge, status indicator (green = active, yellow = pending, red = offline), memory count, and clearance level. Click any row to expand its detail view.</div>
                    </div>
                    <div class="guide-detail-item">
                        <div class="guide-detail-label">Adding an agent</div>
                        <div class="guide-detail-desc">Click the "+" card to launch the Add Agent wizard. You'll set the agent's identity (name, avatar, role), permissions (clearance, domain access), and choose a connection method — either download a configuration bundle or use LAN pairing. The LAN pairing code (e.g. SAG-X7K) is valid for 15 minutes and single-use. On the new machine, run <code>sage-lite pair CODE</code> or enter it in the setup wizard. The new agent auto-fetches its keys and config over the local network — no manual file copying needed.</div>
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
                        <div class="guide-detail-desc">You can change your passphrase in Settings. All memories are re-encrypted with the new key. Make sure you remember it — there is no recovery mechanism if the passphrase is lost.</div>
                    </div>
                </div>
                <div class="guide-callout" style="border-color: var(--warning, #f59e0b);">
                    <strong>Important:</strong> If you lose your passphrase, your encrypted memories cannot be recovered. There is no backdoor. Write it down and store it safely.
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
                        <div class="guide-detail-desc">View connected peers on your SAGE network. Each peer is another node running sage-lite that participates in consensus. Peers sync memories and validate each other's submissions through BFT.</div>
                    </div>
                </div>
            `,
        },
    ];

    return html`
        <div class="help-overlay" onClick=${(e) => { if (e.target === e.currentTarget) handleDismiss(); }}>
            <div class="help-modal guide-modal">
                <div class="help-modal-header">
                    <h2>CEREBRUM Guide</h2>
                    <button class="detail-close" onClick=${handleDismiss}>x</button>
                </div>
                <div class="guide-intro">Welcome to CEREBRUM — the visual dashboard for your SAGE institutional memory. This guide covers everything you need to know.</div>
                <div class="guide-sections">
                    ${sections.map(s => html`
                        <div class="guide-section ${openSection === s.key ? 'open' : ''}" key=${s.key}>
                            <div class="guide-section-header" onClick=${() => toggleSection(s.key)} role="button" tabIndex="0"
                                onKeyDown=${e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleSection(s.key); } }}>
                                <div class="guide-section-icon">${s.icon}</div>
                                <div class="guide-section-text">
                                    <div class="guide-section-title">${s.title}</div>
                                    <div class="guide-section-summary">${s.summary}</div>
                                </div>
                                <svg class="guide-section-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                            </div>
                            <div class="guide-section-body">
                                ${openSection === s.key && html`<div class="guide-section-content">${s.content}</div>`}
                            </div>
                        </div>
                    `)}
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

    return html`
        <div class="deploy-progress">
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
function DomainAccessMatrix({ domains, domainAccess, onChange, disabled }) {
    const [filter, setFilter] = useState('');
    const filtered = domains.filter(d => !filter || d.toLowerCase().includes(filter.toLowerCase()));

    const toggle = (domain, field) => {
        const cur = domainAccess[domain] || { read: false, write: false };
        const upd = { ...cur, [field]: !cur[field] };
        if (field === 'write' && upd.write) upd.read = true;
        if (field === 'read' && !upd.read) upd.write = false;
        onChange({ ...domainAccess, [domain]: upd });
    };
    const bulkSet = (field, val) => {
        const upd = { ...domainAccess };
        domains.forEach(d => {
            upd[d] = { ...(upd[d] || { read: false, write: false }), [field]: val };
            if (field === 'write' && val) upd[d].read = true;
            if (field === 'read' && !val) upd[d].write = false;
        });
        onChange(upd);
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
            <div class="domain-matrix-columns"><span>Domain</span><span style="text-align:center;">Read</span><span style="text-align:center;">Write</span></div>
            <div class="domain-matrix-body">
                ${filtered.length === 0 && domains.length > 0 ? html`<div class="domain-matrix-no-results">No domains matching "${filter}"</div>` : ''}
                ${domains.length === 0 ? html`<div class="domain-matrix-empty">No domains found. Domains appear as memories are submitted.</div>` : ''}
                ${filtered.map(domain => {
                    const a = domainAccess[domain] || { read: false, write: false };
                    return html`<div class="domain-matrix-row" key=${domain}>
                        <div class="domain-matrix-domain">
                            <span class="domain-matrix-dot" style="background:${getDomainColor(domain)};"></span>
                            ${domain}
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
function NetworkPage() {
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
    // Edit mode state
    const [editing, setEditing] = useState(false);
    const [editName, setEditName] = useState('');
    const [editBio, setEditBio] = useState('');
    // Key rotation state
    const [showRotateConfirm, setShowRotateConfirm] = useState(null);
    const [rotating, setRotating] = useState(false);

    const loadAgents = useCallback(async () => {
        try {
            const data = await fetchAgents();
            setAgents(data.agents || []);
        } catch (e) { console.error(e); }
        finally { setLoading(false); }
    }, []);

    useEffect(() => {
        loadAgents();
        fetchStats().then(data => {
            if (data?.by_domain) setAllDomains(Object.keys(data.by_domain).sort());
        }).catch(() => {});
    }, []);

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
        await updateAgent(agentId, { role: editRole, clearance: editClearance, domain_access: JSON.stringify(arr) });
        loadAgents();
        setAccessDirty(false);
        setAccessSaved(true);
        setTimeout(() => setAccessSaved(false), 2000);
    }, [editRole, editClearance, editDomainAccess, loadAgents]);

    const handleOverviewSave = useCallback(async (agentId) => {
        await updateAgent(agentId, { name: editName, boot_bio: editBio });
        loadAgents();
        setEditing(false);
    }, [editName, editBio, loadAgents]);

    const handleRemove = useCallback(async (agent) => {
        try {
            const res = await removeAgent(agent.agent_id, true);
            if (res.error) { alert(res.error); return; }
            const rdRes = await startRedeploy('remove_agent', agent.agent_id);
            if (rdRes.error) alert('Agent removed but redeployment failed: ' + rdRes.error);
            else { setRedeployStatus(rdRes); startRedeployPoll(); }
            setShowRemoveConfirm(null); setExpandedId(null); loadAgents();
        } catch (e) { alert('Failed to remove agent'); }
    }, [loadAgents, startRedeployPoll]);

    const handleRotateKey = useCallback(async (agent) => {
        setRotating(true);
        try {
            const res = await rotateAgentKey(agent.agent_id);
            if (res.error) { alert(res.error); setRotating(false); return; }
            const rdRes = await startRedeploy('rotate_key', res.new_agent_id);
            if (rdRes.error) alert('Key rotated but redeployment failed: ' + rdRes.error);
            else { setRedeployStatus(rdRes); startRedeployPoll(); }
            setShowRotateConfirm(null); setExpandedId(null); loadAgents();
        } catch (e) { alert('Failed to rotate key'); }
        setRotating(false);
    }, [loadAgents, startRedeployPoll]);

    if (loading) return html`<div class="network-page"><p style="color:var(--text-muted);text-align:center;padding:40px;">Loading agents...</p></div>`;

    const isRedeploying = redeployStatus?.status && !['idle','completed','failed'].includes(redeployStatus.status);

    return html`
        <div class="network-page fade-in">
            ${isRedeploying && html`<div class="redeploy-banner"><span class="deploy-spinner"></span> Network reconfiguration in progress... Phase: ${(DEPLOY_PHASES.find(p => p.key === redeployStatus.current_phase) || {}).label || redeployStatus.current_phase}</div>`}
            <div class="network-header">
                <div><h2>Network <${HelpTip} text="Manage agents on your SAGE chain. Each agent is a separate node that participates in BFT consensus. Click any agent to expand its details and permissions." /></h2><div class="network-header-sub">${agents.length} agent${agents.length !== 1 ? 's' : ''} on this network</div></div>
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
                                            <div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Agent ID</span>
                                                <span class="agent-info-value mono">${agent.agent_id}</span>
                                            </div>
                                            ${agent.validator_pubkey ? html`<div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Validator Key</span>
                                                <span class="agent-info-value mono">${agent.validator_pubkey}</span>
                                            </div>` : ''}
                                            <div class="agent-info-block" style="grid-column:1/-1;">
                                                <span class="agent-info-label">Bio</span>
                                                ${editing ? html`<textarea class="wizard-textarea" value=${editBio} onInput=${e => setEditBio(e.target.value)} onClick=${e => e.stopPropagation()} />`
                                                    : html`<span class="agent-info-value" style="font-weight:400;color:var(--text-dim);">${agent.boot_bio || 'No bio set'}</span>`}
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
                                                <button class="btn" onClick=${() => downloadBundle(agent.agent_id)}>Download Bundle</button>
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
    const [template, setTemplate] = useState('');

    // Step 2 state
    const [clearance, setClearance] = useState(1);
    const [domainAccess, setDomainAccess] = useState({});
    const [allDomains, setAllDomains] = useState([]);

    // Step 3 state
    const [connectMethod, setConnectMethod] = useState('bundle');

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
                            <label>Role</label>
                            <select class="wizard-select" value=${role} onChange=${e => setRole(e.target.value)}>
                                <option value="admin">Admin</option>
                                <option value="member">Member</option>
                                <option value="observer">Observer</option>
                            </select>
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
                            <label>Boot Bio</label>
                            <textarea class="wizard-textarea" placeholder="System prompt / job scope for this agent" value=${bio} onInput=${e => setBio(e.target.value)} />
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
                            <div class="summary-row"><span class="label">Connect</span><span class="value">${connectMethod === 'bundle' ? 'Bundle Download' : 'LAN Pairing'}</span></div>
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
                                                ${name} is live on the network. ${connectMethod === 'bundle' ? 'Download the bundle to set up the agent.' : 'Use the pairing code on the target machine.'}
                                            </p>
                                        </div>
                                        ${deployStatus && deployStatus.status === 'completed' && html`
                                            <${DeployProgress}
                                                currentPhase=${'COMPLETED'}
                                                status=${'completed'}
                                            />
                                        `}
                                        <div style="margin-top:20px;text-align:center;">
                                            ${connectMethod === 'bundle' && createdAgent && html`
                                                <button class="btn btn-primary" style="padding:12px 28px;font-size:14px;" onClick=${() => {
                                                    downloadBundle(createdAgent.agent_id);
                                                }}>Download Bundle ZIP</button>
                                            `}
                                            ${connectMethod === 'lan' && html`
                                                ${pairingCode ? html`
                                                    <div class="pairing-code-display">
                                                        ${pairingCode}
                                                    </div>
                                                    <p style="font-size:12px;color:var(--text-muted);margin-top:8px;">
                                                        Valid for 15 minutes. Run <code style="background:var(--bg-elevated);padding:2px 6px;border-radius:4px;">sage-lite pair ${pairingCode}</code> on the new machine.
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
// Root App
// ============================================================================

function App() {
    const [authState, setAuthState] = useState('loading'); // loading | login | ready
    const [page, setPage] = useState('brain');
    const [selectedMemory, setSelectedMemory] = useState(null);
    const [sseConnected, setSseConnected] = useState(false);
    const [showHelp, setShowHelp] = useState(false);
    const [tooltipsEnabled, setTooltipsEnabled] = useState(() => {
        try { return localStorage.getItem('sage-tooltips') === '1'; } catch (e) { return false; }
    });
    const sseRef = useRef(null);

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
            else if (hash === '/network') setPage('network');
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
        <div class="sidebar">
            <div class="sidebar-logo">S</div>
            <button class="sidebar-btn ${page === 'brain' ? 'active' : ''}" onClick=${() => navigate('brain')} title="Cerebrum">
                ${icons.brain}
            </button>
            <button class="sidebar-btn ${page === 'search' ? 'active' : ''}" onClick=${() => navigate('search')} title="Search">
                ${icons.search}
            </button>
            <button class="sidebar-btn ${page === 'import' ? 'active' : ''}" onClick=${() => navigate('import')} title="Import">
                ${icons.import}
            </button>
            <button class="sidebar-btn ${page === 'network' ? 'active' : ''}" onClick=${() => navigate('network')} title="Network">
                ${icons.network}
            </button>
            <button class="sidebar-btn ${page === 'settings' ? 'active' : ''}" onClick=${() => navigate('settings')} title="Settings">
                ${icons.settings}
            </button>
            <div style="flex:1;"></div>
            <button class="sidebar-btn" onClick=${() => setShowHelp(true)} title="Help">
                ${icons.help}
            </button>
        </div>
        <div class="main-content">
            <div class="top-bar">
                <h1>CEREBRUM <span style="font-size:12px;font-weight:400;color:var(--text-muted);margin-left:6px">Your SAGE Brain</span></h1>
                <div class="spacer"></div>
                <div class="connection-badge">
                    <div class="connection-dot ${sseConnected ? '' : 'disconnected'}"></div>
                    ${sseConnected ? 'Live' : 'Connecting...'}
                    <${HelpTip} text="Real-time connection to your SAGE node via Server-Sent Events. When live, new memories appear automatically." align="right" />
                </div>
            </div>
            <${HealthBar} />

            ${page === 'brain' && html`
                <${BrainView} sse=${sseRef.current} onSelectMemory=${onSelectMemory} />
                <${TimelineBar} />
            `}
            ${page === 'search' && html`<${SearchPage} onSelectMemory=${onSelectMemory} />`}
            ${page === 'import' && html`<${ImportPage} />`}
            ${page === 'network' && html`<${NetworkPage} />`}
            ${page === 'settings' && html`<${SettingsPage} />`}

            <${MemoryDetail}
                memory=${selectedMemory}
                onClose=${() => setSelectedMemory(null)}
                onDelete=${() => setSelectedMemory(null)}
                onNavigate=${(p) => { setPage(p); window.location.hash = '#/' + p; }}
            />
        </div>
        ${showHelp && html`<${HelpOverlay} onClose=${() => setShowHelp(false)} />`}
    </${TooltipsContext.Provider}>`;
}

// Mount
render(html`<${App} />`, document.getElementById('app'));
