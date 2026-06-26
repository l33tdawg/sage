// mri-brain.js — the 3D "MRI" memory-brain renderer, shared by the standalone
// /ui/mri.html page and the in-dashboard MRI mode (no iframe; dashboard
// X-Frame-Options/CSP correctly forbid embedding, so we render natively).
//
// Three.js + 3d-force-graph load as ES modules via the host page's importmap,
// sharing a SINGLE Three instance (esm.sh ?external=three) — no "multiple
// instances of Three.js" warning and no deprecated UMD global build.
// Call mountMriBrain(container, opts) → returns a cleanup function.
//
// The complementary-learning-systems reading (SAGE_AGI_BRAIN_ANALOGY.md):
//   size+glow = corroboration (consolidation) · fade = confidence (decay)
//   grey = challenged/deprecated (pruning) · colour = domain (lobe)
//   edge colour = sage_link type (the connectome)
// No embeddings or full content cross the wire — content is truncated
// server-side and the graph respects the same RBAC isolation as every read.

import * as THREE from 'three';
import ForceGraph3D from '3d-force-graph';
import { UnrealBloomPass } from 'three/addons/postprocessing/UnrealBloomPass.js';

const LINK_TYPES = {
  supports:    { color: '#5ee2a0', label: 'supports',    typed: true },
  contradicts: { color: '#ff5c6c', label: 'contradicts', typed: true },
  causes:      { color: '#5ab0ff', label: 'causes',      typed: true },
  precedes:    { color: '#ffd166', label: 'precedes',    typed: true },
  refines:     { color: '#c08bff', label: 'refines',     typed: true },
  related:     { color: '#42587a', label: 'related',     typed: true },
  parent:      { color: '#243450', label: 'lineage',     typed: false },
  domain:      { color: '#1b2942', label: 'same domain', typed: false },
};
const PALETTE = ['#ff6b9d','#ffd166','#5ee2a0','#5ab0ff','#c08bff','#ff9f5a','#4dd6c4','#f7748a','#9ad14b','#7aa0ff'];
function hexToRgb(h){ const n = parseInt(h.slice(1), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; }
function fmtN(n){ n = n||0; return n >= 1000 ? (n/1000).toFixed(n >= 10000 ? 0 : 1) + 'k' : '' + n; }

// Minimal OBJ → BufferGeometry (positions + fan-triangulated faces). Lets us
// drop a CC0 brain mesh at /ui/assets/brain.obj with no extra loader library.
function parseOBJ(text) {
  const pos = [], idx = [];
  for (const line of text.split('\n')) {
    if (line[0] === 'v' && line[1] === ' ') {
      const p = line.split(/\s+/); pos.push(+p[1], +p[2], +p[3]);
    } else if (line[0] === 'f' && line[1] === ' ') {
      const f = line.trim().split(/\s+/).slice(1).map(s => parseInt(s, 10) - 1);
      for (let i = 1; i < f.length - 1; i++) idx.push(f[0], f[i], f[i + 1]);
    }
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  if (idx.length) g.setIndex(idx);
  g.computeVertexNormals();
  return g;
}

// Procedural brain-shaped wireframe hull: a densely-subdivided sphere displaced
// into two hemispheres (a sagittal longitudinal fissure) with multi-octave
// gyri/sulci folding, a cerebellum bulge, and brain proportions. License-free
// (generated), and reads convincingly as a brain. Overridden by an anatomical
// /ui/assets/brain.obj if one is present.
function makeBrainGeometry() {
  // detail 6 (~82k tris) — a much finer, more filament-like wireframe than the
  // old detail-5; still a one-time, zero-per-frame cost.
  const g = new THREE.IcosahedronGeometry(1, 6);
  const p = g.attributes.position, v = new THREE.Vector3();
  for (let i = 0; i < p.count; i++) {
    v.fromBufferAttribute(p, i).normalize();
    const x = v.x, y = v.y, z = v.z;
    // Cortical folding — six octaves of gyri/sulci, increasingly fine, so the
    // surface reads as convoluted cortex rather than a lumpy ball.
    let r = 1
      + 0.052 * Math.sin(8 * z + 3 * y)
      + 0.044 * Math.sin(10 * y + 4 * x)
      + 0.040 * Math.sin(12 * x + 6 * z)
      + 0.028 * Math.sin(17 * z) * Math.cos(15 * y)
      + 0.020 * Math.sin(23 * y + 14 * x)
      + 0.014 * Math.sin(29 * x + 19 * z);
    // Deep sagittal fissure splitting the two hemispheres along the midline.
    r -= Math.exp(-(x * x) * 60) * 0.20 * Math.max(0, y);
    // Cerebellum: a tightly-folded bulge tucked under the posterior-inferior.
    const cb = Math.exp(-((z + 0.8) * (z + 0.8) * 5 + (y + 0.5) * (y + 0.5) * 6 + x * x * 3));
    r += cb * (0.035 + 0.045 * Math.abs(Math.sin(38 * z + 22 * x)));
    v.multiplyScalar(r);
    v.x *= 0.86; v.y *= 0.80; v.z *= 1.20;                // brain proportions (long front-back, narrow across)
    if (v.y < -0.3) v.y = -0.3 + (v.y + 0.3) * 0.5;       // flatten the underside
    p.setXYZ(i, v.x, v.y, v.z);
  }
  p.needsUpdate = true; g.computeVertexNormals();
  return g;
}

const STYLE = `
.mrib{position:absolute;inset:0;overflow:hidden;background:radial-gradient(1200px 800px at 70% 18%,#0a1426 0%,#05070d 60%);
  font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:#cfe3ff}
.mrib-graph{position:absolute;inset:0}
.mrib .panel{position:absolute;background:rgba(10,16,28,.78);border:1px solid #15233b;border-radius:12px;backdrop-filter:blur(8px);box-shadow:0 8px 40px #0008;z-index:5}
.mrib .legend{top:16px;right:16px;width:270px;padding:13px 15px;max-height:84%;overflow:auto}
.mrib .legend h4{margin:0 0 4px;font-size:11px;letter-spacing:1.5px;color:#39d0ff;text-transform:uppercase}
.mrib .legend .cls{color:#5d7395;font-size:11px;margin:0 0 11px;border-bottom:1px solid #15233b;padding-bottom:9px}
.mrib .legend .row{display:flex;align-items:center;gap:9px;margin:6px 0}
.mrib .legend .row .k{width:16px;text-align:center}
.mrib .legend .row .t b{color:#dceaff;font-weight:600}
.mrib .legend .row .t span{color:#5d7395}
.mrib .legend .seg{margin:11px 0 3px;color:#9fb6d8;font-size:10px;letter-spacing:1.5px;text-transform:uppercase}
.mrib .dot{width:12px;height:12px;border-radius:50%;display:inline-block}
.mrib .bar{width:16px;height:3px;border-radius:2px;display:inline-block}
.mrib .hud{bottom:16px;left:16px;padding:10px 14px;display:flex;gap:16px;align-items:center}
.mrib .hud .n{color:#eaf4ff;font-size:17px;font-weight:700}
.mrib .hud .l{color:#5d7395;font-size:10px;letter-spacing:1px;text-transform:uppercase}
.mrib .hud .btn{cursor:pointer;color:#39d0ff;border:1px solid #15233b;border-radius:8px;padding:6px 11px;user-select:none}
.mrib .hud .btn:hover{background:#0e1b30}
.mrib .hud .sld{display:flex;align-items:center;gap:7px;color:#5d7395;font-size:10px;letter-spacing:1px;text-transform:uppercase}
.mrib .hud .sld input{width:84px;accent-color:#39d0ff;cursor:pointer}
.mrib .scan{position:absolute;top:16px;left:16px;padding:10px 14px}
.mrib .scan b{color:#eaf4ff;font-size:14px;letter-spacing:.5px}
.mrib .scan .s{color:#39d0ff;font-size:11px;letter-spacing:2px;margin-top:4px}
.mrib .tip{position:absolute;pointer-events:none;display:none;max-width:280px;padding:8px 11px;background:rgba(6,11,20,.96);border:1px solid #15233b;border-radius:9px;z-index:9;font-size:12px}
.mrib .tip .h{color:#eaf4ff;font-weight:700;margin-bottom:2px}
.mrib .tip .m{color:#5d7395;font-size:11px}
.mrib .tip .chip{font-size:10px;padding:1px 6px;border-radius:6px;background:#0e1b30;color:#aecbf0;margin-right:4px}
.mrib .flag{position:absolute;bottom:16px;right:16px;color:#3a4a66;font-size:10px;letter-spacing:1px}
.mrib .boot{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#5d7395;letter-spacing:2px;font-size:12px}
`;

function injectStyleOnce() {
  if (document.getElementById('mrib-style')) return;
  const s = document.createElement('style');
  s.id = 'mrib-style';
  s.textContent = STYLE;
  document.head.appendChild(s);
}

async function loadGraph(fetchUrl) {
  try {
    const r = await fetch(fetchUrl, { credentials: 'same-origin' });
    if (!r.ok) throw new Error('HTTP '+r.status);
    const g = await r.json();
    // Empty (no memories yet) is a valid state — render a blank brain, never
    // synthetic/placeholder data. Guard every field against a null body.
    const srcNodes = (g && Array.isArray(g.nodes)) ? g.nodes : [];
    const srcEdges = (g && Array.isArray(g.edges)) ? g.edges : [];
    return { live: true,
      nodes: srcNodes.map(n=>({ id:n.id, domain:n.domain||'unknown', label:n.content||n.id,
        status:n.status||'committed', corroboration_count:n.corroboration_count||0,
        confidence: typeof n.confidence==='number'?n.confidence:0.5, memory_type:n.memory_type||'' })),
      links: srcEdges.map(e=>({ source:e.source, target:e.target, link_type:e.type||'related' })),
      total: (g && g.total) || 0, domainCounts: (g && g.domain_counts) || null };
  } catch (err) {
    // No live node / fetch error: render the empty brain, never synthetic data.
    console.warn('[mri] live graph unavailable:', err.message);
    return { live: false, nodes: [], links: [], total: 0, domainCounts: null };
  }
}

export function mountMriBrain(container, opts = {}) {
  injectStyleOnce();
  const fetchUrl = opts.fetchUrl || '/v1/dashboard/memory/graph?status=all&limit=500';
  const showScan = opts.showScan !== false;

  const root = document.createElement('div');
  root.className = 'mrib';
  root.innerHTML = `
    <div class="mrib-graph"></div>
    <div class="boot">◉ ACQUIRING HIPPOCAMPAL FIELD…</div>
    ${showScan ? '<div class="panel scan"><b>CEREBRUM · MRI</b><div class="s">◉ SCANNING</div></div>' : ''}
    <div class="panel legend">
      <h4>The reading</h4>
      <div class="cls">A complementary-learning-systems view: SAGE is the <b>hippocampus</b>
        (episodic capture); corroboration + decay is the <b>sleep/consolidation</b> cycle.</div>
      <div class="seg">Nodes — memories</div>
      <div class="row"><span class="k">◍</span><div class="t"><b>Size + glow = corroboration</b><br><span>consolidation toward cortex</span></div></div>
      <div class="row"><span class="k">◌</span><div class="t"><b>Fade = confidence decay</b><br><span>the forgetting curve</span></div></div>
      <div class="row"><span class="k">⊘</span><div class="t"><b>Greyed = challenged / pruned</b><br><span>synaptic pruning</span></div></div>
      <div class="seg">Position</div>
      <div class="row"><span class="k">⊙</span><div class="t"><b>Depth = consolidation</b><br><span>centre = hippocampus (fresh) → surface = cortex</span></div></div>
      <div class="seg">Lobes — domains</div><div class="lobes"></div>
      <div class="seg">Connectome — typed links</div><div class="linktypes"></div>
    </div>
    <div class="panel hud">
      <div><div class="n nn">0</div><div class="l">memories</div></div>
      <div><div class="n ne">0</div><div class="l">synapses</div></div>
      <div><div class="n nc">0</div><div class="l">consolidated</div></div>
      <div class="btn b-rot">⏸ pause</div>
      <div class="btn b-flow">⚡ flow: on</div>
      <label class="sld">skull <input class="b-op" type="range" min="0" max="60" value="8"></label>
    </div>
    <div class="tip"></div>
    <div class="flag"></div>`;
  container.appendChild(root);
  const $ = s => root.querySelector(s);

  const domainColors = {}; let seq = 0;
  const domainColor = k => { if(!k) k='unknown'; if(!domainColors[k]){ domainColors[k]=PALETTE[seq%PALETTE.length]; seq++; } return domainColors[k]; };

  // Instanced node styling — one draw call for ALL dots (scales to thousands),
  // vs the old mesh+sprite per node. size = corroboration+confidence; colour =
  // domain brightened toward white by corroboration (so the bloom pass makes
  // consolidated memories glow); alpha = confidence (decay); challenged/
  // deprecated greyed.
  const nodeVal = n => 1.4 + (n.corroboration_count||0)*1.1 + (n.confidence||0)*0.8;
  function nodeColorRGBA(n){
    if(n.status==='deprecated') return 'rgba(108,120,145,0.30)';
    if(n.status==='challenged') return 'rgba(150,162,185,0.55)';
    const [r,g,b]=hexToRgb(domainColor(n.domain));
    const boost=Math.min(1,(n.corroboration_count||0)/8);
    const br=r+(255-r)*boost*0.5, bg=g+(255-g)*boost*0.5, bb=b+(255-b)*boost*0.5;
    return `rgba(${br|0},${bg|0},${bb|0},${(0.6+(n.confidence||0)*0.4).toFixed(2)})`;
  }

  // Deterministic anatomical placement — NO force simulation. domain -> azimuthal
  // lobe, consolidation -> radial depth (hippocampus centre -> cortex surface),
  // inside a brain-shaped ellipsoid. Positions are pinned (fx/fy/fz), so there is
  // zero per-tick cost no matter how many nodes; the layout is a pure formula and
  // is stable across reloads (a node always lands in the same place).
  const EX=205, EY=140, EZ=240;
  const hsh=(s,seed)=>{ s=s||''; let h=(seed>>>0)||1; for(let i=0;i<s.length;i++) h=Math.imul(h^s.charCodeAt(i),16777619); return ((h>>>0)%10000)/10000; };
  function placeNodes(nodes){
    const ds=[...new Set(nodes.map(n=>n.domain))], nd=Math.max(1,ds.length), di={};
    ds.forEach((k,i)=>{ di[k]=i; domainColor(k); });
    nodes.forEach(n=>{
      const az=((di[n.domain]||0)/nd)*Math.PI*2 + (hsh(n.id,1)-0.5)*(Math.PI*2/nd)*0.82;
      const el=(hsh(n.id,2)-0.5)*Math.PI*0.92;
      // radius FILLS the lobe volume (cube-root → uniform density, not a hollow
      // shell), with corroboration pushing a memory outward toward the cortex.
      const cons=Math.min(1,(n.corroboration_count||0)/8);
      const depth=Math.min(0.97, 0.30 + Math.cbrt(hsh(n.id,3))*0.55 + cons*0.15);
      const ce=Math.cos(el);
      n.fx=n.x=EX*depth*ce*Math.cos(az);
      n.fy=n.y=EY*depth*Math.sin(el);
      n.fz=n.z=EZ*depth*ce*Math.sin(az);
    });
  }

  let Graph = null, controls = null, disposed = false, flow = true, scanning = true;
  let hullMat = null, brainMat = null, surfMat = null, curOpacity = 0.08;
  let currentDomain = null;                 // drill-down lobe (null = overview)
  const baseUrl = fetchUrl;
  const urlFor = () => baseUrl + (currentDomain ? '&domain=' + encodeURIComponent(currentDomain) : '');
  const subs = [];

  function setHullOpacity(o){
    curOpacity = o;
    if (brainMat) { brainMat.opacity = o; if (surfMat) surfMat.opacity = o * 0.5; if (hullMat) hullMat.opacity = 0; }
    else if (hullMat) { hullMat.opacity = o; }
  }

  function refreshCounts(d){
    // .nn shows the TRUE total (operator view), not just the rendered sample.
    $('.nn').textContent = fmtN(d.total && d.total > d.nodes.length ? d.total : d.nodes.length);
    $('.ne').textContent = fmtN(d.links.length);
    $('.nc').textContent = fmtN(d.nodes.filter(n=>(n.corroboration_count||0)>=4 && n.status==='committed').length);
    const dom = currentDomain && d.domainCounts && d.domainCounts[currentDomain];
    if (currentDomain) {
      $('.flag').textContent = `${currentDomain} · showing ${d.nodes.length}${dom?` of ${fmtN(dom)}`:''}`;
    } else if (d.total && d.total > d.nodes.length) {
      $('.flag').textContent = `showing ${d.nodes.length} of ${fmtN(d.total)} · representative sample`;
    } else {
      $('.flag').textContent = d.live === false ? 'no live data' : (d.nodes.length ? '' : 'no memories yet');
    }
  }

  // Lobe legend with per-domain counts; click a lobe to drill into it, "← all
  // lobes" to return. Built from the true domain set so every lobe stays
  // navigable even while only a sample is shown.
  function buildLobes(d){
    const dc = d.domainCounts || {};
    const doms = (Object.keys(dc).length ? Object.keys(dc) : [...new Set(d.nodes.map(n=>n.domain))]).sort();
    const lobes = $('.lobes'); lobes.innerHTML = '';
    if (currentDomain) {
      const back = document.createElement('div');
      back.className = 'row'; back.style.cursor = 'pointer';
      back.innerHTML = '<span class="k">←</span><div class="t"><b>all lobes</b></div>';
      back.onclick = () => { currentDomain = null; load(); };
      lobes.appendChild(back);
    }
    doms.forEach(k => {
      const row = document.createElement('div');
      row.className = 'row'; row.style.cursor = 'pointer';
      if (currentDomain === k) row.style.background = 'rgba(57,208,255,0.10)';
      row.innerHTML = `<span class="dot" style="background:${domainColor(k)}"></span><div class="t"><b>${k}</b>${dc[k]?` <span style="color:#5d7395">· ${fmtN(dc[k])}</span>`:''}</div>`;
      row.onclick = () => { if (currentDomain !== k) { currentDomain = k; load(); } };
      lobes.appendChild(row);
    });
  }

  // Re-fetch (respecting the drill domain) and re-render. Deterministic placement
  // keeps existing nodes put; no re-heat.
  function load(){
    loadGraph(urlFor()).then(d => {
      if (disposed || !Graph) return;
      placeNodes(d.nodes);
      Graph.graphData(d);
      refreshCounts(d);
      buildLobes(d);
    });
  }

  loadGraph(urlFor()).then(data => {
    if (disposed) return;
    $('.boot').style.display = 'none';
    placeNodes(data.nodes);
    Graph = ForceGraph3D({ controlType:'orbit' })($('.mrib-graph'))
      .backgroundColor('#05070d00')
      .graphData(data).nodeId('id').nodeLabel(()=>'' )
      .nodeVal(nodeVal).nodeColor(nodeColorRGBA).nodeRelSize(2.4).nodeResolution(10).nodeOpacity(0.9)
      .linkColor(l=>(LINK_TYPES[l.link_type]||LINK_TYPES.related).color)
      .linkWidth(l=> l.link_type==='contradicts'?0.6 : (LINK_TYPES[l.link_type]||{}).typed?0.35:0.18)
      .linkOpacity(0.3)
      .linkDirectionalParticles(l=> flow&&(LINK_TYPES[l.link_type]||{}).typed?2:0)
      .linkDirectionalParticleWidth(1.1).linkDirectionalParticleSpeed(0.006)
      .warmupTicks(1).cooldownTicks(6)
      .onNodeHover(showTip)
      .onNodeClick(n=>{ const r=Math.hypot(n.x,n.y,n.z)||1, d=40; Graph.cameraPosition({x:n.x*(1+d/r),y:n.y*(1+d/r),z:n.z*(1+d/r)},n,900); });

    // Positions are pinned by placeNodes() (fx/fy/fz), so disable the force
    // simulation entirely — zero per-tick cost regardless of node count.
    ['charge','link','center','lobe'].forEach(f=>{ try{ Graph.d3Force(f, null); }catch(e){ /* noop */ } });

    // Consolidation glow via a single bloom pass — scales to ANY node count (far
    // cheaper than the old halo-sprite-per-node). Bright (corroborated, white-
    // shifted) nodes bloom; the faint brain wireframe barely does.
    // ONLY enable the bloom composer if the GPU can hold the extra post-processing render
    // targets. On a small MAX_RENDERBUFFER_SIZE (observed 2048) at hi-DPI, the composer target
    // (logical × devicePixelRatio, e.g. 1462×2≈2924) exceeds the ceiling → renderbufferStorage
    // fails (GL_INVALID_VALUE) → COLOR_ATTACHMENT0 "no width or height" → the WHOLE scene is
    // black. In that case we never touch postProcessingComposer() at all, so ForceGraph3D renders
    // straight to the (pixel-ratio-clamped, always-safe) canvas — a glow-less brain beats a black
    // one. Capable GPUs (maxRB 8192+) keep the glow exactly as before.
    let bloom = null;
    try {
      const _r = Graph.renderer(), _gl = _r && _r.getContext && _r.getContext();
      const _maxRB = (_gl && _gl.getParameter(_gl.MAX_RENDERBUFFER_SIZE)) || 8192;
      const _rW = root.clientWidth||1280, _rH = root.clientHeight||720;
      if ((window.devicePixelRatio||1) * Math.max(_rW, _rH) <= _maxRB) {
        bloom = new UnrealBloomPass(new THREE.Vector2(_rW, _rH), 0.55, 0.5, 0.32);
        Graph.postProcessingComposer().addPass(bloom);
      } else {
        console.warn('[mri] bloom disabled: MAX_RENDERBUFFER_SIZE', _maxRB, 'too small for',
          (window.devicePixelRatio||1)+'× DPR — rendering without glow (avoids a black canvas)');
      }
    } catch(e){ console.warn('[mri] bloom unavailable', e); }

    // --- WebGL surface-sizing fix --------------------------------------------
    // ForceGraph3D defaults its renderer + post-processing composer to the FULL
    // window × devicePixelRatio. On hi-DPI / large viewports that product blows
    // past the GPU's MAX_RENDERBUFFER_SIZE (the bloom pass's multisampled targets
    // ~double it), so the framebuffer is created incomplete (COLOR_ATTACHMENT0
    // has no width/height) and nothing draws — a black canvas that only recovers
    // when a `window` resize fires (e.g. opening DevTools shrinks the viewport).
    // FG3D also only listens to WINDOW resize, never the container, so a 0-sized
    // container at first paint never self-corrects. Fix: size to the CONTAINER,
    // clamp the pixel ratio + clamp to the GPU max, and observe the container so
    // it's valid on first paint and on layout changes — not just window resize.
    const gel = $('.mrib-graph');
    function fitGraph(){
      if (disposed || !Graph || !gel) return;
      const W = gel.clientWidth, H = gel.clientHeight;
      if (!W || !H) return; // container not laid out yet — ResizeObserver/rAF/timers will retry
      try {
        const renderer = Graph.renderer();
        const gl = renderer && renderer.getContext && renderer.getContext();
        const maxRB = (gl && gl.getParameter(gl.MAX_RENDERBUFFER_SIZE)) || 8192;
        let pr = Math.min(window.devicePixelRatio || 1, 1.5);
        pr = Math.min(pr, (maxRB / 2) / Math.max(W, H)); // stay under the GPU renderbuffer ceiling
        pr = Math.max(0.5, pr);
        if (renderer && renderer.setPixelRatio) renderer.setPixelRatio(pr);
        Graph.width(W).height(H); // FG3D renderer size
        // THE FIX: ForceGraph3D does NOT resize the EffectComposer or its passes,
        // so the bloom render targets stay at their (often 0x0) first-paint size →
        // COLOR_ATTACHMENT0 "no width or height" → incomplete framebuffer → black.
        // Resize the composer AND the bloom pass explicitly.
        // Only when bloom is active. Calling postProcessingComposer() LAZILY CREATES the
        // composer, so we must not touch it when post-processing was deliberately disabled for a
        // low-MAX_RENDERBUFFER_SIZE GPU — that would resurrect the oversized composer and re-black
        // the canvas. No bloom → FG3D renders straight to the clamped renderer.
        if (bloom) {
          const comp = Graph.postProcessingComposer && Graph.postProcessingComposer();
          if (comp) { if (comp.setPixelRatio) comp.setPixelRatio(pr); if (comp.setSize) comp.setSize(W, H); }
          if (bloom.setSize) bloom.setSize(W, H);
        }
      } catch(e){ /* noop */ }
    }
    fitGraph();
    requestAnimationFrame(fitGraph);
    [120, 400, 1000].forEach(t => setTimeout(fitGraph, t)); // catch late iframe/panel layout
    if (typeof ResizeObserver === 'function' && gel){
      const ro = new ResizeObserver(() => fitGraph());
      ro.observe(gel);
      subs.push(() => ro.disconnect());
    }

    try { const sc=Graph.scene();
      // Procedural brain-shaped wireframe hull (default — no external asset).
      // Additive blending makes overlapping wireframe lines accumulate into a
      // glow (amplified by the bloom pass), so the dense fold structure reads as
      // a luminous neural tangle rather than a flat mesh.
      const hull=new THREE.Mesh(makeBrainGeometry(),
        new THREE.MeshBasicMaterial({color:0x4aa3ff,wireframe:true,transparent:true,opacity:curOpacity,depthWrite:false,blending:THREE.AdditiveBlending}));
      hull.scale.setScalar(185); sc.add(hull); hullMat=hull.material;
      // Optional real anatomical mesh override at /ui/assets/brain.obj. No mesh
      // ships with SAGE, so this normally 404s and we keep the procedural hull.
      // NOTE: the static server SPA-falls-back to index.html (HTTP 200) for a
      // missing asset, so r.ok is NOT enough — parseOBJ would yield empty
      // geometry and we'd hide the procedural hull, leaving no brain at all.
      // Guard on a real vertex+face count before swapping.
      fetch('/ui/assets/brain.obj').then(r=>{ if(!r.ok) throw 0; return r.text(); }).then(txt=>{
        if(disposed||!Graph) return;
        const g=parseOBJ(txt); g.center(); g.computeBoundingSphere();
        const pos=g.getAttribute('position');
        if(!pos || pos.count<3 || !g.index || !g.index.count){ g.dispose(); return; } // not a real mesh (e.g. SPA 200 fallback) — keep procedural
        const s=255/((g.boundingSphere&&g.boundingSphere.radius)||1); // enclose the node cloud
        brainMat=new THREE.MeshBasicMaterial({color:0x6cc0ff,wireframe:true,transparent:true,opacity:curOpacity,depthWrite:false,blending:THREE.AdditiveBlending}); // additive → the dense anatomical wireframe glows under the bloom pass
        const wf=new THREE.Mesh(g,brainMat); wf.scale.setScalar(s); sc.add(wf);
        surfMat=new THREE.MeshBasicMaterial({color:0x14304e,transparent:true,opacity:curOpacity*0.5,side:THREE.BackSide,depthWrite:false});
        const surf=new THREE.Mesh(g,surfMat); surf.scale.setScalar(s); sc.add(surf);
        setHullOpacity(curOpacity); // hide the procedural hull now that the real mesh is in
      }).catch(()=>{ /* no override — keep the procedural brain */ });
    } catch(e){ /* hull optional */ }

    buildLobes(data);
    const lt=$('.linktypes'); Object.values(LINK_TYPES).filter(t=>t.typed).forEach(t=>lt.insertAdjacentHTML('beforeend',
      `<div class="row"><span class="bar" style="background:${t.color}"></span><div class="t"><span>${t.label}</span></div></div>`));
    refreshCounts(data);

    // Frame the brain once, then gentle auto-rotate via OrbitControls.
    // autoRotate respects user zoom/pan/drag — unlike setting cameraPosition
    // every frame, which previously clobbered all interaction.
    Graph.cameraPosition({ x: 0, y: 60, z: 620 }); // frame the whole brain + cloud
    controls = Graph.controls();
    if (controls) { controls.autoRotate = scanning; controls.autoRotateSpeed = 0.45; }

    // Centre the brain in the VISIBLE area. The legend panel (right, 270px) and the
    // left nav rail make the canvas-centred scene look shoved to the right + low. A
    // camera view-offset shifts the projection left/up WITHOUT rotating, so the brain
    // sits centred and autoRotate still spins around it (no orbit-centre drift).
    function centerView(){
      if (disposed || !Graph) return;
      try {
        const cam = Graph.camera();
        const W = root.clientWidth || 1280, H = root.clientHeight || 720;
        cam.setViewOffset(W, H, -180, -100, W, H); // shift the brain left + up into the visible area
        cam.updateProjectionMatrix();
      } catch(e){ /* noop */ }
    }
    centerView();
    const onResize = () => centerView();
    window.addEventListener('resize', onResize);
    subs.push(() => window.removeEventListener('resize', onResize));

    // Live population — re-pull on remember/forget. placeNodes() is deterministic,
    // so existing nodes keep their exact spot and new memories land in place; no
    // re-heat, no reshuffle, no per-node position bookkeeping.
    if (opts.sse && typeof opts.sse.on === 'function') {
      let t = null;
      const reload = () => { clearTimeout(t); t = setTimeout(load, 450); };
      subs.push(opts.sse.on('remember', reload));
      subs.push(opts.sse.on('forget', reload));
    }
  });

  function showTip(n){ const tip=$('.tip'); if(!n){ tip.style.display='none'; return; }
    tip.style.display='block';
    tip.innerHTML=`<div class="h">${(n.label||'').slice(0,90)}</div><div class="m">${n.domain} · ${n.memory_type||'—'} · ${n.status}</div>
      <div style="margin-top:5px"><span class="chip">conf ${(+n.confidence).toFixed(2)}</span><span class="chip">corroborated ×${n.corroboration_count||0}</span></div>`; }
  function onMove(e){ const tip=$('.tip'); if(tip.style.display==='block'){ const r=root.getBoundingClientRect();
    tip.style.left=(e.clientX-r.left+14)+'px'; tip.style.top=(e.clientY-r.top+14)+'px'; } }
  root.addEventListener('mousemove', onMove);
  $('.b-rot').onclick=function(){ scanning=!scanning; if(controls) controls.autoRotate=scanning; this.textContent=scanning?'⏸ pause':'▶ scan'; };
  $('.b-flow').onclick=function(){ flow=!flow; if(Graph) Graph.linkDirectionalParticles(l=>flow&&(LINK_TYPES[l.link_type]||{}).typed?2:0); this.textContent=flow?'⚡ flow: on':'⚡ flow: off'; };
  $('.b-op').oninput=function(){ setHullOpacity(this.value/100); };

  return function cleanup(){
    disposed = true;
    subs.forEach(u => { try { u && u(); } catch(e){ /* noop */ } });
    root.removeEventListener('mousemove', onMove);
    try { if (Graph && Graph._destructor) Graph._destructor(); } catch(e){ /* noop */ }
    if (root.parentNode) root.parentNode.removeChild(root);
  };
}
