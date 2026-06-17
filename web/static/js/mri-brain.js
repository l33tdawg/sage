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

// Procedural brain-shaped wireframe hull: a subdivided sphere displaced into
// two hemispheres (a sagittal longitudinal fissure) with gyri/sulci folds and
// brain proportions. License-free (generated), lightweight, and reads as a
// brain — unlike a plain globe. Overridden by /ui/assets/brain.obj if present.
function makeBrainGeometry() {
  const g = new THREE.IcosahedronGeometry(1, 5);
  const p = g.attributes.position, v = new THREE.Vector3();
  for (let i = 0; i < p.count; i++) {
    v.fromBufferAttribute(p, i).normalize();
    const x = v.x, y = v.y, z = v.z;
    let r = 1
      + 0.055 * Math.sin(9 * z + 3 * y)
      + 0.050 * Math.sin(10 * y + 4 * x)
      + 0.045 * Math.sin(11 * x + 6 * z)
      + 0.030 * Math.sin(16 * z) * Math.cos(14 * y);
    r -= Math.exp(-(x * x) * 55) * 0.16 * Math.max(0, y); // longitudinal fissure
    v.multiplyScalar(r);
    v.x *= 0.90; v.y *= 0.80; v.z *= 1.18;                // brain proportions
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

function synthetic() {
  const D = ['pwn_heap','crypto','web_exploit','recon','ot_ics','malware'];
  const stub = ['ret2win offset','tcache poisoning','padding oracle','nonce reuse','SSTI payload',
    'second-order SQLi','JWT alg-confusion','subdomain takeover','S3 misconfig','Modbus unauth',
    'PLC ladder flaw','unpacker stage-2','C2 beacon jitter','GOT overwrite'];
  const R = (a,b)=>a+Math.random()*(b-a), P = a=>a[Math.floor(Math.random()*a.length)];
  const nodes = [], links = [];
  for (let i=0;i<150;i++){ const d=P(D), r=Math.random();
    const status = r<0.08?'deprecated':r<0.2?'challenged':'committed';
    const corr = status==='committed'?Math.max(0,Math.round(R(-1.5,9))):Math.round(R(0,2));
    const conf = status==='deprecated'?R(0.12,0.35):status==='challenged'?R(0.25,0.55):Math.min(0.99,0.45+corr*0.06+R(-0.1,0.2));
    nodes.push({id:'m'+i,domain:d,label:P(stub),status,corroboration_count:corr,confidence:+conf.toFixed(2),memory_type:P(['fact','observation','inference','task'])}); }
  const tk=['supports','supports','related','related','refines','causes','precedes','contradicts'];
  for (let i=0;i<225;i++){ const a=P(nodes); let b=P(nodes); if(a===b)continue;
    if(Math.random()<0.7){const s=nodes.filter(n=>n.domain===a.domain&&n!==a); if(s.length)b=P(s);}
    links.push({source:a.id,target:b.id,link_type:P(tk)}); }
  return {nodes,links};
}

async function loadGraph(fetchUrl) {
  try {
    const r = await fetch(fetchUrl, { credentials: 'same-origin' });
    if (!r.ok) throw new Error('HTTP '+r.status);
    const g = await r.json();
    if (!g || !Array.isArray(g.nodes) || !g.nodes.length) throw new Error('empty');
    return { live: true,
      nodes: g.nodes.map(n=>({ id:n.id, domain:n.domain||'unknown', label:n.content||n.id,
        status:n.status||'committed', corroboration_count:n.corroboration_count||0,
        confidence: typeof n.confidence==='number'?n.confidence:0.5, memory_type:n.memory_type||'' })),
      links: (g.edges||[]).map(e=>({ source:e.source, target:e.target, link_type:e.type||'related' })) };
  } catch (err) {
    console.warn('[mri] live graph unavailable, synthetic fallback:', err.message);
    return Object.assign(synthetic(), { live: false });
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
      <label class="sld">skull <input class="b-op" type="range" min="0" max="60" value="5"></label>
    </div>
    <div class="tip"></div>
    <div class="flag"></div>`;
  container.appendChild(root);
  const $ = s => root.querySelector(s);

  const domainColors = {}; let seq = 0;
  const domainColor = k => { if(!k) k='unknown'; if(!domainColors[k]){ domainColors[k]=PALETTE[seq%PALETTE.length]; seq++; } return domainColors[k]; };
  const nodeSize = n => 1.6 + (n.corroboration_count||0)*0.9 + (n.confidence||0)*1.4;

  function haloTexture(){ const c=document.createElement('canvas'); c.width=c.height=64;
    const g=c.getContext('2d'), grd=g.createRadialGradient(32,32,0,32,32,32);
    grd.addColorStop(0,'rgba(255,255,255,0.95)'); grd.addColorStop(0.22,'rgba(255,255,255,0.55)');
    grd.addColorStop(1,'rgba(255,255,255,0)'); g.fillStyle=grd; g.fillRect(0,0,64,64); return new THREE.CanvasTexture(c); }

  let Graph = null, controls = null, disposed = false, flow = true, scanning = true;
  let hullMat = null, brainMat = null, surfMat = null, curOpacity = 0.05;
  const subs = [];

  function setHullOpacity(o){
    curOpacity = o;
    if (brainMat) { brainMat.opacity = o; if (surfMat) surfMat.opacity = o * 0.5; if (hullMat) hullMat.opacity = 0; }
    else if (hullMat) { hullMat.opacity = o; }
  }

  function refreshCounts(d){
    $('.nn').textContent = d.nodes.length;
    $('.ne').textContent = d.links.length;
    $('.nc').textContent = d.nodes.filter(n=>(n.corroboration_count||0)>=4 && n.status==='committed').length;
  }

  loadGraph(fetchUrl).then(data => {
    if (disposed) return;
    $('.boot').style.display = 'none';
    const HALO = haloTexture();
    Graph = ForceGraph3D({ controlType:'orbit' })($('.mrib-graph'))
      .backgroundColor('#05070d00')
      .graphData(data).nodeId('id').nodeLabel(()=>'' ).nodeVal(nodeSize)
      .linkColor(l=>(LINK_TYPES[l.link_type]||LINK_TYPES.related).color)
      .linkWidth(l=> l.link_type==='contradicts'?0.6 : (LINK_TYPES[l.link_type]||{}).typed?0.35:0.18)
      .linkOpacity(0.3)
      .linkDirectionalParticles(l=> flow&&(l.link_type==='causes'||l.link_type==='precedes')?2:0)
      .linkDirectionalParticleWidth(1.1).linkDirectionalParticleSpeed(0.006)
      .warmupTicks(60).cooldownTime(8000)
      .onNodeHover(showTip)
      .onNodeClick(n=>{ const r=Math.hypot(n.x,n.y,n.z)||1, d=40; Graph.cameraPosition({x:n.x*(1+d/r),y:n.y*(1+d/r),z:n.z*(1+d/r)},n,900); })
      .nodeThreeObject(n=>{
        const group=new THREE.Group(), size=nodeSize(n), boost=Math.min(1,(n.corroboration_count||0)/9);
        const col=new THREE.Color(domainColor(n.domain)).lerp(new THREE.Color(0xffffff), boost*0.5);
        const opacity=n.status==='deprecated'?0.28:n.status==='challenged'?0.5:(0.5+(n.confidence||0)*0.5);
        group.add(new THREE.Mesh(new THREE.SphereGeometry(size,16,16),
          new THREE.MeshBasicMaterial({color:n.status==='committed'?col:new THREE.Color(0x6a7a93),transparent:true,opacity})));
        if(n.status==='committed'&&(n.corroboration_count||0)>=3){
          const halo=new THREE.Sprite(new THREE.SpriteMaterial({map:HALO,color:col,transparent:true,
            opacity:Math.min(0.85,0.25+boost*0.8),blending:THREE.AdditiveBlending,depthWrite:false}));
          const s=size*(3.2+boost*3.4); halo.scale.set(s,s,1); group.add(halo);
        }
        return group;
      });

    // Anatomical layout (SAGE_AGI_BRAIN_ANALOGY.md): domain -> azimuthal lobe,
    // consolidation (corroboration_count) -> radial depth. Fresh/uncorroborated
    // sit deep/central (hippocampus); well-corroborated migrate out to the cortex
    // (brain surface). Bounded inside a brain-shaped ellipsoid so nothing escapes.
    Graph.d3Force('charge').strength(-13);
    const linkF=Graph.d3Force('link'); if(linkF){ linkF.distance(22).strength(0.07); }
    const EX=165, EY=108, EZ=190; // brain inner semi-axes (R-L, S-I, A-P)
    const hsh=(s,seed)=>{ s=s||''; let h=(seed>>>0)||1; for(let i=0;i<s.length;i++) h=Math.imul(h^s.charCodeAt(i),16777619); return ((h>>>0)%10000)/10000; };
    // Custom force WITH an initialize() hook so it always binds to the CURRENT
    // node set — including nodes added later via SSE. (A closure over data.nodes
    // would leave new nodes uncontained and they'd escape the hull on updates.)
    let lobeNodes=[], lobeDomIdx={}, lobeND=1;
    const lobeForce=(alpha)=>{ lobeNodes.forEach(n=>{
      const sector=((lobeDomIdx[n.domain]||0)/lobeND)*Math.PI*2;
      const az=sector+(hsh(n.id,1)-0.5)*(Math.PI*2/lobeND)*0.82;    // domain lobe + jitter
      const el=(hsh(n.id,2)-0.5)*Math.PI*0.92;                       // vertical spread
      const consol=Math.min(1,(n.corroboration_count||0)/8);
      const depth=0.33+consol*0.60;                                  // hippocampus -> cortex
      const ce=Math.cos(el);
      const tx=EX*depth*ce*Math.cos(az), ty=EY*depth*Math.sin(el), tz=EZ*depth*ce*Math.sin(az);
      const k=0.05*alpha; n.vx+=(tx-n.x)*k; n.vy+=(ty-n.y)*k; n.vz+=(tz-n.z)*k;
      const q=(n.x*n.x)/(EX*EX)+(n.y*n.y)/(EY*EY)+(n.z*n.z)/(EZ*EZ);
      if(q>1){ const f=(Math.sqrt(q)-1)*0.30; n.vx-=n.x*f; n.vy-=n.y*f; n.vz-=n.z*f; } // soft pull-in
      if(q>1.6){ const r=1/Math.sqrt(q); n.x*=r*1.25; n.y*=r*1.25; n.z*=r*1.25;        // hard stop on runaways
        n.vx*=0.3; n.vy*=0.3; n.vz*=0.3; }
    }); };
    lobeForce.initialize=(n)=>{ lobeNodes=n||[]; const ds=[...new Set(lobeNodes.map(x=>x.domain))];
      lobeND=Math.max(1,ds.length); lobeDomIdx={}; ds.forEach((k,i)=>lobeDomIdx[k]=i); ds.forEach(k=>domainColor(k)); };
    Graph.d3Force('lobe', lobeForce);

    try { const sc=Graph.scene();
      // Procedural brain-shaped wireframe hull (default — no external asset).
      const hull=new THREE.Mesh(makeBrainGeometry(),
        new THREE.MeshBasicMaterial({color:0x4aa3ff,wireframe:true,transparent:true,opacity:curOpacity,depthWrite:false}));
      hull.scale.setScalar(185); sc.add(hull); hullMat=hull.material;
      // Optional real anatomical mesh override at /ui/assets/brain.obj.
      fetch('/ui/assets/brain.obj').then(r=>{ if(!r.ok) throw 0; return r.text(); }).then(txt=>{
        if(disposed||!Graph) return;
        const g=parseOBJ(txt); g.center(); g.computeBoundingSphere();
        const s=255/((g.boundingSphere&&g.boundingSphere.radius)||1); // enclose the node cloud
        brainMat=new THREE.MeshBasicMaterial({color:0x6cc0ff,wireframe:true,transparent:true,opacity:curOpacity,depthWrite:false});
        const wf=new THREE.Mesh(g,brainMat); wf.scale.setScalar(s); sc.add(wf);
        surfMat=new THREE.MeshBasicMaterial({color:0x14304e,transparent:true,opacity:curOpacity*0.5,side:THREE.BackSide,depthWrite:false});
        const surf=new THREE.Mesh(g,surfMat); surf.scale.setScalar(s); sc.add(surf);
        setHullOpacity(curOpacity); // hide the procedural hull now that the real mesh is in
      }).catch(()=>{ /* no override — keep the procedural brain */ });
    } catch(e){ /* hull optional */ }

    const lobes=$('.lobes'); [...new Set(data.nodes.map(n=>n.domain))].forEach(k=>lobes.insertAdjacentHTML('beforeend',
      `<div class="row"><span class="dot" style="background:${domainColor(k)}"></span><div class="t"><b>${k}</b></div></div>`));
    const lt=$('.linktypes'); Object.values(LINK_TYPES).filter(t=>t.typed).forEach(t=>lt.insertAdjacentHTML('beforeend',
      `<div class="row"><span class="bar" style="background:${t.color}"></span><div class="t"><span>${t.label}</span></div></div>`));
    refreshCounts(data);
    $('.flag').textContent = data.live ? '' : 'SYNTHETIC FALLBACK · no live data';

    // Frame the brain once, then gentle auto-rotate via OrbitControls.
    // autoRotate respects user zoom/pan/drag — unlike setting cameraPosition
    // every frame, which previously clobbered all interaction.
    Graph.cameraPosition({ x: 0, y: 120, z: 600 }); // frame the whole brain + cloud
    controls = Graph.controls();
    if (controls) { controls.autoRotate = scanning; controls.autoRotateSpeed = 0.45; }

    // Live population — mirror the 2D view: re-pull the graph on remember/forget
    // SSE events. Positions of existing nodes are preserved so the brain does
    // not reshuffle; new memories settle into their lobe, forgotten ones drop.
    if (opts.sse && typeof opts.sse.on === 'function') {
      let t = null;
      const reload = () => { clearTimeout(t); t = setTimeout(() => {
        loadGraph(fetchUrl).then(d => {
          if (disposed || !Graph) return;
          const pos = {};
          Graph.graphData().nodes.forEach(n => { pos[n.id] = { x:n.x, y:n.y, z:n.z }; });
          d.nodes.forEach(n => { const p = pos[n.id]; if (p) { n.x=p.x; n.y=p.y; n.z=p.z; } });
          Graph.graphData(d);
          refreshCounts(d);
        });
      }, 450); };
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
  $('.b-flow').onclick=function(){ flow=!flow; if(Graph) Graph.linkDirectionalParticles(l=>flow&&(l.link_type==='causes'||l.link_type==='precedes')?2:0); this.textContent=flow?'⚡ flow: on':'⚡ flow: off'; };
  $('.b-op').oninput=function(){ setHullOpacity(this.value/100); };

  return function cleanup(){
    disposed = true;
    subs.forEach(u => { try { u && u(); } catch(e){ /* noop */ } });
    root.removeEventListener('mousemove', onMove);
    try { if (Graph && Graph._destructor) Graph._destructor(); } catch(e){ /* noop */ }
    if (root.parentNode) root.parentNode.removeChild(root);
  };
}
