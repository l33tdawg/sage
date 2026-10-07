import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
const fixture=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/ui/css/sage.css"></head><body><main style="max-width:1450px;margin:24px auto;padding:16px" id="fixture"></main>
<script src="/ui/js/vendor/preact.umd.js"></script><script src="/ui/js/vendor/preact.hooks.umd.js"></script><script src="/ui/js/vendor/htm.umd.js"></script>
<script>window.html=htm.bind(preact.h);window.actions=[];window.streams=[];window.EventSource=class {constructor(){this.listeners={};window.streams.push(this)}addEventListener(k,fn){this.listeners[k]=fn}close(){this.closed=true}};window.emitActivity=items=>window.streams.at(-1).listeners.federation_activity({data:JSON.stringify({items})});</script>
<script type="module">import {FederationConnectome} from '/ui/js/federation-connectome.js';
const connections=[{remote_chain_id:'tii',peer_name:'DKAN-TII',status:'active'},{remote_chain_id:'studio',peer_name:'STUDIO-MACMINI',status:'active'}];
preact.render(window.html\`<\$\{FederationConnectome} connections=\$\{connections} statuses=\$\{{tii:{reachable:true},studio:{reachable:false}}} localChain="home" localName="L33TDAWG-SAGE" enabled=\$\{true} onManage=\$\{c=>actions.push(['manage',c.remote_chain_id])} onPause=\$\{(c,p)=>actions.push(['pause',c.remote_chain_id,p])} onRevoke=\$\{c=>actions.push(['revoke',c.remote_chain_id])} />\`,document.getElementById('fixture'));</script></body></html>`;
async function openMap(page) {
 const agent=(id,name)=>({agent_id:id,display_name:name,registered_name:name,provider:'codex',authorization_mode:'node-messaging-v1',available:true,accepting:true,address:id+'@home'});
 const local=Array.from({length:24},(_,i)=>agent('local-'+i,i===0?'codex/sage':'research-agent-'+i));
 await page.route('http://federation-map.test/**',async route=>{
  const u=new URL(route.request().url());
  if(u.pathname==='/')return route.fulfill({contentType:'text/html',body:fixture});
  if(u.pathname.includes('/pipe-contacts')) {
   const tii=u.pathname.includes('/tii/');const next=u.searchParams.get('remote_cursor');
   return route.fulfill({json:{local_node_contacts:{contacts:local},remote_known:tii,remote_contacts:tii?{contacts:next?[agent('next','next-page-agent')]:Array.from({length:8},(_,i)=>({...agent('remote-'+i,i===0?'codex/tii-sage':'laptop-agent-'+i),address:'remote-'+i+'@tii'})),next_cursor:next?'':'opaque-page'}:null}});
  }
  const relative=u.pathname.replace(/^\/ui\//,'');
  if(!/^(js\/(vendor\/(preact\.umd|preact\.hooks\.umd|htm\.umd)|federation-connectome|federation-directory|api)\.js|css\/sage\.css)$/.test(relative))return route.abort();
  return route.fulfill({contentType:relative.endsWith('.css')?'text/css':'application/javascript',body:await readFile(path.resolve('web/static',relative))});
 });
 await page.goto('http://federation-map.test/');
 await expect(page.getByRole('button',{name:'codex/tii-sage on DKAN-TII. Messaging allowed',exact:true})).toBeVisible();
}
test('map selection, search, paging and accessible list use exact node identities',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));await openMap(page);
 await page.getByRole('button',{name:'codex/tii-sage on DKAN-TII. Messaging allowed',exact:true}).click();
 await expect(page.getByLabel('Exact message address')).toHaveValue('remote-0@tii');
 await page.getByRole('button',{name:'Manage memory sharing',exact:true}).click();
 expect(await page.evaluate(()=>window.actions)).toEqual([['manage','tii']]);
 await page.getByRole('button',{name:'Remove trusted connection…',exact:true}).click();
 expect(await page.evaluate(()=>window.actions.at(-1))).toEqual(['revoke','tii']);
 await page.getByRole('searchbox').fill('tii-sage');
 await page.getByRole('button',{name:'List',exact:true}).click();
 await expect(page.locator('.fc-list-agent')).toHaveCount(1);
 await page.getByRole('searchbox').fill('');
 await page.getByRole('button',{name:'Next agents',exact:true}).click();
 await expect(page.locator('.fc-list-agent').filter({hasText:'next-page-agent'})).toHaveCount(1);
 expect(errors).toEqual([]);
});
test('real activity changes pulse; history and reconnect do not; mobile fits',async({page})=>{
 await openMap(page);
 const item={id:'test',chain_id:'tii',source:'local-0',target:'remote-0',direction:'outbound',kind:'send',state:'pending',at:new Date().toISOString()};
 await page.evaluate(item=>window.emitActivity([item]),item);
 await expect(page.locator('.fc-activity-row')).toContainText('Queued');await expect(page.locator('.fc-pulse')).toHaveCount(0);
 await page.evaluate(item=>window.emitActivity([{...item,state:'delivered'}]),item);
 await expect(page.locator('.fc-pulse')).toHaveCount(1);await expect(page.locator('.fc-activity-row')).toContainText('Delivered');
 await page.screenshot({path:test.info().outputPath('connectome-desktop.png'),fullPage:true});
 await page.evaluate(()=>window.streams.at(-1).onerror());
 await expect.poll(()=>page.evaluate(()=>window.streams.length)).toBe(2);
 await page.evaluate(item=>window.emitActivity([{...item,state:'delivered'}]),item);
 await expect(page.locator('.fc-pulse')).toHaveCount(0);
 await page.setViewportSize({width:390,height:844});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
 await page.screenshot({path:test.info().outputPath('connectome-mobile.png'),fullPage:true});
});

test('ambient agents drift, pause for interaction and respect reduced motion',async({page})=>{
 await openMap(page);
 const dot=page.locator('.fc-agent-core').first();
 const before=await dot.getAttribute('cx');
 await expect.poll(()=>dot.getAttribute('cx')).not.toBe(before);
 await page.getByRole('button',{name:'Pause motion',exact:true}).click();
 await page.mouse.move(0,0);
 const paused=await dot.getAttribute('cx');
 await page.waitForTimeout(250);
 expect(await dot.getAttribute('cx')).toBe(paused);
 await page.getByRole('button',{name:'Resume motion',exact:true}).click();
 await page.mouse.move(0,0);
 await expect.poll(()=>dot.getAttribute('cx')).not.toBe(paused);
 await page.emulateMedia({reducedMotion:'reduce'});
 await expect(page.getByRole('button',{name:'Reduced motion',exact:true})).toBeDisabled();
 const reduced=await dot.getAttribute('cx');
 await page.waitForTimeout(250);
 expect(await dot.getAttribute('cx')).toBe(reduced);
});


test('motion is visible over empty map space and resumes after inspecting an agent',async({page})=>{
 await openMap(page);
 const map=page.locator('.fc-map');
 await map.hover({position:{x:30,y:30}});
 const dot=page.locator('.fc-agent-core').first();
 const position=()=>dot.evaluate(el=>{const r=el.getBoundingClientRect();return {x:r.x,y:r.y}});
 const initial=await position();
 await expect.poll(async()=>{const p=await position();return Math.hypot(p.x-initial.x,p.y-initial.y)},{timeout:4000}).toBeGreaterThan(8);
 await page.locator('[data-entity="agent"]').first().hover();
 await expect(map).toHaveClass(/fc-ambient-paused/);
 const paused=await position();
 await page.waitForTimeout(300);
 expect(await position()).toEqual(paused);
 await map.hover({position:{x:30,y:30}});
 await expect(map).toHaveClass(/fc-ambient-running/);
 await expect.poll(async()=>{const p=await position();return Math.hypot(p.x-paused.x,p.y-paused.y)},{timeout:4000}).toBeGreaterThan(8);
});


test('pointer selection releases motion and keyboard inspection holds targets steady',async({page})=>{
 await openMap(page);
 const map=page.locator('.fc-map');
 const agent=page.locator('[data-entity="agent"]').first();
 await agent.click();
 await map.hover({position:{x:30,y:30}});
 await expect(map).toHaveClass(/fc-ambient-running/);
 await page.keyboard.press('Tab');
 await expect(map).toHaveClass(/fc-ambient-paused/);
 await page.getByRole('button',{name:'Pause motion',exact:true}).focus();
 await expect(map).toHaveClass(/fc-ambient-running/);
});
