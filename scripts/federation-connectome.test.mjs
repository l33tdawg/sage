import test from 'node:test';
import assert from 'node:assert/strict';
import { reconcileActivity, activityLabel, constellationLayout } from '../web/static/js/federation-connectome.js';
import { stageFederationDomains } from '../web/static/js/federation-directory.js';
test('first snapshot and reconnect never animate retained history; only changes pulse',()=>{
 const old=[{id:'one',state:'pending',at:'2026-01-01'}];
 const first=reconcileActivity(null,old);assert.equal(first.changed.length,0);
 assert.equal(reconcileActivity(first.next,old).changed.length,0);
 const delivered=[{...old[0],state:'delivered'}];
 assert.equal(reconcileActivity(first.next,delivered).changed.length,1);
 assert.equal(reconcileActivity(null,delivered).changed.length,0);
 assert.equal(reconcileActivity(first.next,[]).next.size,0);
});
test('transport vocabulary never conflates queued and delivered replies',()=>{
 assert.equal(activityLabel({kind:'result',state:'pending'}),'Reply queued');
 assert.equal(activityLabel({kind:'result',state:'delivered'}),'Reply delivered');
 assert.equal(activityLabel({kind:'result',state:'failed'}),'Reply failed');
 assert.equal(activityLabel({kind:'send',state:'received'}),'Received here');
});
test('layout keeps same-name identities distinct across nodes and is deterministic',()=>{
 const nodes=[{id:'local',agents:[{agent_id:'a',display_name:'same'}]},{id:'peer',agents:[{agent_id:'a',display_name:'same'}]}];
 const layout=constellationLayout(nodes);assert.deepEqual(layout,constellationLayout(nodes));
 assert.notEqual(layout[0].agents[0].nodeID,layout[1].agents[0].nodeID);
 assert.notEqual(layout[0].agents[0].x,layout[1].agents[0].x);
});
test('remove sharing stages only authorized domains and leaves saved choices intact',()=>{
 const draft={research:{read:true,copy:true,write:false},private:{read:true,copy:false,write:false}};
 const result=stageFederationDomains(draft,{research:{can_share:true},private:{can_share:false}},['research','private','injected'],'remove');
 assert.deepEqual(result.research,{read:false,copy:false,write:false});
 assert.equal(result.private,draft.private);assert.equal(draft.research.read,true);assert.equal(result.injected,undefined);
});

test('allowed graph paths are bound to each peer and do not infer legacy relations', async()=>{
 const {permittedNodePair}=await import('../web/static/js/federation-connectome.js');
 const local={agent_id:'local',available:true,accepting:true,authorization_mode:'node-messaging-v1'};
 const target={agent_id:'remote',available:true,accepting:true,authorization_mode:'node-messaging-v1'};
 const peer={known:true,conn:{sharing_paused:false},grant:{paused:false},localGrant:{paused:false,contacts:[local]}};
 assert.equal(permittedNodePair(true,peer,local,target),true);
 assert.equal(permittedNodePair(true,{...peer,localGrant:{...peer.localGrant,paused:true}},local,target),false);
 assert.equal(permittedNodePair(true,peer,local,{...target,authorization_mode:''}),false);
 assert.equal(permittedNodePair(true,peer,local,{...target,accepting:false}),false);
 assert.equal(permittedNodePair(false,peer,local,target),false);
});

test('ambient drift is bounded, deterministic and preserves message identities',async()=>{
 const {driftConstellation}=await import('../web/static/js/federation-connectome.js');
 const initial=constellationLayout([{id:'local',agents:[{agent_id:'exact',address:'exact@chain'}]}]);
 const drift=driftConstellation(initial,10);
 assert.equal(drift[0].agents[0].address,'exact@chain');
 assert.notEqual(drift[0].agents[0].x,initial[0].agents[0].x);
 for(const seconds of [0,10,100,1000]) {
  const point=driftConstellation(initial,seconds)[0].agents[0],base=initial[0].agents[0];
  assert.ok(Math.abs(Math.hypot(point.x-initial[0].x,point.y-initial[0].y)-Math.hypot(base.x-initial[0].x,base.y-initial[0].y))<7);
 }
 assert.deepEqual(drift,driftConstellation(initial,10));
});
