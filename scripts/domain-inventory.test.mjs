import assert from 'node:assert/strict';
import test from 'node:test';

import { buildBrainDomainInventory } from '../web/static/js/domain-inventory.js';

test('brain domain inventory separates local storage from authenticated remote grants', () => {
    const inventory = buildBrainDomainInventory({
        stats: {
            total_memories: 12,
            by_domain: { local: 10, copied: 2 },
        },
        localCatalogue: {
            domains: [
                { domain: 'empty-local', memory_count: 0, authority: 'owner', can_share: true },
                { domain: 'local', memory_count: 10, authority: 'admin', can_share: true },
            ],
        },
        connections: [
            { remote_chain_id: 'chain-a', peer_name: 'SAGE A', status: 'active', expired: false },
        ],
        peerStates: {
            'chain-a': {
                permissions: {
                    remote_known: true,
                    remote_paused: false,
                    remote_permissions: [
                        { domain: 'research.shared', read: true, copy: true },
                    ],
                },
                sync: {
                    subscribe_domains: ['research'],
                    remote_publish_domains: ['research.shared'],
                },
            },
        },
    });

    assert.equal(inventory.localMemoryTotal, 12);
    assert.deepEqual(inventory.localDomains.map(domain => domain.domain), ['local', 'copied', 'empty-local']);
    assert.equal(inventory.sharedDomains.length, 1);
    assert.equal(inventory.sharedDomains[0].domain, 'research.shared');
    assert.equal(inventory.sharedDomains[0].read, true);
    assert.equal(inventory.sharedDomains[0].copy, true);
    assert.equal(inventory.sharedDomains[0].savedHere, true);
    assert.equal(inventory.sharedDomains[0].lastKnown, false);
    assert.equal(inventory.sharedDomains[0].sources[0].peerName, 'SAGE A');
});

test('shared domains group multiple source nodes without becoming local domains', () => {
    const inventory = buildBrainDomainInventory({
        stats: { total_memories: 0, by_domain: {} },
        connections: [
            { remote_chain_id: 'chain-a', peer_name: 'Alpha', status: 'active' },
            { remote_chain_id: 'chain-b', peer_name: 'Beta', status: 'active' },
            { remote_chain_id: 'chain-old', peer_name: 'Old', status: 'revoked' },
        ],
        peerStates: {
            'chain-a': {
                permissions: {
                    remote_known: true,
                    remote_permissions: [{ domain: 'shared', read: true, copy: false }],
                },
            },
            'chain-b': {
                permissions: {
                    remote_known: true,
                    remote_permissions: [{ domain: 'shared', read: true, copy: true }],
                },
                sync: { subscribe_domains: [] },
            },
            'chain-old': {
                permissions: {
                    remote_known: true,
                    remote_permissions: [{ domain: 'must-not-appear', read: true }],
                },
            },
        },
    });

    assert.deepEqual(inventory.localDomains, []);
    assert.equal(inventory.sharedDomains.length, 1);
    assert.equal(inventory.sharedDomains[0].domain, 'shared');
    assert.equal(inventory.sharedDomains[0].sources.length, 2);
    assert.deepEqual(inventory.sharedDomains[0].sources.map(source => source.peerName), ['Alpha', 'Beta']);
});

test('offline peers expose only a clearly stale persisted Copy route', () => {
    const inventory = buildBrainDomainInventory({
        connections: [
            { remote_chain_id: 'chain-a', peer_name: 'SAGE A', status: 'active' },
            { remote_chain_id: 'chain-b', peer_name: 'SAGE B', status: 'active' },
        ],
        peerStates: {
            'chain-a': {
                permissions: { remote_known: false },
                sync: {
                    remote_publish_domains: ['last-known-copy'],
                    subscribe_domains: ['last-known-copy'],
                },
                error: 'peer offline',
            },
            'chain-b': {
                permissions: { remote_known: false },
                sync: { remote_publish_domains: [] },
                error: 'peer offline',
            },
        },
    });

    assert.equal(inventory.sharedDomains.length, 1);
    assert.equal(inventory.sharedDomains[0].domain, 'last-known-copy');
    assert.equal(inventory.sharedDomains[0].lastKnown, true);
    assert.equal(inventory.sharedDomains[0].read, false);
    assert.equal(inventory.sharedDomains[0].savedHere, true);
    assert.deepEqual(inventory.unavailablePeers.map(peer => peer.peerName), ['SAGE B']);
});

test('an explicit current deny-all never falls back to a stale publish route', () => {
    const inventory = buildBrainDomainInventory({
        connections: [{ remote_chain_id: 'chain-a', peer_name: 'SAGE A', status: 'active' }],
        peerStates: {
            'chain-a': {
                permissions: {
                    remote_known: true,
                    remote_permissions: [],
                },
                sync: {
                    remote_publish_domains: ['withdrawn'],
                },
            },
        },
    });

    assert.deepEqual(inventory.sharedDomains, []);
    assert.deepEqual(inventory.unavailablePeers, []);
});

test('retained local copies keep their origin after the connection is revoked', () => {
    const inventory = buildBrainDomainInventory({
        stats: {
            total_memories: 3,
            by_domain: { 'sage-autoresearch-benchmark': 3 },
        },
        localCatalogue: {
            domains: [{
                domain: 'sage-autoresearch-benchmark',
                memory_count: 3,
                authority: 'admin',
                can_share: true,
                copy_sources: [
                    { chain_id: 'chain-a', memory_count: 2 },
                    { chain_id: 'chain-a', memory_count: 1 },
                ],
            }],
        },
        connections: [{
            remote_chain_id: 'chain-a',
            peer_name: 'Research SAGE',
            status: 'revoked',
        }],
        peerStates: {},
    });

    assert.deepEqual(inventory.sharedDomains, []);
    assert.equal(inventory.localDomains.length, 1);
    assert.deepEqual(inventory.localDomains[0].copySources, [{
        chainID: 'chain-a',
        peerName: 'Research SAGE',
        memoryCount: 3,
    }]);
});
