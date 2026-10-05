// Federation membership and memory grants are separate product controls.
export function filterFederationAgents(agents, query) {
    const text = String(query || '').trim().toLowerCase();
    return (agents || []).filter(agent => !text ||
        [agent.display_name, agent.registered_name, agent.provider, agent.agent_id]
            .some(value => String(value || '').toLowerCase().includes(text)));
}

export function stageFederationDomains(draft, catalog, domains, permission) {
    if (!draft || !['read', 'copy', 'remove'].includes(permission)) return draft;
    const next = { ...draft };
    for (const domain of domains) {
        if (!Object.hasOwn(catalog, domain) || catalog[domain].can_share === false) continue;
        next[domain] = permission === 'remove' ? { read: false, write: false, copy: false } : { ...(next[domain] || { read: false, write: false, copy: false }), [permission]: true };
    }
    return next;
}

export function FederationDirectory({ peerName, localName, local, remote, automatic, copyAddress, loadMore, catalog, stageDomains, draft, disabled, visibility }) {
    const html = window.html;
    const { useState, useEffect } = preactHooks;
    const [query, setQuery] = useState('');
    const [domainQuery, setDomainQuery] = useState('');
    const [selected, setSelected] = useState([]);
    const [notice, setNotice] = useState('');
    const [loading, setLoading] = useState('');
    const [pages, setPages] = useState({});
    const [copied, setCopied] = useState('');
    const [domainLimit, setDomainLimit] = useState(24);
    const domains = Object.keys(catalog || {}).filter(domain => catalog[domain].can_share !== false && domain.toLowerCase().includes(domainQuery.toLowerCase()));
    const stage = (items, permission) => {
        if (disabled || !items.length) return;
        stageDomains(items, permission);
        setNotice(`${items.length} domains staged for ${permission === 'remove' ? 'removal from memory sharing' : permission === 'read' ? 'Read' : 'Copy'}. Review the permissions below and save to apply. Messaging remains governed by node trust.`);
        setSelected([]);
    };
    const localCursor = pages.local && pages.local.cursor;
    const remoteCursor = pages.remote && pages.remote.cursor;
    // Only explicitly browsed continuation pages add probes. Keep one current
    // page per side, revalidated independently of the parent's first-page poll.
    useEffect(() => {
        let live = true;
        let timer;
        const refresh = async () => {
            await Promise.all([['local', localCursor], ['remote', remoteCursor]].filter(([, cursor]) => cursor).map(async ([side, cursor]) => {
                try {
                    const grant = await loadMore(side, cursor);
                    if (live) setPages(current => current[side] && current[side].cursor === cursor ? { ...current, [side]: { cursor, grant } } : current);
                } catch {
                    if (live) setPages(current => current[side] && current[side].cursor === cursor ? { ...current, [side]: { cursor, grant: null, error: true } } : current);
                }
            }));
            if (live) timer = setTimeout(refresh, 8000);
        };
        if (localCursor || remoteCursor) timer = setTimeout(refresh, 8000);
        return () => { live = false; clearTimeout(timer); };
    }, [localCursor, remoteCursor, peerName]);
    const more = async (side, cursor) => {
        setLoading(side);
        try {
            const grant = await loadMore(side, cursor);
            setPages(current => ({ ...current, [side]: { cursor, grant } }));
        }
        catch (error) { setNotice(error.message || 'Could not load more agents. Try again.'); }
        finally { setLoading(''); }
    };
    const copy = async (agent, nodeName) => {
        try {
            if (await copyAddress(agent) === false) throw new Error('Could not copy the address.');
            setCopied(agent.address);
            setNotice(`Message address copied for ${agent.display_name || agent.registered_name || 'agent'} on ${nodeName}.`);
        } catch { setNotice('Could not copy the agent address. Try again.'); }
    };
    return html`<section class="fed-directory" aria-label="Federation agent directory">
        <div class="fed-directory-heading"><div><h3>Agents across this connection</h3>
            <p>${automatic ? 'Discovery and messaging are on by default. Sharing memory is a separate choice.' : (automatic === false ? 'This peer needs an update for automatic discovery and messaging. Its existing shared contacts are shown below.' : 'Checking the connection’s messaging capabilities…')}</p></div>
            <label>Find an agent<input type="search" placeholder="Name, provider, or agent ID" value=${query} onInput=${e => setQuery(e.target.value)} /></label>
        </div>
        <div class="fed-directory-nodes">
            ${[{ side: 'local', name: localName || 'Viewing this node', grant: local }, { side: 'remote', name: peerName, grant: remote }].map(node => {
                const activePage = pages[node.side];
                if (activePage) node.grant = activePage.grant;
                const contacts = node.grant && Array.isArray(node.grant.contacts) ? node.grant.contacts : [];
                const operatorRoster = node.side === 'local' && visibility && Array.isArray(visibility.agents);
                const agents = filterFederationAgents(operatorRoster ? visibility.agents : contacts, query);
                return html`<article class="fed-directory-node" key=${node.side}>
                    <header><span class="fed-directory-node-dot" aria-hidden="true"></span><h4>${node.name}${node.side === 'local' && html`<small class="fed-node-viewing">Viewing this node</small>`}</h4><span>${agents.length} shown</span></header>
                    <div class="fed-directory-agents">${agents.map(agent => {
                        if (operatorRoster) {
                            const contact = contacts.find(candidate => candidate.agent_id === agent.agent_id);
                            const visible = visibility.mode === 'all' ||
                                (visibility.mode === 'selected' && visibility.selected.has(agent.agent_id));
                            return html`<div class="fed-directory-agent" key=${agent.agent_id}>
                                <strong>${agent.display_name || agent.registered_name || agent.agent_id.slice(0, 12)}</strong>
                                <small>${agent.provider || 'Agent'} · ${agent.agent_id.slice(0, 8)}</small>
                                <span>${visible
                                    ? (contact ? (contact.accepting && contact.available ? 'Messaging allowed' : 'Messaging blocked') : `Visible to ${node.name}`)
                                    : `Not visible to ${node.name}`}</span>
                                <label class="fed-agent-toggle fed-agent-visible">
                                    <input type="checkbox" role="switch" checked=${visible}
                                        disabled=${visibility.busy || disabled}
                                        aria-label=${`${visible ? 'Visible' : 'Not visible'} to ${node.name}`}
                                        onChange=${event => visibility.setVisible(agent.agent_id, event.target.checked)} />
                                    <span class=${visible ? 'on' : ''}>${visible ? 'Visible' : 'Not visible'}</span>
                                </label>
                            </div>`;
                        }
                        return html`<div class="fed-directory-agent" key=${agent.agent_id}>
                            <strong>${agent.display_name || agent.registered_name || agent.agent_id.slice(0, 12)}</strong>
                            <small>${agent.provider || 'Agent'} · ${agent.agent_id.slice(0, 8)}</small>
                            <span>${node.grant && node.grant.paused ? 'Connection paused' : (agent.accepting && agent.available ? 'Messaging allowed' : 'Messaging blocked')}</span>
                            <button type="button" class="fed-address-copy" disabled=${!agent.address} aria-label=${`Copy address for ${agent.display_name || agent.registered_name || agent.agent_id} on ${node.name}`} onClick=${() => copy(agent, node.name)}>${copied === agent.address ? 'Copied' : 'Copy address'}</button>
                        </div>`;
                    })}</div>
                    ${!node.grant && !operatorRoster && html`<p class="muted">Directory not available yet.</p>`}
                    ${node.grant && !agents.length && html`<p class="muted">${query ? 'No matching agents on this page.' : 'No eligible agents on this page.'}</p>`}
                    ${!operatorRoster && activePage && html`<button type="button" class="btn" disabled=${!!loading} onClick=${() => setPages(current => { const next = { ...current }; delete next[node.side]; return next; })}>First agents</button>`}
                    ${!operatorRoster && activePage && activePage.error && html`<p role="status">This page could not be refreshed. Return to the first page or retry.</p><button type="button" class="btn" disabled=${!!loading} onClick=${() => more(node.side, activePage.cursor)}>Retry page</button>`}
                    ${!operatorRoster && node.grant && node.grant.next_cursor && html`<button type="button" class="btn" disabled=${!!loading} onClick=${() => more(node.side, node.grant.next_cursor)}>${loading === node.side ? 'Loading…' : 'Next agents'}</button>`}
                </article>`;
            })}
        </div>
        <p class="muted">Being listed does not mean an agent is currently running, and visibility never grants memory access. Flip Visible on your own agents to control who ${peerName} can find; copy an address when you want to send work from your agent’s messaging tool.</p>
        <details class="fed-directory-sharing">
            <summary>Share memory with ${peerName} — optional</summary>
            <p>Select domains together, then choose Read, Copy, or Clear domain permissions. You can also drag a domain or your selection onto an area. Changes stay in the permission draft until you save.</p>
            <label>Find a domain<input type="search" value=${domainQuery} onInput=${e => { setDomainQuery(e.target.value); setDomainLimit(24); }} /></label>
            <div class="fed-directory-domain-actions"><button type="button" class="btn" disabled=${disabled || !domains.length} onClick=${() => setSelected(domains)}>Select matching domains (${domains.length})</button>
                <button type="button" class="btn" disabled=${!selected.length} onClick=${() => setSelected([])}>Clear selection</button></div>
            <div class="fed-directory-domains">${domains.slice(0, domainLimit).map(domain => html`<label class="fed-directory-domain" key=${domain} draggable=${!disabled}
                onDragStart=${e => { e.dataTransfer.setData('application/x-sage-domains', JSON.stringify(selected.includes(domain) ? selected : [domain])); e.dataTransfer.effectAllowed = 'copy'; }}>
                <input type="checkbox" checked=${selected.includes(domain)} disabled=${disabled} onChange=${e => setSelected(e.target.checked ? [...selected, domain] : selected.filter(item => item !== domain))} />${domain}<small>${draft?.[domain]?.read ? 'Read ' : ''}${draft?.[domain]?.copy ? 'Copy' : ''}${!draft?.[domain]?.read && !draft?.[domain]?.copy ? 'Not shared' : ''}</small></label>`)}</div>
            ${domains.length > domainLimit && html`<button type="button" class="btn" onClick=${() => setDomainLimit(domainLimit + 24)}>Show more domains</button>`}
            <div class="fed-directory-dropzones">${['read', 'copy', 'remove'].map(permission => html`<button type="button" class="fed-directory-dropzone" disabled=${disabled} key=${permission}
                onDragOver=${e => { if (!disabled && Array.from(e.dataTransfer.types).includes('application/x-sage-domains')) e.preventDefault(); }}
                onDrop=${e => { e.preventDefault(); try { const items = JSON.parse(e.dataTransfer.getData('application/x-sage-domains')); if (Array.isArray(items)) stage(items.filter(item => typeof item === 'string' && Object.hasOwn(catalog, item) && catalog[item].can_share !== false), permission); } catch {} }}
                onClick=${() => stage(selected, permission)}>
                <strong>${permission === 'remove' ? 'Clear domain permissions' : permission === 'read' ? 'Live Read' : 'Offer Copy'}</strong><span>${permission === 'remove' ? 'Clear these Read/Copy choices after saving. Other domain or agent sharing may still allow access. Existing copies remain.' : permission === 'read' ? 'They may query these memories here.' : 'They may keep local copies after subscribing.'}</span>
                <small>${selected.length ? `Add ${selected.length} selected domains to draft` : 'Drop domains here'}</small></button>`)}</div>
        </details>
        ${notice && html`<p role="status" aria-live="polite">${notice}</p>`}
    </section>`;
}
