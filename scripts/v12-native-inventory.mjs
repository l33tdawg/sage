#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCHEMA = 'dev.sage.v12-native-capability-inventory/v1';
const SOURCE_PATHS = Object.freeze({
    routes: 'web/static/js/app.js',
    actions: 'web/static/js/api.js',
    deepLinks: 'desktop/sage-shell/src/main.rs',
});

function fail(message) {
    throw new Error(`v12 native inventory: ${message}`);
}

function lineAt(source, offset) {
    return source.slice(0, offset).split('\n').length;
}

function anchor(path, source, offset, symbol) {
    return { file: path, line: lineAt(source, offset), symbol };
}

function sha256(source) {
    return createHash('sha256').update(source).digest('hex');
}

function kebabCase(name) {
    return name
        .replace(/([a-z0-9])([A-Z])/g, '$1-$2')
        .replace(/([A-Z])([A-Z][a-z])/g, '$1-$2')
        .replace(/_/g, '-')
        .toLowerCase();
}

function findBalancedBody(source, openBrace, seam) {
    let depth = 0;
    let state = 'code';
    let quote = '';

    for (let index = openBrace; index < source.length; index += 1) {
        const char = source[index];
        const next = source[index + 1];

        if (state === 'line-comment') {
            if (char === '\n') state = 'code';
            continue;
        }
        if (state === 'block-comment') {
            if (char === '*' && next === '/') {
                state = 'code';
                index += 1;
            }
            continue;
        }
        if (state === 'string') {
            if (char === '\\') {
                index += 1;
            } else if (char === quote) {
                state = 'code';
            }
            continue;
        }
        if (char === '/' && next === '/') {
            state = 'line-comment';
            index += 1;
        } else if (char === '/' && next === '*') {
            state = 'block-comment';
            index += 1;
        } else if (char === "'" || char === '"' || char === '`') {
            state = 'string';
            quote = char;
        } else if (char === '{') {
            depth += 1;
        } else if (char === '}') {
            depth -= 1;
            if (depth === 0) return { body: source.slice(openBrace + 1, index), end: index };
            if (depth < 0) break;
        }
    }

    fail(`malformed ${seam}: unbalanced function body`);
}

function functionBody(source, signature, seam) {
    const match = signature.exec(source);
    if (!match) fail(`missing ${seam}`);
    const openBrace = source.indexOf('{', match.index + match[0].length - 1);
    if (openBrace < 0) fail(`malformed ${seam}: missing opening brace`);
    return { ...findBalancedBody(source, openBrace, seam), start: match.index, openBrace };
}

function extractRoutes(source) {
    const applyHash = functionBody(source, /function\s+applyHash\s*\(\s*requestedHash\s*\)\s*\{/, 'App.applyHash route seam');
    const comparisons = [];
    const routePattern = /(?:if|else\s+if)\s*\(\s*hash\s*===\s*(['"])(\/[a-z0-9-]+)\1\s*\)\s*(?:\{\s*)?setPage\(\s*(['"])([a-z0-9-]+)\3\s*\)/g;
    let match;
    while ((match = routePattern.exec(applyHash.body)) !== null) {
        comparisons.push({ path: match[2], page: match[4], offset: applyHash.openBrace + 1 + match.index });
    }

    const fallbackMatch = /else\s+setPage\(\s*(['"])brain\1\s*\)/.exec(applyHash.body);
    if (!fallbackMatch) fail('missing App.applyHash brain fallback');

    const byPage = new Map();
    for (const route of comparisons) {
        const values = byPage.get(route.page) ?? [];
        values.push(route);
        byPage.set(route.page, values);
    }
    byPage.set('brain', [{
        path: '/',
        page: 'brain',
        offset: applyHash.openBrace + 1 + fallbackMatch.index,
    }]);

    const mountPattern = /\$\{page\s*===\s*(['"])([a-z0-9-]+)\1\s*&&\s*html`<\$\{([A-Za-z_$][\w$]*)\}/g;
    const mounts = new Map();
    while ((match = mountPattern.exec(source)) !== null) {
        mounts.set(match[2], {
            component: match[3],
            offset: match.index,
        });
    }

    const primaryPages = [...mounts.keys()].filter(page => byPage.has(page)).sort();
    if (primaryPages.length === 0) fail('no primary routes found');
    for (const page of byPage.keys()) {
        if (!mounts.has(page)) fail(`route ${page} has no page mount`);
    }
    for (const page of primaryPages) {
        if (!byPage.has(page)) fail(`page mount ${page} has no primary hash route`);
    }

    return primaryPages.map(page => {
        const candidates = byPage.get(page);
        const primary = candidates.find(candidate => candidate.path === `/${page}`)
            ?? candidates.find(candidate => candidate.path === '/')
            ?? candidates[0];
        const aliases = candidates
            .filter(candidate => candidate !== primary)
            .map(candidate => `#${candidate.path}`)
            .sort();
        const mount = mounts.get(page);
        return {
            id: `route.${page}`,
            hash_route: `#${primary.path}`,
            ...(aliases.length > 0 ? { aliases } : {}),
            page,
            mount: mount.component,
            source_anchors: [
                anchor(SOURCE_PATHS.routes, source, primary.offset, 'App.applyHash'),
                anchor(SOURCE_PATHS.routes, source, mount.offset, mount.component),
            ],
        };
    });
}

function stringLiterals(source) {
    const values = [];
    for (let index = 0; index < source.length; index += 1) {
        const quote = source[index];
        if (quote !== "'" && quote !== '"' && quote !== '`') continue;
        const start = index;
        let value = '';
        for (index += 1; index < source.length; index += 1) {
            const char = source[index];
            if (char === '\\') {
                value += char;
                if (index + 1 < source.length) value += source[++index];
            } else if (char === quote) {
                values.push({ value, offset: start });
                break;
            } else {
                value += char;
            }
        }
    }
    return values;
}

function normalizeApiPath(value) {
    const withoutBase = value.startsWith('${API_BASE}') ? value.slice('${API_BASE}'.length) : value;
    if (!withoutBase.startsWith('/v1/')) return null;
    return withoutBase.replace(/\$\{([^}]+)\}/g, (_match, expression) => {
        const encoded = /^encodeURIComponent\(([$\w]+)\)$/.exec(expression.trim());
        return `{${encoded ? encoded[1] : expression.trim()}}`;
    });
}

function extractActions(source) {
    if (!/^const\s+API_BASE\s*=\s*(['"])\1\s*;/m.test(source)) {
        fail('missing or non-empty api.js API_BASE seam');
    }

    const declarations = source.match(/^export\s+(?:async\s+)?function\b/gm) ?? [];
    const signature = /^export\s+(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{/gm;
    const actions = [];
    let match;
    while ((match = signature.exec(source)) !== null) {
        const name = match[1];
        const openBrace = source.indexOf('{', match.index + match[0].length - 1);
        const parsed = findBalancedBody(source, openBrace, `exported API action ${name}`);
        const paths = [...new Set(stringLiterals(parsed.body)
            .map(literal => normalizeApiPath(literal.value))
            .filter(Boolean))].sort();
        actions.push({
            id: `api.${kebabCase(name)}`,
            export: name,
            source_anchor: anchor(SOURCE_PATHS.actions, source, match.index, name),
            api_paths: paths,
        });
        signature.lastIndex = parsed.end + 1;
    }

    if (actions.length === 0) fail('no exported API actions found');
    if (actions.length !== declarations.length) {
        fail(`malformed or unsupported exported API action (${actions.length} of ${declarations.length} parsed)`);
    }
    if (!actions.some(action => action.api_paths.length > 0)) {
        fail('no statically knowable API action paths found');
    }
    return actions.sort((left, right) => left.id.localeCompare(right.id));
}

function extractDeepLinks(source) {
    const parser = functionBody(source, /fn\s+parse_deep_link\s*\([^)]*\)\s*->\s*Option<String>\s*\{/, 'parse_deep_link seam');
    if (!/url\.scheme\(\)\s*!=\s*"sage"/.test(parser.body)) {
        fail('parse_deep_link does not enforce the sage scheme');
    }
    const allowlist = /!matches!\(\s*host\s*,([\s\S]*?)\)\s*\{/.exec(parser.body);
    if (!allowlist) fail('missing parse_deep_link host allowlist');
    const hosts = [...allowlist[1].matchAll(/"([a-z0-9-]+)"/g)].map(match => match[1]);
    if (hosts.length === 0 || new Set(hosts).size !== hosts.length) {
        fail('empty or duplicate parse_deep_link host allowlist');
    }
    return hosts.sort().map(host => ({
        id: `deep-link.${host}`,
        scheme: 'sage',
        host,
        source_anchor: anchor(SOURCE_PATHS.deepLinks, source, parser.openBrace + 1 + allowlist.index, 'parse_deep_link'),
    }));
}

async function loadSources(root) {
    const entries = await Promise.all(Object.values(SOURCE_PATHS).map(async path => {
        try {
            return [path, await readFile(resolve(root, path), 'utf8')];
        } catch (error) {
            fail(`required source ${path} is unreadable: ${error.code ?? error.message}`);
        }
    }));
    return Object.fromEntries(entries);
}

export async function buildInventory(root = process.cwd()) {
    const resolvedRoot = resolve(root);
    const sources = await loadSources(resolvedRoot);
    const routes = extractRoutes(sources[SOURCE_PATHS.routes]);
    const actions = extractActions(sources[SOURCE_PATHS.actions]);
    const deepLinks = extractDeepLinks(sources[SOURCE_PATHS.deepLinks]);
    const unresolvedActions = actions.filter(action => action.api_paths.length === 0).map(action => action.id);

    return {
        schema: SCHEMA,
        source_files: Object.values(SOURCE_PATHS).sort().map(path => ({
            path,
            sha256: sha256(sources[path]),
        })),
        routes,
        actions,
        deep_links: deepLinks,
        blind_spots: [
            {
                id: 'route-availability-is-not-action-completion',
                detail: 'A mounted or deep-linkable route proves navigation availability only; it does not mark any API action or v12 acceptance evidence complete.',
            },
            {
                id: 'dynamic-ui-controls-not-enumerated',
                detail: 'Rendered controls, forms, menus, keyboard commands, role gates, feature flags, and error-state branches require runtime discovery.',
            },
            {
                id: 'server-contracts-not-cross-checked',
                detail: 'This inventory reads exported browser API calls, not server route registrars or authorization gates.',
            },
            {
                id: 'dynamic-api-paths',
                detail: 'Exported actions without an in-body /v1 path literal require interprocedural or runtime resolution and are not assigned a guessed API path.',
                action_ids: unresolvedActions,
            },
        ],
    };
}

function parseArgs(argv) {
    let root = process.cwd();
    let output = null;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag !== '--root' && flag !== '--output') fail(`unknown argument ${flag}`);
        const value = argv[++index];
        if (!value || value.startsWith('--')) fail(`${flag} requires a value`);
        if (flag === '--root') root = resolve(process.cwd(), value);
        else output = resolve(process.cwd(), value);
    }
    return { root, output };
}

export async function main(argv = process.argv.slice(2)) {
    const { root, output } = parseArgs(argv);
    const json = `${JSON.stringify(await buildInventory(root), null, 2)}\n`;
    if (output) await writeFile(output, json, 'utf8');
    else process.stdout.write(json);
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : '';
if (invokedPath === fileURLToPath(import.meta.url)) {
    main().catch(error => {
        process.stderr.write(`${error.message}\n`);
        process.exitCode = 1;
    });
}
