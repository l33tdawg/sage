// Resolve the `symbol` + `file.go:line` citations in docs/reference/.
//
// docs/reference/ is the authoritative, code-verified reference: CLAUDE.md
// points agents there BEFORE reading source, and says it supersedes
// api/openapi.yaml and ARCHITECTURE.md where they disagree. That authority
// rests entirely on the citations being true, and a line number is the one
// kind of claim that rots on its own -- every insertion above a function
// silently invalidates every citation below it, with nothing failing.
//
// A citation of the form (`handleGetMemory`, `memory_handler.go:1118`) makes a
// checkable assertion: line 1118 is inside func handleGetMemory. This module
// resolves that assertion; doc-citations.test.mjs enforces it and `--fix`
// repairs it.
//
// Deliberately conservative -- it only checks what it can resolve unambiguously,
// because a guard that cries wolf gets deleted:
//   - the cited path must match exactly one tracked .go file
//   - the symbol must be a top-level func in that file
//   - anything else is SKIPPED, never guessed at and never failed
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

const root = new URL('../', import.meta.url).pathname;

// `symbol` ... `path/to/file.go:123` (optionally :123-456 or :123+)
const CITATION = /`([A-Za-z_][A-Za-z0-9_]*)`[^`\n]{0,80}?`?([A-Za-z0-9_/.]*[a-z_0-9]+\.go):(\d+)(-(\d+))?/g;

// A top-level func begins at column 0; anything indented is a body line.
const FUNC_DECL = /^func (?:\([^)]*\) )?([A-Za-z0-9_]+)/;

function walk(dir, pred, out = []) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) walk(full, pred, out);
    else if (pred(full)) out.push(full);
  }
  return out;
}

export function goFiles() {
  const skip = new Set(['node_modules', '.git', 'third_party', 'vendor']);
  return walk(root, (p) => p.endsWith('.go'), []).filter(
    (p) => !relative(root, p).split('/').some((s) => skip.has(s)),
  );
}

export function docFiles() {
  return walk(join(root, 'docs/reference'), (p) => p.endsWith('.md'));
}

// Extent of each top-level func, plus the contiguous comment block directly
// above it. Citing the doc comment rather than the `func` line is a normal and
// correct thing for a doc to do, so it counts as inside.
function functionSpans(path) {
  const lines = readFileSync(path, 'utf8').split('\n');
  const decls = [];
  lines.forEach((line, i) => {
    const m = FUNC_DECL.exec(line);
    if (m) decls.push({ name: m[1], start: i + 1 });
  });
  const spans = new Map();
  decls.forEach((decl, i) => {
    const end = i + 1 < decls.length ? decls[i + 1].start - 1 : lines.length;
    let lead = decl.start;
    while (lead > 1 && /^\s*\/\//.test(lines[lead - 2])) lead -= 1;
    const list = spans.get(decl.name) ?? [];
    list.push({ start: decl.start, end, lead });
    spans.set(decl.name, list);
  });
  return spans;
}

export function analyze() {
  const go = goFiles();
  const spanCache = new Map();
  const spansFor = (p) => {
    if (!spanCache.has(p)) spanCache.set(p, functionSpans(p));
    return spanCache.get(p);
  };

  const results = [];
  for (const doc of docFiles()) {
    const text = readFileSync(doc, 'utf8');
    text.split('\n').forEach((line, idx) => {
      for (const m of line.matchAll(CITATION)) {
        const [, symbol, citedPath, startStr, , endStr] = m;
        const start = Number(startStr);
        // Boundary-aware: 'app.go' must not resolve via 'myapp.go'.
        const matches = go.filter((p) => {
          const rel = relative(root, p);
          return rel === citedPath || rel.endsWith(`/${citedPath}`);
        });
        if (matches.length !== 1) {
          results.push({ status: 'skipped', reason: 'path', doc, line: idx + 1, symbol, citedPath });
          continue;
        }
        const spans = spansFor(matches[0]).get(symbol);
        if (!spans) {
          results.push({ status: 'skipped', reason: 'symbol', doc, line: idx + 1, symbol, citedPath });
          continue;
        }
        const inside = spans.some((s) => start >= s.lead && start <= s.end);
        results.push({
          status: inside ? 'ok' : 'wrong',
          doc,
          line: idx + 1,
          symbol,
          citedPath,
          file: relative(root, matches[0]),
          cited: { start, end: endStr ? Number(endStr) : null },
          actual: spans,
          match: m[0],
          index: m.index,
        });
      }
    });
  }
  return results;
}

export function describe(r) {
  const actual = r.actual.map((s) => `${s.start}-${s.end}`).join(', ');
  const cited = r.cited.end ? `${r.cited.start}-${r.cited.end}` : `${r.cited.start}`;
  return `${relative(root, r.doc)}:${r.line}: \`${r.symbol}\` cited at ${r.citedPath}:${cited}, but ${r.symbol} is at ${r.file}:${actual}`;
}
