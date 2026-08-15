// Repair drifted `symbol` + `file.go:line` citations in docs/reference/.
//
// Usage: node scripts/fix-doc-citations.mjs [--dry-run]
//
// SCOPE, AND ITS LIMIT. This repairs WHERE a symbol lives. It cannot verify the
// prose around the citation, and the difference is load-bearing: a stale line
// number where the claim is still true is a clerical error, but a doc that
// QUOTES source text which no longer exists is a false claim, and renumbering
// that one would launder it into something that looks freshly verified.
//
// So the fixer refuses to touch any citation whose line quotes source text that
// is not present in the target function, and reports it for a human instead.
// Fixing the address of a claim is mechanical; fixing the claim is not.
import { readFileSync, writeFileSync } from 'node:fs';
import { relative } from 'node:path';
import { analyze } from './doc-citations.mjs';

const root = new URL('../', import.meta.url).pathname;
const dryRun = process.argv.includes('--dry-run');

// These docs quote source comments in the bold form **"..."**. Only that form
// is treated as a claim about source text; a plain "..." run is ordinary prose
// and matching it spans unrelated quoted phrases, which cries wolf.
const QUOTED = /\*\*"([^"]{25,})"\*\*/g;

const normalize = (s) => s.replace(/[\s/*]+/g, ' ').trim().toLowerCase();

const wrong = analyze().filter((r) => r.status === 'wrong');
const edits = new Map();
const unquotable = [];

for (const r of wrong) {
  const docText = readFileSync(r.doc, 'utf8');
  const docLine = docText.split('\n')[r.line - 1];
  const target = readFileSync(new URL(r.file, `file://${root}`).pathname, 'utf8').split('\n');
  const span = r.actual[0];
  const body = normalize(target.slice(span.lead - 1, span.end).join(' '));

  let quoteMissing = null;
  for (const q of docLine.matchAll(QUOTED)) {
    const claim = normalize(q[1]);
    if (claim && !body.includes(claim)) quoteMissing = q[1];
  }
  if (quoteMissing) {
    unquotable.push({ ...r, quote: quoteMissing });
    continue;
  }

  const replacement = r.cited.end
    ? r.match.replace(`${r.cited.start}-${r.cited.end}`, `${span.start}-${span.end}`)
    : r.match.replace(String(r.cited.start), String(span.start));
  const list = edits.get(r.doc) ?? [];
  list.push({ line: r.line, from: r.match, to: replacement });
  edits.set(r.doc, list);
}

let changed = 0;
for (const [doc, list] of edits) {
  const lines = readFileSync(doc, 'utf8').split('\n');
  for (const e of list) {
    const before = lines[e.line - 1];
    const after = before.replace(e.from, e.to);
    if (before !== after) { lines[e.line - 1] = after; changed += 1; }
  }
  if (!dryRun) writeFileSync(doc, lines.join('\n'));
}

console.log(`${dryRun ? 'would repair' : 'repaired'} ${changed} citation(s) across ${edits.size} file(s)`);
if (unquotable.length) {
  console.log(`\nNOT repaired -- these quote source text that is absent from the named function.`);
  console.log(`A wrong line number is clerical; a quote of code that does not exist is a false claim.\n`);
  for (const u of unquotable) {
    console.log(`  ${relative(root, u.doc)}:${u.line}  \`${u.symbol}\` (${u.file}:${u.actual[0].start})`);
    console.log(`     absent quote: "${u.quote.slice(0, 90)}"`);
  }
}
