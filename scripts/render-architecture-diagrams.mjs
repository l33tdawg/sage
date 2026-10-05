import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';

if (!process.argv[2]) throw new Error('Usage: node scripts/render-architecture-diagrams.mjs OUTPUT_DIRECTORY');
const output = resolve(process.argv[2]);
mkdirSync(output, { recursive: true });
const guide = readFileSync(new URL('../docs/ARCHITECTURE.md', import.meta.url), 'utf8');
const diagrams = [...guide.matchAll(/```mermaid\n([\s\S]*?)```/g)];
for (const [index, match] of diagrams.entries()) {
  const stem = join(output, `architecture-${index + 1}`);
  writeFileSync(`${stem}.mmd`, match[1]);
  for (const format of ['svg', 'png']) {
    execFileSync('mmdc', ['-i', `${stem}.mmd`, '-o', `${stem}.${format}`, '-b', 'white', '-w', '1400'], { stdio: 'inherit' });
  }
}
console.log(`Rendered ${diagrams.length} architecture diagrams in ${output}`);
