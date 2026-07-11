import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const files = [];

function walk(dir) {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      if (entry !== 'vendor') {
        walk(path);
      }
      continue;
    }
    if (entry.endsWith('.js')) {
      files.push(path);
    }
  }
}

walk('web/static/js');

for (const file of files) {
  const result = spawnSync(process.execPath, ['--check', file], { stdio: 'inherit' });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

// Browser-native alerts, confirmations, and prompts break the CEREBRUM theme
// and bypass our accessible dialog behavior. Guard the whole static UI, not
// only app.js, so a later page cannot regress silently.
for (const file of files) {
  const source = readFileSync(file, 'utf8');
  const executableSource = source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/.*$/gm, '');
  const nativeDialog = executableSource.match(/\b(?:window\.)?(alert|confirm|prompt)[ \t]*\(/);
  if (nativeDialog) {
    console.error(`${file} contains native ${nativeDialog[1]}(); use a themed CEREBRUM dialog.`);
    process.exit(1);
  }
}

console.log(`Checked ${files.length} JavaScript files.`);
