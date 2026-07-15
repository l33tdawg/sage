#!/usr/bin/env node

import { createHash, randomUUID } from 'node:crypto';
import {
  link,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  unlink,
  writeFile,
} from 'node:fs/promises';
import {
  basename,
  dirname,
  join,
  posix,
  relative,
  resolve,
  sep,
} from 'node:path';
import { pathToFileURL } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

const SARIF_VERSION = '2.1.0';
const SOURCE_ROOT_BASE_ID = '%SRCROOT%';
const EXPECTED_VENDOR_PREFIX = 'third_party/cometbft/';
const EXPECTED_TEST_SUFFIX = '_sage_test.go';
const EXPECTED_SOURCE_COMMIT = 'feb2aea4dc271d612129afc958cb844713ec792b';
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const LINE_HASH_PATTERN = /^[0-9a-f]{1,64}:[1-9][0-9]*$/u;
const GUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requireRecord(value, context) {
  if (!isRecord(value)) {
    throw new TypeError(`${context} must be an object`);
  }
  return value;
}

function requireNonEmptyString(value, context) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${context} must be a non-empty string`);
  }
  return value;
}

function optionalArray(record, key, context) {
  if (!(key in record)) {
    return undefined;
  }
  if (!Array.isArray(record[key])) {
    throw new TypeError(`${context}.${key} must be an array`);
  }
  return record[key];
}

function positiveInteger(value, context) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${context} must be a positive integer`);
  }
  return value;
}

function repoRelativeURI(uri, uriBaseID) {
  if (typeof uri !== 'string' || uri.length === 0) {
    return undefined;
  }
  if (uriBaseID !== undefined && uriBaseID !== SOURCE_ROOT_BASE_ID) {
    return undefined;
  }

  const pathOnly = uri.split(/[?#]/u, 1)[0];
  let decoded;
  try {
    decoded = decodeURIComponent(pathOnly);
  } catch {
    return undefined;
  }
  if (
    decoded.length === 0
    || decoded.includes('\0')
    || decoded.includes('\\')
    || decoded.startsWith('/')
    || /^[A-Za-z][A-Za-z0-9+.-]*:/u.test(decoded)
  ) {
    return undefined;
  }

  const normalized = posix.normalize(decoded);
  if (
    normalized === '.'
    || normalized === '..'
    || normalized.startsWith('../')
    || normalized.startsWith('/')
  ) {
    return undefined;
  }
  return normalized;
}

function validateManifestPath(path, context, vendorPrefix) {
  requireNonEmptyString(path, context);
  if (repoRelativeURI(path, SOURCE_ROOT_BASE_ID) !== path || !path.startsWith(vendorPrefix)) {
    throw new TypeError(`${context} must be a normalized path below ${vendorPrefix}`);
  }
  return path;
}

function validateSHA256(value, context) {
  if (typeof value !== 'string' || !SHA256_PATTERN.test(value)) {
    throw new TypeError(`${context} must be a lowercase SHA-256 digest`);
  }
  return value;
}

function validateRegion(value, context) {
  const region = requireRecord(value, context);
  const normalized = {
    startLine: positiveInteger(region.startLine, `${context}.startLine`),
    startColumn: positiveInteger(region.startColumn, `${context}.startColumn`),
    endLine: positiveInteger(region.endLine, `${context}.endLine`),
    endColumn: positiveInteger(region.endColumn, `${context}.endColumn`),
  };
  if (
    normalized.endLine < normalized.startLine
    || (
      normalized.endLine === normalized.startLine
      && normalized.endColumn < normalized.startColumn
    )
  ) {
    throw new TypeError(`${context} must end at or after its start`);
  }
  return Object.freeze(normalized);
}

function findingKey(finding) {
  return JSON.stringify([
    finding.ruleId,
    finding.path,
    finding.region.startLine,
    finding.region.startColumn,
    finding.region.endLine,
    finding.region.endColumn,
    finding.primaryLocationLineHash,
  ]);
}

export function validateBaselineManifest(value) {
  const manifest = requireRecord(value, 'baseline manifest');
  if (manifest.schemaVersion !== 1) {
    throw new TypeError('baseline manifest.schemaVersion must be 1');
  }

  const provenance = requireRecord(manifest.provenance, 'baseline manifest.provenance');
  for (const key of ['module', 'version', 'sourceCommit', 'source', 'audit']) {
    requireNonEmptyString(provenance[key], `baseline manifest.provenance.${key}`);
  }
  if (provenance.sourceCommit !== EXPECTED_SOURCE_COMMIT) {
    throw new TypeError(
      `baseline manifest.provenance.sourceCommit must be ${EXPECTED_SOURCE_COMMIT}`,
    );
  }
  const codeql = requireRecord(manifest.codeql, 'baseline manifest.codeql');
  const driverName = requireNonEmptyString(
    codeql.driverName,
    'baseline manifest.codeql.driverName',
  );
  const semanticVersion = requireNonEmptyString(
    codeql.semanticVersion,
    'baseline manifest.codeql.semanticVersion',
  );
  const automationId = requireNonEmptyString(
    codeql.automationId,
    'baseline manifest.codeql.automationId',
  );
  const extensionValues = requireRecord(
    codeql.extensions,
    'baseline manifest.codeql.extensions',
  );
  const extensions = new Map();
  for (const [name, version] of Object.entries(extensionValues)) {
    requireNonEmptyString(name, 'baseline manifest.codeql.extensions key');
    extensions.set(
      name,
      requireNonEmptyString(version, `baseline manifest.codeql.extensions[${name}]`),
    );
  }
  if (extensions.size === 0) {
    throw new TypeError('baseline manifest.codeql.extensions must not be empty');
  }
  if (manifest.vendorPrefix !== EXPECTED_VENDOR_PREFIX) {
    throw new TypeError(`baseline manifest.vendorPrefix must be ${EXPECTED_VENDOR_PREFIX}`);
  }
  if (manifest.protectedTestSuffix !== EXPECTED_TEST_SUFFIX) {
    throw new TypeError(`baseline manifest.protectedTestSuffix must be ${EXPECTED_TEST_SUFFIX}`);
  }

  const counts = requireRecord(manifest.counts, 'baseline manifest.counts');
  const expectedCounts = {
    sinkFiles: positiveInteger(counts.sinkFiles, 'baseline manifest.counts.sinkFiles'),
    evidenceFiles: positiveInteger(
      counts.evidenceFiles,
      'baseline manifest.counts.evidenceFiles',
    ),
    overlays: positiveInteger(counts.overlays, 'baseline manifest.counts.overlays'),
    findings: positiveInteger(counts.findings, 'baseline manifest.counts.findings'),
  };

  const sinkFileValues = requireRecord(manifest.sinkFiles, 'baseline manifest.sinkFiles');
  const sinkFiles = new Map();
  for (const [path, digest] of Object.entries(sinkFileValues)) {
    validateManifestPath(path, `baseline manifest.sinkFiles[${path}]`, manifest.vendorPrefix);
    sinkFiles.set(path, validateSHA256(digest, `baseline manifest.sinkFiles[${path}]`));
  }
  if (sinkFiles.size !== expectedCounts.sinkFiles) {
    throw new TypeError('baseline manifest sink-file count does not match counts.sinkFiles');
  }

  const evidenceFileValues = requireRecord(
    manifest.evidenceFiles,
    'baseline manifest.evidenceFiles',
  );
  const evidenceFiles = new Map();
  for (const [path, digest] of Object.entries(evidenceFileValues)) {
    validateManifestPath(path, `baseline manifest.evidenceFiles[${path}]`, manifest.vendorPrefix);
    if (sinkFiles.has(path)) {
      throw new TypeError(`baseline path cannot be both a sink and evidence file: ${path}`);
    }
    evidenceFiles.set(
      path,
      validateSHA256(digest, `baseline manifest.evidenceFiles[${path}]`),
    );
  }
  if (evidenceFiles.size !== expectedCounts.evidenceFiles) {
    throw new TypeError(
      'baseline manifest evidence-file count does not match counts.evidenceFiles',
    );
  }

  const overlayValues = requireRecord(manifest.overlays, 'baseline manifest.overlays');
  const overlays = new Map();
  for (const [path, overlayValue] of Object.entries(overlayValues)) {
    validateManifestPath(path, `baseline manifest.overlays[${path}]`, manifest.vendorPrefix);
    if (sinkFiles.has(path) || evidenceFiles.has(path)) {
      throw new TypeError(`baseline path cannot be both an upstream evidence file and overlay: ${path}`);
    }
    const overlay = requireRecord(overlayValue, `baseline manifest.overlays[${path}]`);
    const lineCount = positiveInteger(
      overlay.lineCount,
      `baseline manifest.overlays[${path}].lineCount`,
    );
    const ranges = optionalArray(
      overlay,
      'modifiedLineRanges',
      `baseline manifest.overlays[${path}]`,
    );
    if (ranges === undefined || ranges.length === 0) {
      throw new TypeError(`baseline manifest.overlays[${path}] needs modifiedLineRanges`);
    }
    let previousEnd = 0;
    const modifiedLineRanges = ranges.map((range, index) => {
      if (!Array.isArray(range) || range.length !== 2) {
        throw new TypeError(
          `baseline manifest.overlays[${path}].modifiedLineRanges[${index}] must be [start, end]`,
        );
      }
      const start = positiveInteger(range[0], `overlay ${path} range ${index} start`);
      const end = positiveInteger(range[1], `overlay ${path} range ${index} end`);
      if (end < start || start <= previousEnd) {
        throw new TypeError(`overlay ${path} line ranges must be sorted and non-overlapping`);
      }
      previousEnd = end;
      return Object.freeze([start, end]);
    });
    if (previousEnd > lineCount) {
      throw new TypeError(`overlay ${path} modified line range exceeds lineCount`);
    }
    overlays.set(path, Object.freeze({
      sha256: validateSHA256(overlay.sha256, `baseline manifest.overlays[${path}].sha256`),
      unchangedLineSha256: validateSHA256(
        overlay.unchangedLineSha256,
        `baseline manifest.overlays[${path}].unchangedLineSha256`,
      ),
      lineCount,
      modifiedLineRanges: Object.freeze(modifiedLineRanges),
    }));
  }
  if (overlays.size !== expectedCounts.overlays) {
    throw new TypeError('baseline manifest overlay count does not match counts.overlays');
  }

  if (!Array.isArray(manifest.findings) || manifest.findings.length !== expectedCounts.findings) {
    throw new TypeError('baseline manifest finding count does not match counts.findings');
  }
  const findingKeys = new Set();
  const correlationGUIDs = new Set();
  const findings = manifest.findings.map((findingValue, index) => {
    const context = `baseline manifest.findings[${index}]`;
    const finding = requireRecord(findingValue, context);
    const path = validateManifestPath(finding.path, `${context}.path`, manifest.vendorPrefix);
    if (overlays.has(path) || path.endsWith(manifest.protectedTestSuffix)) {
      throw new TypeError(`${context}.path cannot point to SAGE-owned CometBFT code`);
    }
    if (!sinkFiles.has(path)) {
      throw new TypeError(`${context}.path must refer to a hash-bound sink file`);
    }
    const primaryLocationLineHash = requireNonEmptyString(
      finding.primaryLocationLineHash,
      `${context}.primaryLocationLineHash`,
    );
    if (!LINE_HASH_PATTERN.test(primaryLocationLineHash)) {
      throw new TypeError(`${context}.primaryLocationLineHash has an invalid format`);
    }
    const correlationGuid = requireNonEmptyString(
      finding.correlationGuid,
      `${context}.correlationGuid`,
    );
    if (!GUID_PATTERN.test(correlationGuid)) {
      throw new TypeError(`${context}.correlationGuid has an invalid format`);
    }
    if (correlationGUIDs.has(correlationGuid)) {
      throw new TypeError(`${context}.correlationGuid must be unique`);
    }
    correlationGUIDs.add(correlationGuid);
    const normalized = Object.freeze({
      ruleId: requireNonEmptyString(finding.ruleId, `${context}.ruleId`),
      path,
      region: validateRegion(finding.region, `${context}.region`),
      primaryLocationLineHash,
      correlationGuid,
    });
    const key = findingKey(normalized);
    if (findingKeys.has(key)) {
      throw new TypeError(`${context} duplicates another audited finding`);
    }
    findingKeys.add(key);
    return normalized;
  });

  return Object.freeze({
    provenance: Object.freeze({ ...provenance }),
    driverName,
    semanticVersion,
    automationId,
    extensions,
    vendorPrefix: manifest.vendorPrefix,
    protectedTestSuffix: manifest.protectedTestSuffix,
    sinkFiles,
    evidenceFiles,
    overlays,
    findings: Object.freeze(findings),
  });
}

async function readFileFacts(path) {
  const bytes = await readFile(path);
  const source = bytes.toString('utf8');
  const lines = source.split('\n');
  if (lines.at(-1) === '') {
    lines.pop();
  }
  return Object.freeze({
    sha256: createHash('sha256').update(bytes).digest('hex'),
    lineCount: lines.length,
    characterCount: source.length,
    byteCount: bytes.length,
  });
}

async function unchangedOverlayLineDigest(path, modifiedLineRanges) {
  const source = await readFile(path, 'utf8');
  const lines = source.split('\n');
  if (lines.at(-1) === '') {
    lines.pop();
  }
  const finalRangeEnd = modifiedLineRanges.at(-1)?.[1] ?? 0;
  if (finalRangeEnd > lines.length) {
    throw new Error(`modified line range ends at ${finalRangeEnd}, file has ${lines.length} lines`);
  }
  const digest = createHash('sha256');
  digest.update('sage-codeql-overlay-unchanged-lines-v1\0');
  lines.forEach((line, index) => {
    const lineNumber = index + 1;
    const modified = modifiedLineRanges.some(
      ([start, end]) => lineNumber >= start && lineNumber <= end,
    );
    if (!modified) {
      digest.update(String(lineNumber));
      digest.update('\0');
      digest.update(line);
      digest.update('\n');
    }
  });
  return Object.freeze({
    sha256: digest.digest('hex'),
    lineCount: lines.length,
  });
}

export async function loadBaselineManifest(manifestPath, sourceRoot = process.cwd()) {
  let manifestValue;
  try {
    manifestValue = JSON.parse(await readFile(manifestPath, 'utf8'));
  } catch (error) {
    throw new Error(`cannot read valid baseline manifest ${manifestPath}: ${error.message}`, {
      cause: error,
    });
  }
  const baseline = validateBaselineManifest(manifestValue);
  const verifiedPaths = new Set();
  const verifiedFileFacts = new Map();
  const hashMismatches = [];
  const expectedFiles = [
    ...baseline.sinkFiles.entries(),
    ...baseline.evidenceFiles.entries(),
    ...[...baseline.overlays.entries()].map(([path, overlay]) => [path, overlay.sha256]),
  ];
  for (const [path, expected] of expectedFiles) {
    let facts;
    try {
      facts = await readFileFacts(resolve(sourceRoot, path));
    } catch (error) {
      hashMismatches.push(Object.freeze({
        path,
        expected,
        actual: undefined,
        error: error?.code ?? error.message,
      }));
      continue;
    }
    if (facts.sha256 === expected) {
      verifiedPaths.add(path);
      verifiedFileFacts.set(path, facts);
    } else {
      hashMismatches.push(Object.freeze({ path, expected, actual: facts.sha256 }));
    }
  }
  for (const [path, overlay] of baseline.overlays) {
    if (!verifiedPaths.has(path)) {
      continue;
    }
    let actual;
    try {
      actual = await unchangedOverlayLineDigest(
        resolve(sourceRoot, path),
        overlay.modifiedLineRanges,
      );
    } catch (error) {
      verifiedPaths.delete(path);
      verifiedFileFacts.delete(path);
      hashMismatches.push(Object.freeze({
        path: `${path}#unchanged-lines`,
        expected: overlay.unchangedLineSha256,
        actual: undefined,
        error: error?.code ?? error.message,
      }));
      continue;
    }
    if (actual.lineCount !== overlay.lineCount) {
      verifiedPaths.delete(path);
      verifiedFileFacts.delete(path);
      hashMismatches.push(Object.freeze({
        path: `${path}#line-count`,
        expected: String(overlay.lineCount),
        actual: String(actual.lineCount),
      }));
      continue;
    }
    if (actual.sha256 !== overlay.unchangedLineSha256) {
      verifiedPaths.delete(path);
      verifiedFileFacts.delete(path);
      hashMismatches.push(Object.freeze({
        path: `${path}#unchanged-lines`,
        expected: overlay.unchangedLineSha256,
        actual: actual.sha256,
      }));
    }
  }
  return Object.freeze({
    ...baseline,
    verifiedPaths,
    verifiedFileFacts,
    hashMismatches: Object.freeze(hashMismatches),
  });
}

function resolveArtifactPath(artifactLocation, run, seenIndexes = new Set()) {
  requireRecord(artifactLocation, 'artifactLocation');
  if ('uri' in artifactLocation) {
    const path = repoRelativeURI(artifactLocation.uri, artifactLocation.uriBaseId);
    if (path === undefined) {
      return undefined;
    }
    // GitHub's post-processed CodeQL SARIF keeps the correct explicit URI but
    // can retain a stale artifact index on related and flow locations. Prefer
    // the self-contained URI; use the index only when no URI was emitted.
    return path;
  }
  if ('index' in artifactLocation) {
    const index = artifactLocation.index;
    const artifacts = optionalArray(run, 'artifacts', 'run');
    if (
      !Number.isSafeInteger(index)
      || index < 0
      || artifacts === undefined
      || index >= artifacts.length
    ) {
      return undefined;
    } else if (seenIndexes.has(index)) {
      return undefined;
    } else {
      const artifact = requireRecord(artifacts[index], `run.artifacts[${index}]`);
      if (!('location' in artifact)) {
        return undefined;
      }
      const nextSeen = new Set(seenIndexes);
      nextSeen.add(index);
      const indexedPath = resolveArtifactPath(artifact.location, run, nextSeen);
      if (indexedPath === undefined) {
        return undefined;
      }
      return indexedPath;
    }
  }
  return undefined;
}

function regionLineSpan(region) {
  if (
    !isRecord(region)
    || !Number.isSafeInteger(region.startLine)
    || !Number.isSafeInteger(region.endLine)
    || region.startLine <= 0
    || region.endLine < region.startLine
  ) {
    return undefined;
  }
  return [region.startLine, region.endLine];
}

function validEvidenceRegion(region) {
  if (!isRecord(region)) {
    return false;
  }
  const supportedKeys = new Set([
    'startLine',
    'startColumn',
    'endLine',
    'endColumn',
    'charOffset',
    'charLength',
    'byteOffset',
    'byteLength',
  ]);
  if (Object.keys(region).some((key) => !supportedKeys.has(key))) {
    // Extra region metadata is valid SARIF, but it is outside this baseline's
    // coordinate-only provenance contract and therefore remains visible.
    return false;
  }
  const hasLine = 'startLine' in region;
  const hasCharacter = 'charOffset' in region;
  const hasByte = 'byteOffset' in region;
  if (!hasLine && !hasCharacter && !hasByte) {
    return false;
  }

  const positiveCoordinates = ['startLine', 'startColumn', 'endLine', 'endColumn'];
  if (positiveCoordinates.some(
    (key) => key in region && (!Number.isSafeInteger(region[key]) || region[key] <= 0),
  )) {
    return false;
  }
  const nonNegativeCoordinates = ['charOffset', 'charLength', 'byteOffset', 'byteLength'];
  if (nonNegativeCoordinates.some(
    (key) => key in region && (!Number.isSafeInteger(region[key]) || region[key] < 0),
  )) {
    return false;
  }
  if (
    (('startColumn' in region || 'endLine' in region || 'endColumn' in region) && !hasLine)
    || ('charLength' in region && !hasCharacter)
    || ('byteLength' in region && !hasByte)
  ) {
    return false;
  }
  if ('endLine' in region && region.endLine < region.startLine) {
    return false;
  }
  if (
    hasLine
    && 'startColumn' in region
    && 'endColumn' in region
    && (region.endLine ?? region.startLine) === region.startLine
    && region.endColumn < region.startColumn
  ) {
    return false;
  }
  return true;
}

function normalizeSarifLineRegion(region) {
  if (
    isRecord(region)
    && Number.isSafeInteger(region.startLine)
    && !('endLine' in region)
  ) {
    // SARIF defines an omitted endLine as the same line as startLine. The
    // CodeQL action emits this compact form before upload; GitHub expands it
    // while ingesting the same result.
    return { ...region, endLine: region.startLine };
  }
  return region;
}

function regionWithinVerifiedFile(region, facts) {
  if (region === undefined) {
    return true;
  }
  if (
    ('startLine' in region && region.startLine > facts.lineCount)
    || ('endLine' in region && region.endLine > facts.lineCount)
  ) {
    return false;
  }
  if ('charOffset' in region) {
    const characterEnd = region.charOffset + (region.charLength ?? 0);
    if (region.charOffset > facts.characterCount || characterEnd > facts.characterCount) {
      return false;
    }
  }
  if ('byteOffset' in region) {
    const byteEnd = region.byteOffset + (region.byteLength ?? 0);
    if (region.byteOffset > facts.byteCount || byteEnd > facts.byteCount) {
      return false;
    }
  }
  return true;
}

function classifyArtifactEvidence(artifactLocation, evidenceRegion, run, baseline) {
  if (!isRecord(artifactLocation)) {
    return 'unknown';
  }
  if (evidenceRegion !== undefined && !validEvidenceRegion(evidenceRegion)) {
    return 'unknown';
  }
  const normalizedEvidenceRegion = normalizeSarifLineRegion(evidenceRegion);
  const path = resolveArtifactPath(artifactLocation, run);
  if (path === undefined) {
    return 'unknown';
  }
  if (!path.startsWith(baseline.vendorPrefix)) {
    return 'other';
  }
  if (path.endsWith(baseline.protectedTestSuffix)) {
    return 'other';
  }

  const overlay = baseline.overlays.get(path);
  const hashBound = overlay !== undefined
    || baseline.sinkFiles.has(path)
    || baseline.evidenceFiles.has(path);
  if (!hashBound) {
    // The exact audited traces are finite. A future vendor path is not assumed
    // to be upstream merely because it lives below third_party/cometbft: it
    // must be explicitly hash-bound before it can be suppressed.
    return 'unknown';
  }
  const facts = baseline.verifiedFileFacts.get(path);
  if (
    !baseline.verifiedPaths.has(path)
    || facts === undefined
    || !regionWithinVerifiedFile(normalizedEvidenceRegion, facts)
  ) {
    return 'unknown';
  }
  if (overlay !== undefined) {
    const coordinateKeys = ['endColumn', 'endLine', 'startColumn', 'startLine'];
    if (
      !isRecord(evidenceRegion)
      || !isRecord(normalizedEvidenceRegion)
      || Object.keys(normalizedEvidenceRegion).length !== coordinateKeys.length
      || !coordinateKeys.every((key) => key in normalizedEvidenceRegion)
    ) {
      // Overlay ownership is line-based. Mixed line/char/byte representations
      // are ambiguous even when each coordinate is independently in bounds.
      return 'unknown';
    }
    const span = regionLineSpan(normalizedEvidenceRegion);
    if (span === undefined) {
      return 'unknown';
    }
    if (overlay.modifiedLineRanges.some(([start, end]) => span[0] <= end && span[1] >= start)) {
      return 'other';
    }
    return 'vendor';
  }
  return 'vendor';
}

function collectPhysicalLocationEvidence(physicalValue, run, context, baseline) {
  const physical = requireRecord(physicalValue, context);
  if (!('artifactLocation' in physical)) {
    return ['unknown'];
  }
  const evidence = [classifyArtifactEvidence(
    physical.artifactLocation,
    physical.region,
    run,
    baseline,
  )];
  if ('address' in physical) {
    // Address records (inline or run-indexed) carry independent module/symbol
    // identity that is not covered by the source-file digest.
    evidence.push('unknown');
  }
  if ('contextRegion' in physical) {
    const contextRegion = requireRecord(
      physical.contextRegion,
      `${context}.contextRegion`,
    );
    evidence.push(classifyArtifactEvidence(
      physical.artifactLocation,
      contextRegion,
      run,
      baseline,
    ));
  }
  return evidence;
}

function collectLocationEvidence(locationValue, run, context, baseline) {
  const location = requireRecord(locationValue, context);
  if (!('physicalLocation' in location)) {
    return ['unknown'];
  }
  const physical = requireRecord(location.physicalLocation, `${context}.physicalLocation`);
  if (!('artifactLocation' in physical)) {
    return ['unknown'];
  }
  const evidence = collectPhysicalLocationEvidence(
    physical,
    run,
    `${context}.physicalLocation`,
    baseline,
  );
  const logicalLocations = optionalArray(location, 'logicalLocations', context);
  if (logicalLocations !== undefined && logicalLocations.length > 0) {
    // Logical names can identify first-party code even when the accompanying
    // physical location points at an upstream file. They have no byte-level
    // provenance binding, so an audited result carrying one remains visible.
    evidence.push('unknown');
  }
  const annotations = optionalArray(location, 'annotations', context);
  if (annotations !== undefined) {
    annotations.forEach((annotationValue, annotationIndex) => {
      const annotation = requireRecord(
        annotationValue,
        `${context}.annotations[${annotationIndex}]`,
      );
      evidence.push(classifyArtifactEvidence(
        physical.artifactLocation,
        annotation,
        run,
        baseline,
      ));
    });
  }
  return evidence;
}

function collectStackEvidence(stackValue, run, context, baseline, evidence) {
  const stack = requireRecord(stackValue, context);
  const frames = optionalArray(stack, 'frames', context);
  if (frames === undefined || frames.length === 0) {
    evidence.push('unknown');
    return;
  }
  frames.forEach((frameValue, frameIndex) => {
    const frameContext = `${context}.frames[${frameIndex}]`;
    const frame = requireRecord(frameValue, frameContext);
    if ('module' in frame) {
      evidence.push('unknown');
    }
    if (!('location' in frame)) {
      evidence.push('unknown');
      return;
    }
    evidence.push(...collectLocationEvidence(
      frame.location,
      run,
      `${frameContext}.location`,
      baseline,
    ));
  });
}

function collectGraphNodeEvidence(nodeValue, run, context, baseline, evidence) {
  const node = requireRecord(nodeValue, context);
  if (!('location' in node)) {
    evidence.push('unknown');
  } else {
    evidence.push(...collectLocationEvidence(
      node.location,
      run,
      `${context}.location`,
      baseline,
    ));
  }
  const children = optionalArray(node, 'children', context);
  if (children !== undefined) {
    children.forEach((childValue, childIndex) => {
      collectGraphNodeEvidence(
        childValue,
        run,
        `${context}.children[${childIndex}]`,
        baseline,
        evidence,
      );
    });
  }
}

function resolveThreadFlowLocation(threadLocationValue, run, context) {
  const threadLocation = requireRecord(threadLocationValue, context);
  if (!('index' in threadLocation)) {
    return threadLocation;
  }
  const index = threadLocation.index;
  const cachedLocations = optionalArray(run, 'threadFlowLocations', 'run');
  if (
    !Number.isSafeInteger(index)
    || index < 0
    || cachedLocations === undefined
    || index >= cachedLocations.length
  ) {
    return undefined;
  }
  const cached = requireRecord(cachedLocations[index], `run.threadFlowLocations[${index}]`);
  if ('index' in cached && cached.index !== index) {
    return undefined;
  }
  for (const [key, value] of Object.entries(threadLocation)) {
    if (key !== 'index' && !isDeepStrictEqual(value, cached[key])) {
      return undefined;
    }
  }
  return cached;
}

function collectThreadFlowLocationEvidence(
  threadLocationValue,
  run,
  context,
  baseline,
  evidence,
) {
  const threadLocation = resolveThreadFlowLocation(threadLocationValue, run, context);
  if (threadLocation === undefined) {
    evidence.push('unknown');
    return;
  }
  if (!('location' in threadLocation)) {
    evidence.push('unknown');
  } else {
    evidence.push(...collectLocationEvidence(
      threadLocation.location,
      run,
      `${context}.location`,
      baseline,
    ));
  }
  if ('module' in threadLocation) {
    evidence.push('unknown');
  }
  if ('stack' in threadLocation) {
    collectStackEvidence(
      threadLocation.stack,
      run,
      `${context}.stack`,
      baseline,
      evidence,
    );
  }
}

function collectAdditionalLocationCarriers(result, run, context, baseline, evidence) {
  if ('analysisTarget' in result) {
    if (isRecord(result.analysisTarget)) {
      evidence.push(classifyArtifactEvidence(result.analysisTarget, undefined, run, baseline));
    } else {
      evidence.push('unknown');
    }
  }

  const attachments = optionalArray(result, 'attachments', context);
  if (attachments !== undefined) {
    attachments.forEach((attachmentValue, attachmentIndex) => {
      const attachmentContext = `${context}.attachments[${attachmentIndex}]`;
      const attachment = requireRecord(attachmentValue, attachmentContext);
      if (!('artifactLocation' in attachment)) {
        evidence.push('unknown');
        return;
      }
      const regions = optionalArray(attachment, 'regions', attachmentContext);
      const rectangles = optionalArray(attachment, 'rectangles', attachmentContext);
      if (rectangles !== undefined && rectangles.length > 0) {
        // Image rectangles cannot be mapped to reviewed Go line ranges.
        evidence.push('unknown');
      }
      if (regions === undefined || regions.length === 0) {
        evidence.push(classifyArtifactEvidence(
          attachment.artifactLocation,
          undefined,
          run,
          baseline,
        ));
        return;
      }
      regions.forEach((attachmentRegion) => {
        if (!isRecord(attachmentRegion)) {
          evidence.push('unknown');
          return;
        }
        evidence.push(classifyArtifactEvidence(
          attachment.artifactLocation,
          attachmentRegion,
          run,
          baseline,
        ));
      });
    });
  }

  const fixes = optionalArray(result, 'fixes', context);
  if (fixes !== undefined) {
    fixes.forEach((fixValue, fixIndex) => {
      const fixContext = `${context}.fixes[${fixIndex}]`;
      const fix = requireRecord(fixValue, fixContext);
      const changes = optionalArray(fix, 'artifactChanges', fixContext);
      if (changes === undefined || changes.length === 0) {
        evidence.push('unknown');
        return;
      }
      changes.forEach((changeValue, changeIndex) => {
        const changeContext = `${fixContext}.artifactChanges[${changeIndex}]`;
        const change = requireRecord(changeValue, changeContext);
        if (!('artifactLocation' in change)) {
          evidence.push('unknown');
          return;
        }
        const replacements = optionalArray(change, 'replacements', changeContext);
        if (replacements === undefined || replacements.length === 0) {
          evidence.push(classifyArtifactEvidence(
            change.artifactLocation,
            undefined,
            run,
            baseline,
          ));
          return;
        }
        replacements.forEach((replacementValue, replacementIndex) => {
          const replacement = requireRecord(
            replacementValue,
            `${changeContext}.replacements[${replacementIndex}]`,
          );
          if (!('deletedRegion' in replacement)) {
            evidence.push('unknown');
            return;
          }
          if (!isRecord(replacement.deletedRegion)) {
            evidence.push('unknown');
            return;
          }
          evidence.push(classifyArtifactEvidence(
            change.artifactLocation,
            replacement.deletedRegion,
            run,
            baseline,
          ));
        });
      });
    });
  }

  const graphs = optionalArray(result, 'graphs', context);
  if (graphs !== undefined) {
    graphs.forEach((graphValue, graphIndex) => {
      const graphContext = `${context}.graphs[${graphIndex}]`;
      const graph = requireRecord(graphValue, graphContext);
      const nodes = optionalArray(graph, 'nodes', graphContext);
      if (nodes === undefined || nodes.length === 0) {
        evidence.push('unknown');
        return;
      }
      nodes.forEach((nodeValue, nodeIndex) => {
        const nodeContext = `${graphContext}.nodes[${nodeIndex}]`;
        collectGraphNodeEvidence(
          nodeValue,
          run,
          nodeContext,
          baseline,
          evidence,
        );
      });
    });
  }

  // Graph traversals can refer to run-level graphs whose node locations live
  // outside the result object. Without resolving that separate graph table we
  // cannot prove all traversed nodes are vendored, so retain the result.
  const graphTraversals = optionalArray(result, 'graphTraversals', context);
  if (graphTraversals !== undefined && graphTraversals.length > 0) {
    evidence.push('unknown');
  }

  const suppressions = optionalArray(result, 'suppressions', context);
  if (suppressions !== undefined) {
    suppressions.forEach((suppressionValue, suppressionIndex) => {
      const suppressionContext = `${context}.suppressions[${suppressionIndex}]`;
      const suppression = requireRecord(suppressionValue, suppressionContext);
      if ('location' in suppression) {
        evidence.push(...collectLocationEvidence(
          suppression.location,
          run,
          `${suppressionContext}.location`,
          baseline,
        ));
      }
    });
  }

  if ('provenance' in result) {
    const provenanceContext = `${context}.provenance`;
    const provenance = requireRecord(result.provenance, provenanceContext);
    const conversionSources = optionalArray(
      provenance,
      'conversionSources',
      provenanceContext,
    );
    if (conversionSources !== undefined) {
      conversionSources.forEach((physicalValue, physicalIndex) => {
        evidence.push(...collectPhysicalLocationEvidence(
          physicalValue,
          run,
          `${provenanceContext}.conversionSources[${physicalIndex}]`,
          baseline,
        ));
      });
    }
  }
}

function collectResultLocations(result, run, context, baseline) {
  const evidence = [];
  const primaryEvidence = [];
  const primaryLocations = optionalArray(result, 'locations', context);
  if (primaryLocations !== undefined) {
    primaryLocations.forEach((location, index) => {
      const classifications = collectLocationEvidence(
        location,
        run,
        `${context}.locations[${index}]`,
        baseline,
      );
      primaryEvidence.push(classifications[0] ?? 'unknown');
      evidence.push(...classifications);
    });
  }

  const relatedLocations = optionalArray(result, 'relatedLocations', context);
  if (relatedLocations !== undefined) {
    relatedLocations.forEach((location, index) => {
      evidence.push(...collectLocationEvidence(
        location,
        run,
        `${context}.relatedLocations[${index}]`,
        baseline,
      ));
    });
  }

  const codeFlows = optionalArray(result, 'codeFlows', context);
  if (codeFlows !== undefined) {
    codeFlows.forEach((codeFlowValue, codeFlowIndex) => {
      const codeFlowContext = `${context}.codeFlows[${codeFlowIndex}]`;
      const codeFlow = requireRecord(codeFlowValue, codeFlowContext);
      const threadFlows = optionalArray(codeFlow, 'threadFlows', codeFlowContext);
      if (threadFlows === undefined || threadFlows.length === 0) {
        evidence.push('unknown');
        return;
      }
      threadFlows.forEach((threadFlowValue, threadFlowIndex) => {
        const threadFlowContext = `${codeFlowContext}.threadFlows[${threadFlowIndex}]`;
        const threadFlow = requireRecord(threadFlowValue, threadFlowContext);
        const locations = optionalArray(threadFlow, 'locations', threadFlowContext);
        if (locations === undefined || locations.length === 0) {
          evidence.push('unknown');
          return;
        }
        locations.forEach((threadLocationValue, locationIndex) => {
          const threadLocationContext = `${threadFlowContext}.locations[${locationIndex}]`;
          collectThreadFlowLocationEvidence(
            threadLocationValue,
            run,
            threadLocationContext,
            baseline,
            evidence,
          );
        });
      });
    });
  }

  const stacks = optionalArray(result, 'stacks', context);
  if (stacks !== undefined) {
    stacks.forEach((stackValue, stackIndex) => {
      collectStackEvidence(
        stackValue,
        run,
        `${context}.stacks[${stackIndex}]`,
        baseline,
        evidence,
      );
    });
  }

  collectAdditionalLocationCarriers(result, run, context, baseline, evidence);

  return {
    evidence,
    hasVendorPrimary: primaryEvidence.length > 0
      && primaryEvidence.every((classification) => classification === 'vendor'),
  };
}

function resultFindingKey(result, run, context) {
  const locations = optionalArray(result, 'locations', context);
  if (locations === undefined || locations.length !== 1 || typeof result.ruleId !== 'string') {
    return undefined;
  }
  const location = requireRecord(locations[0], `${context}.locations[0]`);
  if (!('physicalLocation' in location)) {
    return undefined;
  }
  const physical = requireRecord(location.physicalLocation, `${context}.locations[0].physicalLocation`);
  if (!('artifactLocation' in physical) || !isRecord(physical.region)) {
    return undefined;
  }
  const path = resolveArtifactPath(physical.artifactLocation, run);
  const partialFingerprints = isRecord(result.partialFingerprints)
    ? result.partialFingerprints
    : undefined;
  if (
    path === undefined
    || typeof partialFingerprints?.primaryLocationLineHash !== 'string'
  ) {
    return undefined;
  }
  const fields = [
    physical.region.startLine,
    physical.region.startColumn,
    physical.region.endLine ?? physical.region.startLine,
    physical.region.endColumn,
  ];
  if (!fields.every((field) => Number.isSafeInteger(field) && field > 0)) {
    return undefined;
  }
  return findingKey({
    ruleId: result.ruleId,
    path,
    region: {
      startLine: fields[0],
      startColumn: fields[1],
      endLine: fields[2],
      endColumn: fields[3],
    },
    primaryLocationLineHash: partialFingerprints.primaryLocationLineHash,
  });
}

function auditedToolRun(run, baseline) {
  if (
    !isRecord(run.tool)
    || !isRecord(run.tool.driver)
    || run.tool.driver.name !== baseline.driverName
    || run.tool.driver.semanticVersion !== baseline.semanticVersion
    || !isRecord(run.automationDetails)
    || run.automationDetails.id !== baseline.automationId
    || 'originalUriBaseIds' in run
    || !Array.isArray(run.tool.extensions)
    || run.tool.extensions.length !== baseline.extensions.size
  ) {
    return false;
  }
  const actualExtensions = new Map();
  for (const extension of run.tool.extensions) {
    if (
      !isRecord(extension)
      || typeof extension.name !== 'string'
      || typeof extension.semanticVersion !== 'string'
      || actualExtensions.has(extension.name)
    ) {
      return false;
    }
    actualExtensions.set(extension.name, extension.semanticVersion);
  }
  return [...baseline.extensions.entries()].every(
    ([name, version]) => actualExtensions.get(name) === version,
  );
}

function remainingFindingMultiset(baseline) {
  const findings = new Map();
  for (const finding of baseline.findings) {
    if (!baseline.verifiedPaths.has(finding.path)) {
      continue;
    }
    const key = findingKey(finding);
    const entry = findings.get(key);
    if (entry === undefined) {
      findings.set(key, { finding, remaining: 1 });
    } else {
      entry.remaining += 1;
    }
  }
  return findings;
}

function baselineAuditState(baseline, expectedAutomationId) {
  return {
    expectedAutomationId,
    expectedAutomationRuns: 0,
    auditedRuns: 0,
    expectedFindingKeys: new Set(
      baseline.findings
        .filter((finding) => baseline.verifiedPaths.has(finding.path))
        .map((finding) => findingKey(finding)),
    ),
    observedFindingKeys: new Set(),
  };
}

function observeAuditedFinding(result, run, context, baseline, auditState) {
  if (!auditedToolRun(run, baseline)) {
    return;
  }
  const key = resultFindingKey(result, run, context);
  if (key !== undefined && auditState.expectedFindingKeys.has(key)) {
    auditState.observedFindingKeys.add(key);
  }
}

function assertExpectedAuditCoverage(auditState, baseline) {
  if (auditState.expectedAutomationId === undefined) {
    return;
  }
  if (auditState.expectedAutomationRuns === 0) {
    throw new Error(
      `SARIF contains no run for expected automation ID ${auditState.expectedAutomationId}`,
    );
  }
  if (auditState.expectedAutomationId !== baseline.automationId) {
    return;
  }
  if (auditState.auditedRuns !== auditState.expectedAutomationRuns) {
    throw new Error('audited CodeQL run metadata does not match the pinned Go baseline');
  }
  const missing = [...auditState.expectedFindingKeys]
    .filter((key) => !auditState.observedFindingKeys.has(key));
  if (missing.length === 0) {
    return;
  }
  throw new Error(
    `audited CodeQL run is missing ${missing.length} expected CometBFT baseline finding(s)`,
  );
}

function shouldSuppressResult(result, run, context, baseline, remainingFindings) {
  if (
    'occurrenceCount' in result
    && (!Number.isSafeInteger(result.occurrenceCount) || result.occurrenceCount !== 1)
  ) {
    return false;
  }
  const { evidence, hasVendorPrimary } = collectResultLocations(
    result,
    run,
    context,
    baseline,
  );
  if (
    !auditedToolRun(run, baseline)
    || !hasVendorPrimary
    || evidence.length === 0
    || !evidence.every((classification) => classification === 'vendor')
  ) {
    return false;
  }
  const key = resultFindingKey(result, run, context);
  const entry = key === undefined ? undefined : remainingFindings.get(key);
  if (entry === undefined || entry.remaining === 0) {
    return false;
  }
  // GitHub adds correlationGuid while ingesting SARIF, so it is absent from
  // the pinned CodeQL action's pre-upload document that this filter consumes.
  // When a producer does provide it, retain any result that disagrees with the
  // server-audited GUID recorded in the manifest.
  if (
    'correlationGuid' in result
    && result.correlationGuid !== entry.finding.correlationGuid
  ) {
    return false;
  }
  entry.remaining -= 1;
  return true;
}

function filterSarifDocumentWithState(value, baseline, remainingFindings, auditState) {
  const document = requireRecord(value, 'SARIF document');
  if (document.version !== SARIF_VERSION) {
    throw new TypeError(`SARIF document.version must be ${SARIF_VERSION}`);
  }
  if (!Array.isArray(document.runs) || document.runs.length === 0) {
    throw new TypeError('SARIF document.runs must be a non-empty array');
  }

  let total = 0;
  let suppressed = 0;
  const runs = document.runs.map((runValue, runIndex) => {
    const run = requireRecord(runValue, `SARIF document.runs[${runIndex}]`);
    if (
      isRecord(run.automationDetails)
      && run.automationDetails.id === auditState.expectedAutomationId
    ) {
      auditState.expectedAutomationRuns += 1;
    }
    if (auditedToolRun(run, baseline)) {
      auditState.auditedRuns += 1;
    }
    const results = optionalArray(run, 'results', `SARIF document.runs[${runIndex}]`);
    if (results === undefined) {
      return run;
    }
    const retained = [];
    results.forEach((resultValue, resultIndex) => {
      const context = `SARIF document.runs[${runIndex}].results[${resultIndex}]`;
      const result = requireRecord(resultValue, context);
      total += 1;
      observeAuditedFinding(result, run, context, baseline, auditState);
      if (shouldSuppressResult(result, run, context, baseline, remainingFindings)) {
        suppressed += 1;
      } else {
        retained.push(result);
      }
    });
    return { ...run, results: retained };
  });

  return {
    document: { ...document, runs },
    stats: { total, suppressed, retained: total - suppressed },
  };
}

export function filterSarifDocument(value, baseline) {
  if (
    !(baseline?.verifiedPaths instanceof Set)
    || !(baseline?.verifiedFileFacts instanceof Map)
  ) {
    throw new TypeError('filterSarifDocument requires a loaded baseline manifest');
  }
  return filterSarifDocumentWithState(
    value,
    baseline,
    remainingFindingMultiset(baseline),
    baselineAuditState(baseline, undefined),
  );
}

async function pathExists(path) {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return false;
    }
    throw error;
  }
}

async function findSarifFiles(inputDirectory, currentDirectory = inputDirectory) {
  const entries = await readdir(currentDirectory, { withFileTypes: true });
  const files = [];
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const path = join(currentDirectory, entry.name);
    if (entry.isDirectory()) {
      files.push(...await findSarifFiles(inputDirectory, path));
    } else if (entry.isFile() && entry.name.endsWith('.sarif')) {
      files.push(relative(inputDirectory, path));
    }
  }
  return files;
}

async function loadAndFilter(inputPath, baseline, remainingFindings, auditState) {
  let value;
  try {
    value = JSON.parse(await readFile(inputPath, 'utf8'));
  } catch (error) {
    throw new Error(`cannot read valid JSON from ${inputPath}: ${error.message}`, { cause: error });
  }
  return filterSarifDocumentWithState(value, baseline, remainingFindings, auditState);
}

function assertHashBindings(baseline) {
  if (baseline.hashMismatches.length === 0) {
    return;
  }
  const details = baseline.hashMismatches.map(({ path, expected, actual, error }) => (
    `${path}: expected ${expected}, got ${actual ?? error}`
  ));
  throw new Error(`CometBFT baseline hash mismatch:\n${details.join('\n')}`);
}

function stagingPathFor(outputPath) {
  return join(
    dirname(outputPath),
    `.${basename(outputPath)}.sage-codeql-${randomUUID()}`,
  );
}

async function writeAtomicFile(outputPath, contents) {
  await mkdir(dirname(outputPath), { recursive: true });
  const stagingPath = stagingPathFor(outputPath);
  try {
    await writeFile(stagingPath, contents, { encoding: 'utf8', flag: 'wx' });
    // A hard link publishes the fully written inode without replacing an
    // output that appeared after the preflight existence check.
    await link(stagingPath, outputPath);
  } finally {
    await unlink(stagingPath).catch((error) => {
      if (error?.code !== 'ENOENT') {
        throw error;
      }
    });
  }
}

export async function filterSarifPath(
  input,
  output,
  manifestPath,
  { sourceRoot = process.cwd(), expectedAutomationId } = {},
) {
  const baseline = await loadBaselineManifest(manifestPath, sourceRoot);
  assertHashBindings(baseline);
  const remainingFindings = remainingFindingMultiset(baseline);
  const auditState = baselineAuditState(baseline, expectedAutomationId);
  const inputPath = resolve(input);
  const outputPath = resolve(output);
  if (inputPath === outputPath) {
    throw new Error('input and output paths must be different');
  }
  if (await pathExists(outputPath)) {
    throw new Error(`output path already exists: ${outputPath}`);
  }

  const inputStat = await stat(inputPath);
  if (inputStat.isFile()) {
    const filtered = await loadAndFilter(inputPath, baseline, remainingFindings, auditState);
    assertExpectedAuditCoverage(auditState, baseline);
    await writeAtomicFile(
      outputPath,
      `${JSON.stringify(filtered.document, null, 2)}\n`,
    );
    return { files: 1, ...filtered.stats };
  }
  if (!inputStat.isDirectory()) {
    throw new Error(`input path is not a regular file or directory: ${inputPath}`);
  }
  if (outputPath.startsWith(`${inputPath}${sep}`)) {
    throw new Error('output directory must not be inside the input directory');
  }

  const files = await findSarifFiles(inputPath);
  if (files.length === 0) {
    throw new Error(`input directory contains no .sarif files: ${inputPath}`);
  }
  const filteredFiles = [];
  for (const file of files) {
    filteredFiles.push({
      file,
      filtered: await loadAndFilter(
        join(inputPath, file),
        baseline,
        remainingFindings,
        auditState,
      ),
    });
  }

  assertExpectedAuditCoverage(auditState, baseline);

  await mkdir(dirname(outputPath), { recursive: true });
  const stagingDirectory = stagingPathFor(outputPath);
  await mkdir(stagingDirectory);
  const summary = { files: files.length, total: 0, suppressed: 0, retained: 0 };
  try {
    for (const { file, filtered } of filteredFiles) {
      const destination = join(stagingDirectory, file);
      await mkdir(dirname(destination), { recursive: true });
      await writeFile(destination, `${JSON.stringify(filtered.document, null, 2)}\n`, {
        encoding: 'utf8',
        flag: 'wx',
      });
      summary.total += filtered.stats.total;
      summary.suppressed += filtered.stats.suppressed;
      summary.retained += filtered.stats.retained;
    }
    await rename(stagingDirectory, outputPath);
  } finally {
    await rm(stagingDirectory, { recursive: true, force: true });
  }
  return summary;
}

async function main(args) {
  if (
    args.length !== 6
    || args[0] !== '--manifest'
    || args[2] !== '--expected-automation-id'
  ) {
    throw new Error(
      'usage: node scripts/filter-codeql-sarif.mjs --manifest MANIFEST '
        + '--expected-automation-id ID INPUT OUTPUT',
    );
  }
  const summary = await filterSarifPath(
    args[4],
    args[5],
    args[1],
    { expectedAutomationId: args[3] },
  );
  console.log(
    `Filtered ${summary.files} SARIF file(s): ${summary.suppressed} audited upstream result(s) `
      + `suppressed, ${summary.retained} retained (${summary.total} total).`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(`SARIF filter failed: ${error.message}`);
    process.exitCode = 1;
  });
}
