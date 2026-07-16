import assert from 'node:assert/strict';
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  filterSarifDocument,
  filterSarifPath,
  loadBaselineManifest,
  validateBaselineManifest,
} from './filter-codeql-sarif.mjs';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifestPath = join(repoRoot, 'scripts', 'codeql-cometbft-baseline.json');
const manifestValue = JSON.parse(await readFile(manifestPath, 'utf8'));
const baseline = await loadBaselineManifest(manifestPath, repoRoot);

function region(startLine = 1, startColumn = 1, endLine = startLine, endColumn = 2) {
  return { startLine, startColumn, endLine, endColumn };
}

function location(uri, locationRegion = region()) {
  const physicalLocation = {
    artifactLocation: { uri, uriBaseId: '%SRCROOT%' },
  };
  if (locationRegion !== undefined) {
    physicalLocation.region = { ...locationRegion };
  }
  return { physicalLocation };
}

function auditedResult(finding, overrides = {}) {
  const result = {
    ruleId: overrides.ruleId ?? finding.ruleId,
    partialFingerprints: {
      primaryLocationLineHash: overrides.primaryLocationLineHash
        ?? finding.primaryLocationLineHash,
    },
    locations: [location(
      overrides.path ?? finding.path,
      overrides.region ?? finding.region,
    )],
    ...(overrides.extra ?? {}),
  };
  if (overrides.omitStartColumnFingerprint !== true) {
    result.partialFingerprints.primaryLocationStartColumnFingerprint =
      overrides.primaryLocationStartColumnFingerprint
      ?? finding.primaryLocationStartColumnFingerprint;
  }
  if (overrides.omitCorrelationGuid !== true) {
    result.correlationGuid = overrides.correlationGuid ?? finding.correlationGuid;
  }
  return result;
}

function sarif(results, run = {}) {
  return {
    version: '2.1.0',
    runs: [{
      tool: {
        driver: {
          name: baseline.driverName,
          semanticVersion: baseline.semanticVersion,
        },
        extensions: [...baseline.extensions.entries()].map(([name, semanticVersion]) => ({
          name,
          semanticVersion,
        })),
      },
      automationDetails: { id: baseline.automationId },
      results,
      ...run,
    }],
  };
}

function prDiffSarif(results, extensionOverrides = {}) {
  const document = sarif(results);
  document.runs[0].tool.extensions.unshift({
    name: 'codeql-action/pr-diff-range',
    semanticVersion: '0.0.0',
    ...extensionOverrides,
  });
  return document;
}

function filteredResults(document, selectedBaseline = baseline) {
  return filterSarifDocument(document, selectedBaseline).document.runs[0].results;
}

test('manifest binds the exact audited tool, files, overlays, and 29 findings', () => {
  assert.equal(baseline.driverName, 'CodeQL');
  assert.equal(baseline.semanticVersion, '2.26.0');
  assert.equal(baseline.automationId, '/language:go/');
  assert.equal(baseline.extensions.size, 3);
  assert.equal(baseline.provenance.sourceCommit, 'feb2aea4dc271d612129afc958cb844713ec792b');
  assert.equal(baseline.sinkFiles.size, 11);
  assert.equal(baseline.evidenceFiles.size, 8);
  assert.equal(baseline.overlays.size, 6);
  assert.deepEqual(
    [...baseline.overlays.values()].map(({ lineCount }) => lineCount),
    [635, 989, 779, 838, 308, 940],
  );
  assert.equal(baseline.findings.length, 29);
  assert.equal(
    baseline.findings.every(({ primaryLocationStartColumnFingerprint }) => (
      /^[1-9][0-9]*$/u.test(primaryLocationStartColumnFingerprint)
    )),
    true,
  );
  assert.equal(baseline.verifiedPaths.size, 25);
  assert.deepEqual(baseline.hashMismatches, []);
  assert.equal(new Set(baseline.findings.map(({ correlationGuid }) => correlationGuid)).size, 29);
});

test('manifest forbids allowlisted findings in overlays or additive SAGE tests', () => {
  const overlayManifest = structuredClone(manifestValue);
  overlayManifest.findings[0].path = Object.keys(overlayManifest.overlays)[0];
  assert.throws(
    () => validateBaselineManifest(overlayManifest),
    /cannot point to SAGE-owned CometBFT code/u,
  );

  const testManifest = structuredClone(manifestValue);
  testManifest.findings[0].path = 'third_party/cometbft/state/hidden_sage_test.go';
  assert.throws(
    () => validateBaselineManifest(testManifest),
    /cannot point to SAGE-owned CometBFT code/u,
  );
});

test('manifest rejects duplicate identities and overlapping overlay ranges', () => {
  const duplicateGUID = structuredClone(manifestValue);
  duplicateGUID.findings[1].correlationGuid = duplicateGUID.findings[0].correlationGuid;
  assert.throws(
    () => validateBaselineManifest(duplicateGUID),
    /correlationGuid must be unique/u,
  );

  const overlappingRanges = structuredClone(manifestValue);
  const overlay = Object.values(overlappingRanges.overlays)[0];
  overlay.modifiedLineRanges = [[4, 8], [8, 9]];
  assert.throws(
    () => validateBaselineManifest(overlappingRanges),
    /sorted and non-overlapping/u,
  );

  const wrongCommit = structuredClone(manifestValue);
  wrongCommit.provenance.sourceCommit = '0000000000000000000000000000000000000000';
  assert.throws(
    () => validateBaselineManifest(wrongCommit),
    /provenance.sourceCommit must be feb2aea4/u,
  );
});

test('suppresses all and only the 29 exact audited upstream findings', () => {
  const results = baseline.findings.map((finding) => auditedResult(finding));
  const filtered = filterSarifDocument(sarif(results), baseline);

  assert.deepEqual(filtered.document.runs[0].results, []);
  assert.deepEqual(filtered.stats, { total: 29, suppressed: 29, retained: 0 });
});

test('PR-diff metadata preserves exact per-result suppression invariants', () => {
  const exact = auditedResult(baseline.findings[0]);
  const future = auditedResult(baseline.findings[1], { ruleId: 'go/a-new-rule' });
  const filtered = filterSarifDocument(prDiffSarif([exact, future]), baseline);

  assert.deepEqual(filtered.stats, { total: 2, suppressed: 1, retained: 1 });
  assert.deepEqual(filtered.document.runs[0].results, [future]);
});

test('suppresses exact pre-upload CodeQL findings before GitHub adds correlation GUIDs', () => {
  const results = baseline.findings.map((finding) => {
    const result = auditedResult(finding, { omitCorrelationGuid: true });
    delete result.locations[0].physicalLocation.region.endLine;
    return result;
  });
  const filtered = filterSarifDocument(sarif(results), baseline);

  assert.deepEqual(filtered.document.runs[0].results, []);
  assert.deepEqual(filtered.stats, { total: 29, suppressed: 29, retained: 0 });
});

test('suppresses server-processed findings with exact correlation GUIDs', () => {
  const results = baseline.findings.map((finding) => auditedResult(finding, {
    omitStartColumnFingerprint: true,
  }));
  const filtered = filterSarifDocument(sarif(results), baseline);

  assert.deepEqual(filtered.document.runs[0].results, []);
  assert.deepEqual(filtered.stats, { total: 29, suppressed: 29, retained: 0 });
});

test('retains a future vendor finding when any identity component changes', () => {
  const finding = baseline.findings[0];
  const variants = [
    auditedResult(finding, { ruleId: 'go/a-new-rule' }),
    auditedResult(finding, { path: 'third_party/cometbft/blocksync/new_pool.go' }),
    auditedResult(finding, {
      region: { ...finding.region, startColumn: finding.region.startColumn + 1 },
    }),
    auditedResult(finding, { primaryLocationLineHash: '0000000000000000:1' }),
    auditedResult(finding, { primaryLocationStartColumnFingerprint: '999999' }),
    auditedResult(finding, { correlationGuid: '00000000-0000-4000-8000-000000000000' }),
  ];

  assert.equal(filteredResults(sarif(variants)).length, variants.length);
});

test('retains unbound SARIF stable-identity carriers', () => {
  const finding = baseline.findings[0];
  const withGuid = auditedResult(finding, { extra: { guid: finding.correlationGuid } });
  const withFingerprints = auditedResult(finding, {
    extra: { fingerprints: { stable: 'new-identity' } },
  });
  const withExtraPartial = auditedResult(finding);
  withExtraPartial.partialFingerprints.newStableIdentity = 'new-identity';
  const withoutEitherStageIdentity = auditedResult(finding, {
    omitCorrelationGuid: true,
    omitStartColumnFingerprint: true,
  });

  assert.deepEqual(
    filteredResults(sarif([
      withGuid,
      withFingerprints,
      withExtraPartial,
      withoutEitherStageIdentity,
    ])),
    [withGuid, withFingerprints, withExtraPartial, withoutEitherStageIdentity],
  );
});

test('retains duplicate occurrences beyond the audited multiset count', () => {
  const result = auditedResult(baseline.findings[0]);
  const filtered = filterSarifDocument(
    sarif([result, structuredClone(result)]),
    baseline,
  );

  assert.deepEqual(filtered.stats, { total: 2, suppressed: 1, retained: 1 });
  assert.equal(filtered.document.runs[0].results[0].correlationGuid, result.correlationGuid);
});

test('suppresses only a single valid occurrence of each audited result', () => {
  const finding = baseline.findings[0];
  const one = auditedResult(finding, { extra: { occurrenceCount: 1 } });
  const two = auditedResult(finding, { extra: { occurrenceCount: 2 } });
  const zero = auditedResult(finding, { extra: { occurrenceCount: 0 } });
  const malformed = auditedResult(finding, { extra: { occurrenceCount: 'many' } });

  assert.deepEqual(filteredResults(sarif([one])), []);
  assert.deepEqual(filteredResults(sarif([two])), [two]);
  assert.deepEqual(filteredResults(sarif([zero])), [zero]);
  assert.deepEqual(filteredResults(sarif([malformed])), [malformed]);
});

test('retains an audited sink when related evidence reaches first-party code', () => {
  const result = auditedResult(baseline.findings[0], {
    extra: {
      relatedLocations: [
        location('third_party/cometbft/p2p/switch.go', region(100)),
        location('internal/statesync/session.go', region(40)),
      ],
    },
  });

  assert.deepEqual(filteredResults(sarif([result])), [result]);
});

test('retains an audited sink when a code flow reaches first-party code', () => {
  const result = auditedResult(baseline.findings[0], {
    extra: {
      codeFlows: [{
        threadFlows: [{
          locations: [
            { location: location('third_party/cometbft/p2p/switch.go', region(100)) },
            { location: location('cmd/sage-gui/main.go', region(100)) },
          ],
        }],
      }],
    },
  });

  assert.deepEqual(filteredResults(sarif([result])), [result]);
});

test('retains findings touching every modified production-overlay range', () => {
  const overlayEntries = [...baseline.overlays.entries()];
  const results = overlayEntries.map(([path, overlay], index) => {
    const [start, end] = overlay.modifiedLineRanges.at(-1);
    return auditedResult(baseline.findings[index], {
      extra: { relatedLocations: [location(path, region(start, 1, end, 2))] },
    });
  });

  assert.equal(filteredResults(sarif(results)).length, overlayEntries.length);
});

test('allows unchanged regions in hash-bound overlay files to remain upstream evidence', () => {
  const overlayEntries = [...baseline.overlays.entries()];
  const results = overlayEntries.map(([path], index) => auditedResult(
    baseline.findings[index],
    { extra: { relatedLocations: [location(path, region(1))] } },
  ));

  assert.deepEqual(filteredResults(sarif(results)), []);
});

test('normalizes the pre-upload single-line shorthand in unchanged overlays', () => {
  const [path] = baseline.overlays.keys();
  const shorthand = region(1, 1, 1, 2);
  delete shorthand.endLine;
  const result = auditedResult(baseline.findings[0], {
    omitCorrelationGuid: true,
    extra: { relatedLocations: [location(path, shorthand)] },
  });
  delete result.locations[0].physicalLocation.region.endLine;

  assert.deepEqual(filteredResults(sarif([result])), []);
});

test('matches real unchanged node/setup flow spans without protecting whole overlay files', () => {
  const result = auditedResult(baseline.findings[0], {
    extra: {
      codeFlows: [{
        threadFlows: [{
          locations: [
            { location: location('third_party/cometbft/node/setup.go', region(510, 3, 517, 42)) },
            { location: location('third_party/cometbft/node/node.go', region(587, 10, 590, 60)) },
          ],
        }],
      }],
    },
  });

  assert.deepEqual(filteredResults(sarif([result])), []);
});

test('retains ambiguous or out-of-file coordinates instead of trusting them', () => {
  const mixedRegion = region(1);
  mixedRegion.charOffset = 1;
  const results = [
    auditedResult(baseline.findings[0], {
      extra: {
        relatedLocations: [
          location('third_party/cometbft/node/node.go', mixedRegion),
        ],
      },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        relatedLocations: [
          location('third_party/cometbft/node/node.go', region(999999)),
        ],
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        relatedLocations: [
          location('third_party/cometbft/libs/service/service.go', region(999999)),
        ],
      },
    }),
  ];

  assert.deepEqual(filteredResults(sarif(results)), results);
});

test('retains overlay evidence with an unknown region and all additive SAGE tests', () => {
  const unknownOverlayLocation = location('third_party/cometbft/node/node.go');
  delete unknownOverlayLocation.physicalLocation.region;
  const unknownOverlay = auditedResult(baseline.findings[0], {
    extra: {
      relatedLocations: [unknownOverlayLocation],
    },
  });
  const sageTest = auditedResult(baseline.findings[1], {
    extra: {
      relatedLocations: [
        location('third_party/cometbft/internal/deep/recovery_sage_test.go', region(1)),
      ],
    },
  });

  assert.equal(filteredResults(sarif([unknownOverlay, sageTest])).length, 2);
});

test('lookalike vendor paths remain unbound rather than inheriting trust', () => {
  const result = auditedResult(baseline.findings[0], {
    extra: {
      relatedLocations: [
        location('third_party/cometbft/node/node.go.bak', region(396)),
        location('third_party/cometbft/node/reactor_sage_test.go.bak', region(1)),
      ],
    },
  });

  assert.deepEqual(filteredResults(sarif([result])), [result]);
});

test('retains first-party stack frames and logical-only related locations', () => {
  const stack = auditedResult(baseline.findings[0], {
    extra: { stacks: [{ frames: [{ location: location('internal/abci/app.go', region(1)) }] }] },
  });
  const logical = auditedResult(baseline.findings[1], {
    extra: { relatedLocations: [{ logicalLocations: [{ name: 'unknown' }] }] },
  });

  assert.equal(filteredResults(sarif([stack, logical])).length, 2);
});

test('retains mixed physical/logical locations and attachment rectangles', () => {
  const mixed = location(
    'third_party/cometbft/libs/service/service.go',
    region(1),
  );
  mixed.logicalLocations = [{
    fullyQualifiedName: 'github.com/l33tdawg/sage/internal/abci.FirstParty',
  }];
  const logical = auditedResult(baseline.findings[0], {
    extra: { relatedLocations: [mixed] },
  });
  const rectangle = auditedResult(baseline.findings[1], {
    extra: {
      attachments: [{
        artifactLocation: { uri: 'third_party/cometbft/node/node.go' },
        regions: [region(1)],
        rectangles: [{ left: 0, top: 0, right: 1, bottom: 1 }],
      }],
    },
  });

  assert.deepEqual(filteredResults(sarif([logical, rectangle])), [logical, rectangle]);
});

test('retains physical addresses and module identities outside source digests', () => {
  const evidencePath = 'third_party/cometbft/libs/service/service.go';
  const inlineAddressLocation = location(evidencePath, region(1));
  inlineAddressLocation.physicalLocation.address = {
    fullyQualifiedName: 'github.com/l33tdawg/sage/internal/abci.FirstParty',
  };
  const indexedAddressLocation = location(evidencePath, region(1));
  indexedAddressLocation.physicalLocation.address = { index: 0 };
  const results = [
    auditedResult(baseline.findings[0], {
      extra: { relatedLocations: [inlineAddressLocation] },
    }),
    auditedResult(baseline.findings[1], {
      extra: { relatedLocations: [indexedAddressLocation] },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        codeFlows: [{
          threadFlows: [{
            locations: [{
              module: 'github.com/l33tdawg/sage/internal/abci',
              location: location(evidencePath, region(1)),
            }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[3], {
      extra: {
        stacks: [{
          frames: [{
            module: 'github.com/l33tdawg/sage/internal/abci',
            location: location(evidencePath, region(1)),
          }],
        }],
      },
    }),
  ];
  const run = {
    addresses: [{
      fullyQualifiedName: 'github.com/l33tdawg/sage/internal/abci.FirstParty',
    }],
  };

  assert.deepEqual(filteredResults(sarif(results, run)), results);
});

test('retains first-party analysis targets, attachments, fixes, and graph nodes', () => {
  const results = [
    auditedResult(baseline.findings[0], {
      extra: { analysisTarget: { uri: 'internal/abci/app.go' } },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        attachments: [{
          artifactLocation: { uri: 'cmd/sage-gui/main.go' },
          regions: [region(1)],
        }],
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        fixes: [{
          artifactChanges: [{
            artifactLocation: { uri: 'internal/statesync/session.go' },
            replacements: [{ deletedRegion: region(10) }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[3], {
      extra: {
        graphs: [{
          nodes: [{ id: 'first-party', location: location('internal/abci/app.go', region(2)) }],
        }],
      },
    }),
  ];

  assert.equal(filteredResults(sarif(results)).length, 4);
});

test('retains additional carriers that touch modified overlay regions', () => {
  const results = [
    auditedResult(baseline.findings[0], {
      extra: {
        attachments: [{
          artifactLocation: { uri: 'third_party/cometbft/node/node.go' },
          regions: [region(396)],
        }],
      },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        fixes: [{
          artifactChanges: [{
            artifactLocation: { uri: 'third_party/cometbft/node/setup.go' },
            replacements: [{ deletedRegion: region(542, 1, 550, 2) }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        graphs: [{
          nodes: [{
            id: 'overlay',
            location: location('third_party/cometbft/state/store.go', region(261)),
          }],
        }],
      },
    }),
  ];

  assert.equal(filteredResults(sarif(results)).length, 3);
});

test('hash-bound vendor-only additional carriers do not broaden the retained set', () => {
  const evidencePath = 'third_party/cometbft/libs/service/service.go';
  const results = [
    auditedResult(baseline.findings[0], {
      extra: { analysisTarget: { uri: evidencePath } },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        attachments: [{
          artifactLocation: { uri: evidencePath },
          regions: [region(1)],
        }],
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        fixes: [{
          artifactChanges: [{
            artifactLocation: { uri: evidencePath },
            replacements: [{ deletedRegion: region(1) }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[3], {
      extra: {
        graphs: [{
          nodes: [{
            id: 'vendor',
            location: location(evidencePath, region(1)),
          }],
        }],
      },
    }),
  ];

  assert.deepEqual(filteredResults(sarif(results)), []);
});

test('retains every unbound path below the vendor prefix', () => {
  const results = [
    auditedResult(baseline.findings[0], {
      extra: {
        relatedLocations: [
          location('third_party/cometbft/p2p/peer.go', region(1)),
        ],
      },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        codeFlows: [{
          threadFlows: [{
            locations: [{
              location: location('third_party/cometbft/future_sage.go', region(1)),
            }],
          }],
        }],
      },
    }),
  ];

  assert.deepEqual(filteredResults(sarif(results)), results);
});

test('retains suppressions, conversion sources, nested stacks, and annotations in owned code', () => {
  const annotationLocation = location('third_party/cometbft/node/node.go', region(1));
  annotationLocation.annotations = [region(396)];
  const results = [
    auditedResult(baseline.findings[0], {
      extra: {
        suppressions: [{
          kind: 'inSource',
          location: location('internal/abci/app.go', region(2)),
        }],
      },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        provenance: {
          conversionSources: [
            location('internal/statesync/session.go', region(4)).physicalLocation,
          ],
        },
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: {
        codeFlows: [{
          threadFlows: [{
            locations: [{
              location: location(
                'third_party/cometbft/libs/service/service.go',
                region(1),
              ),
              stack: {
                frames: [{ location: location('internal/abci/app.go', region(5)) }],
              },
            }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[3], {
      extra: { relatedLocations: [annotationLocation] },
    }),
  ];

  assert.deepEqual(filteredResults(sarif(results)), results);
});

test('retains context regions and nested graph children that reach owned code', () => {
  const contextual = location('third_party/cometbft/node/node.go', region(1));
  contextual.physicalLocation.contextRegion = region(396);
  const graph = auditedResult(baseline.findings[1], {
    extra: {
      graphs: [{
        nodes: [{
          id: 'parent',
          location: location(
            'third_party/cometbft/libs/service/service.go',
            region(1),
          ),
          children: [{
            id: 'owned-child',
            location: location('internal/abci/app.go', region(2)),
          }],
        }],
      }],
    },
  });
  const context = auditedResult(baseline.findings[0], {
    extra: { relatedLocations: [contextual] },
  });

  assert.deepEqual(filteredResults(sarif([context, graph])), [context, graph]);
});

test('retains malformed regions in every supported source-location carrier', () => {
  const evidencePath = 'third_party/cometbft/libs/service/service.go';
  const relatedLocation = location(evidencePath, region(1));
  relatedLocation.physicalLocation.region = null;
  const flowLocation = location(evidencePath, region(1));
  flowLocation.physicalLocation.region = 'bad';
  const stackLocation = location(evidencePath, region(1));
  stackLocation.physicalLocation.region = {};
  const annotationLocation = location(evidencePath, region(1));
  annotationLocation.annotations = [{}];
  const contextLocation = location(evidencePath, region(1));
  contextLocation.physicalLocation.contextRegion = {};
  const results = [
    auditedResult(baseline.findings[0], {
      extra: { relatedLocations: [relatedLocation] },
    }),
    auditedResult(baseline.findings[1], {
      extra: {
        codeFlows: [{
          threadFlows: [{ locations: [{ location: flowLocation }] }],
        }],
      },
    }),
    auditedResult(baseline.findings[2], {
      extra: { stacks: [{ frames: [{ location: stackLocation }] }] },
    }),
    auditedResult(baseline.findings[3], {
      extra: {
        attachments: [{
          artifactLocation: { uri: evidencePath },
          regions: [{}],
        }],
      },
    }),
    auditedResult(baseline.findings[4], {
      extra: {
        fixes: [{
          artifactChanges: [{
            artifactLocation: { uri: evidencePath },
            replacements: [{ deletedRegion: {} }],
          }],
        }],
      },
    }),
    auditedResult(baseline.findings[5], {
      extra: { relatedLocations: [annotationLocation] },
    }),
    auditedResult(baseline.findings[6], {
      extra: { relatedLocations: [contextLocation] },
    }),
  ];

  assert.deepEqual(filteredResults(sarif(results)), results);
});

test('resolves cached thread-flow locations and fails closed on invalid inline copies', () => {
  const cachedFirstParty = auditedResult(baseline.findings[0], {
    extra: {
      codeFlows: [{ threadFlows: [{ locations: [{ index: 0 }] }] }],
    },
  });
  const conflictingInline = auditedResult(baseline.findings[1], {
    extra: {
      codeFlows: [{
        threadFlows: [{
          locations: [{
            index: 1,
            location: location('internal/abci/app.go', region(2)),
          }],
        }],
      }],
    },
  });
  const cachedVendor = auditedResult(baseline.findings[2], {
    extra: {
      codeFlows: [{ threadFlows: [{ locations: [{ index: 1 }] }] }],
    },
  });
  const run = {
    threadFlowLocations: [
      { location: location('internal/abci/app.go', region(1)) },
      {
        location: location(
          'third_party/cometbft/libs/service/service.go',
          region(1),
        ),
      },
    ],
  };

  assert.deepEqual(
    filteredResults(sarif([cachedFirstParty, conflictingInline, cachedVendor], run)),
    [cachedFirstParty, conflictingInline],
  );
});

test('unknown or incomplete additional carriers retain the result', () => {
  const results = [
    auditedResult(baseline.findings[0], { extra: { analysisTarget: null } }),
    auditedResult(baseline.findings[1], { extra: { attachments: [{}] } }),
    auditedResult(baseline.findings[2], { extra: { fixes: [{ artifactChanges: [] }] } }),
    auditedResult(baseline.findings[3], { extra: { graphs: [{ nodes: [{ id: 'unknown' }] }] } }),
    auditedResult(baseline.findings[4], { extra: { graphTraversals: [{ graphId: 'run-graph' }] } }),
    auditedResult(baseline.findings[5], {
      extra: {
        attachments: [{
          artifactLocation: { uri: 'third_party/cometbft/p2p/peer.go' },
          regions: ['not-a-region'],
        }],
      },
    }),
  ];

  assert.equal(filteredResults(sarif(results)).length, 6);
});

test('normalizes repository URIs but retains traversal, foreign-base, and absolute paths', () => {
  const finding = baseline.findings[0];
  const normalized = auditedResult(finding, {
    path: './third_party/cometbft/blocksync/../blocksync/pool.go?line=536#sink',
  });
  const traversed = auditedResult(finding, {
    path: 'third_party/cometbft/blocksync/%2e%2e/first_party.go',
  });
  const foreignBase = auditedResult(finding);
  foreignBase.locations[0].physicalLocation.artifactLocation.uriBaseId = 'BUILDROOT';
  const absolute = auditedResult(finding, {
    path: 'file:///home/runner/work/sage/sage/third_party/cometbft/blocksync/pool.go',
  });

  const retained = filteredResults(sarif([normalized, traversed, foreignBase, absolute]));
  assert.deepEqual(retained, [traversed, foreignBase, absolute]);
});

test('retains audited identities when the run remaps a repository URI base', () => {
  const result = auditedResult(baseline.findings[0]);
  const remapped = sarif([result], {
    originalUriBaseIds: {
      '%SRCROOT%': { uri: 'file:///tmp/untrusted-source-root/' },
    },
  });
  const emptyMap = sarif([result], { originalUriBaseIds: {} });

  assert.deepEqual(filteredResults(remapped), [result]);
  assert.deepEqual(filteredResults(emptyMap), [result]);
});

test('uses explicit URIs and falls back to artifact indexes when URI is absent', () => {
  const finding = baseline.findings[0];
  const indexed = auditedResult(finding);
  indexed.locations[0].physicalLocation.artifactLocation = { index: 0 };
  const explicit = auditedResult(baseline.findings[1]);
  explicit.locations[0].physicalLocation.artifactLocation.index = 1;
  const indexOnlyFirstParty = auditedResult(baseline.findings[2]);
  indexOnlyFirstParty.locations[0].physicalLocation.artifactLocation = { index: 1 };
  const run = {
    artifacts: [
      { location: { uri: finding.path, uriBaseId: '%SRCROOT%' } },
      { location: { uri: 'internal/abci/app.go', uriBaseId: '%SRCROOT%' } },
    ],
  };

  assert.deepEqual(
    filteredResults(sarif([indexed, explicit, indexOnlyFirstParty], run)),
    [indexOnlyFirstParty],
  );
});

test('retains results from any missing or different CodeQL driver version', () => {
  const result = auditedResult(baseline.findings[0]);
  const wrongVersion = sarif([result]);
  wrongVersion.runs[0].tool.driver.semanticVersion = '2.26.1';
  const wrongName = sarif([result]);
  wrongName.runs[0].tool.driver.name = 'Another scanner';
  const missingTool = sarif([result]);
  delete missingTool.runs[0].tool;
  const wrongAutomation = sarif([result]);
  wrongAutomation.runs[0].automationDetails.id = '/language:javascript-typescript/';
  const wrongQueryPack = sarif([result]);
  wrongQueryPack.runs[0].tool.extensions[0].semanticVersion = 'next-query-pack';

  assert.equal(filteredResults(wrongVersion).length, 1);
  assert.equal(filteredResults(wrongName).length, 1);
  assert.equal(filteredResults(missingTool).length, 1);
  assert.equal(filteredResults(wrongAutomation).length, 1);
  assert.equal(filteredResults(wrongQueryPack).length, 1);
});

test('rejects malformed SARIF instead of producing a falsely clean upload', () => {
  assert.throws(
    () => filterSarifDocument({ version: '2.1.0', runs: 'not-an-array' }, baseline),
    /runs must be a non-empty array/u,
  );
  assert.throws(
    () => filterSarifDocument(sarif([auditedResult(baseline.findings[0], {
      extra: { codeFlows: [{ threadFlows: 'not-an-array' }] },
    })]), baseline),
    /threadFlows must be an array/u,
  );
  assert.throws(
    () => filterSarifDocument(sarif([auditedResult(baseline.findings[0], {
      extra: { suppressions: 'not-an-array' },
    })]), baseline),
    /suppressions must be an array/u,
  );
});

test('hash drift fails CLI mode before any filtered output can be created', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const output = join(root, 'output.sarif');
  t.after(() => rm(root, { recursive: true, force: true }));

  const driftedBaseline = await loadBaselineManifest(manifestPath, root);
  assert.equal(driftedBaseline.hashMismatches.length, 25);
  assert.equal(filteredResults(
    sarif([auditedResult(baseline.findings[0])]),
    driftedBaseline,
  ).length, 1);
  await assert.rejects(
    filterSarifPath(join(root, 'missing.sarif'), output, manifestPath, { sourceRoot: root }),
    /CometBFT baseline hash mismatch/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('overlay unchanged-line drift removes trust even when the full hash is accepted', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const driftedManifestPath = join(root, 'baseline.json');
  const driftedManifest = structuredClone(manifestValue);
  const [overlayPath, overlay] = Object.entries(driftedManifest.overlays)[0];
  overlay.unchangedLineSha256 = '0'.repeat(64);
  await writeFile(driftedManifestPath, JSON.stringify(driftedManifest));
  t.after(() => rm(root, { recursive: true, force: true }));

  const driftedBaseline = await loadBaselineManifest(driftedManifestPath, repoRoot);
  assert.equal(driftedBaseline.verifiedPaths.has(overlayPath), false);
  assert.equal(
    driftedBaseline.hashMismatches.some(({ path }) => path === `${overlayPath}#unchanged-lines`),
    true,
  );
});

test('directory mode validates every SARIF input before creating output', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input');
  const output = join(root, 'output');
  await mkdir(input);
  await writeFile(
    join(input, 'valid.sarif'),
    JSON.stringify(sarif([auditedResult(baseline.findings[0])])),
  );
  await writeFile(join(input, 'broken.sarif'), '{');
  t.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    filterSarifPath(input, output, manifestPath, { sourceRoot: repoRoot }),
    /cannot read valid JSON/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('CLI coverage gate rejects an audited run missing a baseline finding', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  await writeFile(input, JSON.stringify(sarif(
    baseline.findings.slice(0, -1).map((finding) => auditedResult(finding, {
      omitCorrelationGuid: true,
    })),
  )));
  t.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    filterSarifPath(input, output, manifestPath, {
      sourceRoot: repoRoot,
      expectedAutomationId: baseline.automationId,
    }),
    /missing 1 expected CometBFT baseline finding/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('CLI coverage gate rejects a changed pre-upload stable fingerprint', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  const results = baseline.findings.map((finding) => auditedResult(finding, {
    omitCorrelationGuid: true,
  }));
  results[0].partialFingerprints.primaryLocationStartColumnFingerprint = '999999';
  await writeFile(input, JSON.stringify(sarif(results)));
  t.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    filterSarifPath(input, output, manifestPath, {
      sourceRoot: repoRoot,
      expectedAutomationId: baseline.automationId,
    }),
    /missing 1 expected CometBFT baseline finding/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('CLI coverage gate rejects Go tool drift even with no baseline results', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  const drifted = sarif([]);
  drifted.runs[0].tool.extensions[0].semanticVersion = 'next-query-pack';
  await writeFile(input, JSON.stringify(drifted));
  t.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    filterSarifPath(input, output, manifestPath, {
      sourceRoot: repoRoot,
      expectedAutomationId: baseline.automationId,
    }),
    /metadata does not match the pinned Go baseline/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('CLI coverage gate accepts exact zero-result CodeQL PR-diff metadata', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  const document = prDiffSarif([]);
  await writeFile(input, JSON.stringify(document));
  t.after(() => rm(root, { recursive: true, force: true }));

  const summary = await filterSarifPath(input, output, manifestPath, {
    sourceRoot: repoRoot,
    expectedAutomationId: baseline.automationId,
  });
  const written = JSON.parse(await readFile(output, 'utf8'));

  assert.deepEqual(summary, { files: 1, total: 0, suppressed: 0, retained: 0 });
  assert.deepEqual(written, document);
});

test('PR-diff metadata cannot waive an incomplete full-run coverage audit', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  const differentialRun = prDiffSarif([]).runs[0];
  const fullRun = sarif([]).runs[0];
  await writeFile(input, JSON.stringify({
    version: '2.1.0',
    runs: [differentialRun, fullRun],
  }));
  t.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    filterSarifPath(input, output, manifestPath, {
      sourceRoot: repoRoot,
      expectedAutomationId: baseline.automationId,
    }),
    /missing 29 expected CometBFT baseline finding/u,
  );
  await assert.rejects(readFile(output), /ENOENT/u);
});

test('CLI coverage gate rejects spoofed or additional PR-diff extensions', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const cases = [
    {
      name: 'wrong generated-extension version',
      document: prDiffSarif([], { semanticVersion: '0.0.1' }),
    },
    {
      name: 'lookalike generated-extension name',
      document: prDiffSarif([], { name: 'codeql-action/pr-diff-range-spoof' }),
    },
    {
      name: 'unexpected fifth extension',
      document: (() => {
        const document = prDiffSarif([]);
        document.runs[0].tool.extensions.push({
          name: 'attacker/extra-pack',
          semanticVersion: '1.0.0',
        });
        return document;
      })(),
    },
    {
      name: 'duplicate generated extension',
      document: (() => {
        const document = prDiffSarif([]);
        document.runs[0].tool.extensions.push({
          name: 'codeql-action/pr-diff-range',
          semanticVersion: '0.0.0',
        });
        return document;
      })(),
    },
    {
      name: 'drifted pinned query pack',
      document: (() => {
        const document = prDiffSarif([]);
        document.runs[0].tool.extensions[1].semanticVersion = 'next-query-pack';
        return document;
      })(),
    },
  ];

  for (const [index, testCase] of cases.entries()) {
    const input = join(root, `input-${index}.sarif`);
    const output = join(root, `output-${index}.sarif`);
    await writeFile(input, JSON.stringify(testCase.document));
    await assert.rejects(
      filterSarifPath(input, output, manifestPath, {
        sourceRoot: repoRoot,
        expectedAutomationId: baseline.automationId,
      }),
      /metadata does not match the pinned Go baseline/u,
      testCase.name,
    );
    await assert.rejects(readFile(output), /ENOENT/u, testCase.name);
  }
});

test('file mode writes filtered SARIF and reports exact counts', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'sage-sarif-filter-'));
  const input = join(root, 'input.sarif');
  const output = join(root, 'output.sarif');
  const firstParty = auditedResult(baseline.findings[1], {
    extra: { relatedLocations: [location('internal/abci/app.go', region(1))] },
  });
  await writeFile(input, JSON.stringify(sarif([
    auditedResult(baseline.findings[0]),
    firstParty,
  ])));
  t.after(() => rm(root, { recursive: true, force: true }));

  const summary = await filterSarifPath(
    input,
    output,
    manifestPath,
    { sourceRoot: repoRoot },
  );
  const written = JSON.parse(await readFile(output, 'utf8'));

  assert.deepEqual(summary, { files: 1, total: 2, suppressed: 1, retained: 1 });
  assert.deepEqual(written.runs[0].results, [firstParty]);
});
