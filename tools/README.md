# Offline CEREBRUM sample captures

These four website images show production CEREBRUM v11.23.14 UI assets with entirely fabricated data. The visible banner and website captions disclose the sample. They are UI previews, not evidence of a live node, real memories, federation operation, or retrieval quality.

## Regenerate

Use a clean, reviewed, current main-based SAGE checkout containing `web/static/` and its pinned root npm lockfile. In that checkout, install the pinned frontend runtime and Chromium:

```sh
npm ci
npx playwright install chromium
```

From this gh-pages checkout, run:

```sh
node tools/capture-dashboard.mjs --source /absolute/path/to/main-checkout --runtime /absolute/path/to/main-checkout --output /absolute/path/to/gh-pages-checkout
node --test tests/*.mjs
node tools/verify-captures.mjs
```

`--runtime` may point to a separate checkout with the same pinned Playwright dependency installed. No SAGE server is started. The script reads only UI assets and Git provenance from `--source`; it does not read identities, configuration, memory databases, private state, or localhost services.

Chromium visits `https://cerebrum-sample.test`. Every HTTP request is fulfilled locally from source assets or `dashboard-sample.mjs`; unknown routes, other origins, and non-GET requests are aborted and fail the capture. Service workers are blocked. EventSource is replaced with an idle synthetic connection. Sample agents, domains, connections, node health, model status and configuration are fabricated. The optional sample reranker is shown off.

Each page gets an isolated browser context with a seeded PRNG, reduced-motion preference, a fixed base date with an advancing clock, and software WebGL. The brain controls are paused before capture. Settings and Overview use measured viewport heights so their contents fit without clipping. Animation scheduling and software rendering can vary between machines; regenerated PNGs are not expected to be byte-identical.

The script requires no tracked source changes and fails if its source checkout changes during the run. Images are captured to a temporary directory. They replace the four website PNGs only after all pages finish without page/console errors or unexpected requests. `dashboard-captures.json` records the source version/commit, any tracked source changes, exact loaded UI asset hashes, fixture/script hashes, Playwright/Chromium/runtime versions, capture time, viewport sizes, requested routes, warnings, errors and output PNG hashes. Review all four images after regeneration, and update the version in website captions and this README if the source version changes. Chromium can report software WebGL ReadPixels performance warnings; those are retained separately from errors.

The October 5 refresh removed the unreferenced old `screen-security.png` and `screen-update.png` after checking all site text references. The existing October 3 Open Graph cards are retained and checked by `tests/social-preview.test.mjs`.

The Website Checks workflow runs both commands for pull requests targeting `gh-pages` and pushes to that branch. The integrity check needs only Node and committed files; it does not regenerate images, download dependencies, deploy, or contact any service. It verifies the exact four PNG filenames, SHA256s and IHDR dimensions, synthetic disclosure, clean-source metadata, empty errors/unexpected requests and matching fixture/capture-script hashes. Source UI hashes are validated for format here; their bytes were independently checked against the recorded main-based source commit before commit.
