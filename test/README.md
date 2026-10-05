# Test surfaces and CI evidence

A green CI run verifies the commands wired in `.github/workflows/ci.yml`; it does not establish that every manual suite or benchmark ran.

| Surface | CI coverage | Prerequisites and limits |
|---|---|---|
| Default Go tests and selected race checks | `Test Suite` / `Test Race`, combined by required `Test` | Excludes files behind `integration`/`byzantine` build tags |
| Frontend static/contract tests | `npm test` | Node 24 |
| Federation browser fixtures | `npm run test:e2e:fixtures` | Pinned Playwright/Chromium; exactly 9 tests in 3 named specs at this change; API/static content is fulfilled by fixtures, with no SAGE server |
| Retrieval benchmark protocol | `python -m unittest discover -s bench -p 'test_*.py' -v` | Pinned httpx/PyNaCl; signed in-process fake transport, no SAGE node, model service, dataset download or paid API |
| Byzantine network tests | Existing CI `Byzantine Fault Tests` | Disposable CI-owned 4-validator Docker/PostgreSQL network; build tag `byzantine` |
| App-v20/app-v28 fault evidence | Existing required fault-gate workflow | Dedicated subprocess/real CometBFT+ABCI harnesses; see their workflow and reference documents |
| Ordinary Go integration tests | Manual legacy surface; **not** wired into CI | Build tag `integration`; 4 validators plus PostgreSQL and applicable TLS fixtures |
| Legacy browser dashboard tests | Manual legacy surface; **not** wired into CI | Disposable isolated deployment at hardcoded `http://localhost:8080`, seeded state and appropriate dashboard authentication |
| LongMemEval/LoCoMo quality runs | Manual, **not** CI | See [benchmark protocol and reproducibility](../bench/REPRODUCIBILITY.md); persistent writes, semantic models, and optional paid expansion |

## Serverless browser fixtures

```bash
npm ci
npx playwright install chromium
npm run test:e2e:fixtures
npm run test:e2e:fixtures -- --list
```

The target explicitly selects `federation-directory.spec.js` (2), `federation-connectome.spec.js` (5) and `federation-onboarding.spec.js` (2). CI also installs Chromium's system dependencies. This verifies browser behavior over fabricated federation data, not live federation, consensus or authorization. Test screenshots are fixture evidence, not screenshots of a real node.

## Legacy suites

Playwright discovers **200 tests in 8 files** at this change: 9 fixture tests plus **191** legacy tests across agent identity (25), bulk (16), dashboard (47), governance (36) and network (67). The old count of 203 included three JavaScript regular-expression `.test()` calls, which are not Playwright tests. Discover actual tests with `npx playwright test --list`. The default config has no `webServer`; a bare `npx playwright test` does not provision a deployment.

The 191 legacy tests hardcode localhost:8080 and assume particular populated dashboard/network state. They are not certified against the current locked-dashboard/authentication or app-v23 ordinary-agent behavior. Do not run them against a personal/shared node. A dedicated disposable VM/container environment must provide their exact loopback port, fixture data, auth state and (where required) 4-validator/PostgreSQL/TLS deployment. Wiring these tests into CI requires modern fixtures and explicit auth/port isolation first; passing the nine federation fixtures does not verify them.

`make integration` runs the ordinary tagged integration surface (25 `Test...` declarations at this change). Its helpers can skip when a network is absent; a green skip is not evidence of a running network. Some tests still sign without the modern nonce, and two block-production assertions wait for height growth without submitting work. Current SAGE deliberately mints no idle heartbeat blocks, so those assertions need modernization before ordinary integration becomes a reliable CI gate. See [idle block production](../docs/reference/concepts/block-production-and-idle.md).

The existing Byzantine job and dedicated consensus fault gates provide network evidence, but they do not run the ordinary integration directory. Use a fresh disposable environment for manual network work; never clean or recreate an existing personal node to satisfy these suites. The determinism harness has its own isolated Compose project/ports and configurable test selection; see `deploy/scripts/run-determinism.sh` rather than assuming ordinary integration is covered by the fault workflows.
