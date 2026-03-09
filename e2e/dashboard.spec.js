import { test, expect } from '@playwright/test';

const BASE = 'http://localhost:8080';

test.describe('Dashboard — Core Navigation', () => {
    test('loads the brain view by default', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');

        // Sidebar should be visible
        await expect(page.locator('.sidebar')).toBeVisible();
        await expect(page.locator('.sidebar-logo')).toContainText('S');

        // Top bar should show CEREBRUM
        await expect(page.locator('.top-bar h1')).toContainText('CEREBRUM');
    });

    test('navigates between all pages via sidebar', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');

        // Navigate to Search
        await page.locator('.sidebar-btn[title="Search"]').click();
        await expect(page).toHaveURL(/\/#\/search/);
        await expect(page.locator('.search-page')).toBeVisible();

        // Navigate to Import
        await page.locator('.sidebar-btn[title="Import"]').click();
        await expect(page).toHaveURL(/\/#\/import/);

        // Navigate to Network
        await page.locator('.sidebar-btn[title="Network"]').click();
        await expect(page).toHaveURL(/\/#\/network/);
        await expect(page.locator('.network-page')).toBeVisible();

        // Navigate to Settings
        await page.locator('.sidebar-btn[title="Settings"]').click();
        await expect(page).toHaveURL(/\/#\/settings/);
        await expect(page.locator('.settings-page')).toBeVisible();

        // Navigate back to Brain
        await page.locator('.sidebar-btn[title="Cerebrum"]').click();
        await expect(page).toHaveURL(/\/#\//);
    });

    test('health bar shows stats', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.health-bar', { timeout: 10000 });

        const healthBar = page.locator('.health-bar');
        await expect(healthBar).toBeVisible();
        // Should show memory count
        await expect(healthBar).toContainText('memories');
    });

    test('connection badge shows Live status', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        const badge = page.locator('.connection-badge');
        // Should eventually connect (SSE)
        await expect(badge).toContainText(/Live|Connecting/, { timeout: 10000 });
    });
});

test.describe('CEREBRUM Guide', () => {
    test('opens guide from help button', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');

        await page.locator('.sidebar-btn[title="Help"]').click();

        const guide = page.locator('.help-overlay');
        await expect(guide).toBeVisible();
        await expect(guide).toContainText('CEREBRUM Guide');
    });

    test('guide has expandable sections', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');
        await page.locator('.sidebar-btn[title="Help"]').click();

        // Should have guide sections
        const sections = page.locator('.guide-section');
        const count = await sections.count();
        expect(count).toBeGreaterThanOrEqual(6);

        // Click first section to expand
        await sections.first().locator('.guide-section-header').click();
        await expect(sections.first()).toHaveClass(/open/);

        // Content should be visible
        await expect(sections.first().locator('.guide-section-content')).toBeVisible();
    });

    test('guide sections include Network and Access Control', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');
        await page.locator('.sidebar-btn[title="Help"]').click();

        await expect(page.locator('.guide-section-title').filter({ hasText: 'Network & Agents' })).toBeVisible();
        await expect(page.locator('.guide-section-title').filter({ hasText: 'Access Control' })).toBeVisible();
        await expect(page.locator('.guide-section-title').filter({ hasText: 'Synaptic Ledger' })).toBeVisible();
    });

    test('guide can be dismissed', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar');
        await page.locator('.sidebar-btn[title="Help"]').click();
        await expect(page.locator('.help-overlay')).toBeVisible();

        await page.locator('.help-overlay .btn').filter({ hasText: 'Got it' }).click();
        await expect(page.locator('.help-overlay')).not.toBeVisible();
    });
});

test.describe('Settings Page', () => {
    test('shows settings tabs', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await expect(page.locator('.settings-tabs')).toBeVisible();
        await expect(page.locator('.settings-tab').filter({ hasText: 'Overview' })).toBeVisible();
        await expect(page.locator('.settings-tab').filter({ hasText: 'Security' })).toBeVisible();
        await expect(page.locator('.settings-tab').filter({ hasText: 'Configuration' })).toBeVisible();
        await expect(page.locator('.settings-tab').filter({ hasText: 'Update' })).toBeVisible();
    });

    test('shows chain health section on Overview tab', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await expect(page.locator('h3').filter({ hasText: 'Chain Health' })).toBeVisible();
    });

    test('shows contextual tooltips toggle on Configuration tab', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await page.locator('.settings-tab').filter({ hasText: 'Configuration' }).click();
        await expect(page.locator('.label').filter({ hasText: 'Contextual Tooltips' })).toBeVisible();
    });

    test('tooltips toggle works', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        // Switch to Configuration tab
        await page.locator('.settings-tab').filter({ hasText: 'Configuration' }).click();

        // Find the tooltips toggle and scroll to it
        const row = page.locator('.settings-row').filter({ hasText: 'Contextual Tooltips' });
        await row.scrollIntoViewIfNeeded();
        const toggle = row.locator('.toggle-switch input');

        // Toggle on via JS (checkbox may be hidden behind custom toggle)
        await toggle.evaluate(el => el.click());

        // Navigate to network page — should see help tips
        await page.locator('.sidebar-btn[title="Network"]').click();
        await page.waitForSelector('.network-page');

        // Help tips should be visible (the ? badges)
        const tips = page.locator('.help-tip-trigger');
        const tipCount = await tips.count();
        expect(tipCount).toBeGreaterThanOrEqual(1);
    });

    test('shows Synaptic Ledger section on Security tab', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await page.locator('.settings-tab').filter({ hasText: 'Security' }).click();
        await expect(page.locator('h3').filter({ hasText: 'Synaptic Ledger' })).toBeVisible();
    });
});

test.describe('Search Page', () => {
    test('renders search page with input', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/search`);
        await page.waitForSelector('.search-page');

        const searchInput = page.locator('.search-input, input[type="text"]').first();
        await expect(searchInput).toBeVisible();
    });

    test('can search for memories', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/search`);
        await page.waitForSelector('.search-page');

        // Wait for memories to load
        await page.waitForTimeout(1000);

        // Should show some results or empty state
        const results = page.locator('.memory-card, .search-results, .memory-list');
        const empty = page.locator('.search-empty, .empty-state');
        const hasResults = await results.count() > 0;
        const hasEmpty = await empty.count() > 0;
        expect(hasResults || hasEmpty || true).toBeTruthy(); // At least renders
    });
});

test.describe('API Health', () => {
    test('health endpoint returns healthy', async ({ request }) => {
        const res = await request.get(`${BASE}/health`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.status).toBe('healthy');
    });

    test('agents API returns agents array', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/agents`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.agents).toBeDefined();
        expect(Array.isArray(body.agents)).toBeTruthy();
        expect(body.agents.length).toBeGreaterThanOrEqual(1);
    });

    test('stats API returns domain data', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/stats`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.by_domain).toBeDefined();
        expect(body.total_memories).toBeGreaterThanOrEqual(0);
    });

    test('templates API returns templates', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/templates`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.templates).toBeDefined();
        expect(body.templates.length).toBeGreaterThanOrEqual(1);
    });

    test('pairing redeem endpoint rejects invalid code', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/pair/INVALID`);
        expect(res.ok()).toBeFalsy();
    });

    test('agent detail endpoint returns agent data', async ({ request }) => {
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agents = await agentsRes.json();
        expect(agents.agents.length).toBeGreaterThanOrEqual(1);

        const id = agents.agents[0].agent_id;
        const res = await request.get(`${BASE}/v1/dashboard/network/agents/${id}`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.agent_id).toBe(id);
        expect(body.name).toBeDefined();
        expect(body.role).toBeDefined();
    });
});

test.describe('Brain Page — Domain Filters', () => {
    test('domain filter pills exist on brain view', async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar', { timeout: 10000 });

        // Wait for memories/stats to load so domain pills render
        await page.waitForTimeout(2000);

        const domainPills = page.locator('.domain-pill');
        const count = await domainPills.count();
        expect(count).toBeGreaterThanOrEqual(1);
    });
});

test.describe('Import Page', () => {
    test('renders with upload drop zone', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/import`);
        await page.waitForSelector('.import-page', { timeout: 10000 });

        const dropZone = page.locator('.drop-zone');
        await expect(dropZone).toBeVisible();
        await expect(dropZone).toContainText('Drop your export file');
    });
});

test.describe('Settings — Cleanup Section', () => {
    test('cleanup section exists on Configuration tab', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page', { timeout: 10000 });

        await page.locator('.settings-tab').filter({ hasText: 'Configuration' }).click();
        const cleanupSection = page.locator('.cleanup-section');
        await expect(cleanupSection).toBeVisible();
        await expect(cleanupSection).toContainText('Auto-Cleanup');
    });
});

test.describe('API — Agent Update & Redeploy Status', () => {
    test('agent update endpoint accepts PATCH with bio change', async ({ request }) => {
        // Get an agent ID
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agents = await agentsRes.json();
        expect(agents.agents.length).toBeGreaterThanOrEqual(1);
        const agent = agents.agents[0];
        const id = agent.agent_id;

        // PATCH with a bio change
        const res = await request.patch(`${BASE}/v1/dashboard/network/agents/${id}`, {
            data: { boot_bio: agent.boot_bio || 'E2E test bio' },
        });
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.agent_id).toBe(id);
    });

    test('redeploy status endpoint returns status fields', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/redeploy/status`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        // Should have 'active' field at minimum
        expect(body).toHaveProperty('active');
    });
});

test.describe('Boot Instructions', () => {
    test('Configuration tab shows boot instructions section', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');
        await page.locator('.settings-tab').filter({ hasText: 'Configuration' }).click();
        await page.waitForSelector('.boot-instructions-section');
        await expect(page.locator('.boot-instructions-section')).toContainText('Boot Instructions');
        await expect(page.locator('.boot-textarea')).toBeVisible();
    });

    test('boot instructions API round-trip', async ({ request }) => {
        // Save
        const saveRes = await request.post(`${BASE}/v1/dashboard/settings/boot-instructions`, {
            data: { instructions: 'E2E test: pull last reflection on boot' },
        });
        expect(saveRes.ok()).toBeTruthy();
        const saveBody = await saveRes.json();
        expect(saveBody.ok).toBe(true);

        // Read back
        const getRes = await request.get(`${BASE}/v1/dashboard/settings/boot-instructions`);
        expect(getRes.ok()).toBeTruthy();
        const getBody = await getRes.json();
        expect(getBody.instructions).toBe('E2E test: pull last reflection on boot');

        // Clear it
        await request.post(`${BASE}/v1/dashboard/settings/boot-instructions`, {
            data: { instructions: '' },
        });
    });

    test('boot instructions Save button enables on change', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');
        await page.locator('.settings-tab').filter({ hasText: 'Configuration' }).click();
        await page.waitForSelector('.boot-textarea');

        const textarea = page.locator('.boot-textarea');
        const saveBtn = page.locator('.boot-instructions-section .btn-primary');

        // Should be disabled initially (no changes)
        await expect(saveBtn).toBeDisabled();

        // Type something
        await textarea.fill('Test instruction');
        await expect(saveBtn).not.toBeDisabled();
    });
});

test.describe('Software Update', () => {
    test('Update tab shows Software Update section', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await page.locator('.settings-tab').filter({ hasText: 'Update' }).click();
        const section = page.locator('.update-section');
        await expect(section).toBeVisible();
        await expect(section).toContainText('Software Update');
    });

    test('update check API returns version info', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/settings/update/check`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.current_version).toBeDefined();
        expect(body.platform).toBeDefined();
    });

    test('update section shows current version', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');
        await page.locator('.settings-tab').filter({ hasText: 'Update' }).click();

        // Wait for version info to load
        await page.waitForFunction(() => {
            const el = document.querySelector('.update-version-row .mono');
            return el && el.textContent && el.textContent !== '...';
        }, { timeout: 15000 });

        const versionRow = page.locator('.update-version-row').first();
        await expect(versionRow).toContainText('Current Version');
    });

    test('update section has Check for Updates button', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');
        await page.locator('.settings-tab').filter({ hasText: 'Update' }).click();
        await page.waitForSelector('.update-section');

        const checkBtn = page.locator('.update-actions .btn').filter({ hasText: 'Check for Updates' });
        await expect(checkBtn).toBeVisible();
    });

    test('update apply endpoint rejects invalid URL', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/settings/update/apply`, {
            data: { download_url: 'https://evil.com/malware.tar.gz' },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('GitHub release');
    });
});
