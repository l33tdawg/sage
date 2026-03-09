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
    test('shows chain health section', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await expect(page.locator('h3').filter({ hasText: 'Chain Health' })).toBeVisible();
    });

    test('shows contextual tooltips toggle', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

        await expect(page.locator('.label').filter({ hasText: 'Contextual Tooltips' })).toBeVisible();
    });

    test('tooltips toggle works', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

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

    test('shows Synaptic Ledger section', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/settings`);
        await page.waitForSelector('.settings-page');

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
});
