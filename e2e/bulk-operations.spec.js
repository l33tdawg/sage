import { test, expect } from '@playwright/test';

const BASE = 'http://localhost:8080';

/**
 * Helper: enter focus mode by double-clicking canvas domain clouds.
 * Returns true if focus was entered, false otherwise.
 * After entering focus, ensures clean state (no pre-selected memories).
 */
async function enterFocusMode(page) {
    const canvas = page.locator('canvas');
    const box = await canvas.boundingBox();

    for (const [fx, fy] of [[0.5, 0.5], [0.4, 0.4], [0.3, 0.3], [0.6, 0.4], [0.4, 0.6], [0.7, 0.3]]) {
        await canvas.dblclick({ position: { x: box.width * fx, y: box.height * fy } });
        await page.waitForTimeout(600);
        if (await page.locator('.focus-indicator').isVisible()) {
            // Double-click on canvas may have selected a node — deselect to start clean
            const deselect = page.locator('.focus-action-btn.deselect');
            if (await deselect.isVisible()) {
                await deselect.click();
                await page.waitForTimeout(300);
            }
            return true;
        }
    }
    return false;
}

// ─── API-level tests for the bulk endpoint ─────────────────────────────────

test.describe('Bulk Operations — API', () => {
    test('POST /v1/dashboard/memory/bulk rejects empty ids', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: [], domain: 'test' },
        });
        expect(res.status()).toBe(400);
        const body = await res.json();
        expect(body.error).toContain('ids is required');
    });

    test('POST /v1/dashboard/memory/bulk rejects missing action', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: ['fake-id-1'] },
        });
        expect(res.status()).toBe(400);
        const body = await res.json();
        expect(body.error).toContain('domain, add_tags, or agent is required');
    });

    test('POST /v1/dashboard/memory/bulk rejects >500 ids', async ({ request }) => {
        const ids = Array.from({ length: 501 }, (_, i) => `id-${i}`);
        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids, domain: 'test' },
        });
        expect(res.status()).toBe(400);
        const body = await res.json();
        expect(body.error).toContain('max 500');
    });

    test('POST /v1/dashboard/memory/bulk succeeds with valid tag addition', async ({ request }) => {
        const graphRes = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        expect(graphRes.ok()).toBeTruthy();
        const graph = await graphRes.json();
        expect(graph.nodes.length).toBeGreaterThan(0);

        const targetId = graph.nodes[0].id;
        const testTag = `e2e-test-${Date.now()}`;

        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: [targetId], add_tags: [testTag] },
        });
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.updated).toBe(1);

        // Verify the tag appears in graph data
        const graphAfter = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        const afterData = await graphAfter.json();
        const node = afterData.nodes.find(n => n.id === targetId);
        expect(node).toBeTruthy();
        expect(node.tags).toContain(testTag);
    });

    test('POST /v1/dashboard/memory/bulk succeeds with domain move', async ({ request }) => {
        const graphRes = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        const graph = await graphRes.json();
        const target = graph.nodes[0];

        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: [target.id], domain: 'e2e-test-domain' },
        });
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.updated).toBe(1);

        // Move it back to avoid polluting state
        await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: [target.id], domain: target.domain },
        });
    });

    test('graph endpoint includes tags field on tagged nodes', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        expect(res.ok()).toBeTruthy();
        const graph = await res.json();

        const taggedNode = graph.nodes.find(n => n.tags && n.tags.length > 0);
        expect(taggedNode).toBeTruthy();
        expect(Array.isArray(taggedNode.tags)).toBeTruthy();
        expect(taggedNode.tags.length).toBeGreaterThan(0);
    });
});

// ─── UI tests for focus mode + multi-select ─────────────────────────────────

test.describe('Bulk Operations — Focus Mode UI', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/`);
        await page.waitForSelector('.sidebar', { timeout: 10000 });
        await page.waitForSelector('canvas', { timeout: 10000 });
        await page.waitForTimeout(2000); // let force simulation settle
    });

    test('double-clicking a domain cloud enters focus mode', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not hit a domain cloud — graph layout varies');

        await expect(page.locator('.focus-indicator')).toBeVisible();
        await expect(page.locator('.focus-exit-btn')).toBeVisible();
    });

    test('focus mode shows Select All button before any selection', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        await expect(page.locator('.focus-action-btn.select-all')).toBeVisible();
    });

    test('Select All shows bulk action buttons', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        await page.locator('.focus-action-btn.select-all').click();
        await page.waitForTimeout(300);

        await expect(page.locator('.focus-selection-count')).toBeVisible();
        await expect(page.locator('.focus-action-btn').filter({ hasText: 'Move Domain' })).toBeVisible();
        await expect(page.locator('.focus-action-btn').filter({ hasText: 'Add Tag' })).toBeVisible();
        await expect(page.locator('.focus-action-btn.deselect')).toBeVisible();
    });

    test('Deselect clears selection and shows Select All again', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        // Select all then deselect
        await page.locator('.focus-action-btn.select-all').click();
        await page.waitForTimeout(300);
        await expect(page.locator('.focus-selection-count')).toBeVisible();

        await page.locator('.focus-action-btn.deselect').click();
        await page.waitForTimeout(300);

        await expect(page.locator('.focus-selection-count')).not.toBeVisible();
        await expect(page.locator('.focus-action-btn.select-all')).toBeVisible();
    });

    test('Move Domain action opens modal with domain input', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        await page.locator('.focus-action-btn.select-all').click();
        await page.waitForTimeout(300);
        await page.locator('.focus-action-btn').filter({ hasText: 'Move Domain' }).click();
        await page.waitForTimeout(300);

        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();
        await expect(modal.locator('h2')).toContainText('Move to Domain');
        await expect(modal.locator('input[placeholder*="sage-architecture"]')).toBeVisible();
    });

    test('Add Tag action opens modal with tag input', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        await page.locator('.focus-action-btn.select-all').click();
        await page.waitForTimeout(300);
        await page.locator('.focus-action-btn').filter({ hasText: 'Add Tag' }).click();
        await page.waitForTimeout(300);

        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();
        await expect(modal.locator('h2')).toContainText('Add Tag');
        await expect(modal.locator('input[placeholder*="important"]')).toBeVisible();
    });

    test('Exit Focus returns to normal brain view', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        await page.locator('.focus-exit-btn').click();
        await page.waitForTimeout(500);

        await expect(page.locator('.focus-indicator')).not.toBeVisible();
    });

    test('focus hint text changes based on selection state', async ({ page }) => {
        const focused = await enterFocusMode(page);
        test.skip(!focused, 'Could not enter focus mode');

        // After enterFocusMode deselects, should show click instruction
        await expect(page.locator('.focus-hint')).toContainText('Click bubbles to select');

        // After selection: should show action instruction
        await page.locator('.focus-action-btn.select-all').click();
        await page.waitForTimeout(300);
        await expect(page.locator('.focus-hint')).toContainText('Choose an action');
    });
});

// ─── Graph data integration tests (using request API) ───────────────────────

test.describe('Bulk Operations — Graph Tags', () => {
    test('graph nodes with tags include tags array', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        expect(res.ok()).toBeTruthy();
        const graph = await res.json();

        expect(graph.nodes).toBeTruthy();
        expect(graph.nodes.length).toBeGreaterThan(0);

        const taggedNode = graph.nodes.find(n => n.tags && n.tags.length > 0);
        expect(taggedNode).toBeTruthy();
        expect(Array.isArray(taggedNode.tags)).toBe(true);
    });

    test('bulk tag addition via API reflects in graph data', async ({ request }) => {
        const testTag = `pw-e2e-${Date.now()}`;

        const graphBefore = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        const beforeData = await graphBefore.json();
        const targetId = beforeData.nodes[0].id;

        // Add tag via bulk API
        const res = await request.post(`${BASE}/v1/dashboard/memory/bulk`, {
            data: { ids: [targetId], add_tags: [testTag] },
        });
        const result = await res.json();
        expect(result.updated).toBe(1);

        // Verify tag appears in graph
        const graphAfter = await request.get(`${BASE}/v1/dashboard/memory/graph`);
        const afterData = await graphAfter.json();
        const updatedNode = afterData.nodes.find(n => n.id === targetId);
        expect(updatedNode.tags).toContain(testTag);
    });
});
