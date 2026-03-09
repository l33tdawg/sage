import { test, expect } from '@playwright/test';

const BASE = 'http://localhost:8080';

test.describe('Network Page', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list', { timeout: 10000 });
    });

    test('renders agent list with at least one agent', async ({ page }) => {
        const cards = page.locator('.agent-card-row');
        await expect(cards.first()).toBeVisible();
        const count = await cards.count();
        expect(count).toBeGreaterThanOrEqual(1);
    });

    test('shows network header with agent count', async ({ page }) => {
        const header = page.locator('.network-header');
        await expect(header).toContainText('Network');
        await expect(header).toContainText('agent');
    });

    test('shows Add Agent card', async ({ page }) => {
        const addCard = page.locator('.agent-card-add');
        await expect(addCard).toBeVisible();
        await expect(addCard).toContainText('Add Agent');
    });

    test('expands agent card on click with tabs', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        const expanded = page.locator('.agent-expanded.open');
        await expect(expanded).toBeVisible();

        // Should show 3 tabs
        const tabs = page.locator('.agent-tab');
        await expect(tabs).toHaveCount(3);
        await expect(tabs.nth(0)).toContainText('Overview');
        await expect(tabs.nth(1)).toContainText('Access Control');
        await expect(tabs.nth(2)).toContainText('Activity');
    });

    test('Overview tab shows agent info', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-overview-grid');

        // Should show key info fields
        await expect(page.locator('.agent-info-label').filter({ hasText: 'Name' }).first()).toBeVisible();
        await expect(page.locator('.agent-info-label').filter({ hasText: 'Status' }).first()).toBeVisible();
        await expect(page.locator('.agent-info-label').filter({ hasText: 'Memories' }).first()).toBeVisible();
        await expect(page.locator('.agent-info-label').filter({ hasText: 'Agent ID' }).first()).toBeVisible();
    });

    test('Overview tab Edit mode shows input fields', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        // Click Edit
        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Edit' }).click();

        // Name should be an input
        const nameInput = page.locator('.agent-overview-grid input.wizard-input');
        await expect(nameInput).toBeVisible();

        // Bio should be a textarea
        const bioTextarea = page.locator('.agent-overview-grid textarea.wizard-textarea');
        await expect(bioTextarea).toBeVisible();

        // Should show Save and Cancel buttons
        await expect(page.locator('.agent-action-bar .btn-primary').filter({ hasText: 'Save' })).toBeVisible();
        await expect(page.locator('.agent-action-bar .btn').filter({ hasText: 'Cancel' })).toBeVisible();
    });

    test('Overview tab Cancel exits edit mode', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Edit' }).click();
        await expect(page.locator('.agent-overview-grid input.wizard-input')).toBeVisible();

        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Cancel' }).click();
        await expect(page.locator('.agent-overview-grid input.wizard-input')).not.toBeVisible();
    });

    test('Access Control tab shows role selector', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        // Switch to Access Control tab
        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        // Should show role cards
        const roleCards = page.locator('.role-card');
        await expect(roleCards).toHaveCount(3);
        await expect(roleCards.nth(0)).toContainText('Admin');
        await expect(roleCards.nth(1)).toContainText('Member');
        await expect(roleCards.nth(2)).toContainText('Observer');
    });

    test('Access Control tab shows domain access matrix', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        // If the agent is a member, should show the domain matrix
        // If admin, should show "Admins have full access"
        const matrix = page.locator('.domain-matrix');
        await expect(matrix).toBeVisible();
    });

    test('Access Control tab shows clearance slider', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        const slider = page.locator('.clearance-row input[type="range"]');
        await expect(slider).toBeVisible();

        const label = page.locator('.clearance-row .clearance-label');
        await expect(label).toBeVisible();
    });

    test('Access Control tab Save button is disabled when no changes', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        const saveBtn = page.locator('.access-save-bar .btn-primary');
        await expect(saveBtn).toBeVisible();
        await expect(saveBtn).toBeDisabled();
    });

    test('Access Control tab changing role enables Save', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        // Click a different role
        await page.locator('.role-card').filter({ hasText: 'Observer' }).click();

        const saveBtn = page.locator('.access-save-bar .btn-primary');
        await expect(saveBtn).toBeEnabled();

        // Should show "Unsaved changes"
        await expect(page.locator('.access-dirty')).toBeVisible();
    });

    test('Activity tab shows stats and memory list', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        await page.locator('.agent-tab').filter({ hasText: 'Activity' }).click();

        // Should show stat cards
        const statCards = page.locator('.activity-stat-card');
        await expect(statCards.first()).toBeVisible();
    });

    test('action bar only visible on Overview tab', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();

        // Overview tab — action bar visible
        await expect(page.locator('.agent-action-bar')).toBeVisible();

        // Access Control tab — action bar hidden
        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();
        await expect(page.locator('.agent-action-bar')).not.toBeVisible();

        // Activity tab — action bar hidden
        await page.locator('.agent-tab').filter({ hasText: 'Activity' }).click();
        await expect(page.locator('.agent-action-bar')).not.toBeVisible();
    });

    test('collapse accordion by clicking expanded card', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await expect(page.locator('.agent-expanded.open')).toBeVisible();

        // Click same card again to collapse
        await firstCard.click();
        await expect(page.locator('.agent-expanded.open')).not.toBeVisible();
    });

    test('tab switching resets edit mode', async ({ page }) => {
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        // Enter edit mode
        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Edit' }).click();
        await expect(page.locator('.agent-overview-grid input.wizard-input')).toBeVisible();

        // Switch to Access Control
        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();

        // Switch back to Overview — should NOT be in edit mode
        await page.locator('.agent-tab').filter({ hasText: 'Overview' }).click();
        await expect(page.locator('.agent-overview-grid input.wizard-input')).not.toBeVisible();
    });
});

test.describe('Network Page — Last Admin Protection', () => {
    test('Remove button is disabled for last admin', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list');

        // Find the admin agent card
        const adminCard = page.locator('.agent-card-row').filter({ hasText: 'ADMIN' });
        if (await adminCard.count() > 0) {
            await adminCard.first().click();
            await page.waitForSelector('.agent-action-bar');

            const removeBtn = page.locator('.agent-action-bar .btn-danger');
            await expect(removeBtn).toBeVisible();
            // Should be disabled (has btn-disabled class)
            await expect(removeBtn).toHaveClass(/btn-disabled/);
        }
    });
});

test.describe('Add Agent Wizard', () => {
    test('opens wizard on Add Agent click', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        const wizard = page.locator('.wizard-overlay');
        await expect(wizard).toBeVisible();
        await expect(wizard).toContainText('Add Agent');
    });

    test('Step 1: can enter name and select template', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        const nameInput = page.locator('.wizard-input').first();
        await expect(nameInput).toBeVisible();
        await nameInput.fill('Test Agent');

        // Templates are in a dropdown select
        const templateSelect = page.locator('select').first();
        await expect(templateSelect).toBeVisible();
        const options = await templateSelect.locator('option').count();
        expect(options).toBeGreaterThanOrEqual(2); // At least custom + one template
    });

    test('Step 2: shows role selector and domain matrix (not JSON)', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        // Fill step 1 and advance
        await page.locator('.wizard-input').first().fill('Test Agent');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Step 2 should show role selector
        const roleCards = page.locator('.role-card');
        await expect(roleCards).toHaveCount(3);

        // Should show domain matrix, NOT a JSON textarea
        const matrix = page.locator('.domain-matrix');
        await expect(matrix).toBeVisible();

        // Should NOT have a JSON textarea
        const jsonLabel = page.locator('label').filter({ hasText: /JSON/ });
        await expect(jsonLabel).not.toBeVisible();

        // Should show clearance slider
        const slider = page.locator('.clearance-row input[type="range"]');
        await expect(slider).toBeVisible();
    });

    test('wizard can be closed', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        const wizard = page.locator('.wizard-overlay');
        await expect(wizard).toBeVisible();

        // Close button
        await page.locator('.wizard-close, .detail-close').first().click();
        await expect(wizard).not.toBeVisible();
    });

    test('Step 3: shows connect method cards (Bundle and LAN)', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        // Step 1 → fill and advance
        await page.locator('.wizard-input').first().fill('Test Agent');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Step 2 → advance
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Step 3 — should show two connect cards
        const connectCards = page.locator('.connect-card');
        await expect(connectCards).toHaveCount(2);

        // Bundle card
        await expect(connectCards.nth(0)).toContainText('Download Bundle');
        await expect(connectCards.nth(0)).toHaveClass(/selected/); // default selection

        // LAN card
        await expect(connectCards.nth(1)).toContainText('Easy Setup');
        await expect(connectCards.nth(1)).toContainText('LAN');
    });

    test('Step 3: can switch connect method to LAN', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        await page.locator('.wizard-input').first().fill('Test Agent');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Click LAN card
        await page.locator('.connect-card').nth(1).click();
        await expect(page.locator('.connect-card').nth(1)).toHaveClass(/selected/);

        // Summary should show LAN Pairing
        await expect(page.locator('.summary-card')).toContainText('LAN Pairing');
    });

    test('Step 3: shows warning banner about chain pause', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        await page.locator('.wizard-input').first().fill('Test Agent');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        await expect(page.locator('.warning-banner')).toContainText('pause the chain');
    });

    test('Step 3: shows summary with all settings', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        await page.locator('.wizard-input').first().fill('My Agent');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        const summary = page.locator('.summary-card');
        await expect(summary).toContainText('My Agent');
        await expect(summary).toContainText('member'); // default role
        await expect(summary).toContainText('Bundle Download'); // default connect
    });
});

test.describe('Key Rotation', () => {
    test('shows Rotate Key button in action bar', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list');

        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        const rotateBtn = page.locator('.agent-action-bar .btn').filter({ hasText: 'Rotate Key' });
        await expect(rotateBtn).toBeVisible();
    });

    test('Rotate Key opens confirmation modal', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list');

        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Rotate Key' }).click();

        // Confirmation modal should appear
        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();
        await expect(modal).toContainText('Rotate Agent Key');
        await expect(modal).toContainText('new Ed25519 identity key');
        await expect(modal).toContainText('old key will be permanently retired');
    });

    test('Rotate Key confirmation can be cancelled', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list');

        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Rotate Key' }).click();
        await expect(page.locator('.wizard-overlay')).toBeVisible();

        // Cancel button
        await page.locator('.wizard-footer .btn').filter({ hasText: 'Cancel' }).click();
        await expect(page.locator('.wizard-overlay')).not.toBeVisible();
    });

    test('Rotate Key confirmation has warning banner', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list');

        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Rotate Key' }).click();

        await expect(page.locator('.wizard-overlay .warning-banner')).toContainText('new bundle after rotation');
    });
});

test.describe('API — Pairing & Rotation', () => {
    test('pairing endpoint returns code for valid agent', async ({ request }) => {
        // First get an agent ID
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agents = await agentsRes.json();
        expect(agents.agents.length).toBeGreaterThanOrEqual(1);
        const agentId = agents.agents[0].agent_id;

        // Create pairing code
        const pairRes = await request.post(`${BASE}/v1/dashboard/network/agents/${agentId}/pair`);
        expect(pairRes.ok()).toBeTruthy();
        const pairData = await pairRes.json();
        expect(pairData.code).toBeDefined();
        expect(pairData.code).toMatch(/^SAG-[A-Z0-9]+$/);
        expect(pairData.expires_at).toBeDefined();
        expect(pairData.ttl_seconds).toBeGreaterThanOrEqual(895); // ~15 min, allow for rounding
    });

    test('pairing code is consumed after redeem attempt', async ({ request }) => {
        // Create a pairing code
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agents = await agentsRes.json();
        const agentId = agents.agents[0].agent_id;

        const pairRes = await request.post(`${BASE}/v1/dashboard/network/agents/${agentId}/pair`);
        const pairData = await pairRes.json();
        expect(pairData.code).toBeDefined();

        // First redeem attempt — may fail (no bundle for auto-seeded agents) but consumes the code
        await request.get(`${BASE}/v1/dashboard/network/pair/${pairData.code}`);

        // Second attempt — code should be consumed, returns 404
        const redeemRes2 = await request.get(`${BASE}/v1/dashboard/network/pair/${pairData.code}`);
        expect(redeemRes2.ok()).toBeFalsy();
    });

    test('invalid pairing code returns 404', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/pair/SAG-ZZZZZ`);
        expect(res.ok()).toBeFalsy();
    });

    test('multiple pairing codes can be generated', async ({ request }) => {
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agents = await agentsRes.json();
        const agentId = agents.agents[0].agent_id;

        const res1 = await request.post(`${BASE}/v1/dashboard/network/agents/${agentId}/pair`);
        const res2 = await request.post(`${BASE}/v1/dashboard/network/agents/${agentId}/pair`);
        const data1 = await res1.json();
        const data2 = await res2.json();

        // Each call should produce a unique code
        expect(data1.code).not.toBe(data2.code);
    });

    test('templates API has expected fields', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/network/templates`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.templates.length).toBeGreaterThanOrEqual(1);
        // Each template should have name, role, bio, clearance
        const t = body.templates[0];
        expect(t.name).toBeDefined();
        expect(t.role).toBeDefined();
        expect(t.bio).toBeDefined();
        expect(t.clearance).toBeDefined();
    });
});

test.describe('Access Control — Domain & Clearance Interactions', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list', { timeout: 10000 });

        // Expand first agent and switch to Access Control tab
        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-expanded.open');
        await page.locator('.agent-tab').filter({ hasText: 'Access Control' }).click();
    });

    test('domain matrix checkbox toggle enables Save button', async ({ page }) => {
        const matrix = page.locator('.domain-matrix');
        await expect(matrix).toBeVisible();

        // Find a checkbox in the domain matrix and toggle it
        const checkbox = matrix.locator('input[type="checkbox"]').first();
        await checkbox.evaluate(el => el.click());

        const saveBtn = page.locator('.access-save-bar .btn-primary');
        await expect(saveBtn).toBeEnabled();
    });

    test('clearance slider change enables Save button', async ({ page }) => {
        const slider = page.locator('.clearance-row input[type="range"]');
        await expect(slider).toBeVisible();

        // Change slider value via JS
        await slider.evaluate(el => {
            const newVal = parseInt(el.value) >= parseInt(el.max) ? parseInt(el.min) + 1 : parseInt(el.value) + 1;
            el.value = newVal;
            el.dispatchEvent(new Event('input', { bubbles: true }));
        });

        const saveBtn = page.locator('.access-save-bar .btn-primary');
        await expect(saveBtn).toBeEnabled();
    });

    test('Save button saves and shows confirmation', async ({ page }) => {
        // Make a change to enable save
        const slider = page.locator('.clearance-row input[type="range"]');
        await slider.evaluate(el => {
            const newVal = parseInt(el.value) >= parseInt(el.max) ? parseInt(el.min) + 1 : parseInt(el.value) + 1;
            el.value = newVal;
            el.dispatchEvent(new Event('input', { bubbles: true }));
        });

        const saveBtn = page.locator('.access-save-bar .btn-primary');
        await expect(saveBtn).toBeEnabled();
        await saveBtn.click();

        // Should show "Saved" confirmation text
        const savedConfirm = page.locator('.access-saved');
        await expect(savedConfirm).toBeVisible({ timeout: 5000 });
        await expect(savedConfirm).toContainText('Saved');
    });
});

test.describe('Edit Mode — Save Persists', () => {
    test('saving name change in edit mode succeeds', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list', { timeout: 10000 });

        const firstCard = page.locator('.agent-card-row').first();
        await firstCard.click();
        await page.waitForSelector('.agent-action-bar');

        // Enter edit mode
        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Edit' }).click();

        const nameInput = page.locator('.agent-overview-grid input.wizard-input');
        await expect(nameInput).toBeVisible();

        // Read current name and modify it
        const currentName = await nameInput.inputValue();
        const newName = currentName + ' E2E';
        await nameInput.fill(newName);

        // Click Save
        await page.locator('.agent-action-bar .btn-primary').filter({ hasText: 'Save' }).click();

        // After save, edit mode should exit (no input visible)
        await expect(nameInput).not.toBeVisible({ timeout: 5000 });

        // Restore original name to avoid test pollution
        await page.locator('.agent-action-bar .btn').filter({ hasText: 'Edit' }).click();
        const restoredInput = page.locator('.agent-overview-grid input.wizard-input');
        await restoredInput.fill(currentName);
        await page.locator('.agent-action-bar .btn-primary').filter({ hasText: 'Save' }).click();
    });
});

test.describe('Wizard — Template & Step 3 Defaults', () => {
    test('template selection populates bio field', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        // Wait for templates to load (async fetch)
        const templateSelect = page.locator('select').first();
        await expect(templateSelect).toBeVisible();

        // Wait for template options to populate (fetched from /templates API)
        await page.waitForFunction(() => {
            const sel = document.querySelector('select');
            return sel && sel.options.length >= 2;
        }, { timeout: 5000 });

        const options = templateSelect.locator('option');
        const optionCount = await options.count();
        expect(optionCount).toBeGreaterThanOrEqual(2);

        // Select the first actual template (index 1, since 0 is placeholder)
        await templateSelect.selectOption({ index: 1 });

        // Bio textarea should now be populated
        const bioTextarea = page.locator('.wizard-textarea');
        await expect(bioTextarea).toBeVisible();
        const bioValue = await bioTextarea.inputValue();
        expect(bioValue.length).toBeGreaterThan(0);
    });

    test('Step 3 — Bundle card is selected by default', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-card-add');
        await page.locator('.agent-card-add').click();

        // Step 1 → fill and advance
        await page.locator('.wizard-input').first().fill('Default Test');
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Step 2 → advance
        await page.locator('.btn').filter({ hasText: 'Next' }).click();

        // Step 3 — Bundle card (first connect card) should have 'selected' class
        const bundleCard = page.locator('.connect-card').nth(0);
        await expect(bundleCard).toHaveClass(/selected/);
    });
});

test.describe('Agent Card — Role Badge', () => {
    test('agent cards display role badges', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.agent-list', { timeout: 10000 });

        // At least one agent card should show a role badge
        const roleBadges = page.locator('.agent-role-badge');
        const count = await roleBadges.count();
        expect(count).toBeGreaterThanOrEqual(1);

        // Badge text should be a valid role
        const badgeText = await roleBadges.first().textContent();
        expect(['admin', 'member', 'observer']).toContain(badgeText.trim().toLowerCase());
    });
});
