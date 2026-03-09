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
});
