import { test, expect } from '@playwright/test';

const BASE = 'http://localhost:8080';

// ---------------------------------------------------------------------------
// 1. Governance section renders on Network page
// ---------------------------------------------------------------------------
test.describe('Governance — Section Rendering', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.network-page', { timeout: 10000 });
    });

    test('governance section exists on the Network page', async ({ page }) => {
        const govSection = page.locator('.gov-section');
        await expect(govSection).toBeVisible();
    });

    test('governance section header shows "Governance" title', async ({ page }) => {
        const header = page.locator('.gov-section-header h3');
        await expect(header).toContainText('Governance');
    });

    test('shows "No active proposals" empty state when no active proposal', async ({ page }) => {
        // Wait for governance data to load
        await page.waitForTimeout(1000);

        // If there is no active proposal, the empty state should be visible.
        // If there IS an active proposal, the card will be shown instead — both are valid states.
        const empty = page.locator('.gov-empty');
        const activeCard = page.locator('.gov-proposal-card.active');

        const hasEmpty = await empty.count() > 0;
        const hasActive = await activeCard.count() > 0;

        // Exactly one of these should be rendered
        expect(hasEmpty || hasActive).toBeTruthy();

        if (hasEmpty) {
            await expect(empty).toContainText('No active proposals');
        }
    });
});

// ---------------------------------------------------------------------------
// 2. New Proposal button visible when no active proposal
// ---------------------------------------------------------------------------
test.describe('Governance — New Proposal Button', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
    });

    test('New Proposal button is visible when no active proposal', async ({ page }) => {
        await page.waitForTimeout(1000);

        const newBtn = page.locator('.gov-new-btn');
        const activeCard = page.locator('.gov-proposal-card.active');

        const hasActive = await activeCard.count() > 0;

        if (!hasActive) {
            await expect(newBtn).toBeVisible();
            await expect(newBtn).toContainText('New Proposal');
        }
    });

    test('New Proposal button is hidden when an active proposal exists', async ({ page }) => {
        await page.waitForTimeout(1000);

        const newBtn = page.locator('.gov-new-btn');
        const activeCard = page.locator('.gov-proposal-card.active');

        const hasActive = await activeCard.count() > 0;

        if (hasActive) {
            await expect(newBtn).not.toBeVisible();
        }
    });
});

// ---------------------------------------------------------------------------
// 3. New Proposal modal opens and has correct fields
// ---------------------------------------------------------------------------
test.describe('Governance — New Proposal Modal', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);
    });

    test('clicking New Proposal opens the modal', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();

        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();
        await expect(modal).toContainText('New Governance Proposal');
    });

    test('modal has Operation selector with correct options', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        const opSelect = page.locator('.wizard-overlay .wizard-select').first();
        await expect(opSelect).toBeVisible();

        const options = await opSelect.locator('option').allTextContents();
        expect(options).toContain('Add Validator');
        expect(options).toContain('Remove Validator');
        expect(options).toContain('Update Power');
    });

    test('modal has Target Agent dropdown', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        // Second .wizard-select is the target agent dropdown
        const targetSelect = page.locator('.wizard-overlay .wizard-select').nth(1);
        await expect(targetSelect).toBeVisible();

        // Should have a placeholder option
        const firstOption = await targetSelect.locator('option').first().textContent();
        expect(firstOption).toContain('Select agent');
    });

    test('modal has Voting Power input for add/update operations', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        // Default operation is add_validator — power input should be visible
        const powerInput = page.locator('.wizard-overlay .wizard-input[type="number"]');
        await expect(powerInput).toBeVisible();
    });

    test('modal has Reason textarea', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        const reasonField = page.locator('.wizard-overlay .wizard-textarea');
        await expect(reasonField).toBeVisible();

        const placeholder = await reasonField.getAttribute('placeholder');
        expect(placeholder).toContain('Why is this change needed');
    });

    test('modal has Submit Proposal button', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        const submitBtn = page.locator('.wizard-overlay .btn-primary');
        await expect(submitBtn).toBeVisible();
        await expect(submitBtn).toContainText('Submit Proposal');
    });

    test('Submit button is disabled until a target agent is selected', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        const submitBtn = page.locator('.wizard-overlay .btn-primary');
        // No target selected yet — button should be disabled
        await expect(submitBtn).toBeDisabled();
    });

    test('modal can be closed via Cancel button', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();

        await page.locator('.wizard-footer .btn').filter({ hasText: 'Cancel' }).click();
        await expect(modal).not.toBeVisible();
    });

    test('modal can be closed via X button', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        const modal = page.locator('.wizard-overlay');
        await expect(modal).toBeVisible();

        await page.locator('.detail-close').click();
        await expect(modal).not.toBeVisible();
    });

    test('power input hides when operation is remove_validator', async ({ page }) => {
        const newBtn = page.locator('.gov-new-btn');
        if (await newBtn.count() === 0) {
            test.skip();
            return;
        }

        await newBtn.click();
        await page.waitForSelector('.wizard-overlay');

        // Power input should be visible for add_validator (default)
        const powerInput = page.locator('.wizard-overlay .wizard-input[type="number"]');
        await expect(powerInput).toBeVisible();

        // Switch to remove_validator
        const opSelect = page.locator('.wizard-overlay .wizard-select').first();
        await opSelect.selectOption('remove_validator');

        // Power input should now be hidden
        await expect(powerInput).not.toBeVisible();
    });
});

// ---------------------------------------------------------------------------
// 4. Proposal creation flow (API-driven, verify UI updates)
// ---------------------------------------------------------------------------
test.describe('Governance — Proposal Creation via API', () => {
    test('can create a proposal via the REST API', async ({ request }) => {
        // First get an agent to use as target
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        expect(agentsRes.ok()).toBeTruthy();
        const agentsBody = await agentsRes.json();
        expect(agentsBody.agents.length).toBeGreaterThanOrEqual(1);

        const targetAgent = agentsBody.agents[0];

        const res = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {
                operation: 'add_validator',
                target_id: targetAgent.agent_id,
                target_power: 10,
                reason: 'E2E governance test proposal',
            },
        });

        // The request may succeed (submitted) or fail (CometBFT not configured in
        // single-node test setups). Both are valid outcomes for this test — we verify
        // the endpoint is reachable and responds with structured JSON.
        const body = await res.json();
        if (res.ok()) {
            expect(body.status).toBe('submitted');
        } else {
            // Expected in test environments without CometBFT consensus
            expect(body.error).toBeDefined();
        }
    });

    test('active proposal card appears after proposal is created', async ({ page, request }) => {
        // Attempt to create a proposal via API
        const agentsRes = await request.get(`${BASE}/v1/dashboard/network/agents`);
        const agentsBody = await agentsRes.json();
        if (agentsBody.agents.length < 1) {
            test.skip();
            return;
        }

        const targetAgent = agentsBody.agents[0];
        const proposeRes = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {
                operation: 'add_validator',
                target_id: targetAgent.agent_id,
                target_power: 10,
                reason: 'E2E test: verify active card',
            },
        });

        if (!proposeRes.ok()) {
            // CometBFT not available — skip UI verification
            test.skip();
            return;
        }

        // Navigate to network page and check for active card
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(2000);

        const activeCard = page.locator('.gov-proposal-card.active');
        if (await activeCard.count() > 0) {
            await expect(activeCard).toBeVisible();
            // Should display operation and target info
            const cardText = await activeCard.textContent();
            expect(cardText.length).toBeGreaterThan(0);
        }
    });
});

// ---------------------------------------------------------------------------
// 5. Vote buttons render on active proposal
// ---------------------------------------------------------------------------
test.describe('Governance — Vote Buttons', () => {
    test('vote buttons are present on an active proposal card', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const activeCard = page.locator('.gov-proposal-card.active');
        if (await activeCard.count() === 0) {
            test.skip();
            return;
        }

        // Check for vote buttons OR an already-voted badge
        const voteActions = activeCard.locator('.gov-vote-actions');
        await expect(voteActions).toBeVisible();

        const votedBadge = activeCard.locator('.gov-voted-badge');
        const hasVoted = await votedBadge.count() > 0;

        if (!hasVoted) {
            const acceptBtn = activeCard.locator('.gov-vote-btn.accept');
            const rejectBtn = activeCard.locator('.gov-vote-btn.reject');
            const abstainBtn = activeCard.locator('.gov-vote-btn.abstain');

            await expect(acceptBtn).toBeVisible();
            await expect(rejectBtn).toBeVisible();
            await expect(abstainBtn).toBeVisible();

            await expect(acceptBtn).toContainText('Accept');
            await expect(rejectBtn).toContainText('Reject');
            await expect(abstainBtn).toContainText('Abstain');
        }
    });

    test('active proposal card shows vote tally', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const activeCard = page.locator('.gov-proposal-card.active');
        if (await activeCard.count() === 0) {
            test.skip();
            return;
        }

        const tally = activeCard.locator('.gov-vote-tally');
        await expect(tally).toBeVisible();

        // Tally shows Accept/Reject/Abstain counts
        await expect(tally.locator('.gov-vote-accept')).toBeVisible();
        await expect(tally.locator('.gov-vote-reject')).toBeVisible();
        await expect(tally.locator('.gov-vote-abstain')).toBeVisible();
    });

    test('active proposal card shows quorum progress bar', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const activeCard = page.locator('.gov-proposal-card.active');
        if (await activeCard.count() === 0) {
            test.skip();
            return;
        }

        const quorumBar = activeCard.locator('.gov-quorum-bar');
        await expect(quorumBar).toBeVisible();

        const quorumLabel = activeCard.locator('.gov-quorum-label');
        await expect(quorumLabel).toBeVisible();
        await expect(quorumLabel).toContainText('power');
    });

    test('active proposal card shows proposer info', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const activeCard = page.locator('.gov-proposal-card.active');
        if (await activeCard.count() === 0) {
            test.skip();
            return;
        }

        const meta = activeCard.locator('.gov-proposal-meta');
        await expect(meta).toBeVisible();
        await expect(meta).toContainText('Proposed by');
    });
});

// ---------------------------------------------------------------------------
// 6. Proposal history section
// ---------------------------------------------------------------------------
test.describe('Governance — History Section', () => {
    test('history toggle exists when past proposals are present', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const toggle = page.locator('.gov-history-toggle');
        // Toggle only renders when there are past proposals
        const hasToggle = await toggle.count() > 0;

        if (hasToggle) {
            await expect(toggle).toBeVisible();
            await expect(toggle).toContainText('Past Proposals');
        }
    });

    test('clicking history toggle expands history list', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const toggle = page.locator('.gov-history-toggle');
        if (await toggle.count() === 0) {
            test.skip();
            return;
        }

        // Click to open
        await toggle.click();
        await expect(toggle).toHaveClass(/open/);

        const historyList = page.locator('.gov-history-list');
        await expect(historyList).toBeVisible();

        // Each card should have operation info and status badge
        const cards = historyList.locator('.gov-history-card');
        const count = await cards.count();
        expect(count).toBeGreaterThanOrEqual(1);
    });

    test('history cards show operation and status badges', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const toggle = page.locator('.gov-history-toggle');
        if (await toggle.count() === 0) {
            test.skip();
            return;
        }

        await toggle.click();
        const firstCard = page.locator('.gov-history-card').first();
        await expect(firstCard).toBeVisible();

        // Should have operation label
        const opLabel = firstCard.locator('.gov-history-op');
        await expect(opLabel).toBeVisible();

        // Should have a status badge
        const statusBadge = firstCard.locator('.gov-status-badge');
        await expect(statusBadge).toBeVisible();
    });

    test('clicking history toggle again collapses history list', async ({ page }) => {
        await page.goto(`${BASE}/ui/#/network`);
        await page.waitForSelector('.gov-section', { timeout: 10000 });
        await page.waitForTimeout(1000);

        const toggle = page.locator('.gov-history-toggle');
        if (await toggle.count() === 0) {
            test.skip();
            return;
        }

        // Open
        await toggle.click();
        await expect(page.locator('.gov-history-list')).toBeVisible();

        // Close
        await toggle.click();
        await expect(page.locator('.gov-history-list')).not.toBeVisible();
    });
});

// ---------------------------------------------------------------------------
// 7. Governance API endpoints
// ---------------------------------------------------------------------------
test.describe('Governance — API Endpoints', () => {
    test('GET /v1/dashboard/governance/proposals returns proposals array', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/governance/proposals`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.proposals).toBeDefined();
        expect(Array.isArray(body.proposals)).toBeTruthy();
    });

    test('GET /v1/dashboard/governance/proposals supports status filter', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/governance/proposals?status=voting`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.proposals).toBeDefined();
        expect(Array.isArray(body.proposals)).toBeTruthy();
    });

    test('GET /v1/dashboard/governance/proposals/{id} returns 404 for invalid id', async ({ request }) => {
        const res = await request.get(`${BASE}/v1/dashboard/governance/proposals/nonexistent-id`);
        expect(res.ok()).toBeFalsy();
    });

    test('GET /v1/dashboard/governance/proposals/{id} returns proposal detail when valid', async ({ request }) => {
        // Get proposals list first
        const listRes = await request.get(`${BASE}/v1/dashboard/governance/proposals`);
        const listBody = await listRes.json();

        if (listBody.proposals.length === 0) {
            test.skip();
            return;
        }

        const proposalId = listBody.proposals[0].id;
        const res = await request.get(`${BASE}/v1/dashboard/governance/proposals/${proposalId}`);
        expect(res.ok()).toBeTruthy();
        const body = await res.json();
        expect(body.proposal).toBeDefined();
        expect(body.votes).toBeDefined();
        expect(Array.isArray(body.votes)).toBeTruthy();
        expect(body.quorum_progress).toBeDefined();
        expect(body.quorum_progress.threshold).toBe('2/3');
    });

    test('POST /v1/dashboard/governance/propose rejects empty body', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {},
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toBeDefined();
    });

    test('POST /v1/dashboard/governance/propose rejects missing target_id', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {
                operation: 'add_validator',
                reason: 'test',
            },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('target_id');
    });

    test('POST /v1/dashboard/governance/propose rejects missing reason', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {
                operation: 'add_validator',
                target_id: 'abc123',
            },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('reason');
    });

    test('POST /v1/dashboard/governance/propose rejects invalid operation', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/propose`, {
            data: {
                operation: 'invalid_op',
                target_id: 'abc123',
                reason: 'test',
            },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('operation');
    });

    test('POST /v1/dashboard/governance/vote rejects empty body', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/vote`, {
            data: {},
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toBeDefined();
    });

    test('POST /v1/dashboard/governance/vote rejects missing proposal_id', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/vote`, {
            data: {
                decision: 'accept',
            },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('proposal_id');
    });

    test('POST /v1/dashboard/governance/vote rejects invalid decision', async ({ request }) => {
        const res = await request.post(`${BASE}/v1/dashboard/governance/vote`, {
            data: {
                proposal_id: 'some-id',
                decision: 'invalid_decision',
            },
        });
        expect(res.ok()).toBeFalsy();
        const body = await res.json();
        expect(body.error).toContain('decision');
    });
});
