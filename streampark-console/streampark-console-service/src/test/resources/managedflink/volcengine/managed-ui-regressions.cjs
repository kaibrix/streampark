/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Run with Playwright, an English Console session, a stopped published job and a finished
// directly submitted job. Pass the Console URL, storage-state file, and the two app IDs in order.
const assert = require('node:assert/strict');
const { chromium } = require('playwright');

async function checkCatalog(page, url) {
    await page.route(
        (uri) => uri.pathname.endsWith('/flink/managed/environments'),
        async (route) => {
            const response = await route.fetch();
            const body = await response.json();
            const entry = body.data[0];
            assert.ok(entry);
            const offset = Number(new URL(route.request().url()).searchParams.get('offset'));
            const count = offset === 0 ? 100 : 1;
            body.data = Array.from({ length: count }, (_, index) => ({
                ...entry,
                id: String(index + offset),
                name: 'Catalog fixture ' + (index + offset),
                enabled: true,
            }));
            await route.fulfill({ response, json: body });
        },
    );
    await page.goto(url + '/#/flink/app/add');
    await page.getByRole('button', { name: 'Create managed Flink job', exact: true }).click();
    const form = page
        .locator('.ant-card')
        .filter({ has: page.getByText('Create managed Flink job', { exact: true }) });
    await form
        .locator('.ant-form-item')
        .filter({ has: page.getByText('Managed environment', { exact: true }) })
        .locator('.ant-select-selector')
        .click();
    const dropdown = page.locator('.ant-select-dropdown:visible');
    const last = dropdown.getByText('Catalog fixture 100', { exact: true });
    const list = dropdown.locator('.rc-virtual-list-holder');
    for (let i = 0; i < 40 && !(await last.count()); i++) {
        await list.evaluate((element) => {
            element.scrollTop += element.clientHeight;
        });
        await page.waitForTimeout(50);
    }
    await last.click({ timeout: 3000 });
}

async function checkConfirmation(page, url, app, action) {
    let changed = false;
    const detail = (uri) => uri.pathname.endsWith('/flink/managed/apps/' + app);
    await page.route(detail, async (route) => {
        const response = await route.fetch();
        const body = await response.json();
        if (changed) body.data.configVersion = Number(body.data.configVersion) + 1;
        await route.fulfill({ response, json: body });
    });
    await page.goto(url + '/#/flink/app/detail?appId=' + encodeURIComponent(app));
    const pane = page.locator('.ant-tabs-tabpane-active');
    if (action === 'Publish historical version') {
        await page.getByRole('tab', { name: 'Versions', exact: true }).click();
        await pane.locator('.ant-select-selector').click();
        await page.locator('.ant-select-dropdown:visible .ant-select-item-option').first().click();
    }
    const button = pane.getByRole('button', { name: action, exact: true });
    await button.click();
    const confirmation = page.locator('.ant-popover:visible button.ant-btn-primary');
    await confirmation.waitFor();
    changed = true;
    await page.waitForResponse((response) => detail(new URL(response.url())));
    await confirmation.waitFor({ state: 'hidden', timeout: 5000 });
    await button.click();
    await confirmation.waitFor();
}

const success = (data) => ({ status: 'success', code: '200', data });

async function choose(form, page, label, option) {
    const input = form
        .locator('.ant-form-item')
        .filter({ has: page.locator('.ant-form-item-label').getByText(label, { exact: true }) })
        .locator('input[role=combobox]:not([disabled])');
    await input.focus();
    await input.press('ArrowDown');
    await page
        .locator('.ant-select-dropdown:visible')
        .getByText(option, { exact: true })
        .first()
        .waitFor({ state: 'attached' });
    await input.press('Enter');
}

async function checkManagedSettings(page, url) {
    const connection = {
        id: 'fixture-connection',
        name: 'Read-only connection',
        provider: 'volcengine',
        accountRef: 'fixture-account',
        credentialsManagedByServer: false,
        credentialVersion: 1,
    };
    const environment = {
        id: 'fixture-environment',
        name: 'Managed test environment',
        region: 'cn-beijing',
        provider: 'volcengine',
        connectionId: connection.id,
        configVersion: 2,
        enabled: true,
        target: JSON.stringify({
            projectId: '1',
            directoryId: '2',
            resourcePoolId: '3',
            resourcePoolName: 'test-pool',
            engineRef: 'FLINK_VERSION_1_17',
        }),
    };
    const report = { IDENTITY: 'AVAILABLE', PUBLISH_SQL: 'UNVERIFIED' };
    const reads = {
        '/connections': [connection],
        '/environments': [environment],
        '/providers': ['volcengine'],
        ['/environments/' + environment.id + '/capabilities']: report,
    };
    let rotations = 0;
    let updates = 0;
    await page.route((uri) => uri.pathname.includes('/flink/managed/'), async (route) => {
        const request = route.request();
        const target = new URL(request.url());
        const path = target.pathname.split('/flink/managed')[1];
        if (request.method() === 'GET' && path in reads) {
            await route.fulfill({ json: success(reads[path]) });
        } else if (path === '/connections/' + connection.id + '/rotate') {
            assert.equal(target.searchParams.get('environmentId'), environment.id);
            assert.ok(target.searchParams.get('teamId'));
            const credentials = request.postDataJSON();
            assert.equal(credentials.accountRef, connection.accountRef);
            assert.equal(credentials.accessKey, 'fixture-new-access-key');
            assert.equal(credentials.secretKey, 'fixture-new-secret-key');
            rotations++;
            connection.credentialVersion++;
            await route.fulfill({ json: success(report) });
        } else if (path === '/environments/' + environment.id && request.method() === 'PATCH') {
            const data = request.postDataJSON();
            assert.equal(data.expectedConfigVersion, 2);
            assert.equal(data.connectionId, connection.id);
            assert.equal(data.target.projectId, '1');
            assert.equal(data.name, 'Renamed environment');
            assert.equal(data.enabled, false);
            updates++;
            environment.configVersion++;
            environment.name = data.name;
            environment.enabled = data.enabled;
            await route.fulfill({ json: success(environment.id) });
        } else if (path === '/environments/' + environment.id + '/probe') {
            await route.fulfill({ json: success(report) });
        } else {
            await route.fallback();
        }
    });
    await page.goto(url + '/#/flink/app/add');
    await page.getByRole('button', { name: 'Create managed Flink job', exact: true }).click();
    await page.getByRole('button', { name: 'Managed Flink settings', exact: true }).click();
    const settings = page.getByRole('dialog', { name: 'Managed Flink settings' });
    await settings.getByText('Cloud connection', { exact: true }).click();
    const connectionForm = settings.locator('.ant-collapse-item').nth(0);
    const profile = connectionForm.getByRole('radio', { name: 'Console server profile', exact: true });
    assert.equal(await profile.isChecked(), true);
    assert.equal(await connectionForm.locator('input[type=password]').count(), 0);
    await connectionForm.getByRole('radio', { name: 'Access Key / Secret Key', exact: true }).check();
    await connectionForm.locator('input[type=password]').nth(1).waitFor();
    await profile.check();
    assert.equal(await connectionForm.locator('input[type=password]').count(), 0);
    await choose(connectionForm, page, 'Existing connection', connection.name);
    await choose(connectionForm, page, 'Environment for credential verification', environment.name);
    const secrets = connectionForm.locator('input[type=password]');
    await secrets.nth(0).fill('fixture-new-access-key');
    await secrets.nth(1).fill('fixture-new-secret-key');
    await connectionForm.getByRole('button', { name: 'Rotate credentials', exact: true }).click();
    await connectionForm.getByText('Stored credentials · version 2', { exact: true }).waitFor();
    assert.equal(await secrets.nth(0).inputValue(), '');
    assert.equal(await secrets.nth(1).inputValue(), '');
    await settings.getByText('Managed environment', { exact: true }).click();
    const environmentForm = settings.locator('.ant-collapse-item').nth(1);
    await choose(environmentForm, page, 'Existing environment', environment.name);
    await environmentForm.getByText('IDENTITY', { exact: true }).waitFor();
    await environmentForm
        .locator('.ant-form-item')
        .filter({ has: page.locator('.ant-form-item-label').getByText('Name', { exact: true }) })
        .locator('input')
        .fill('Renamed environment');
    await environmentForm.getByRole('switch').click();
    await environmentForm.getByRole('button', { name: 'Save environment', exact: true }).click();
    await settings.getByText('Environment saved', { exact: true }).waitFor();
    assert.equal(rotations, 1);
    assert.equal(updates, 1);
}

async function checkOperationReview(page, url, app) {
    const operation = {
        id: 'review-operation',
        command_type: 'PUBLISH',
        status: 'VERIFY_REQUIRED',
        step_index: 0,
        updated_at: 1700000000000,
        canConfirmDraft: true,
        canContinue: false,
        steps: [{ step_index: 0, action: 'CREATE_DRAFT', state: 'DISPATCHED' }],
        reviewHistory: [],
    };
    const path = '/flink/managed/apps/' + app + '/operations';
    let resolutions = 0;
    await page.route((uri) => uri.pathname.includes(path), async (route) => {
        const request = route.request();
        const target = new URL(request.url());
        if (target.pathname.endsWith('/resolve')) {
            const payload = request.postDataJSON();
            assert.equal(payload.expectedStepIndex, operation.step_index);
            assert.equal(payload.expectedUpdatedAt, operation.updated_at);
            if (resolutions === 0) {
                assert.equal(payload.decision, 'CONFIRM_DRAFT');
                assert.equal(payload.draftId, '10001');
                assert.equal(payload.evidenceReference, 'AUDIT-10001');
                operation.canConfirmDraft = false;
                operation.canContinue = true;
                operation.steps[0].state = 'CONFIRMED';
                operation.reviewHistory.push({
                    decision: 'CONFIRM_DRAFT',
                    stepIndex: 0,
                    actor: 'fixture-user',
                    reviewedAt: operation.updated_at,
                    draftId: payload.draftId,
                    evidenceReference: payload.evidenceReference,
                });
            } else {
                assert.equal(payload.decision, 'CONTINUE');
                operation.canContinue = false;
                operation.status = 'PENDING';
            }
            resolutions++;
            operation.updated_at++;
            await route.fulfill({ json: success(null) });
        } else {
            await route.fulfill({
                json: success(target.pathname.endsWith(path) ? [operation] : operation),
            });
        }
    });
    await page.goto(url + '/#/flink/app/detail?appId=' + encodeURIComponent(app));
    await page.getByRole('tab', { name: 'Operations', exact: true }).click();
    await page.getByRole('button', { name: 'PUBLISH · VERIFY_REQUIRED', exact: true }).click();
    const review = page.locator('.ant-drawer-body');
    await review.locator('input').nth(0).fill('10001');
    await review.locator('input').nth(1).fill('AUDIT-10001');
    await review.getByRole('button', { name: 'Confirm cloud draft', exact: true }).click();
    await page.locator('.ant-popover:visible button.ant-btn-primary').click();
    await review.getByText('AUDIT-10001', { exact: false }).waitFor();
    await review.getByRole('button', { name: 'Continue confirmed operation', exact: true }).click();
    await page.locator('.ant-popover:visible button.ant-btn-primary').click();
    await review.getByText('PUBLISH · PENDING', { exact: true }).waitFor();
    assert.equal(resolutions, 2);
}

async function main() {
    const [url, storageState, published, direct] = process.argv.slice(2);
    assert.ok(
        url && storageState && published && direct,
        'Expected URL, storage state, published ID, direct ID',
    );
    const browser = await chromium.launch({ executablePath: process.env.CHROME_BIN });
    try {
        const context = await browser.newContext({ storageState, locale: 'en-US' });
        const cases = [
            (page) => checkCatalog(page, url),
            (page) => checkManagedSettings(page, url),
            (page) => checkConfirmation(page, url, published, 'Publish'),
            (page) => checkConfirmation(page, url, published, 'Publish historical version'),
            (page) => checkConfirmation(page, url, direct, 'Submit directly'),
            (page) => checkOperationReview(page, url, published),
        ];
        for (const run of cases) {
            const page = await context.newPage();
            try {
                await run(page);
            } finally {
                await page.close();
            }
        }
    } finally {
        await browser.close();
    }
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
