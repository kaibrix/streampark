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
            (page) => checkConfirmation(page, url, published, 'Publish'),
            (page) => checkConfirmation(page, url, published, 'Publish historical version'),
            (page) => checkConfirmation(page, url, direct, 'Submit directly'),
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
