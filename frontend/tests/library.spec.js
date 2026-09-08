import { test, expect } from '@playwright/test';
import { descendants, orderedFolders } from '../src/library.js';

const fixture = {
  folders: [
    { name: 'A1', count: 1, parent: null },
    { name: 'Basics', count: 1, parent: 'A1' },
    { name: 'A2', count: 1, parent: null },
    { name: 'B1', count: 0, parent: null },
    { name: 'Uncategorized', count: 1, parent: null },
  ],
  decks: [
    { name: 'Greetings', folder: 'A1' },
    { name: 'Everyday words', folder: 'Basics' },
    { name: 'Conversations', folder: 'A2' },
    { name: 'Quick review', folder: null },
  ],
  folder_order: ['A1', 'Basics', 'A2', 'B1'],
};

async function openLibrary(page, data = fixture, query = '') {
  await page.route('**/home-data', route => route.fulfill({ json: data }));
  await page.goto(`/static/react/index.html${query}`);
  await expect(page.getByText('Loading your library…', { exact: true })).toHaveCount(0);
}

test('mobile library has no overflow and searches nested folders and decks', async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await openLibrary(page);
  await expect(page.locator('.folder-card')).toHaveCount(4);
  await expect(page.locator('.folder-card').first()).toContainText('1 subfolder · 2 decks');
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({ path: testInfo.outputPath('home-mobile.png'), fullPage: true });
  const search = page.getByRole('searchbox', { name: 'Search folders and decks' });
  await search.fill('basics');
  await expect(page.getByRole('link', { name: 'Basics 1 deck' })).toHaveAttribute('href', '/folder?name=Basics');
  await search.fill('EVERYDAY');
  await page.getByRole('button', { name: 'Everyday words Basics' }).click();
  await expect(page.getByRole('dialog').getByRole('link', { name: 'Flashcards', exact: true })).toHaveAttribute('href', '/?mode=flash&deck=Everyday%20words');
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).toHaveCount(0);
  await search.fill('missing');
  await expect(page.getByText('No matches yet')).toBeVisible();
  await page.getByRole('button', { name: 'Clear search' }).click();
  await expect(page.locator('.folder-card')).toHaveCount(4);
  expect(errors).toEqual([]);
});

test('desktop layout and folder scope preserve navigation', async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await openLibrary(page);
  await expect(page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'PDF', exact: true })).toHaveAttribute('href', '/pdf');
  await expect(page.getByRole('link', { name: 'Create deck' })).toHaveAttribute('href', '/create');
  await page.screenshot({ path: testInfo.outputPath('home-desktop.png'), fullPage: true });
  await openLibrary(page, fixture, '?name=A1');
  await expect(page.getByRole('heading', { name: 'A1', exact: true })).toBeVisible();
  await expect(page.locator('.folder-card')).toHaveCount(1);
  await expect(page.getByRole('button', { name: 'Greetings A1' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Manage decks' })).toHaveAttribute('href', '/folder?name=A1&legacy=1');
  await page.getByRole('button', { name: 'Greetings A1' }).click();
  await page.getByRole('button', { name: 'Choose another deck' }).click();
  await expect(page.getByRole('dialog').locator('.picker-row')).toHaveCount(2);
});

test('folder location drills down and clears deeper choices when the parent changes', async ({ page }) => {
  await openLibrary(page, { ...fixture, folders: [...fixture.folders, { name: 'Verbs', parent: 'Basics', count: 0 }] });
  const payloads = [];
  await page.route('**/folder/create', route => {
    payloads.push(route.request().postDataJSON());
    return route.fulfill({ json: { ok: true } });
  });
  await page.getByRole('button', { name: 'New folder' }).click();
  await page.getByLabel('Folder name').fill('Practice');
  await expect(page.getByLabel('Location').locator('option')).toHaveText(['Your library', 'A1', 'A2', 'B1']);
  await page.getByLabel('Location').selectOption('A1');
  await page.getByLabel('Subfolder in A1', { exact: true }).selectOption('Basics');
  await page.getByLabel('Subfolder in Basics').selectOption('Verbs');
  await page.getByLabel('Location').selectOption('A2');
  await expect(page.getByLabel('Subfolder in Basics')).toHaveCount(0);
  await page.getByLabel('Location').selectOption('A1');
  await expect(page.getByLabel('Subfolder in A1', { exact: true })).toHaveValue('');
  await page.getByLabel('Subfolder in A1', { exact: true }).selectOption('Basics');
  await page.getByLabel('Subfolder in Basics').selectOption('Verbs');
  await page.getByRole('button', { name: 'Save folder' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(payloads[0]).toEqual({ name: 'Practice', parent: 'Verbs' });
});

test('folder creation reports errors and refreshes after a successful save', async ({ page }) => {
  await openLibrary(page);
  const payloads = [];
  await page.route('**/folder/create', route => {
    payloads.push(route.request().postDataJSON());
    return route.fulfill(payloads.length === 1
      ? { status: 409, json: { detail: 'Folder already exists' } }
      : { json: { ok: true } });
  });
  await page.getByRole('button', { name: 'New folder' }).click();
  await page.getByLabel('Folder name').fill('Travel');
  await page.getByLabel('Location').selectOption('A2');
  await page.getByRole('button', { name: 'Save folder' }).click();
  await expect(page.getByRole('alert')).toContainText('Folder already exists');
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByLabel('Folder name').fill('Travel 2');
  await page.getByRole('button', { name: 'Save folder' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(payloads[1]).toEqual({ name: 'Travel 2', parent: 'A2' });
  await expect(page.getByRole('status').filter({ hasText: 'Folder updated.' })).toBeVisible();
});

test('rename, move, deletion confirmation and ordering use the existing API contracts', async ({ page }) => {
  await openLibrary(page);
  const mutations = [];
  await page.route('**/folder/rename', route => { mutations.push(route.request().postDataJSON()); return route.fulfill({ json: { ok: true } }); });
  await page.route('**/folder/move', route => { mutations.push(route.request().postDataJSON()); return route.fulfill({ json: { ok: true } }); });
  await page.route('**/folder/delete', route => { mutations.push({ method: route.request().method(), ...route.request().postDataJSON() }); return route.fulfill({ json: { ok: true } }); });
  await page.route('**/order/folders', route => { mutations.push(route.request().postDataJSON()); return route.fulfill({ json: { ok: true } }); });
  const menu = page.locator('.folder-card').filter({ has: page.getByRole('link', { name: 'A1 1 subfolder · 2 decks' }) });
  await menu.locator('summary').click();
  await menu.getByRole('button', { name: 'Rename', exact: true }).click();
  await page.getByLabel('Folder name').fill('Beginner');
  await page.getByRole('button', { name: 'Save folder' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(mutations[0]).toEqual({ old_name: 'A1', new_name: 'Beginner' });
  await menu.locator('summary').click();
  await menu.getByRole('button', { name: 'Move', exact: true }).click();
  await expect(page.getByLabel('Location').locator('option[value="Basics"]')).toHaveCount(0);
  await page.getByLabel('Location').selectOption('A2');
  await page.getByRole('button', { name: 'Save folder' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(mutations[1]).toEqual({ name: 'A1', parent: 'A2' });
  await menu.locator('summary').click();
  await menu.getByRole('button', { name: 'Delete', exact: true }).click();
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  expect(mutations).toHaveLength(2);
  await menu.locator('summary').click();
  await menu.getByRole('button', { name: 'Delete', exact: true }).click();
  await page.getByRole('button', { name: 'Delete folder', exact: true }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(mutations[2]).toEqual({ method: 'DELETE', name: 'A1' });
  await menu.locator('summary').click();
  await menu.getByRole('button', { name: 'Move down' }).click();
  await expect.poll(() => mutations.length).toBe(4);
  expect(mutations[3].order).toEqual(['A2', 'Basics', 'A1', 'B1', 'Uncategorized']);
});

test('empty and failed libraries have usable recovery states', async ({ page }) => {
  await openLibrary(page, { folders: [], decks: [], folder_order: [] });
  await expect(page.getByRole('button', { name: 'Study', exact: true })).toHaveCount(0);
  await expect(page.getByRole('link', { name: 'Create your first deck' })).toBeVisible();
  await page.route('**/home-data', route => route.fulfill({ status: 503, json: { detail: 'Library unavailable' } }));
  await page.reload();
  await expect(page.getByRole('alert')).toContainText('Library unavailable');
  await page.route('**/home-data', route => route.fulfill({ json: fixture }));
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.locator('.folder-card')).toHaveCount(4);
});

test('folder hierarchy terminates on cycles and ordering retains unlisted folders', () => {
  expect([...descendants('A', [{ name: 'B', parent: 'A' }, { name: 'A', parent: 'B' }])]).toEqual(['A', 'B']);
  expect(orderedFolders([{ name: 'B' }, { name: 'A' }, { name: 'C' }], ['A']).map(f => f.name)).toEqual(['A', 'B', 'C']);
});
