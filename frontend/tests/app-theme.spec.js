import { test, expect } from '@playwright/test';

const cards = [{ en: 'Hello', de: 'Hallo' }, { en: 'Thank you', de: 'Danke' }, { en: 'Goodbye', de: 'Tschüss' }];
const decks = [{ name: 'Greetings', folder: 'A1' }];
const folders = [{ name: 'A1', parent: null, count: 1 }];
const library = { folders, decks, folder_order: ['A1'] };

async function mockData(page) {
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.hostname !== '127.0.0.1') {
      // Typography has a local fallback; do not require external services in tests.
      return route.abort();
    }
    if (url.pathname.startsWith('/templates/') || url.pathname.startsWith('/static/')) return route.continue();
    if (url.pathname === '/home-data') return route.fulfill({ json: library });
    if (url.pathname === '/cards') return route.fulfill({ json: cards });
    if (url.pathname === '/decks') return route.fulfill({ json: decks });
    if (url.pathname === '/folders') return route.fulfill({ json: { folders } });
    if (url.pathname.startsWith('/order/')) return route.fulfill({ json: [] });
    if (url.pathname === '/lines/generate') return route.fulfill({ json: { count: 1, items: [{ en: 'Hello', de: 'Hallo', line_de: 'Hallo, wie geht es dir?', line_en: 'Hello, how are you?' }] } });
    if (url.pathname === '/pdf/folders') return route.fulfill({ json: { folders: [{ name: 'German reading', count: 0 }] } });
    if (url.pathname === '/pdfs') return route.fulfill({ json: [] });
    if (url.pathname === '/videos') return route.fulfill({ json: { videos: [] } });
    if (url.pathname === '/stories/list') return route.fulfill({ json: { stories: [{ deck: 'Greetings', title_de: 'Ein neuer Freund', title_en: 'A new friend', level: 'A1' }] } });
    if (url.pathname === '/tts') return route.fulfill({ status: 404, json: { detail: 'Audio unavailable in test' } });
    return route.fulfill({ json: {} });
  });
}

const screens = [
  ['library', '/static/react/index.html', 'library'],
  ['flashcards', '/templates/index.html?mode=flash&deck=Greetings', 'library'],
  ['folder', '/templates/folder.html?name=A1', 'library'],
  ['learn', '/templates/hi.html?deck=Greetings', 'library'],
  ['spelling', '/templates/spelling.html?deck=Greetings', 'library'],
  ['match', '/templates/match.html?deck=Greetings', 'library'],
  ['line', '/templates/line.html?deck=Greetings', 'library'],
  ['create', '/templates/create.html', 'create'],
  ['edit', '/templates/edit.html?deck=Greetings', 'create'],
  ['pdf', '/templates/pdf.html', 'pdf'],
  ['video', '/templates/video.html', 'video'],
  ['story', '/templates/story.html', 'story'],
];

for (const width of [390, 1280]) {
  for (const [name, path, active] of screens) {
    test(`${name} has a consistent theme at ${width}px`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      await mockData(page);
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.goto(path);
      const navigation = page.getByRole('navigation', { name: 'Main navigation' });
      await expect(navigation).toBeVisible();
      await expect(page.locator('.ui-brand')).toHaveCount(0);
      const navBounds = await navigation.boundingBox();
      expect(Math.round(navBounds.y + navBounds.height)).toBe(900);
      if (name === 'create') {
        const actionBounds = await page.locator('#actionBar').boundingBox();
        expect(actionBounds.y + actionBounds.height).toBeLessThanOrEqual(navBounds.y);
      }
      await expect(navigation.locator('a.ui-nav-link')).toHaveCount(5);
      await expect(navigation.locator('[aria-current="page"]')).toHaveAttribute('href', active === 'library' ? '/' : `/${active}`);
      await expect(page.locator('.loading-overlay.is-active')).toHaveCount(0);
      if (name !== 'flashcards') await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(255, 255, 255)');
      await page.evaluate(() => document.fonts.ready);
      expect(await page.evaluate(() => document.fonts.check('16px Nunito'))).toBe(true);
      if (name !== 'flashcards') expect(await page.locator('body').evaluate(el => getComputedStyle(el).fontFamily)).toContain('Nunito');
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      if (name === 'library') {
        await expect(page.locator('.intro')).toHaveCount(0);
        await expect(page.locator('.search input')).toHaveCSS('border-top-width', '0px');
      }
      if (name === 'story') await expect(page.locator('.story-item__title')).toHaveText('Ein neuer Freund');
      await page.screenshot({ path: testInfo.outputPath(`${name}-${width}.png`), fullPage: true });
      expect(errors).toEqual([]);
    });
  }
}

test('navigation progress loops during a pending page change and clears on arrival', async ({ page }) => {
  await mockData(page);
  await page.goto('/static/react/index.html');
  await expect(page.locator('#navigation-progress')).toBeHidden();
  let observed;
  await page.exposeFunction('reportNavigationProgress', state => { observed = state; });
  await page.evaluate(() => {
    const bar = document.getElementById('navigation-progress');
    new MutationObserver(() => window.reportNavigationProgress({ hidden: bar.hidden, loop: getComputedStyle(bar.firstElementChild).animationIterationCount })).observe(bar, { attributes: true, attributeFilter: ['hidden'] });
  });
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  await page.route('**/pdf', async route => {
    await pending;
    await route.fulfill({ contentType: 'text/html', body: '<body><script src="/static/js/app-shell.js"></script></body>' });
  });
  try {
    await page.getByRole('navigation', { name: 'Main navigation' }).getByRole('link', { name: 'PDF', exact: true }).click({ noWaitAfter: true });
    await expect.poll(() => observed).toEqual({ hidden: false, loop: 'infinite' });
  } finally { release(); }
  await page.waitForURL('**/pdf');
  await expect(page.locator('#navigation-progress')).toBeHidden();
});

test('spelling feedback and real session progress retain their behaviour', async ({ page }) => {
  await mockData(page);
  await page.goto('/templates/spelling.html?deck=Greetings');
  await expect(page.locator('#englishWord')).not.toHaveText('Select a deck');
  await expect(page.getByRole('progressbar', { name: 'Session progress' })).toHaveAttribute('aria-valuemax', '3');
  await page.locator('#spellingInput').fill('an incorrect answer');
  await page.locator('#checkBtn').click();
  await expect(page.locator('#feedback')).not.toBeEmpty();
  await page.locator('#nextBtn').click();
  await expect(page.getByRole('progressbar', { name: 'Session progress' })).toHaveAttribute('aria-valuenow', '2');
});

test('multiple PDFs use individual filenames and retry only failures', async ({ page }) => {
  await mockData(page);
  const uploads = [];
  await page.route('**/pdf/upload', route => {
    const body = route.request().postDataBuffer().toString();
    uploads.push(body);
    return route.fulfill(uploads.length === 2 ? { status: 500, json: { detail: 'Try again' } } : { json: { ok: true } });
  });
  await page.goto('/templates/pdf.html');
  await page.locator('#uploadPdfBtn').click();
  await page.locator('#uploadPdfFileInput').setInputFiles([
    { name: 'German basics.pdf', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4 test') },
    { name: 'Practice.PDF', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4 test') },
  ]);
  await expect(page.locator('#uploadPdfNameInput')).toBeDisabled();
  await page.locator('#uploadPdfSaveBtn').click();
  await expect(page.locator('#uploadPdfStatus')).toContainText('1 uploaded');
  expect(uploads[0]).toContain('name="name"\r\n\r\nGerman basics\r\n');
  expect(uploads[1]).toContain('name="name"\r\n\r\nPractice\r\n');
  await page.locator('#uploadPdfSaveBtn').click();
  await expect(page.locator('#uploadPdfModal')).not.toHaveClass(/is-open/);
  expect(uploads).toHaveLength(3);
  expect(uploads[2]).toContain('name="name"\r\n\r\nPractice\r\n');
});

test('video and story creation dialogs use the shared form styling', async ({ page }) => {
  await mockData(page);
  await page.goto('/templates/video.html');
  await page.getByRole('button', { name: 'Add video', exact: true }).click();
  await expect(page.locator('#addModal')).toHaveClass(/is-open/);
  await expect(page.locator('#videoUrl')).toHaveCSS('border-radius', '14px');
  await page.goto('/templates/story.html');
  await page.getByRole('button', { name: 'Create story', exact: true }).click();
  await expect(page.locator('#createModal')).toHaveClass(/visible/);
  await page.locator('#tabAI').click();
  await expect(page.locator('#storyTopicInput')).toBeVisible();
});
