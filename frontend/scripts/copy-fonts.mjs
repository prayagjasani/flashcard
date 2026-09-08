import { copyFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const destination = new URL('../../static/fonts/', import.meta.url);
await mkdir(destination, { recursive: true });
for (const [source, target] of [
  ['@fontsource-variable/nunito/files/nunito-latin-wght-normal.woff2', 'nunito-latin.woff2'],
  ['@fontsource/material-symbols-outlined/files/material-symbols-outlined-latin-400-normal.woff2', 'material-symbols.woff2'],
  ['@fontsource-variable/nunito/LICENSE', 'Nunito-LICENSE.txt'],
  ['@fontsource/material-symbols-outlined/LICENSE', 'Material-Symbols-LICENSE.txt'],
]) {
  await copyFile(new URL(`../node_modules/${source}`, import.meta.url), new URL(target, destination));
}
console.log(`Font assets ready in ${fileURLToPath(destination)}`);
