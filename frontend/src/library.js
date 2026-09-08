export const folderUrl = name => `/folder?name=${encodeURIComponent(name)}`;
export const deckFolder = deck => deck.folder || 'Uncategorized';

export function descendants(name, folders) {
  const names = new Set([name]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const folder of folders) {
      if (names.has(folder.parent) && !names.has(folder.name)) {
        names.add(folder.name);
        changed = true;
      }
    }
  }
  return names;
}

export function orderedFolders(folders, order = []) {
  const positions = new Map(order.map((name, index) => [name, index]));
  return [...folders].sort((a, b) =>
    (positions.get(a.name) ?? order.length) - (positions.get(b.name) ?? order.length));
}

export async function request(path, options = {}) {
  const response = await fetch(path, { cache: 'no-store', ...options });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(typeof data.detail === 'string' ? data.detail : 'Something went wrong. Please try again.');
  }
  return data;
}

export function normalizeLibrary(data) {
  if (!Array.isArray(data.folders) || !Array.isArray(data.decks)) {
    throw new Error('Could not read your library. Please try again.');
  }
  return { folders: data.folders, decks: data.decks, folder_order: data.folder_order || [] };
}
