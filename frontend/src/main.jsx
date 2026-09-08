import React, { useEffect, useState, lazy, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import { deckFolder, descendants, folderUrl, normalizeLibrary, orderedFolders, request } from './library';
import './styles.css';
import '../../static/css/app-theme.css';
import '../../static/js/app-shell.js';

import { Icon } from './ui';
let dialogsPromise;
const loadDialogs = () => {
  if (!dialogsPromise) dialogsPromise = import('./dialogs').catch(error => { dialogsPromise = null; throw error; });
  return dialogsPromise;
};
const FolderForm = lazy(() => loadDialogs().then(module => ({ default: module.FolderForm })));
const StudyPicker = lazy(() => loadDialogs().then(module => ({ default: module.StudyPicker })));

function App() {
  const folderName = new URLSearchParams(location.search).get('name') || '';
  const [library, setLibrary] = useState(null);
  useEffect(() => {
    const connection = navigator.connection;
    if (!library || connection?.saveData || /(^|-)2g$/.test(connection?.effectiveType || '')) return;
    let idle;
    const timer = setTimeout(() => {
      const warm = () => {
        if (document.visibilityState === 'visible' && navigator.onLine) loadDialogs().catch(() => {});
      };
      if ('requestIdleCallback' in window) idle = window.requestIdleCallback(warm, { timeout: 3000 });
      else warm();
    }, 1000);
    return () => {
      clearTimeout(timer);
      if (idle !== undefined) window.cancelIdleCallback(idle);
    };
  }, [Boolean(library)]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState('');
  const [action, setAction] = useState(null);
  const [study, setStudy] = useState(null);
  const [notice, setNotice] = useState('');
  const [reordering, setReordering] = useState(false);
  async function refresh() {
    setLoading(true); setError('');
    try { setLibrary(normalizeLibrary(await request('/home-data'))); }
    catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }
  useEffect(() => { refresh(); }, []);
  const folders = orderedFolders(library?.folders || [], library?.folder_order);
  const decks = library?.decks || [];
  const current = folders.find(f => f.name === folderName);
  const search = query.trim().toLowerCase();
  const scopedNames = folderName ? descendants(folderName, folders) : null;
  const visibleFolders = folders.filter(f => search
    ? f.name.toLowerCase().includes(search) && (!scopedNames || (scopedNames.has(f.name) && f.name !== folderName))
    : (f.parent || '') === folderName);
  const visibleDecks = decks.filter(d => search
    ? d.name.toLowerCase().includes(search) && (!scopedNames || scopedNames.has(deckFolder(d)))
    : folderName && deckFolder(d) === folderName);
  async function reorder(folder, offset) {
    const siblings = folders.filter(f => (f.parent || '') === folderName);
    const neighbor = siblings[siblings.findIndex(f => f.name === folder.name) + offset];
    if (!neighbor) return;
    const order = folders.map(f => f.name);
    const a = order.indexOf(folder.name), b = order.indexOf(neighbor.name);
    [order[a], order[b]] = [order[b], order[a]];
    setReordering(true); setError('');
    try {
      await request('/order/folders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ order }) });
      setLibrary(prev => ({ ...prev, folder_order: order }));
      setNotice('Folder order updated.');
    } catch (err) { setError(err.message); }
    finally { setReordering(false); }
  }
  const studyDecks = scopedNames ? decks.filter(d => scopedNames.has(deckFolder(d))) : decks;
  return <><study-navigation page="library" /><div className="app-shell">
    <main id="ui-main" tabIndex="-1">
      {folderName && <nav className="breadcrumb" aria-label="Breadcrumb"><a href="/">Library</a><span>/</span>{current?.parent && <><a href={folderUrl(current.parent)}>{current.parent}</a><span>/</span></>}<span aria-current="page">{folderName}</span></nav>}
      {folderName && <h1 className="folder-page-title">{folderName}</h1>}
      <section className="library" aria-labelledby="library-title"><div className="section-header"><h2 id="library-title">{folderName ? 'In this folder' : 'Your library'}{library && <span className="count">{folderName ? studyDecks.length : decks.length} decks</span>}</h2><div className="library-actions"><button className="text-button" onClick={() => setAction({ type: 'create', parent: folderName })} disabled={!library}><Icon name="plus" />New folder</button></div></div>
        {folderName && <a className="text-button" href={`${folderUrl(folderName)}&legacy=1`}>Manage decks</a>}
        <label className="search"><Icon name="search" /><input type="search" aria-label="Search folders and decks" placeholder="Search folders and decks…" value={query} onChange={e => setQuery(e.target.value)} /></label>
        {notice && <p className="notice" role="status">{notice}</p>}
        {error && <div className="error" role="alert"><span>{error}</span><button onClick={refresh} disabled={loading}>Try again</button></div>}
        {loading && <p className="empty" role="status">Loading your library…</p>}
        {!loading && library && folderName && !current && <p className="empty">This folder no longer exists. <a href="/">Back to your library</a></p>}
        {library && <div className="folder-grid">{visibleFolders.map((folder, index) => {
          const nested = descendants(folder.name, folders);
          const count = decks.filter(d => nested.has(deckFolder(d))).length;
          const children = folders.filter(f => f.parent === folder.name).length;
          return <article className="folder-card" key={folder.name}><a className="folder-link" href={folderUrl(folder.name)}><span className="folder-icon"><Icon /></span><span><strong>{folder.name}</strong><small>{children > 0 && `${children} subfolder${children === 1 ? '' : 's'} · `}{count} deck{count === 1 ? '' : 's'}</small></span></a>
            {folder.name !== 'Uncategorized' && <details className="folder-menu"><summary aria-label={`Manage ${folder.name}`}>⋮</summary><div className="menu-panel">{['rename', 'move', 'delete'].map(type => <button key={type} onClick={e => { e.currentTarget.closest('details').open = false; setAction({ type, folder }); }}>{type[0].toUpperCase() + type.slice(1)}</button>)}{!search && <><button disabled={reordering || index === 0} onClick={() => reorder(folder, -1)}>Move up</button><button disabled={reordering || index === visibleFolders.length - 1} onClick={() => reorder(folder, 1)}>Move down</button></>}</div></details>}
          </article>;
        })}</div>}
        {visibleDecks.length > 0 && <><h3 className="list-label">Decks</h3><div className="folder-grid">{visibleDecks.map(deck => <button className="deck-row" key={deck.name} onClick={() => setStudy(deck.name)}><span className="folder-icon"><Icon name="cards" /></span><span><strong>{deck.name}</strong><small>{deckFolder(deck)}</small></span><Icon name="arrow" /></button>)}</div></>}
        {!loading && library && !visibleFolders.length && !visibleDecks.length && !error && <div className="empty"><Icon name="cards" width="32" height="32" /><h3>{search ? 'No matches yet' : 'Room for something new'}</h3><p>{search ? 'Try another folder or deck name.' : 'Create a deck to start your next study session.'}</p>{search ? <button className="secondary" onClick={() => setQuery('')}>Clear search</button> : <a className="primary" href="/create">Create your first deck</a>}</div>}
      </section>
      <footer>Small steps. Lasting progress.</footer>
    </main>
    <Suspense fallback={<p role="status">Opening dialog…</p>}>
    {action && <FolderForm action={action} folders={folders} onClose={() => setAction(null)} onSaved={() => { setAction(null); setNotice('Folder updated.'); refresh(); }} />}
    {study !== null && <StudyPicker decks={studyDecks} initialDeck={study} onClose={() => setStudy(null)} />}
    </Suspense>
  </div></>;
}

createRoot(document.getElementById('root')).render(<App />);
if ('serviceWorker' in navigator && import.meta.env.PROD) {
  window.addEventListener('load', () => navigator.serviceWorker.register('/static/sw.js').catch(() => {}));
}
