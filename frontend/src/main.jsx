import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { deckFolder, descendants, folderUrl, normalizeLibrary, orderedFolders, request } from './library';
import './styles.css';
import '../../static/css/app-theme.css';
import '../../static/js/app-shell.js';

function Icon({ name = 'folder', ...props }) {
  const paths = {
    folder: <path d="M3 7V5a1 1 0 0 1 1-1h5l2 3h9a1 1 0 0 1 1 1v11a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V7Z" />,
    cards: <><rect x="7" y="6" width="13" height="15" rx="2" /><path d="M4 17V4a1 1 0 0 1 1-1h11" /></>,
    search: <><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 5 5" /></>,
    arrow: <path d="M5 12h14m-5-5 5 5-5 5" />,
    plus: <path d="M12 5v14M5 12h14" />,
    pdf: <><path d="M14 3H5v18h14V8l-5-5Zm0 0v5h5M8 12h8M8 16h5" /></>,
    video: <><rect x="3" y="4" width="18" height="16" rx="3" /><path d="m10 9 5 3-5 3V9Z" /></>,
    story: <><path d="M12 5v16M12 5C9 3 6 3 3 4v15c3-1 6-1 9 2 3-3 6-3 9-2V4c-3-1-6-1-9 1Z" /></>,
  };
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name]}</svg>;
}

function Modal({ title, children, onClose, busy = false }) {
  const ref = useRef(null);
  useEffect(() => {
    const active = document.activeElement;
    const previous = active?.closest('details')?.querySelector('summary') || active;
    ref.current.showModal();
    return () => { previous?.focus(); };
  }, []);
  return <dialog ref={ref} aria-labelledby="modal-title" onCancel={event => { event.preventDefault(); if (!busy) onClose(); }} onClick={event => { if (event.target === ref.current && !busy) onClose(); }}>
    <div className="dialog-header"><h2 id="modal-title">{title}</h2><button className="icon-button" aria-label="Close dialog" onClick={onClose} disabled={busy}>×</button></div>
    {children}
  </dialog>;
}

function FolderLocation({ folders, excluded, value, onChange, disabled }) {
  const available = folders.filter(folder => !excluded.has(folder.name) && folder.name !== 'Uncategorized');
  const path = [];
  const seen = new Set();
  let current = available.find(folder => folder.name === value);
  while (current && !seen.has(current.name)) {
    seen.add(current.name);
    path.unshift(current);
    current = available.find(folder => folder.name === current.parent);
  }
  const levels = ['', ...path.map(folder => folder.name)];
  return <div className="folder-location">
    {levels.map((parentName, index) => {
      const children = available.filter(folder => (folder.parent || '') === parentName && !path.slice(0, index).some(ancestor => ancestor.name === folder.name));
      if (index > 0 && !children.length) return null;
      return <label className="field" key={parentName || 'library-root'}>
        {index === 0 ? 'Location' : `Subfolder in ${parentName}`}
        <select aria-label={index === 0 ? 'Location' : `Subfolder in ${parentName}`} disabled={disabled} value={path[index]?.name || ''} onChange={event => onChange(event.target.value || parentName)}>
          <option value="">{index === 0 ? 'Your library' : `Use ${parentName}`}</option>
          {children.map(folder => <option key={folder.name} value={folder.name}>{folder.name}</option>)}
        </select>
      </label>;
    })}
  </div>;
}

function FolderForm({ action, folders, onClose, onSaved }) {
  const [name, setName] = useState(action.folder?.name || '');
  const [parent, setParent] = useState(action.folder?.parent || action.parent || '');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const excluded = action.folder ? descendants(action.folder.name, folders) : new Set();
  const titles = { create: 'New folder', rename: 'Rename folder', move: 'Move folder', delete: 'Delete folder?' };
  async function submit(event) {
    event.preventDefault();
    setBusy(true); setError('');
    const payload = action.type === 'create' ? { name: name.trim(), parent: parent || null }
      : action.type === 'rename' ? { old_name: action.folder.name, new_name: name.trim() }
      : action.type === 'move' ? { name: action.folder.name, parent: parent || null }
      : { name: action.folder.name };
    try {
      await request(`/folder/${action.type}`, {
        method: action.type === 'delete' ? 'DELETE' : 'POST',
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
      });
      onSaved();
    } catch (err) { setError(err.message); setBusy(false); }
  }
  return <Modal title={titles[action.type]} onClose={onClose} busy={busy}>
    <form onSubmit={submit}>
      {['create', 'rename'].includes(action.type) && <label className="field">Folder name<input autoFocus required maxLength={50} value={name} onChange={e => setName(e.target.value)} placeholder="e.g. German A1" /></label>}
      {['create', 'move'].includes(action.type) && <FolderLocation folders={folders} excluded={excluded} value={parent} onChange={setParent} disabled={busy} />}
      {action.type === 'delete' && <p>Delete “{name}”? Its decks will move to Uncategorized and its subfolders will move to your library.</p>}
      {error && <p className="error" role="alert">{error}</p>}
      <div className="dialog-actions"><button type="button" className="secondary" onClick={onClose} disabled={busy}>Cancel</button><button className={action.type === 'delete' ? 'danger' : 'primary'} disabled={busy || (['create', 'rename'].includes(action.type) && !name.trim())}>{busy ? 'Saving…' : action.type === 'delete' ? 'Delete folder' : 'Save folder'}</button></div>
    </form>
  </Modal>;
}

function StudyPicker({ decks, initialDeck, onClose }) {
  const [selected, setSelected] = useState(initialDeck || '');
  const [query, setQuery] = useState('');
  const modes = [['Flashcards', '/?mode=flash&deck='], ['Learn', '/learn?deck='], ['Spelling', '/spelling?deck='], ['Match', '/match?deck='], ['Line', '/line?deck=']];
  return <Modal title={selected || 'Choose a deck'} onClose={onClose}>
    {selected ? <><p className="muted">How would you like to practice?</p><div className="study-modes">{modes.map(([label, url], i) => <a className={i === 0 ? 'primary' : 'secondary'} key={label} href={url + encodeURIComponent(selected)}>{label}<Icon name="arrow" /></a>)}</div><div className="section-header"><button className="text-button" onClick={() => setSelected('')}>Choose another deck</button><a className="text-button" href={`/edit?deck=${encodeURIComponent(selected)}`}>Edit deck</a></div></> : <>
      <label className="search"><Icon name="search" /><input autoFocus aria-label="Find a study deck" placeholder="Find a deck…" value={query} onChange={e => setQuery(e.target.value)} /></label>
      <div className="deck-picker">{decks.filter(d => d.name.toLowerCase().includes(query.toLowerCase())).map(d => <button className="picker-row" key={d.name} onClick={() => setSelected(d.name)}><span>{d.name}<small>{deckFolder(d)}</small></span><Icon name="arrow" /></button>)}</div>
      {!decks.some(d => d.name.toLowerCase().includes(query.toLowerCase())) && <p className="empty">No decks found.</p>}
    </>}
  </Modal>;
}

function App() {
  const folderName = new URLSearchParams(location.search).get('name') || '';
  const [library, setLibrary] = useState(null);
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
    {action && <FolderForm action={action} folders={folders} onClose={() => setAction(null)} onSaved={() => { setAction(null); setNotice('Folder updated.'); refresh(); }} />}
    {study !== null && <StudyPicker decks={studyDecks} initialDeck={study} onClose={() => setStudy(null)} />}
  </div></>;
}

createRoot(document.getElementById('root')).render(<App />);
if ('serviceWorker' in navigator && import.meta.env.PROD) {
  window.addEventListener('load', () => navigator.serviceWorker.register('/static/sw.js').catch(() => {}));
}
