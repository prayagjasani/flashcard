import React, { useState } from 'react';
import { Icon, Modal } from './ui';
import { deckFolder, descendants, request } from './library';

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

export function FolderForm({ action, folders, onClose, onSaved }) {
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

export function StudyPicker({ decks, initialDeck, onClose }) {
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
