import React, { useEffect, useMemo, useState } from 'react';
import { Modal } from './ui';
import { request } from './library';
import './streaks.css';

const CACHE_KEY = 'flashcard_habits_cache_v1';
const ICON_PATHS = {
  cards: <><rect x="7" y="6" width="13" height="15" rx="2" /><path d="M4 17V4a1 1 0 0 1 1-1h11" /></>,
  book: <path d="M12 5v16M12 5C9 3 6 3 3 4v15c3-1 6-1 9 2 3-3 6-3 9-2V4c-3-1-6-1-9 1Z" />,
  language: <><path d="M4 5h9M8.5 3v2M6 5c.5 3 2.5 6 6 8M11 5c-.5 3-3 6.5-7 8" /><path d="m13 21 4-9 4 9M14.5 18h5" /></>,
  pencil: <path d="M4 20h4L19 9l-4-4L4 16v4ZM13.5 6.5l4 4" />,
  video: <><rect x="3" y="4" width="18" height="16" rx="3" /><path d="m10 9 5 3-5 3V9Z" /></>,
  pdf: <path d="M14 3H5v18h14V8l-5-5Zm0 0v5h5M8 12h8M8 16h5" />,
  mic: <><rect x="9" y="3" width="6" height="11" rx="3" /><path d="M5 11a7 7 0 0 0 14 0M12 18v3" /></>,
  run: <><circle cx="14" cy="4.5" r="1.5" /><path d="m6 21 3.5-6 3 2.5V21M9.5 15l1.5-6 4 2 3 1M11 9 7 10l-1 3" /></>,
  water: <path d="M12 3s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11Z" />,
  star: <path d="m12 3 2.7 5.6 6.1.9-4.4 4.3 1 6.1L12 17l-5.4 2.9 1-6.1-4.4-4.3 6.1-.9L12 3Z" />,
};
const ICON_NAMES = Object.keys(ICON_PATHS);
const TINTS = ['green', 'blue', 'purple', 'yellow', 'red'];

function Glyph({ name, size = 22, ...props }) {
  const path = name === 'flame' ? <path d="M12 3c1 3.5 5 5.5 5 10a5 5 0 0 1-10 0c0-2 1-3.5 2-4.5.2 1.7 1 2.8 2 3.2C10.5 8.6 11 5.6 12 3Z" />
    : name === 'check' ? <path d="m5 12.5 4.5 4.5L19 7.5" />
    : name === 'plus' ? <path d="M12 5v14M5 12h14" />
    : ICON_PATHS[name] || ICON_PATHS.star;
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{path}</svg>;
}

// Dates are the user's local calendar days, written as YYYY-MM-DD.
const isoDay = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const shift = (iso, days) => { const [y, m, d] = iso.split('-').map(Number); return isoDay(new Date(y, m - 1, d + days)); };

export function streakStats(done, today) {
  const days = new Set(done);
  // A streak stays alive until a whole day passes without the habit.
  let cursor = days.has(today) ? today : shift(today, -1);
  let current = 0;
  while (days.has(cursor)) { current += 1; cursor = shift(cursor, -1); }
  let best = 0, run = 0, previous = null;
  for (const day of [...days].sort()) {
    run = previous && shift(previous, 1) === day ? run + 1 : 1;
    best = Math.max(best, run);
    previous = day;
  }
  return { current, best, doneToday: days.has(today) };
}

const dayLabel = n => `${n} day${n === 1 ? '' : 's'}`;
const readCache = () => { try { const v = JSON.parse(localStorage.getItem(CACHE_KEY)); return Array.isArray(v) ? v : null; } catch { return null; } };
const writeCache = habits => { try { localStorage.setItem(CACHE_KEY, JSON.stringify(habits)); } catch {} };

function HabitForm({ habit, onClose, onSaved, onDeleted }) {
  const [name, setName] = useState(habit?.name || '');
  const [icon, setIcon] = useState(habit?.icon || 'cards');
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(event) {
    event.preventDefault();
    if (!name.trim()) { setError('Enter a habit name.'); return; }
    setBusy(true); setError('');
    try {
      const saved = await request(habit ? `/habits/${habit.id}/rename` : '/habits', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, icon }) });
      onSaved(saved);
    } catch (err) { setError(err.message); setBusy(false); }
  }
  async function remove() {
    setBusy(true); setError('');
    try { await request(`/habits/${habit.id}`, { method: 'DELETE' }); onDeleted(habit.id); }
    catch (err) { setError(err.message); setBusy(false); }
  }
  if (confirming) return <Modal title="Delete habit?" onClose={onClose} busy={busy}>
    <p>{habit.name} and its {dayLabel(streakStats(habit.done, isoDay(new Date())).best)} best streak will be removed.</p>
    {error && <p className="error" role="alert">{error}</p>}
    <div className="dialog-actions"><button className="secondary" onClick={() => setConfirming(false)} disabled={busy}>Keep it</button><button className="danger" onClick={remove} disabled={busy}>{busy ? 'Deleting…' : 'Delete'}</button></div>
  </Modal>;
  return <Modal title={habit ? 'Edit habit' : 'New habit'} onClose={onClose} busy={busy}>
    <form onSubmit={submit}>
      <label className="field">Name<input autoFocus maxLength={60} placeholder="Study 20 flashcards" value={name} onChange={e => { setName(e.target.value); setError(''); }} disabled={busy} /></label>
      <fieldset className="icon-picker" disabled={busy}><legend>Icon</legend>
        {ICON_NAMES.map((key, i) => <button type="button" key={key} className={`habit-icon tint-${TINTS[i % TINTS.length]}`} aria-label={key} aria-pressed={icon === key} onClick={() => setIcon(key)}><Glyph name={key} /></button>)}
      </fieldset>
      {error && <p className="error" role="alert">{error}</p>}
      <div className="dialog-actions">
        {habit ? <button type="button" className="danger" onClick={() => setConfirming(true)} disabled={busy}>Delete</button> : <button type="button" className="secondary" onClick={onClose} disabled={busy}>Cancel</button>}
        <button className="primary" disabled={busy}>{busy ? 'Saving…' : habit ? 'Save' : 'Add habit'}</button>
      </div>
    </form>
  </Modal>;
}

export default function Streaks() {
  const [habits, setHabits] = useState(readCache);
  const [loading, setLoading] = useState(() => !habits);
  const [error, setError] = useState('');
  const [mode, setMode] = useState('current');
  const [editing, setEditing] = useState(null);
  const [today, setToday] = useState(() => isoDay(new Date()));
  useEffect(() => {
    const tick = () => setToday(isoDay(new Date()));
    const timer = setInterval(tick, 60000);
    document.addEventListener('visibilitychange', tick);
    return () => { clearInterval(timer); document.removeEventListener('visibilitychange', tick); };
  }, []);
  async function load() {
    setError(''); if (!habits) setLoading(true);
    try { const data = await request('/habits'); setHabits(data.habits); writeCache(data.habits); }
    catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }
  useEffect(() => { load(); }, []);
  const update = fn => setHabits(prev => { const next = fn(prev || []); writeCache(next); return next; });

  async function toggle(habit) {
    const done = !streakStats(habit.done, today).doneToday;
    const before = habit.done;
    const optimistic = done ? [...before, today] : before.filter(d => d !== today);
    update(list => list.map(h => h.id === habit.id ? { ...h, done: optimistic } : h));
    setError('');
    try {
      const saved = await request(`/habits/${habit.id}/check`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ date: today, done }) });
      update(list => list.map(h => h.id === saved.id ? saved : h));
    } catch (err) {
      update(list => list.map(h => h.id === habit.id ? { ...h, done: before } : h));
      setError(err.message);
    }
  }

  const ranked = useMemo(() => (habits || [])
    .map(h => ({ ...h, ...streakStats(h.done || [], today) }))
    .sort((a, b) => b[mode] - a[mode] || b.current - a.current || a.name.localeCompare(b.name)), [habits, mode, today]);
  const doneCount = ranked.filter(h => h.doneToday).length;
  const podium = [ranked[1], ranked[0], ranked[2]];
  const rest = ranked.slice(3);
  const tintOf = h => TINTS[Math.max(0, ICON_NAMES.indexOf(h.icon)) % TINTS.length];

  const Check = ({ habit, small }) => <button className={`habit-check${habit.doneToday ? ' is-done' : ''}${small ? ' is-small' : ''}`} aria-pressed={habit.doneToday} aria-label={habit.doneToday ? `${habit.name}: done today. Undo` : `Mark ${habit.name} done today`} onClick={() => toggle(habit)}><Glyph name="check" size={small ? 16 : 18} strokeWidth="3" /></button>;

  return <div className="streaks-page">
    <header className="streaks-hero">
      <div className="streaks-bar">
        <h1>Streaks</h1>
        <button className="streaks-add" aria-label="Add habit" onClick={() => setEditing('new')}><Glyph name="plus" size={24} strokeWidth="3" /></button>
      </div>
      <div className="streaks-toggle" role="group" aria-label="Rank by">
        <button aria-pressed={mode === 'current'} onClick={() => setMode('current')}>Current</button>
        <button aria-pressed={mode === 'best'} onClick={() => setMode('best')}>Best ever</button>
      </div>
      {ranked.length > 0 && <>
        <p className="streaks-summary" role="status">{doneCount === ranked.length ? 'All done today. Nice work.' : `${doneCount} of ${ranked.length} done today`}</p>
        <ol className="podium">{podium.map((habit, slot) => {
          const place = [2, 1, 3][slot];
          if (!habit) return <li key={`empty-${place}`} className={`podium-spot place-${place} is-empty`} aria-hidden="true"><div className="podium-step">{place}</div></li>;
          return <li key={habit.id} className={`podium-spot place-${place}`}>
            <div className="podium-avatar-wrap">
              <button className={`habit-icon is-large tint-${tintOf(habit)}`} aria-label={`Edit ${habit.name}`} onClick={() => setEditing(habit)}><Glyph name={habit.icon} size={place === 1 ? 30 : 26} /></button>
              <Check habit={habit} small />
            </div>
            <strong>{habit.name}</strong>
            <span className="podium-pill"><Glyph name="flame" size={14} />{dayLabel(habit[mode])}</span>
            <div className="podium-step">{place}</div>
          </li>;
        })}</ol>
      </>}
    </header>
    <main id="ui-main" tabIndex="-1" className="streaks-sheet">
      {error && <div className="error" role="alert"><span>{error}</span><button onClick={load}>Try again</button></div>}
      {loading && <p className="empty" role="status">Loading your habits…</p>}
      {!loading && habits && !habits.length && <div className="empty">
        <Glyph name="flame" size={40} />
        <h3>Start your first streak</h3>
        <p>Add a habit, tick it off each day, and watch the count climb.</p>
        <button className="primary" onClick={() => setEditing('new')}>Add habit</button>
      </div>}
      {ranked.length > 0 && <ul className="habit-list">
        {rest.length === 0 && <li className="habit-hint">Tap a circle under the podium to mark today done.</li>}
        {rest.map((habit, i) => <li key={habit.id} className="habit-row">
          <span className="habit-rank">{String(i + 4).padStart(2, '0')}</span>
          <button className={`habit-icon tint-${tintOf(habit)}`} aria-label={`Edit ${habit.name}`} onClick={() => setEditing(habit)}><Glyph name={habit.icon} /></button>
          <span className="habit-text"><strong>{habit.name}</strong><small className={habit[mode] ? 'is-hot' : ''}><Glyph name="flame" size={13} />{dayLabel(habit[mode])}</small></span>
          <Check habit={habit} />
        </li>)}
      </ul>}
    </main>
    {editing && <HabitForm habit={editing === 'new' ? null : editing} onClose={() => setEditing(null)}
      onSaved={saved => { update(list => list.some(h => h.id === saved.id) ? list.map(h => h.id === saved.id ? saved : h) : [...list, saved]); setEditing(null); }}
      onDeleted={id => { update(list => list.filter(h => h.id !== id)); setEditing(null); }} />}
  </div>;
}
