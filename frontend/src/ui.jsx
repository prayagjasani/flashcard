import React, { useEffect, useRef } from 'react';

export function Icon({ name = 'folder', ...props }) {
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

export function Modal({ title, children, onClose, busy = false }) {
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
