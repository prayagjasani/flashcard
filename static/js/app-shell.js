/* This small shared shell is used by React and the original HTML pages. */
(() => {
  // Indeterminate navigation progress shared by React and the HTML pages.
  if (!document.getElementById('navigation-progress')) {
    const progress = document.createElement('div');
    progress.id = 'navigation-progress';
    progress.className = 'ui-navigation-progress';
    progress.setAttribute('role', 'progressbar');
    progress.setAttribute('aria-label', 'Loading page');
    progress.innerHTML = '<span></span>';
    progress.hidden = document.readyState === 'complete';
    document.body.append(progress);
    let timeout;
    const finish = () => { progress.hidden = true; clearTimeout(timeout); };
    const start = () => {
      progress.hidden = false;
      clearTimeout(timeout);
      // Recover if the browser cancels a navigation without firing an error.
      timeout = setTimeout(finish, 30000);
    };
    window.addEventListener('load', finish);
    window.addEventListener('pageshow', finish);
    if (window.navigation) {
      window.navigation.addEventListener('navigate', event => {
        if (!event.hashChange && !event.downloadRequest && !event.defaultPrevented) start();
      });
      window.navigation.addEventListener('navigateerror', finish);
      window.navigation.addEventListener('navigatesuccess', finish);
    } else {
      document.addEventListener('click', event => {
        const link = event.target.closest?.('a[href]');
        if (!link || event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || link.hasAttribute('download') || (link.target && link.target !== '_self')) return;
        const destination = new URL(link.href, location.href);
        if (destination.origin === location.origin && (destination.pathname !== location.pathname || destination.search !== location.search)) start();
      });
      window.addEventListener('beforeunload', start);
    }
  }
  const paths = {
    library: '<path d="M3 10 12 3l9 7M5 9v12h14V9M9 21v-7h6v7"/>',
    create: '<rect x="4" y="4" width="16" height="16" rx="4"/><path d="M12 8v8M8 12h8"/>',
    pdf: '<path d="M14 3H5v18h14V8l-5-5ZM14 3v5h5M8 12h8M8 16h5"/>',
    video: '<rect x="3" y="4" width="18" height="16" rx="4"/><path d="m10 9 5 3-5 3V9Z"/>',
    story: '<path d="M12 5v16M12 5C9 3 6 3 3 4v15c3-1 6-1 9 2 3-3 6-3 9-2V4c-3-1-6-1-9 1Z"/>',
  };
  const icon = name => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name]}</svg>`;
  class StudyNavigation extends HTMLElement {
    connectedCallback() {
      if (this.childElementCount) return;
      const page = this.getAttribute('page') || 'library';
      this.innerHTML = `<a class="ui-skip-link" href="#ui-main">Skip to content</a>
        <nav class="ui-navigation" aria-label="Main navigation">
          <div class="ui-nav-links">${[
            ['library', 'Learn', '/', '#58b614'], ['create', 'Create deck', '/create', '#1cb0f6'],
            ['pdf', 'PDF', '/pdf', '#d6a300'], ['video', 'Video', '/video', '#e67e66'], ['story', 'Stories', '/story', '#9069cd'],
          ].map(([key, title, href, color]) => `<a class="ui-nav-link" href="${href}" aria-label="${title}" style="--nav-color:${color}" ${key === page ? 'aria-current="page"' : ''}>${icon(key)}<span class="ui-nav-label-${key}">${title}</span></a>`).join('')}</div>
        </nav>`;
    }
  }
  if (!customElements.get('study-navigation')) customElements.define('study-navigation', StudyNavigation);

  function initializePage() {
    const main = document.querySelector('main, #flashApp, #homeDeckList');
    if (main && !document.getElementById('ui-main')) {
      // Keep existing IDs, which are used by each screen's behaviour.
      const target = document.createElement('span');
      target.id = 'ui-main'; target.tabIndex = -1;
      main.prepend(target);
    }
    const progress = document.getElementById('progressText');
    if (progress && !document.getElementById('progressBar')) {
      const track = document.createElement('div');
      track.className = 'ui-progress-track'; track.hidden = true;
      track.setAttribute('role', 'progressbar'); track.setAttribute('aria-label', 'Session progress');
      const fill = document.createElement('div'); fill.className = 'ui-progress-fill'; track.append(fill);
      progress.closest('header')?.append(track);
      const update = () => {
        const match = progress.textContent.match(/(\d+)\s*\/\s*(\d+)/);
        if (!match || Number(match[2]) === 0) { track.hidden = true; return; }
        const max = Number(match[2]), value = Math.min(Number(match[1]), max);
        track.hidden = false; track.setAttribute('aria-valuemin', '0');
        track.setAttribute('aria-valuemax', String(max)); track.setAttribute('aria-valuenow', String(value));
        fill.style.width = `${value / max * 100}%`;
      };
      new MutationObserver(update).observe(progress, { childList: true, characterData: true, subtree: true });
      update();
    }
    const back = document.getElementById('backBtn');
    if (back && !back.hasAttribute('aria-label')) back.setAttribute('aria-label', 'Back');
    for (const [id, label] of [['createStoryBtn', 'Create story'], ['addVideoBtn', 'Add video']]) {
      document.getElementById(id)?.setAttribute('aria-label', label);
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initializePage);
  else initializePage();
})();
