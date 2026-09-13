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

      // Pre-warm route when hovering or touching any navigation link
      this.querySelectorAll('.ui-nav-link').forEach(link => {
        const prewarm = () => {
          const href = link.getAttribute('href');
          if (href && !document.querySelector(`link[rel="prefetch"][href="${href}"]`)) {
            const l = document.createElement('link');
            l.rel = 'prefetch';
            l.href = href;
            l.as = 'document';
            document.head.appendChild(l);
          }
        };
        link.addEventListener('pointerenter', prewarm, { passive: true, once: true });
        link.addEventListener('touchstart', prewarm, { passive: true, once: true });
      });
    }
  }
  if (!customElements.get('study-navigation')) customElements.define('study-navigation', StudyNavigation);

  // Background preloader: once app loads, fetch and cache data for all navigation buttons
  let isPreloading = false;
  function preloadAllButtonData() {
    if (isPreloading) return;
    const now = Date.now();
    let lastPreload = 0;
    try {
      lastPreload = Number(sessionStorage.getItem('last_all_nav_preload') || 0);
    } catch (e) {}
    // Don't repeat if done within last 10 seconds in this session
    if (now - lastPreload < 10000) return;
    isPreloading = true;
    try {
      sessionStorage.setItem('last_all_nav_preload', String(now));
    } catch (e) {}

    // 1. Pre-warm HTML pages
    const routes = ['/', '/create', '/pdf', '/video', '/story'];
    routes.forEach(path => {
      try {
        if (!document.querySelector(`link[rel="prefetch"][href="${path}"]`)) {
          const l = document.createElement('link');
          l.rel = 'prefetch';
          l.href = path;
          l.as = 'document';
          document.head.appendChild(l);
        }
      } catch (e) {}
    });

    // 2. Preload data for all buttons in background concurrently
    // Learn: /home-data
    fetch('/home-data')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && (data.folders || data.decks)) {
          try {
            localStorage.setItem('flashcard_home_cache_v1', JSON.stringify(data));
            localStorage.setItem('home_data_cache', JSON.stringify(data));
          } catch (e) {}
        }
      })
      .catch(() => {});

    // PDF: /pdfs and /pdf/folders
    fetch('/pdfs')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (Array.isArray(data)) {
          try { localStorage.setItem('pdfs_cache', JSON.stringify(data)); } catch (e) {}
        }
      })
      .catch(() => {});

    fetch('/pdf/folders')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && Array.isArray(data.folders)) {
          try { localStorage.setItem('pdf_folders_cache', JSON.stringify({ folders: data.folders })); } catch (e) {}
        }
      })
      .catch(() => {});

    // Video: /videos
    fetch('/videos')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && Array.isArray(data.videos)) {
          try { localStorage.setItem('videos_cache', JSON.stringify(data.videos)); } catch (e) {}
        }
      })
      .catch(() => {});

    // Stories: /stories/list and /decks
    fetch('/stories/list')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && Array.isArray(data.stories)) {
          const json = JSON.stringify(data.stories);
          try { sessionStorage.setItem('cached_stories', json); } catch (e) {}
          try { localStorage.setItem('cached_stories', json); } catch (e) {}
        }
      })
      .catch(() => {});

    fetch('/decks')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (Array.isArray(data)) {
          const json = JSON.stringify(data);
          try { sessionStorage.setItem('cached_decks_for_stories', json); } catch (e) {}
          try { localStorage.setItem('cached_decks_for_stories', json); } catch (e) {}
        }
      })
      .catch(() => {});
  }

  const schedulePreload = () => {
    if (window.requestIdleCallback) {
      window.requestIdleCallback(preloadAllButtonData, { timeout: 2000 });
    } else {
      setTimeout(preloadAllButtonData, 400);
    }
  };

  if (document.readyState === 'complete') {
    schedulePreload();
  } else {
    window.addEventListener('load', schedulePreload, { once: true });
  }

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
