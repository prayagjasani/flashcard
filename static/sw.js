const CACHE_NAME = 'flashcard-v1';
const ASSETS_TO_CACHE = [
  '/',
  '/static/favicon.png',
  '/static/css/style.css', // Assuming you have a style.css, checking later
  '/static/manifest.json' // If exists
];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      // We don't want to fail installation if some assets are missing, so we add them individually
      // or use addAll but catch errors if you prefer strict caching.
      // For now, let's cache what we know exists or critical paths.
      return cache.addAll(['/']); 
    })
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Strategy for API calls: Network First, falling back to Cache (if we decide to cache APIs)
  // Or Stale-While-Revalidate.
  // For the logs showing repeated requests, Stale-While-Revalidate is good for /folders and /decks
  // if we want to reduce server hit but keep freshness.
  
  // However, the user wants SPEED.
  // Cache First for static assets.
  
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(event.request).then((response) => {
        return response || fetch(event.request);
      })
    );
    return;
  }

  // API Caching
  if (url.pathname === '/folders' || url.pathname === '/decks' || url.pathname.startsWith('/order/')) {
     event.respondWith(
      caches.open(CACHE_NAME).then((cache) => {
        return cache.match(event.request).then((response) => {
          const fetchPromise = fetch(event.request).then((networkResponse) => {
            if (networkResponse.ok) {
              cache.put(event.request, networkResponse.clone());
            }
            return networkResponse;
          });
          return response || fetchPromise;
        });
      })
    );
    return;
  }

  // Default: Network First
  event.respondWith(
    fetch(event.request).catch(() => {
      return caches.match(event.request);
    })
  );
});
