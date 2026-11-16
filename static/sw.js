// Service Worker for Flashcard App - Enables offline support

const CACHE_NAME = 'flashcard-v8';
const STATIC_ASSETS = [
    '/static/manifest.json',
];

// API routes to cache (matches pathname prefix)
const API_CACHE_NAME = 'flashcard-api-v2';
const CACHEABLE_APIS = [
    '/order/folders',
    '/order/decks',
    '/pdfs',
    '/pdf/folders',
    '/videos',
    '/stories/list',
];

// Install Service Worker
self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME).then((cache) => {
            return cache.addAll(STATIC_ASSETS);
        })
    );
    self.skipWaiting();
});

// Activate and clean old caches
self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames.filter((name) => {
                    return name.startsWith('flashcard-') && name !== CACHE_NAME && name !== API_CACHE_NAME;
                }).map((name) => caches.delete(name))
            );
        })
    );
    self.clients.claim();
});

// Fetch handler - Network first, fallback to cache
self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // Skip non-GET requests
    if (event.request.method !== 'GET') return;

    // Library data must reflect creates, moves, and renames immediately.
    if (url.pathname === '/home-data' || url.pathname === '/folders' || url.pathname === '/decks') return;

    // Handle API requests with network-first strategy
    if (CACHEABLE_APIS.some(api => url.pathname === api || url.pathname.startsWith(api))) {
        event.respondWith(
            fetch(event.request)
                .then((response) => {
                    // Clone and cache successful responses
                    if (response.ok) {
                        const responseClone = response.clone();
                        caches.open(API_CACHE_NAME).then((cache) => {
                            cache.put(event.request, responseClone);
                        });
                    }
                    return response;
                })
                .catch(() => {
                    // Network failed - try cache
                    return caches.match(event.request);
                })
        );
        return;
    }

    // Show what we already have instantly, then update the cache in the background.
    const staleWhileRevalidate = (request, cacheName) => caches.open(cacheName).then((cache) =>
        cache.match(request, { ignoreVary: true }).then((cached) => {
            const network = fetch(request).then((response) => {
                if (response.ok) cache.put(request, response.clone());
                return response;
            });
            if (cached) {
                event.waitUntil(network.catch(() => {}));
                return cached;
            }
            return network.catch(() => caches.match('/'));
        })
    );

    // Pages: open instantly from cache (including pages prefetched on hover).
    // Hashed assets of older pages stay cached below, so a stale page still works.
    if (event.request.mode === 'navigate') {
        event.respondWith(staleWhileRevalidate(event.request, CACHE_NAME));
        return;
    }

    if (url.origin !== self.location.origin) return;

    // Content-hashed build files never change: cache first, no network round trip.
    if (url.pathname.startsWith('/static/react/assets/') || url.pathname.startsWith('/static/fonts/')) {
        event.respondWith(
            caches.match(event.request).then((cached) => cached || fetch(event.request).then((response) => {
                if (response.ok) {
                    const copy = response.clone();
                    caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
                }
                return response;
            }))
        );
        return;
    }

    // Other static files (CSS/JS for the HTML pages) and prefetched documents.
    if (url.pathname.startsWith('/static/') || event.request.destination === 'document' || event.request.destination === '') {
        if (url.pathname.startsWith('/static/') || ['/', '/create', '/pdf', '/video', '/story', '/learn', '/match', '/spelling', '/line', '/folder', '/edit'].includes(url.pathname)) {
            event.respondWith(staleWhileRevalidate(event.request, CACHE_NAME));
            return;
        }
    }
});

// Listen for messages from the app
self.addEventListener('message', (event) => {
    if (event.data === 'skipWaiting') {
        self.skipWaiting();
    }
});
