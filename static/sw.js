// Service Worker for Flashcard App - Enables offline support

const CACHE_NAME = 'flashcard-v6';
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

    // Always prefer the current HTML shell. A cached shell can reference hashed
    // JavaScript files that no longer exist after a deployment.
    if (event.request.mode === 'navigate') {
        event.respondWith(
            fetch(event.request, { cache: 'no-store' })
                .then((response) => {
                    // Cache successful page loads
                    if (response.ok) {
                        const responseClone = response.clone();
                        caches.open(CACHE_NAME).then((cache) => {
                            cache.put(event.request, responseClone);
                        });
                    }
                    return response;
                })
                .catch(() => {
                    // Offline - serve from cache
                    return caches.match(event.request).then((cached) => {
                        if (cached) return cached;
                        return caches.match('/');
                    });
                })
        );
        return;
    }

    // For versioned assets, prefer the deployment and use cache only offline.
    event.respondWith(
        fetch(event.request).then((response) => {
                // Cache successful fetches
                if (response.ok && url.origin === self.location.origin) {
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then((cache) => {
                        cache.put(event.request, responseClone);
                    });
                }
                return response;
            }).catch(() => caches.match(event.request))
    );
});

// Listen for messages from the app
self.addEventListener('message', (event) => {
    if (event.data === 'skipWaiting') {
        self.skipWaiting();
    }
});
