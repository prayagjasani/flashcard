// Service Worker for Flashcard App - Enables offline support

const CACHE_NAME = 'flashcard-v2';
const STATIC_ASSETS = [
    '/',
    '/static/manifest.json',
];

// API routes to cache (matches pathname prefix)
const API_CACHE_NAME = 'flashcard-api-v1';
const CACHEABLE_APIS = [
    '/folders',
    '/decks',
    '/order/folders',
    '/order/decks',
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

    // Handle page navigation requests
    if (event.request.mode === 'navigate') {
        event.respondWith(
            fetch(event.request)
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
                        // Fallback to homepage
                        return caches.match('/');
                    });
                })
        );
        return;
    }

    // For other assets (CSS, JS, images) - cache first, network fallback
    event.respondWith(
        caches.match(event.request).then((cached) => {
            if (cached) return cached;
            return fetch(event.request).then((response) => {
                // Cache successful fetches
                if (response.ok && url.origin === self.location.origin) {
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then((cache) => {
                        cache.put(event.request, responseClone);
                    });
                }
                return response;
            });
        })
    );
});

// Listen for messages from the app
self.addEventListener('message', (event) => {
    if (event.data === 'skipWaiting') {
        self.skipWaiting();
    }
});
