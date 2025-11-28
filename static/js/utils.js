/**
 * Shared utility functions for the flashcard application
 */

// ============================================
// Loader Functions
// ============================================

/**
 * Show the global loading overlay
 */
function showLoader() {
    const loader = document.getElementById('globalLoader');
    if (loader) loader.classList.add('is-active');
}

/**
 * Hide the global loading overlay
 */
function hideLoader() {
    const loader = document.getElementById('globalLoader');
    if (loader) loader.classList.remove('is-active');
}

// ============================================
// Audio Functions
// ============================================

const AudioUtils = {
    player: new Audio(),
    cache: new Map(),
    MAX_CACHE_SIZE: 50,

    /**
     * Convert a Blob to base64 string
     */
    blobToBase64(blob) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onloadend = () => resolve(reader.result);
            reader.onerror = reject;
            reader.readAsDataURL(blob);
        });
    },

    /**
     * Cleanup old entries from audio cache
     */
    cleanupCache() {
        if (this.cache.size > this.MAX_CACHE_SIZE) {
            const entries = Array.from(this.cache.entries());
            const toRemove = entries.slice(0, entries.length - this.MAX_CACHE_SIZE);
            toRemove.forEach(([key, url]) => {
                if (typeof url === 'string' && url.startsWith('blob:')) {
                    try { URL.revokeObjectURL(url); } catch {}
                }
                this.cache.delete(key);
            });
        }
    },

    /**
     * Hydrate audio cache from localStorage
     */
    hydrateFromLocalStorage(items, lang = 'de') {
        items.forEach(item => {
            const text = typeof item === 'string' ? item : item.de || item.text;
            if (!text) return;
            const key = `audio:${lang}:${text}`;
            const val = localStorage.getItem(key);
            if (val) this.cache.set(text, val);
        });
    },

    /**
     * Fetch audio from URL and cache it
     */
    async fetchAndCache(text, url, lang = 'de') {
        const cacheKey = `audio:${lang}:${text}`;
        if (this.cache.has(text) || localStorage.getItem(cacheKey)) return;
        
        try {
            const response = await fetch(url);
            if (!response.ok) return;
            
            const blob = await response.blob();
            const objUrl = URL.createObjectURL(blob);
            
            this.cleanupCache();
            this.cache.set(text, objUrl);
            
            try {
                const base64 = await this.blobToBase64(blob);
                localStorage.setItem(cacheKey, base64);
            } catch {
                // Storage quota exceeded, keep in-memory cache
            }
        } catch {
            // Failed to fetch, skip caching
        }
    },

    /**
     * Play audio for given text
     */
    async speak(text, lang = 'de') {
        if (!text) return;

        try {
            this.player.pause();
            this.player.currentTime = 0;

            const cacheKey = `audio:${lang}:${text}`;
            const stored = localStorage.getItem(cacheKey);
            if (stored && !this.cache.has(text)) {
                this.cache.set(text, stored);
            }

            if (this.cache.has(text)) {
                this.player.src = this.cache.get(text);
                await this.player.play();
                return;
            }

            const resp = await fetch(`/tts?text=${encodeURIComponent(text)}&lang=${lang}`);
            if (resp.ok) {
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                
                this.cleanupCache();
                this.cache.set(text, url);
                this.player.src = url;
                await this.player.play();
            }
        } catch {
            // Fallback to speech synthesis
            try {
                const u = new SpeechSynthesisUtterance(text);
                u.lang = `${lang}-${lang.toUpperCase()}`;
                speechSynthesis.speak(u);
            } catch {}
        }
    },

    /**
     * Preload audio for a deck
     */
    async preloadDeck(deckName, lang = 'de') {
        if (!deckName) return;

        try {
            const resp = await fetch(`/preload_deck_audio?deck=${encodeURIComponent(deckName)}&lang=${lang}`);
            if (!resp.ok) return;

            const data = await resp.json();
            if (!data?.audio_urls) return;

            const entries = Object.entries(data.audio_urls);
            const tasks = entries.map(([text, url]) => this.fetchAndCache(text, url, lang));
            await Promise.allSettled(tasks);
        } catch {
            // Failed to preload, continue anyway
        }
    },

    /**
     * Remove cached audio for a specific word
     */
    removeFromCache(text, lang = 'de') {
        if (!text) return;
        
        const val = this.cache.get(text);
        if (val && typeof val === 'string' && val.startsWith('blob:')) {
            try { URL.revokeObjectURL(val); } catch {}
        }
        this.cache.delete(text);
        
        try {
            localStorage.removeItem(`audio:${lang}:${text}`);
        } catch {}
    }
};

// ============================================
// URL/Navigation Helpers
// ============================================

/**
 * Get URL parameters as an object
 */
function getUrlParams() {
    return new URLSearchParams(location.search);
}

/**
 * Sanitize a string for use as a deck/folder name
 */
function sanitizeName(name) {
    return (name || '').trim().replace(/[^a-zA-Z0-9_\-]+/g, '_').substring(0, 50);
}

// ============================================
// Event Listener Management
// ============================================

const EventManager = {
    listeners: [],

    /**
     * Add event listener with automatic cleanup tracking
     */
    add(element, event, handler) {
        if (!element) return;
        element.addEventListener(event, handler);
        this.listeners.push({ element, event, handler });
    },

    /**
     * Remove all tracked event listeners
     */
    cleanup() {
        this.listeners.forEach(({ element, event, handler }) => {
            try {
                element.removeEventListener(event, handler);
            } catch {}
        });
        this.listeners = [];
    }
};

// Cleanup on page unload
window.addEventListener('beforeunload', () => EventManager.cleanup());

// ============================================
// API Helpers
// ============================================

const API = {
    /**
     * Fetch deck cards
     */
    async getCards(deckName) {
        if (!deckName) return [];
        
        try {
            const response = await fetch(`/cards?deck=${encodeURIComponent(deckName)}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            return Array.isArray(data) ? data : [];
        } catch {
            return [];
        }
    },

    /**
     * Fetch all decks
     */
    async getDecks() {
        try {
            const response = await fetch('/decks');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            return Array.isArray(data) ? data : [];
        } catch {
            return [];
        }
    },

    /**
     * Fetch all folders
     */
    async getFolders() {
        try {
            const response = await fetch('/folders');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            return data.folders || [];
        } catch {
            return [];
        }
    },

    /**
     * Update deck content
     */
    async updateDeck(name, content) {
        try {
            const response = await fetch('/deck/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, content })
            });
            return await response.json();
        } catch {
            return { ok: false };
        }
    },

    /**
     * Move deck to folder
     */
    async moveDeck(name, folder) {
        try {
            const response = await fetch('/deck/move', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, folder: folder || null })
            });
            return await response.json();
        } catch {
            return { ok: false };
        }
    }
};

// Export for module usage if needed
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { showLoader, hideLoader, AudioUtils, getUrlParams, sanitizeName, EventManager, API };
}

