        const card = document.getElementById('card');
        const wrongBtn = document.getElementById('wrong');
        const rightBtn = document.getElementById('right');
        const resetBtn = document.getElementById('reset');
        const prevBtn = document.getElementById('prev');
        const speakBtn = document.getElementById('speak');
        const autoPlayToggle = document.getElementById('autoPlayToggle');
        const lineHint = document.getElementById('lineHint');
        const lineHintText = document.getElementById('lineHintText');
        const lineHintAudio = document.getElementById('lineHintAudio');
        const wordListGrid = document.getElementById('wordListGrid');
        const wordListContainer = document.getElementById('wordListContainer');
        const wordListHeader = document.getElementById('wordListHeader');
        const practicedCountEl = document.getElementById('practicedCount');

        // Line data cache (de word -> {line_de, line_en})
        const lineDataCache = new Map();
        // Line audio cache (line_de text -> blob object URL)
        const lineAudioCache = new Map();
        // Line audio URL map (line_de text -> R2 URL, lightweight)
        const lineAudioURLMap = new Map();
        // Track how many cards user has seen to trigger preload batches
        let lineAudioCardsSeen = 0;
        const LINE_AUDIO_BATCH_SIZE = 5;
        const LINE_AUDIO_TRIGGER_AT = 4;
        // Track which words have had their line revealed (stays revealed)
        const revealedLineWords = new Set();
        const header = document.querySelector('.site-header');

        // Track practiced words
        let practicedWords = [];
        const wordRevealTimers = new WeakMap();
        const spellingBtn = document.getElementById('spellingBtn');
        const homeBtn = document.getElementById('homeBtn');
        const deckGrid = document.getElementById('deckGrid');
        const homeDeckList = document.getElementById('homeDeckList');
        const chooseActionModal = document.getElementById('chooseActionModal');
        const chooseActionCloseBtn = document.getElementById('chooseActionCloseBtn');
        const deckSectionHead = document.getElementById('deckSectionHead');
        const deckSectionTitle = document.getElementById('deckSectionTitle');
        const folderGrid = document.getElementById('folderGrid');
        const createFolderBtn = document.getElementById('createFolderBtn');
        const createFolderModal = document.getElementById('createFolderModal');
        const folderNameInput = document.getElementById('folderNameInput');
        const createFolderCancelBtn = document.getElementById('createFolderCancelBtn');
        const createFolderSaveBtn = document.getElementById('createFolderSaveBtn');
        const createFolderStatus = document.getElementById('createFolderStatus');

        const renameFolderModal = document.getElementById('renameFolderModal');
        const renameFolderInput = document.getElementById('renameFolderInput');
        const renameFolderCancelBtn = document.getElementById('renameFolderCancelBtn');
        const renameFolderSaveBtn = document.getElementById('renameFolderSaveBtn');
        const deleteFolderBtn = document.getElementById('deleteFolderBtn');
        const renameFolderStatus = document.getElementById('renameFolderStatus');
        const moveFolderBtn = document.getElementById('moveFolderBtn');
        const moveFolderModal = document.getElementById('moveFolderModal');
        const moveFolderList = document.getElementById('moveFolderList');
        const moveFolderCancelBtn = document.getElementById('moveFolderCancelBtn');
        const moveFolderSaveBtn = document.getElementById('moveFolderSaveBtn');
        const moveFolderStatus = document.getElementById('moveFolderStatus');
        let cachedFolderList = [];
        let cachedDeckList = [];
        let selectedFolder = '';
        let folderToManage = '';
        const openLearnBtn = document.getElementById('openLearnBtn');
        const openFlashBtn = document.getElementById('openFlashBtn');
        let openSpellingBtn = document.getElementById('openSpellingBtn');
        let openMatchBtn = document.getElementById('openMatchBtn');
        let openLineBtn = document.getElementById('openLineBtn');
        const openSettingsBtn = document.getElementById('openSettingsBtn');
        const studyActionsEl = document.querySelector('#chooseActionModal .study-actions');
        if (!openSpellingBtn && studyActionsEl) {
            const btn = document.createElement('button');
            btn.id = 'openSpellingBtn';
            btn.className = 'study-action study-action--secondary';
            btn.type = 'button';
            btn.innerHTML = '<span class="material-symbols-outlined study-action__icon" aria-hidden="true">spellcheck</span><span>Spelling</span>';
            studyActionsEl.appendChild(btn);
            openSpellingBtn = btn;
        }
        if (!openMatchBtn && studyActionsEl) {
            const btn2 = document.createElement('button');
            btn2.id = 'openMatchBtn';
            btn2.className = 'study-action study-action--secondary';
            btn2.type = 'button';
            btn2.innerHTML = '<span class="material-symbols-outlined study-action__icon" aria-hidden="true">swap_horiz</span><span>Match</span>';
            studyActionsEl.appendChild(btn2);
            openMatchBtn = btn2;
        }
        if (!openLineBtn && studyActionsEl) {
            const btn3 = document.createElement('button');
            btn3.id = 'openLineBtn';
            btn3.className = 'study-action study-action--secondary';
            btn3.type = 'button';
            btn3.innerHTML = '<span class="material-symbols-outlined study-action__icon" aria-hidden="true">subtitles</span><span>Line</span>';
            studyActionsEl.appendChild(btn3);
            openLineBtn = btn3;
        }
        const moveDeckModal = document.getElementById('moveDeckModal');
        const moveDeckList = document.getElementById('moveDeckList');
        const moveDeckCancelBtn = document.getElementById('moveDeckCancelBtn');
        const moveDeckSaveBtn = document.getElementById('moveDeckSaveBtn');
        const moveDeckStatus = document.getElementById('moveDeckStatus');
        const MOVE_BROWSER_ROOT = '__root__';
        let moveDeckBrowserCursor = MOVE_BROWSER_ROOT;
        let moveDeckParentByName = {};
        let moveDeckChildrenByParent = {};
        const deckSettingsModal = document.getElementById('deckSettingsModal');
        const deckSettingsMoveBtn = document.getElementById('deckSettingsMoveBtn');
        const deckSettingsDeleteBtn = document.getElementById('deckSettingsDeleteBtn');
        const flashApp = document.getElementById('flashApp');
        const globalLoader = document.getElementById('globalLoader');
        let selectedDeckForAction = null;

        function shuffle(arr) {
            for (let i = arr.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                const t = arr[i];
                arr[i] = arr[j];
                arr[j] = t;
            }
            return arr;
        }

        const ordersCache = {
            folders: [],
            decks: {}
        };
        // Helper to clear Service Worker API cache
        async function clearSwApiCache() {
            if ('caches' in window) {
                try {
                    await caches.delete('flashcard-api-v1');
                    await caches.delete('flashcard-api-v2');
                } catch (e) {
                    console.log('Failed to clear SW cache:', e);
                }
            }
        }

        async function writeOrder(type, names, scope) {
            if (type === 'folder') {
                const resp = await fetch('/order/folders', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        order: names
                    })
                });
                if (resp.ok) {
                    ordersCache.folders = Array.isArray(names) ? names.slice() : [];
                    // Clear localStorage & sessionStorage caches so next load reflects new order
                    try { localStorage.removeItem(CACHE_KEY_HOME); } catch { }
                    try { localStorage.removeItem(CACHE_KEY_FOLDERS); } catch { }
                    try { localStorage.removeItem(CACHE_KEY_DECKS); } catch { }
                    try { localStorage.removeItem('flashcard_folder_order_cache'); } catch { }
                    try { sessionStorage.removeItem(CACHE_KEY_FOLDERS); } catch { }
                    try { sessionStorage.removeItem(CACHE_KEY_DECKS); } catch { }
                    try { sessionStorage.removeItem('flashcard_folder_order_cache'); } catch { }
                    saveToCache('flashcard_folder_order_cache', ordersCache.folders);
                    // Clear Service Worker API cache
                    await clearSwApiCache();
                }
            } else {
                const key = scope || 'root';
                const resp = await fetch('/order/decks', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        scope: key,
                        order: names
                    })
                });
                if (resp.ok) {
                    ordersCache.decks[key] = Array.isArray(names) ? names.slice() : [];
                    // Clear localStorage & sessionStorage caches so next load reflects new order
                    try { localStorage.removeItem(CACHE_KEY_HOME); } catch { }
                    try { localStorage.removeItem(CACHE_KEY_FOLDERS); } catch { }
                    try { localStorage.removeItem(CACHE_KEY_DECKS); } catch { }
                    try { localStorage.removeItem('flashcard_folder_order_cache'); } catch { }
                    try { sessionStorage.removeItem(CACHE_KEY_FOLDERS); } catch { }
                    try { sessionStorage.removeItem(CACHE_KEY_DECKS); } catch { }
                    try { sessionStorage.removeItem('flashcard_folder_order_cache'); } catch { }
                    // Clear Service Worker API cache
                    await clearSwApiCache();
                }
            }
        }
        async function refreshFolderOrder() {
            try {
                const resp = await fetch('/order/folders');
                const arr = await resp.json().catch(() => []);
                ordersCache.folders = Array.isArray(arr) ? arr : [];
            } catch {
                ordersCache.folders = [];
            }
        }
        async function refreshDeckOrder(scope) {
            const key = scope || 'root';
            try {
                const resp = await fetch(`/order/decks?scope=${encodeURIComponent(key)}`);
                const arr = await resp.json().catch(() => []);
                ordersCache.decks[key] = Array.isArray(arr) ? arr : [];
            } catch {
                ordersCache.decks[key] = [];
            }
        }

        function applyOrder(list, type, scope) {
            const saved = type === 'folder' ? ordersCache.folders : (ordersCache.decks[scope || 'root'] || []);
            if (!Array.isArray(saved) || !saved.length) return list.slice();
            const indexByName = new Map(list.map((x, i) => [x.name, i]));
            const ordered = [];
            saved.forEach((n) => {
                const idx = indexByName.get(n);
                if (idx !== undefined) ordered.push(list[idx]);
            });
            list.forEach((x) => {
                if (!saved.includes(x.name)) ordered.push(x);
            });
            return ordered;
        }
        let draggingFolder = null;
        let draggingDeck = null;
        let dragTileEl = null;
        let dragTileHeight = 0;
        const isCoarsePointer = ("ontouchstart" in window) || window.matchMedia('(pointer: coarse)').matches;

        function ensurePlaceholder(container) {
            if (!container) return null;
            let ph = container.querySelector('.drop-placeholder');
            if (!ph) {
                ph = document.createElement('div');
                ph.className = 'drop-placeholder';
                ph.style.height = '48px';
                ph.style.margin = '8px 0';
                ph.style.border = '2px dashed var(--text-secondary)';
                ph.style.borderRadius = '12px';
                ph.style.background = '#eef0f4';
            } {
                let __h = dragTileHeight || 48;
                if (!__h) {
                    const __src = container.querySelector('.deck-tile');
                    if (__src) {
                        const __r = __src.getBoundingClientRect();
                        __h = Math.max(40, Math.round(__r.height));
                    }
                }
                ph.style.height = __h + 'px';
            }
            if (!ph.parentElement) container.appendChild(ph);
            return ph;
        }

        function removePlaceholder(container) {
            const ph = container ? container.querySelector('.drop-placeholder') : null;
            if (ph && ph.parentElement) ph.parentElement.removeChild(ph);
        }

        function placePlaceholder(container, index) {
            if (!container) return;
            const tiles = Array.from(container.querySelectorAll('.deck-tile'));
            const ph = ensurePlaceholder(container);
            if (!tiles.length) return;
            const clamped = Math.max(0, Math.min(index, tiles.length));
            if (clamped >= tiles.length) container.appendChild(ph);
            else container.insertBefore(ph, tiles[clamped]);
        }

        function indexFromFinger(container, fingerY) {
            const tiles = Array.from(container.querySelectorAll('.deck-tile'));
            for (let i = 0; i < tiles.length; i++) {
                const r = tiles[i].getBoundingClientRect();
                const mid = r.top + r.height / 2;
                if (fingerY < mid) return i;
            }
            return tiles.length;
        }

        function attachTouchDnD(tile, name, type, scope, container) {
            if (!isCoarsePointer) return;
            let dragging = false;
            let pressTimer = null;
            let fingerY = 0;
            let startY = 0;
            const LONG_PRESS_MS = 2000;
            const MOVE_CANCEL_PX = 10;

            function start(y) {
                startY = y;
                pressTimer = setTimeout(() => {
                    dragging = true;
                    dragTileEl = tile;
                    dragTileHeight = Math.round(tile.getBoundingClientRect().height);
                    tile.classList.add('is-dragging-hidden');
                    tile.style.touchAction = 'none';
                    document.body.style.overflow = 'hidden';
                    const tiles = Array.from(container.querySelectorAll('.deck-tile'));
                    const idx = tiles.indexOf(tile);
                    placePlaceholder(container, idx < 0 ? 0 : idx);
                }, LONG_PRESS_MS);
            }

            function clearPress() {
                if (pressTimer) {
                    clearTimeout(pressTimer);
                    pressTimer = null;
                }
            }
            async function commit() {
                if (!dragging) return;
                dragging = false;
                document.body.style.overflow = '';
                const ph = container.querySelector('.drop-placeholder');
                if (ph && dragTileEl) {
                    container.insertBefore(dragTileEl, ph);
                }
                removePlaceholder(container);
                tile.classList.remove('is-dragging-hidden');
                tile.style.touchAction = 'auto';
                const newOrder = Array.from(container.querySelectorAll('.deck-title')).map(el => el.textContent);
                if (type === 'folder') {
                    await writeOrder('folder', newOrder);
                } else {
                    await writeOrder('deck', newOrder, scope || 'root');
                }
                dragTileEl = null;
                dragTileHeight = 0;
            }
            tile.style.touchAction = 'auto';
            tile.style.userSelect = 'none';
            tile.addEventListener('touchstart', (e) => {
                const t = e.touches && e.touches[0];
                if (!t) return;
                start(t.clientY);
            }, {
                passive: true
            });
            tile.addEventListener('touchmove', (e) => {
                const t = e.touches && e.touches[0];
                if (!t) return;
                fingerY = t.clientY;
                if (!dragging) {
                    if (Math.abs((t.clientY) - startY) > MOVE_CANCEL_PX) {
                        clearPress();
                    }
                    return;
                }
                e.preventDefault();
                const idx = indexFromFinger(container, fingerY);
                placePlaceholder(container, idx);
            }, {
                passive: false
            });
            tile.addEventListener('touchend', () => {
                clearPress();
                commit();
            });
            tile.addEventListener('touchcancel', () => {
                clearPress();
                dragging = false;
                document.body.style.overflow = '';
                tile.classList.remove('is-dragging-hidden');
                tile.style.touchAction = 'auto';
                removePlaceholder(container);
                dragTileEl = null;
                dragTileHeight = 0;
            });
        }

        let folderDropTimer = null;
        let folderDropTarget = null;

        function attachFolderDnD(tile, name) {
            tile.setAttribute('draggable', 'true');
            tile.addEventListener('dragstart', (e) => {
                draggingFolder = name;
                e.dataTransfer.effectAllowed = 'move';
                dragTileEl = tile;
                dragTileHeight = Math.round(tile.getBoundingClientRect().height);
                tile.classList.add('is-dragging-hidden');
                const tiles = Array.from(folderGrid.querySelectorAll('.deck-tile'));
                const idx = tiles.indexOf(tile);
                placePlaceholder(folderGrid, idx < 0 ? 0 : idx);
            });
            tile.addEventListener('dragenter', (e) => {
                if (!draggingFolder || draggingFolder === name) return;
                // Clear any existing timer
                if (folderDropTimer) clearTimeout(folderDropTimer);
                // Remove highlight from previous target
                document.querySelectorAll('.folder-drop-target').forEach(el => el.classList.remove('folder-drop-target'));
                // Highlight this folder as drop target
                tile.classList.add('folder-drop-target');
                folderDropTarget = name;
                // Start timer - if held for 800ms, move folder inside
                folderDropTimer = setTimeout(async () => {
                    if (draggingFolder && folderDropTarget === name && draggingFolder !== name) {
                        // Move folder inside target
                        try {
                            const resp = await fetch('/folder/move', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json'
                                },
                                body: JSON.stringify({
                                    name: draggingFolder,
                                    parent: name
                                })
                            });
                            const out = await resp.json().catch(() => ({
                                ok: false
                            }));
                            if (resp.ok && out.ok) {
                                // Reset drag state
                                draggingFolder = null;
                                removePlaceholder(folderGrid);
                                if (dragTileEl) dragTileEl.classList.remove('is-dragging-hidden');
                                dragTileEl = null;
                                dragTileHeight = 0;
                                // Reload folders
                                await loadFolders();
                            }
                        } catch { }
                    }
                    tile.classList.remove('folder-drop-target');
                    folderDropTarget = null;
                }, 800);
            });
            tile.addEventListener('dragleave', (e) => {
                tile.classList.remove('folder-drop-target');
                if (folderDropTarget === name) {
                    if (folderDropTimer) clearTimeout(folderDropTimer);
                    folderDropTimer = null;
                    folderDropTarget = null;
                }
            });
            tile.addEventListener('dragover', (e) => {
                e.preventDefault();
                if (folderDropTarget === name) return; // Don't show placeholder if about to drop inside
                const tiles = Array.from(folderGrid.querySelectorAll('.deck-tile'));
                const idx = tiles.indexOf(tile);
                placePlaceholder(folderGrid, idx < 0 ? 0 : idx);
            });
            tile.addEventListener('drop', async (e) => {
                e.preventDefault();
                // Clear timer and highlight
                if (folderDropTimer) clearTimeout(folderDropTimer);
                folderDropTimer = null;
                tile.classList.remove('folder-drop-target');
                folderDropTarget = null;

                const target = name;
                const dragged = draggingFolder;
                draggingFolder = null;
                const ph = folderGrid.querySelector('.drop-placeholder');
                if (ph && dragTileEl) {
                    folderGrid.insertBefore(dragTileEl, ph);
                }
                removePlaceholder(folderGrid);
                if (dragTileEl) dragTileEl.classList.remove('is-dragging-hidden');
                const newOrder = Array.from(folderGrid.querySelectorAll('.deck-title')).map(el => el.textContent);
                await writeOrder('folder', newOrder);
                dragTileEl = null;
                dragTileHeight = 0;
            });
            tile.addEventListener('dragend', () => {
                // Clear timer and highlight
                if (folderDropTimer) clearTimeout(folderDropTimer);
                folderDropTimer = null;
                document.querySelectorAll('.folder-drop-target').forEach(el => el.classList.remove('folder-drop-target'));
                folderDropTarget = null;

                if (dragTileEl) dragTileEl.classList.remove('is-dragging-hidden');
                removePlaceholder(folderGrid);
                dragTileEl = null;
                dragTileHeight = 0;
            });
            attachTouchDnD(tile, name, 'folder', null, folderGrid);
        }

        function attachDeckDnD(tile, name, scope) {
            tile.setAttribute('draggable', 'true');
            tile.addEventListener('dragstart', (e) => {
                draggingDeck = name;
                e.dataTransfer.effectAllowed = 'move';
                dragTileEl = tile;
                dragTileHeight = Math.round(tile.getBoundingClientRect().height);
                tile.classList.add('is-dragging-hidden');
                const tiles = Array.from(deckGrid.querySelectorAll('.deck-tile'));
                const idx = tiles.indexOf(tile);
                placePlaceholder(deckGrid, idx < 0 ? 0 : idx);
            });
            tile.addEventListener('dragover', (e) => {
                e.preventDefault();
                const tiles = Array.from(deckGrid.querySelectorAll('.deck-tile'));
                const idx = tiles.indexOf(tile);
                placePlaceholder(deckGrid, idx < 0 ? 0 : idx);
            });
            tile.addEventListener('drop', async (e) => {
                e.preventDefault();
                const target = name;
                const dragged = draggingDeck;
                draggingDeck = null;
                const ph = deckGrid.querySelector('.drop-placeholder');
                if (ph && dragTileEl) {
                    deckGrid.insertBefore(dragTileEl, ph);
                }
                removePlaceholder(deckGrid);
                tile.classList.remove('is-dragging-hidden');
                const newOrder = Array.from(deckGrid.querySelectorAll('.deck-title')).map(el => el.textContent);
                await writeOrder('deck', newOrder, scope || 'root');
                dragTileEl = null;
                dragTileHeight = 0;
            });
            tile.addEventListener('dragend', () => {
                tile.classList.remove('is-dragging-hidden');
                removePlaceholder(deckGrid);
                dragTileEl = null;
                dragTileHeight = 0;
            });
            attachTouchDnD(tile, name, 'deck', scope, deckGrid);
        }

        function closeKebabMenus() {
            document.querySelectorAll('.kebab-menu').forEach(el => {
                el.classList.remove('is-open');
            });
            document.querySelectorAll('.deck-tile.menu-open').forEach(el => {
                el.classList.remove('menu-open');
            });
        }

        function toggleMenu(menu) {
            const isOpen = menu.classList.contains('is-open');
            closeKebabMenus();
            if (!isOpen) {
                menu.classList.add('is-open');
                const parentTile = menu.closest('.deck-tile');
                if (parentTile) parentTile.classList.add('menu-open');
            }
        }
        window.addEventListener('click', () => closeKebabMenus());

        let currentCard = {};
        let words = [];
        let originalWords = [];
        let showingFront = true;
        let frontLang = 'de';
        let backLang = 'en';
        let isAutoPlayEnabled = false;
        let cardHistory = [];

        // Random test tracking
        let isRandomTest = false;
        let randomTestWrongWords = [];
        let randomTestCompletedCount = 0;
        let allAvailableWords = [];
        let randomTestPracticedKeys = new Set();

        function showLoader() {
            if (globalLoader) globalLoader.classList.add('is-active');
        }

        function hideLoader() {
            if (globalLoader) globalLoader.classList.remove('is-active');
        }

        // Ensure loader is hidden when navigating back from BFCache (not on first load)
        window.addEventListener('pageshow', (event) => {
            if (event && event.persisted) {
                hideLoader();
            }
        });

        // Auto-play toggle functionality
        function updateAutoPlayToggle() {
            const toggleIcon = autoPlayToggle.querySelector('.toggle-icon');
            const toggleText = autoPlayToggle.querySelector('.toggle-text');

            if (isAutoPlayEnabled) {
                autoPlayToggle.classList.add('active');
                autoPlayToggle.setAttribute('aria-checked', 'true');
                toggleIcon.textContent = '🔊';
                autoPlayToggle.title = 'Auto-play is enabled - audio will play automatically';
            } else {
                autoPlayToggle.classList.remove('active');
                autoPlayToggle.setAttribute('aria-checked', 'false');
                toggleIcon.textContent = '🔇';
                autoPlayToggle.title = 'Auto-play is disabled - click speaker button to play audio';
            }
        }

        // Initialize auto-play state: default OFF and persist as off
        function initAutoPlayState() {
            isAutoPlayEnabled = false;
            try {
                localStorage.setItem('autoPlayEnabled', 'false');
            } catch { }
            updateAutoPlayToggle();
        }

        // Auto-play toggle event listener
        autoPlayToggle.addEventListener('click', () => {
            isAutoPlayEnabled = !isAutoPlayEnabled;
            try {
                localStorage.setItem('autoPlayEnabled', String(isAutoPlayEnabled));
            } catch { }
            updateAutoPlayToggle();
        });

        // Auto-play audio function with error handling
        async function autoPlayAudio() {
            if (!isAutoPlayEnabled || !currentCard.de) return;

            try {
                const text = currentCard.de;

                // Use cached URL first if available
                const cached = audioCache.get(text);
                if (cached) {
                    try {
                        audioPlayer.pause();
                        audioPlayer.src = cached;
                        audioPlayer.currentTime = 0;
                        await audioPlayer.play();
                        return;
                    } catch (e) {
                        console.warn('Auto-play failed with cached audio:', e);
                    }
                }

                // Fetch and play audio
                try {
                    const resp = await fetch(`/r2/tts?text=${encodeURIComponent(text)}&lang=de`);
                    if (resp.ok) {
                        const blob = await resp.blob();
                        const objUrl = URL.createObjectURL(blob);
                        audioPlayer.pause();
                        audioPlayer.src = objUrl;
                        audioPlayer.currentTime = 0;
                        await audioPlayer.play();
                        audioCache.set(text, objUrl);
                        audioBlobUrls.set(text, objUrl);
                        return;
                    }
                } catch (e) {
                    console.warn('Auto-play failed with fetch:', e);
                }

                // If audio could not be fetched, do nothing rather than using a fallback voice
            } catch (e) {
                console.error('Auto-play error:', e);
            }
        }

        const deckSelect = document.getElementById('deckSelect');
        const selectedDeckNameEl = document.getElementById('selectedDeckName');
        const englishFrontToggle = document.getElementById('englishFrontToggle');
        const pdfBtn = document.getElementById('pdfBtn');

        function updateSelectedDeckName(names) {
            if (!selectedDeckNameEl) return;
            if (Array.isArray(names) && names.length > 0) {
                selectedDeckNameEl.textContent = names.join(', ');
                selectedDeckNameEl.style.display = 'block';
            } else if (typeof names === 'string' && names) {
                selectedDeckNameEl.textContent = names;
                selectedDeckNameEl.style.display = 'block';
            } else {
                selectedDeckNameEl.textContent = '';
                selectedDeckNameEl.style.display = 'none';
            }
        }

        function applyFrontPreference(isEnglishFront) {
            frontLang = isEnglishFront ? 'en' : 'de';
            backLang = isEnglishFront ? 'de' : 'en';
        }

        function refreshCardText() {
            if (!currentCard) return;
            const text = showingFront ? currentCard[frontLang] : currentCard[backLang];
            if (text) card.innerText = text;
        }
        // Initialize English-front preference
        (function initFrontPref() {
            let saved = false;
            try {
                saved = localStorage.getItem('englishFront') === 'true';
            } catch { }
            applyFrontPreference(saved);
            if (englishFrontToggle) englishFrontToggle.checked = saved;
        })();
        if (englishFrontToggle) englishFrontToggle.addEventListener('change', (e) => {
            const isEnglishFront = e.target.checked;
            try {
                localStorage.setItem('englishFront', String(isEnglishFront));
            } catch { }
            applyFrontPreference(isEnglishFront);
            // Re-render current card after language flip
            refreshCardText();
            updateLineHint();
            renderWordList();
        });

        async function loadWordsForDeck(deckName) {
            isAutoPlayEnabled = false;
            isRandomTest = false;
            randomTestWrongWords = [];
            randomTestCompletedCount = 0;
            randomTestPracticedKeys.clear();
            try {
                localStorage.setItem('autoPlayEnabled', 'false');
            } catch { }
            updateAutoPlayToggle();
            cardHistory = [];
            updateSelectedDeckName(deckName);
            const url = deckName ? `/cards?deck=${encodeURIComponent(deckName)}` : '/cards';
            let data = [];
            try {
                const response = await fetch(url);
                if (response.ok) {
                    data = await response.json().catch(() => []);
                    // Cache cards for instant navigation to study screens
                    if (deckName && Array.isArray(data)) {
                        try {
                            sessionStorage.setItem(`flashcard_cards_${deckName}`, JSON.stringify(data));
                        } catch { }
                    }
                } else {
                    data = [];
                }
            } catch {
                data = [];
            }
            originalWords = Array.isArray(data) ? data : [];
            words = originalWords.slice();
            shuffle(words);

            // Reset all state BEFORE showing first card
            clearDeckPreloads();
            clearPracticedWords();
            revealedLineWords.clear();
            lineAudioCardsSeen = 0;
            lineAudioURLMap.clear();
            wordAudioURLMap.clear();

            // Start preloading (populates URL maps, triggers first batch)
            preloadDeckAudio(deckName);
            loadLineDataForDeck(deckName);

            // Show first card (triggers preloadNextWordAudios on card 1)
            getNewCard();

            if (deckName) {
                try {
                    localStorage.setItem('selectedDeck', deckName);
                } catch { }
            }
        }

        async function preloadDeckAudio(deckName) {
            try {
                const resp = await fetch(`/preload_deck_audio?deck=${encodeURIComponent(deckName)}`);
                const data = await resp.json();
                if (data && data.audio_urls) {
                    // Store URL map only (lightweight, no blob downloads)
                    Object.entries(data.audio_urls).forEach(([text, url]) => wordAudioURLMap.set(text, url));
                    // Trigger initial batch of blob downloads
                    preloadNextWordAudios();
                }
            } catch (e) {
                console.error('Error preloading audio:', e);
            }
        }

        // Download blobs for only the next N upcoming word audios
        async function preloadNextWordAudios() {
            const BATCH = LINE_AUDIO_BATCH_SIZE;
            const wordsToLoad = [];
            // Include current card
            if (currentCard && currentCard.de && !audioCache.has(currentCard.de)) {
                wordsToLoad.push(currentCard.de);
            }
            // Include upcoming cards from words array
            for (let i = 0; i < words.length && wordsToLoad.length < BATCH; i++) {
                const w = words[i];
                if (!w || !w.de) continue;
                if (!audioCache.has(w.de) && !wordsToLoad.includes(w.de)) {
                    wordsToLoad.push(w.de);
                }
            }
            if (!wordsToLoad.length) return;
            const tasks = wordsToLoad.map(text => (async () => {
                try {
                    if (audioCache.has(text)) return;
                    const url = wordAudioURLMap.get(text);
                    if (!url) return;
                    const resp2 = await fetch(url);
                    if (!resp2.ok) return;
                    const blob = await resp2.blob();
                    const objUrl = URL.createObjectURL(blob);
                    audioCache.set(text, objUrl);
                    audioBlobUrls.set(text, objUrl);
                    try {
                        const base64 = await new Promise((resolve, reject) => {
                            const r = new FileReader();
                            r.onloadend = () => resolve(r.result);
                            r.onerror = reject;
                            r.readAsDataURL(blob);
                        });
                        localStorage.setItem(`audio:de:${text}`, String(base64));
                    } catch { }
                } catch { }
            })());
            await Promise.allSettled(tasks);
        }

        async function preloadRandomTestAudio(words) {
            if (!words || !words.length) return;
            const tasks = words.map(w => (async () => {
                const text = w.de;
                if (!text || audioCache.has(text)) return;
                try {
                    const resp = await fetch(`/tts?text=${encodeURIComponent(text)}&lang=de`);
                    if (resp.ok) {
                        const blob = await resp.blob();
                        const objUrl = URL.createObjectURL(blob);
                        audioCache.set(text, objUrl);
                        audioBlobUrls.set(text, objUrl);
                        try {
                            const base64 = await new Promise((resolve, reject) => {
                                const r = new FileReader();
                                r.onloadend = () => resolve(r.result);
                                r.onerror = reject;
                                r.readAsDataURL(blob);
                            });
                            localStorage.setItem(`audio:de:${text}`, String(base64));
                        } catch { }
                    }
                } catch (err) {
                    console.warn('Failed to preload audio for:', text);
                }
            })());
            await Promise.allSettled(tasks);
        }

        function clearDeckPreloads() {
            try {
                preloadedAudios.forEach((a) => {
                    try {
                        a.pause();
                        a.src = '';
                    } catch { }
                });
                preloadedAudios.clear();
                audioBlobUrls.forEach((url) => {
                    try {
                        URL.revokeObjectURL(url);
                    } catch { }
                });
                audioBlobUrls.clear();
                audioCache.clear();
            } catch { }
        }

        // Cache keys for instant loading
        const CACHE_KEY_HOME = 'flashcard_home_cache_v1';
        const CACHE_KEY_DECKS = 'flashcard_decks_cache';
        const CACHE_KEY_FOLDERS = 'flashcard_folders_cache';
        const CACHE_EXPIRY = 5 * 60 * 1000; // 5 minutes

        function saveToCache(key, data) {
            try {
                localStorage.setItem(key, JSON.stringify({ data, ts: Date.now() }));
            } catch { }
        }

        function getFromCache(key) {
            try {
                const raw = localStorage.getItem(key);
                if (!raw) return null;
                const parsed = JSON.parse(raw);
                if (!parsed || !parsed.data) return null;
                return parsed.data;
            } catch { return null; }
        }

        function clearCache() {
            // Clear all home / folder / deck caches to force a fresh network load
            try { localStorage.removeItem(CACHE_KEY_HOME); } catch { }
            try { localStorage.removeItem(CACHE_KEY_FOLDERS); } catch { }
            try { localStorage.removeItem(CACHE_KEY_DECKS); } catch { }
            try { localStorage.removeItem('pdfs_cache'); } catch { }
            try { localStorage.removeItem('pdf_folders_cache'); } catch { }
        }

        // Fast home screen loader: single /home-data request + localStorage cache
        async function loadAllData() {
            // Check localStorage for cached data (show instantly)
            const cached = getFromCache(CACHE_KEY_HOME);
            if (cached) {
                cachedFolderList = cached.folders || [];
                cachedDeckList = cached.decks || [];
                ordersCache.folders = cached.folder_order || [];
                updateDeckCounts();
                renderFolderGrid();
                renderDeckGrid();
                populateDeckSelect();
                handleUrlParams();
                requestAnimationFrame(() => hideLoader());
                // Refresh data silently in background
                refreshDataInBackground();
                preloadPdfsInBackground();
                return; // Instant — no network needed
            }

            // Cache miss: single combined request to backend
            showLoader();
            try {
                const resp = await fetch('/home-data');
                const data = resp.ok ? await resp.json().catch(() => ({})) : {};

                cachedFolderList = Array.isArray(data.folders) ? data.folders : [];
                cachedDeckList = Array.isArray(data.decks) ? data.decks : [];
                ordersCache.folders = Array.isArray(data.folder_order) ? data.folder_order : [];

                // Persist to localStorage for instant next visit
                saveToCache(CACHE_KEY_HOME, {
                    folders: cachedFolderList,
                    decks: cachedDeckList,
                    folder_order: ordersCache.folders,
                });
                saveToCache(CACHE_KEY_FOLDERS, cachedFolderList);
                saveToCache(CACHE_KEY_DECKS, cachedDeckList);

                updateDeckCounts();
            } catch { }

            hideLoader();
            renderFolderGrid();
            renderDeckGrid();
            populateDeckSelect();
            handleUrlParams();
            preloadPdfsInBackground();
        }

        // Background refresh - updates cache and UI if data changed
        async function refreshDataInBackground() {
            try {
                const [foldersResp, decksResp, folderOrderResp] = await Promise.all([
                    fetch('/folders').then(r => r.json()).catch(() => null),
                    fetch('/decks').then(r => r.json()).catch(() => null),
                    fetch('/order/folders').then(r => r.json()).catch(() => null)
                ]);

                let needsRerender = false;

                // Helper to compare folder lists by name and parent only (ignore count changes)
                function compareFolders(a, b) {
                    if (!a || !b || a.length !== b.length) return false;
                    const aMap = new Map(a.map(f => [f.name, f.parent || null]));
                    return b.every(f => aMap.get(f.name) === (f.parent || null));
                }

                // Update folders if structure changed (not just counts)
                if (foldersResp && foldersResp.folders) {
                    const newFolders = Array.isArray(foldersResp.folders) ? foldersResp.folders : [];
                    if (!compareFolders(newFolders, cachedFolderList)) {
                        cachedFolderList = newFolders;
                        saveToCache(CACHE_KEY_FOLDERS, cachedFolderList);
                        needsRerender = true;
                    } else {
                        // Just update counts silently without re-rendering
                        cachedFolderList = newFolders;
                        saveToCache(CACHE_KEY_FOLDERS, cachedFolderList);
                    }
                }

                // Update folder order if changed
                if (folderOrderResp) {
                    const newOrder = Array.isArray(folderOrderResp) ? folderOrderResp : [];
                    if (JSON.stringify(newOrder) !== JSON.stringify(ordersCache.folders)) {
                        ordersCache.folders = newOrder;
                        saveToCache('flashcard_folder_order_cache', ordersCache.folders);
                        needsRerender = true;
                    }
                }

                // Update decks if changed
                if (decksResp) {
                    const newDecks = Array.isArray(decksResp) ? decksResp : [];
                    // Compare deck names AND folders to detect moves
                    const getKey = d => `${d.name}:${d.folder || ''}`;
                    const oldKeys = cachedDeckList.map(getKey).sort().join(',');
                    const newKeys = newDecks.map(getKey).sort().join(',');

                    if (oldKeys !== newKeys) {
                        cachedDeckList = newDecks;
                        saveToCache(CACHE_KEY_DECKS, cachedDeckList);
                        updateDeckCounts();
                        needsRerender = true;
                    } else {
                        // Just update cache silently
                        cachedDeckList = newDecks;
                        saveToCache(CACHE_KEY_DECKS, cachedDeckList);
                    }
                }

                // Always save updated data to home cache to keep it in sync
                saveToCache(CACHE_KEY_HOME, {
                    folders: cachedFolderList,
                    decks: cachedDeckList,
                    folder_order: ordersCache.folders,
                });

                // Re-render only if structure actually changed
                if (needsRerender) {
                    renderFolderGrid();
                    renderDeckGrid();
                }
            } catch { }
        }

        // Preload PDF folders and PDFs in background so /pdf is instant
        async function preloadPdfsInBackground() {
            try {
                const [pdfsResp, pdfFoldersResp] = await Promise.all([
                    fetch('/pdfs').then(r => r.json()).catch(() => null),
                    fetch('/pdf/folders').then(r => r.json()).catch(() => null)
                ]);
                if (pdfsResp) {
                    localStorage.setItem('pdfs_cache', JSON.stringify(pdfsResp));
                }
                if (pdfFoldersResp && pdfFoldersResp.folders) {
                    localStorage.setItem('pdf_folders_cache', JSON.stringify({ folders: pdfFoldersResp.folders }));
                }
            } catch (e) { }
        }


        function populateDeckSelect() {
            deckSelect.innerHTML = '';
            const placeholder = document.createElement('option');
            placeholder.value = '';
            placeholder.textContent = 'Select deck';
            placeholder.disabled = true;
            placeholder.selected = true;
            deckSelect.appendChild(placeholder);
            cachedDeckList.forEach(d => {
                const opt = document.createElement('option');
                opt.value = d.name;
                opt.textContent = d.name;
                deckSelect.appendChild(opt);
            });
            card.innerText = 'Welcome!\n Please select a deck.';
            if (deckSectionHead) deckSectionHead.style.display = 'none';
            if (deckSectionTitle) deckSectionTitle.textContent = 'Decks';
            if (deckGrid) deckGrid.style.display = 'none';
            if (folderGrid) folderGrid.style.display = 'grid';
        }

        async function loadDecks() {
            showLoader();

            // Fetch fresh data in background
            try {
                const resp = await fetch('/decks');
                const list = await resp.json();
                cachedDeckList = Array.isArray(list) ? list : [];
                saveToCache(CACHE_KEY_DECKS, cachedDeckList);
                updateDeckCounts();
                renderFolderGrid();
                renderDeckGrid();
            } catch (err) {
                console.error('loadDecks error:', err);
            }

            hideLoader();

            deckSelect.innerHTML = '';
            const placeholder = document.createElement('option');
            placeholder.value = '';
            placeholder.textContent = 'Select deck';
            placeholder.disabled = true;
            placeholder.selected = true;
            deckSelect.appendChild(placeholder);
            cachedDeckList.forEach(d => {
                const opt = document.createElement('option');
                opt.value = d.name;
                opt.textContent = d.name;
                deckSelect.appendChild(opt);
            });
            card.innerText = 'Welcome!\n Please select a deck.';
            if (deckSectionHead) deckSectionHead.style.display = 'none';
            if (deckSectionTitle) deckSectionTitle.textContent = 'Decks';
            if (deckGrid) deckGrid.style.display = 'none';
            if (folderGrid) folderGrid.style.display = 'grid';

            // Handle URL parameters
            handleUrlParams();
        }

        function updateDeckCounts() {
            try {
                // Update deck counts without losing parent info
                const counts = {};
                cachedDeckList.forEach(d => {
                    const f = d.folder || 'Uncategorized';
                    counts[f] = (counts[f] || 0) + 1;
                });
                // Merge counts into existing folder data (preserve parent info)
                cachedFolderList.forEach(folder => {
                    folder.count = counts[folder.name] || 0;
                });
                // Add any folders that only exist in decks but not in folder list
                Object.keys(counts).forEach(name => {
                    if (!cachedFolderList.find(f => f.name === name)) {
                        cachedFolderList.push({
                            name,
                            count: counts[name]
                        });
                    }
                });
            } catch { }
        }

        // Handle URL parameters for direct deck access
        function handleUrlParams() {
            try {
                const params = new URLSearchParams(location.search);
                const qsDeck = params.get('deck') || '';
                const qsMode = params.get('mode') || '';
                if (qsDeck && cachedDeckList.find(d => d.name === qsDeck)) {
                    if (qsMode === 'flash') {
                        deckSelect.value = qsDeck;
                        loadWordsForDeck(qsDeck);
                        if (flashApp) flashApp.style.display = 'block';
                        if (homeDeckList) homeDeckList.style.display = 'none';
                        if (createFolderBtn) createFolderBtn.style.display = 'none';
                        isOnFlashcardScreen = true;
                        updateFlashMultiBtn();
                    } else if (qsMode === 'learn') {
                        window.location.href = `/learn?deck=${encodeURIComponent(qsDeck)}`;
                    }
                }
            } catch { }
        }

        async function loadFolders() {
            try {
                const resp = await fetch('/folders');
                const data = await resp.json().catch(() => ({
                    folders: []
                }));
                cachedFolderList = Array.isArray(data.folders) ? data.folders : [];
                saveToCache(CACHE_KEY_FOLDERS, cachedFolderList);
                await refreshFolderOrder();
                renderFolderGrid();
            } catch { }
        }

        let isBulkSelectMode = false;
        let bulkSelectedDecks = new Set();
        
        function updateBulkActionBar() {
            const bar = document.getElementById('bulkActionBar');
            const info = document.getElementById('bulkActionCount');
            const btn = document.getElementById('bulkSelectModeBtn');
            if (isBulkSelectMode) {
                bar.style.display = 'flex';
                info.textContent = `${bulkSelectedDecks.size} selected`;
                if(btn) btn.textContent = 'Done';
            } else {
                bar.style.display = 'none';
                if(btn) btn.textContent = 'Select';
                bulkSelectedDecks.clear();
            }
        }
        
        document.addEventListener('DOMContentLoaded', () => {
            const bulkSelectBtn = document.getElementById('bulkSelectModeBtn');
            const bulkCancelBtn = document.getElementById('bulkActionCancelBtn');
            const bulkMoveBtn = document.getElementById('bulkActionMoveBtn');
            
            if (bulkSelectBtn) {
                bulkSelectBtn.addEventListener('click', () => {
                    isBulkSelectMode = !isBulkSelectMode;
                    updateBulkActionBar();
                    renderDeckGrid(true); // Re-render to clear visual selected states if canceled
                });
            }
            if (bulkCancelBtn) {
                bulkCancelBtn.addEventListener('click', () => {
                    isBulkSelectMode = false;
                    updateBulkActionBar();
                    renderDeckGrid(true);
                });
            }
            if (bulkMoveBtn) {
                bulkMoveBtn.addEventListener('click', () => {
                    if (bulkSelectedDecks.size === 0) return;
                    // Open the move modal but let it act on bulkSelectedDecks
                    folderToManage = null; 
                    selectedDeckForAction = null;
                    isBulkMoveOperation = true;
                    openMoveDeckModal(); 
                });
            }
        });
        
        // Let's ensure isBulkMoveOperation flag exists globally
        let isBulkMoveOperation = false;

        async function renderDeckGrid(skipOrderRefresh = false) {
            if (!deckGrid) return;
            deckGrid.innerHTML = '';
            if (!Array.isArray(cachedDeckList) || !cachedDeckList.length) return;
            const base = selectedFolder ? cachedDeckList.filter(d => ((d.folder || 'Uncategorized') === selectedFolder)) : cachedDeckList;
            // Only refresh order from server if not already loaded and not skipped
            const scope = selectedFolder || 'root';
            if (!skipOrderRefresh && !ordersCache.decks[scope]) {
                await refreshDeckOrder(scope);
            }
            const ordered = applyOrder(base, 'deck', scope);
            ordered.forEach((d, i) => {
                const wrap = document.createElement('div');
                wrap.className = 'deck-tile tile-enter';
                wrap.style.animationDelay = `${i * 40}ms`;
                const title = document.createElement('div');
                title.className = 'deck-title';
                title.textContent = d.name;
                wrap.appendChild(title);
                wrap.addEventListener('click', () => {
                    if (isBulkSelectMode) {
                        if (bulkSelectedDecks.has(d.name)) {
                            bulkSelectedDecks.delete(d.name);
                            wrap.classList.remove('is-selected');
                            wrap.style.border = '';
                            wrap.style.background = '';
                        } else {
                            bulkSelectedDecks.add(d.name);
                            wrap.classList.add('is-selected');
                            wrap.style.border = '2px solid var(--accent)';
                            wrap.style.background = 'rgba(56, 189, 248, 0.05)';
                        }
                        updateBulkActionBar();
                        return;
                    }
                    selectedDeckForAction = d.name;
                    const ttl = document.getElementById('chooseActionTitle');
                    if (ttl) ttl.textContent = d.name;
                    chooseActionModal.classList.add('is-open');
                    chooseActionModal.setAttribute('aria-hidden', 'false');
                });
                attachDeckDnD(wrap, d.name, scope);
                deckGrid.appendChild(wrap);
            });
        }

        // Recursively count all decks in a folder and its sub-folders
        function getTotalDeckCount(folderName, allFolders) {
            const folder = allFolders.find(f => f.name === folderName);
            let total = folder ? (folder.count || 0) : 0;
            // Find all child folders and add their counts recursively
            const children = allFolders.filter(f => f.parent === folderName);
            children.forEach(child => {
                total += getTotalDeckCount(child.name, allFolders);
            });
            return total;
        }

        function renderFolderGrid() {
            if (!folderGrid) return;
            folderGrid.innerHTML = '';
            const base = cachedFolderList.length ? cachedFolderList.slice() : [{
                name: 'Uncategorized',
                count: 0
            }];
            // Only show root folders (no parent) on home screen
            const rootFolders = base.filter(f => !f.parent);
            const ordered = applyOrder(rootFolders, 'folder');
            ordered.forEach((f, i) => {
                const wrap = document.createElement('div');
                wrap.className = 'deck-tile tile-enter';
                wrap.style.animationDelay = `${i * 40}ms`;
                const title = document.createElement('div');
                title.className = 'deck-title';
                title.textContent = f.name;
                const sub = document.createElement('div');
                sub.className = 'deck-subtitle';
                // Count sub-folders
                const subFolderCount = base.filter(x => x.parent === f.name).length;
                // Count total decks including all nested sub-folders
                const totalDecks = getTotalDeckCount(f.name, base);
                if (subFolderCount > 0) {
                    sub.textContent = `${subFolderCount} folder${subFolderCount === 1 ? '' : 's'} · ${totalDecks} deck${totalDecks === 1 ? '' : 's'}`;
                } else {
                    sub.textContent = `${totalDecks} deck${totalDecks === 1 ? '' : 's'}`;
                }
                wrap.appendChild(title);
                wrap.appendChild(sub);
                wrap.addEventListener('click', () => {
                    window.location.href = `/folder?name=${encodeURIComponent(f.name)}`;
                });
                if (f.name !== 'Uncategorized') {
                    const kebab = document.createElement('button');
                    kebab.className = 'kebab-btn';
                    kebab.type = 'button';
                    kebab.textContent = '⋮';
                    const menu = document.createElement('div');
                    menu.className = 'kebab-menu';
                    const mRename = document.createElement('button');
                    mRename.className = 'kebab-item';
                    mRename.type = 'button';
                    mRename.textContent = 'Rename';
                    mRename.addEventListener('click', (e) => {
                        e.stopPropagation();
                        closeKebabMenus();
                        openRenameFolderModal(f.name);
                    });
                    const mMove = document.createElement('button');
                    mMove.className = 'kebab-item';
                    mMove.type = 'button';
                    mMove.textContent = 'Move';
                    mMove.addEventListener('click', (e) => {
                        e.stopPropagation();
                        closeKebabMenus();
                        folderToManage = f.name;
                        openMoveFolderModal();
                    });
                    const mDelete = document.createElement('button');
                    mDelete.className = 'kebab-item';
                    mDelete.type = 'button';
                    mDelete.textContent = 'Delete';
                    mDelete.addEventListener('click', async (e) => {
                        e.stopPropagation();
                        closeKebabMenus();
                        await deleteFolder(f.name);
                    });
                    menu.appendChild(mRename);
                    menu.appendChild(mMove);
                    menu.appendChild(mDelete);
                    kebab.addEventListener('click', (e) => {
                        e.stopPropagation();
                        toggleMenu(menu);
                    });
                    wrap.appendChild(kebab);
                    wrap.appendChild(menu);
                }
                attachFolderDnD(wrap, f.name);
                folderGrid.appendChild(wrap);
            });
            const createTile = document.createElement('div');
            createTile.className = 'deck-tile tile-enter create-tile';
            createTile.style.animationDelay = `${ordered.length * 40}ms`;
            const plus = document.createElement('div');
            plus.className = 'create-tile__icon';
            plus.textContent = '+';
            createTile.appendChild(plus);
            createTile.addEventListener('click', openCreateFolderModal);
            folderGrid.appendChild(createTile);
        }

        function sanitizeName(name) {
            return (name || '').trim().replace(/[^a-zA-Z0-9_\-]+/g, '_').substring(0, 50);
        }

        function openCreateFolderModal() {
            if (!createFolderModal) return;
            createFolderStatus.textContent = '';
            folderNameInput.value = '';
            
            const parentSelect = document.getElementById('createFolderParentSelect');
            if (parentSelect) {
                parentSelect.innerHTML = '<option value="">None (Top Level)</option>';
                if (window.cachedFolderList) {
                    window.cachedFolderList.forEach(f => {
                        const opt = document.createElement('option');
                        opt.value = f.name;
                        opt.textContent = f.name;
                        parentSelect.appendChild(opt);
                    });
                }
                if (window.selectedFolder) {
                    parentSelect.value = window.selectedFolder;
                }
            }
            
            createFolderModal.classList.add('is-open');
            createFolderModal.setAttribute('aria-hidden', 'false');
        }

        function closeCreateFolderModal() {
            if (!createFolderModal) return;
            createFolderModal.classList.remove('is-open');
            createFolderModal.setAttribute('aria-hidden', 'true');
        }
        if (createFolderBtn) createFolderBtn.addEventListener('click', openCreateFolderModal);
        if (createFolderCancelBtn) createFolderCancelBtn.addEventListener('click', closeCreateFolderModal);
        if (createFolderModal) createFolderModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeCreateFolder === 'true') closeCreateFolderModal();
        });
        if (createFolderSaveBtn) createFolderSaveBtn.addEventListener('click', async () => {
            const raw = folderNameInput.value;
            const name = sanitizeName(raw);
            const parentSelect = document.getElementById('createFolderParentSelect');
            const parent = parentSelect ? parentSelect.value : null;

            if (!name) {
                createFolderStatus.textContent = 'Please enter a folder name.';
                return;
            }
            createFolderSaveBtn.disabled = true;
            createFolderSaveBtn.textContent = 'Creating...';
            try {
                const resp = await fetch('/folder/create', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        name,
                        parent
                    })
                });
                const out = await resp.json().catch(() => ({
                    ok: false
                }));
                if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to create folder');
                createFolderStatus.textContent = 'Created';
                clearCache(); // Invalidate cache to fetch fresh data
                await loadFolders();
                closeCreateFolderModal();
                createFolderSaveBtn.disabled = false;
                createFolderSaveBtn.textContent = 'Create';
            } catch (err) {
                createFolderStatus.textContent = String(err.message || err);
                createFolderSaveBtn.disabled = false;
                createFolderSaveBtn.textContent = 'Create';
            }
        });


        function openRenameFolderModal(name) {
            folderToManage = name;
            renameFolderStatus.textContent = '';
            renameFolderInput.value = name;
            renameFolderModal.classList.add('is-open');
            renameFolderModal.setAttribute('aria-hidden', 'false');
        }

        function closeRenameFolderModal() {
            renameFolderModal.classList.remove('is-open');
            renameFolderModal.setAttribute('aria-hidden', 'true');
        }
        if (renameFolderCancelBtn) renameFolderCancelBtn.addEventListener('click', closeRenameFolderModal);
        if (renameFolderModal) renameFolderModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeRenameFolder === 'true') closeRenameFolderModal();
        });
        async function deleteFolder(name) {
            try {
                const resp = await fetch('/folder/delete', {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        name
                    })
                });
                const out = await resp.json().catch(() => ({
                    ok: false
                }));
                if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to delete folder');
                if (selectedFolder === name) selectedFolder = '';
                await loadDecks();
                await loadFolders();
                closeRenameFolderModal();
            } catch (err) {
                renameFolderStatus.textContent = String(err.message || err);
            }
        }
        if (renameFolderSaveBtn) renameFolderSaveBtn.addEventListener('click', async () => {
            const newName = sanitizeName(renameFolderInput.value);
            if (!folderToManage) return;
            if (!newName) {
                renameFolderStatus.textContent = 'Please enter a name.';
                return;
            }
            try {
                const resp = await fetch('/folder/rename', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        old_name: folderToManage,
                        new_name: newName
                    })
                });
                const out = await resp.json().catch(() => ({
                    ok: false
                }));
                if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to rename');
                if (selectedFolder === folderToManage) selectedFolder = newName;
                await loadDecks();
                await loadFolders();
                closeRenameFolderModal();
            } catch (err) {
                renameFolderStatus.textContent = String(err.message || err);
            }
        });

        // Move Folder functionality
        function openMoveFolderModal() {
            if (!folderToManage) return;
            moveFolderStatus.textContent = '';
            closeRenameFolderModal();
            loadFoldersIntoMoveFolderList();
            moveFolderModal.classList.add('is-open');
            moveFolderModal.setAttribute('aria-hidden', 'false');
        }

        function closeMoveFolderModal() {
            moveFolderModal.classList.remove('is-open');
            moveFolderModal.setAttribute('aria-hidden', 'true');
        }
        async function loadFoldersIntoMoveFolderList() {
            moveFolderList.innerHTML = '';
            try {
                const resp = await fetch('/folders');
                const data = await resp.json().catch(() => ({
                    folders: []
                }));
                const raw = Array.isArray(data.folders) ? data.folders : [];

                // Find current parent of the folder being moved
                const currentFolder = raw.find(f => (f.name || f) === folderToManage);
                const currentParent = currentFolder ? currentFolder.parent : null;

                // Filter out the folder being moved and any of its descendants
                function getDescendants(name) {
                    const result = new Set([name]);
                    let added = true;
                    while (added) {
                        added = false;
                        raw.forEach(f => {
                            const fname = f.name || f;
                            const fparent = f.parent;
                            if (fparent && result.has(fparent) && !result.has(fname)) {
                                result.add(fname);
                                added = true;
                            }
                        });
                    }
                    return result;
                }
                const excluded = getDescendants(folderToManage);

                // Add "Root (No parent)" option first
                const rootRow = document.createElement('div');
                rootRow.className = 'multi-deck-item';
                const rootLabel = document.createElement('label');
                rootLabel.style.display = 'flex';
                rootLabel.style.alignItems = 'center';
                rootLabel.style.gap = '12px';
                const rootRadio = document.createElement('input');
                rootRadio.type = 'radio';
                rootRadio.name = 'moveFolderTarget';
                rootRadio.value = '';
                rootRadio.className = 'multi-deck-checkbox';
                if (!currentParent) rootRadio.checked = true;
                const rootSpan = document.createElement('span');
                rootSpan.textContent = '(Root - No parent)';
                rootSpan.style.fontStyle = 'italic';
                rootLabel.appendChild(rootRadio);
                rootLabel.appendChild(rootSpan);
                rootRow.appendChild(rootLabel);
                moveFolderList.appendChild(rootRow);

                // Add other folders
                raw.forEach(f => {
                    const fname = f.name || f;
                    if (excluded.has(fname)) return;
                    if (fname === 'Uncategorized') return;

                    const row = document.createElement('div');
                    row.className = 'multi-deck-item';
                    const label = document.createElement('label');
                    label.style.display = 'flex';
                    label.style.alignItems = 'center';
                    label.style.gap = '12px';
                    const radio = document.createElement('input');
                    radio.type = 'radio';
                    radio.name = 'moveFolderTarget';
                    radio.value = fname;
                    radio.className = 'multi-deck-checkbox';
                    if (currentParent === fname) radio.checked = true;
                    const span = document.createElement('span');
                    span.textContent = fname;
                    label.appendChild(radio);
                    label.appendChild(span);
                    row.appendChild(label);
                    moveFolderList.appendChild(row);
                });
            } catch { }
        }
        if (moveFolderBtn) moveFolderBtn.addEventListener('click', openMoveFolderModal);
        if (moveFolderCancelBtn) moveFolderCancelBtn.addEventListener('click', closeMoveFolderModal);
        if (moveFolderModal) moveFolderModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeMoveFolder === 'true') closeMoveFolderModal();
        });
        if (moveFolderSaveBtn) moveFolderSaveBtn.addEventListener('click', async () => {
            if (!folderToManage) return;
            const picked = document.querySelector('input[name="moveFolderTarget"]:checked');
            const targetParent = picked ? picked.value : '';
            moveFolderSaveBtn.disabled = true;
            moveFolderSaveBtn.textContent = 'Moving...';
            try {
                const resp = await fetch('/folder/move', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        name: folderToManage,
                        parent: targetParent || null
                    })
                });
                const out = await resp.json().catch(() => ({
                    ok: false
                }));
                if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to move');
                moveFolderStatus.textContent = 'Moved!';
                await loadFolders();
                closeMoveFolderModal();
            } catch (err) {
                moveFolderStatus.textContent = String(err.message || err);
            } finally {
                moveFolderSaveBtn.disabled = false;
                moveFolderSaveBtn.textContent = 'Move';
            }
        });

        if (homeBtn) homeBtn.addEventListener('click', () => {
            window.location.href = '/';
        });

        if (openLearnBtn) openLearnBtn.addEventListener('click', () => {
            if (!selectedDeckForAction) return;
            const fromParam = selectedFolder ? `&from=${encodeURIComponent(selectedFolder)}` : '';
            window.location.href = `/learn?deck=${encodeURIComponent(selectedDeckForAction)}${fromParam}`;
        });
        if (openSpellingBtn) openSpellingBtn.addEventListener('click', () => {
            if (!selectedDeckForAction) return;
            const fromParam = selectedFolder ? `&from=${encodeURIComponent(selectedFolder)}` : '';
            window.location.href = `/spelling?deck=${encodeURIComponent(selectedDeckForAction)}${fromParam}`;
        });
        if (openLineBtn) openLineBtn.addEventListener('click', () => {
            if (!selectedDeckForAction) return;
            const fromParam = selectedFolder ? `&from=${encodeURIComponent(selectedFolder)}` : '';
            window.location.href = `/line?deck=${encodeURIComponent(selectedDeckForAction)}${fromParam}`;
        });
        if (openMatchBtn) openMatchBtn.addEventListener('click', () => {
            if (!selectedDeckForAction) return;
            const fromParam = selectedFolder ? `&from=${encodeURIComponent(selectedFolder)}` : '';
            window.location.href = `/match?deck=${encodeURIComponent(selectedDeckForAction)}${fromParam}`;
        });
        if (openFlashBtn) openFlashBtn.addEventListener('click', () => {
            if (!selectedDeckForAction) return;
            deckSelect.value = selectedDeckForAction;
            loadWordsForDeck(selectedDeckForAction);
            chooseActionModal.classList.remove('is-open');
            chooseActionModal.setAttribute('aria-hidden', 'true');
            if (flashApp) flashApp.style.display = 'block';
            if (homeDeckList) homeDeckList.style.display = 'none';
            if (createFolderBtn) createFolderBtn.style.display = 'none';
            isOnFlashcardScreen = true;
            updateFlashMultiBtn();
        });
        if (chooseActionCloseBtn) chooseActionCloseBtn.addEventListener('click', () => {
            chooseActionModal.classList.remove('is-open');
            chooseActionModal.setAttribute('aria-hidden', 'true');
        });
        if (chooseActionModal) chooseActionModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeAction === 'true') {
                chooseActionModal.classList.remove('is-open');
                chooseActionModal.setAttribute('aria-hidden', 'true');
            }
        });

        function buildMoveDeckBrowserTree() {
            moveDeckParentByName = {};
            moveDeckChildrenByParent = {};
            const raw = Array.isArray(cachedFolderList) ? cachedFolderList.slice() : [];
            const seen = new Set();
            raw.forEach((f) => {
                const nm = (f && f.name) ? f.name : String(f);
                if (nm === 'Uncategorized') return;
                if (seen.has(nm)) return;
                seen.add(nm);
                const parent = f && f.parent ? f.parent : '';
                moveDeckParentByName[nm] = parent || '';
                const key = parent || MOVE_BROWSER_ROOT;
                if (!moveDeckChildrenByParent[key]) moveDeckChildrenByParent[key] = [];
                moveDeckChildrenByParent[key].push(nm);
            });
            if (!moveDeckChildrenByParent[MOVE_BROWSER_ROOT]) moveDeckChildrenByParent[MOVE_BROWSER_ROOT] = [];
            if (!moveDeckChildrenByParent[MOVE_BROWSER_ROOT].includes('Uncategorized')) {
                moveDeckChildrenByParent[MOVE_BROWSER_ROOT].push('Uncategorized');
                moveDeckParentByName['Uncategorized'] = '';
            }
        }

        function getMoveDeckPath() {
            const path = [];
            let current = moveDeckBrowserCursor;
            const visited = new Set();
            while (current && current !== MOVE_BROWSER_ROOT && !visited.has(current)) {
                visited.add(current);
                path.unshift(current);
                const parent = moveDeckParentByName[current] || '';
                if (!parent) break;
                current = parent;
            }
            return path;
        }

        function renderMoveDeckBrowser() {
            if (!moveDeckList) return;
            moveDeckList.innerHTML = '';
            const path = getMoveDeckPath();
            const headerRow = document.createElement('div');
            headerRow.className = 'multi-deck-item';
            const headerSpan = document.createElement('span');
            headerSpan.style.fontWeight = '600';
            headerSpan.textContent = path.length ? `Location: Root / ${path.join(' / ')}` : 'Location: Root';
            headerRow.appendChild(headerSpan);
            moveDeckList.appendChild(headerRow);
            if (moveDeckBrowserCursor !== MOVE_BROWSER_ROOT) {
                const upBtn = document.createElement('button');
                upBtn.type = 'button';
                upBtn.className = 'multi-deck-item';
                upBtn.textContent = 'Up one level';
                upBtn.addEventListener('click', () => {
                    const current = moveDeckBrowserCursor;
                    const parent = moveDeckParentByName[current] || '';
                    moveDeckBrowserCursor = parent ? parent : MOVE_BROWSER_ROOT;
                    renderMoveDeckBrowser();
                });
                moveDeckList.appendChild(upBtn);
            }
            const key = moveDeckBrowserCursor === MOVE_BROWSER_ROOT ? MOVE_BROWSER_ROOT : moveDeckBrowserCursor;
            const children = (moveDeckChildrenByParent[key] || []).slice().sort((a, b) => a.localeCompare(b));
            children.forEach((name) => {
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'multi-deck-item';
                const row = document.createElement('div');
                row.style.display = 'flex';
                row.style.alignItems = 'center';
                row.style.justifyContent = 'space-between';
                const nameSpan = document.createElement('span');
                nameSpan.textContent = name;
                row.appendChild(nameSpan);
                if (name !== 'Uncategorized') {
                    const icon = document.createElement('span');
                    icon.className = 'material-symbols-outlined';
                    icon.textContent = 'chevron_right';
                    row.appendChild(icon);
                }
                btn.appendChild(row);
                btn.addEventListener('click', () => {
                    moveDeckBrowserCursor = name;
                    renderMoveDeckBrowser();
                });
                moveDeckList.appendChild(btn);
            });
            if (moveDeckSaveBtn) moveDeckSaveBtn.disabled = moveDeckBrowserCursor === MOVE_BROWSER_ROOT;
        }

        function populateMoveFolderList() {
            buildMoveDeckBrowserTree();
            moveDeckBrowserCursor = MOVE_BROWSER_ROOT;
            renderMoveDeckBrowser();
        }

        function openMoveDeckModal() {
            if (!selectedDeckForAction && (!isBulkMoveOperation || bulkSelectedDecks.size === 0)) return;
            moveDeckStatus.textContent = '';
            populateMoveFolderList();
            if (deckSettingsModal) {
                deckSettingsModal.classList.remove('is-open');
                deckSettingsModal.setAttribute('aria-hidden', 'true');
            }
            moveDeckModal.classList.add('is-open');
            moveDeckModal.setAttribute('aria-hidden', 'false');
        }

        function closeMoveDeckModal() {
            moveDeckModal.classList.remove('is-open');
            moveDeckModal.setAttribute('aria-hidden', 'true');
        }
        if (moveDeckCancelBtn) moveDeckCancelBtn.addEventListener('click', closeMoveDeckModal);
        if (moveDeckModal) moveDeckModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeMove === 'true') closeMoveDeckModal();
        });
        if (moveDeckSaveBtn) moveDeckSaveBtn.addEventListener('click', async () => {
            if (!selectedDeckForAction && (!isBulkMoveOperation || bulkSelectedDecks.size === 0)) return;
            if (!moveDeckBrowserCursor || moveDeckBrowserCursor === MOVE_BROWSER_ROOT) {
                moveDeckStatus.textContent = 'Choose a folder first';
                return;
            }
            const folder = moveDeckBrowserCursor;
            moveDeckSaveBtn.disabled = true;
            moveDeckSaveBtn.textContent = 'Moving...';
            try {
                if (isBulkMoveOperation) {
                    const resp = await fetch('/decks/move-bulk', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            names: Array.from(bulkSelectedDecks),
                            folder: folder === 'Uncategorized' ? null : folder
                        })
                    });
                    const out = await resp.json().catch(() => ({ ok: false }));
                    if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to bulk move');
                    moveDeckStatus.textContent = `Moved ${out.count} items`;
                    isBulkSelectMode = false;
                    isBulkMoveOperation = false;
                    bulkSelectedDecks.clear();
                    updateBulkActionBar();
                } else {
                    const resp = await fetch('/deck/move', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            name: selectedDeckForAction,
                            folder: folder === 'Uncategorized' ? null : folder
                        })
                    });
                    const out = await resp.json().catch(() => ({ ok: false }));
                    if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to move');
                    moveDeckStatus.textContent = 'Moved';
                }
                
                await loadDecks();
                await loadFolders();
                closeMoveDeckModal();
                if (chooseActionModal) {
                    chooseActionModal.classList.remove('is-open');
                    chooseActionModal.setAttribute('aria-hidden', 'true');
                }
            } catch (err) {
                moveDeckStatus.textContent = String(err.message || err);
            } finally {
                moveDeckSaveBtn.disabled = false;
                moveDeckSaveBtn.textContent = 'Move';
            }
        });

        function openDeckSettingsModal() {
            if (!selectedDeckForAction) return;
            if (chooseActionModal) {
                chooseActionModal.classList.remove('is-open');
                chooseActionModal.setAttribute('aria-hidden', 'true');
            }
            if (deckSettingsModal) {
                const titleEl = document.getElementById('deckSettingsTitle');
                if (titleEl) titleEl.textContent = selectedDeckForAction;
                deckSettingsModal.classList.add('is-open');
                deckSettingsModal.setAttribute('aria-hidden', 'false');
            }
        }

        function closeDeckSettingsModal() {
            if (!deckSettingsModal) return;
            deckSettingsModal.classList.remove('is-open');
            deckSettingsModal.setAttribute('aria-hidden', 'true');
        }

        if (openSettingsBtn) openSettingsBtn.addEventListener('click', openDeckSettingsModal);
        if (deckSettingsModal) deckSettingsModal.addEventListener('click', (e) => {
            if (e.target && e.target.dataset && e.target.dataset.closeSettings === 'true') {
                closeDeckSettingsModal();
            }
        });
        if (deckSettingsMoveBtn) deckSettingsMoveBtn.addEventListener('click', () => {
            openMoveDeckModal();
        });
        if (deckSettingsDeleteBtn) deckSettingsDeleteBtn.addEventListener('click', async () => {
            if (!selectedDeckForAction) return;
            const ok = confirm(`Delete deck "${selectedDeckForAction}"? This cannot be undone.`);
            if (!ok) return;
            try {
                const resp = await fetch('/deck/delete', {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        name: selectedDeckForAction
                    })
                });
                const out = await resp.json().catch(() => ({
                    ok: false
                }));
                if (!resp.ok || !out.ok) throw new Error(out.detail || 'Failed to delete deck');
                closeDeckSettingsModal();
                showLoader();
                // Clear home cache so fresh data is loaded
                try { localStorage.removeItem(CACHE_KEY_HOME); } catch { }
                try {
                    const homeResp = await fetch('/home-data');
                    const homeData = homeResp.ok ? await homeResp.json().catch(() => ({})) : {};
                    cachedFolderList = Array.isArray(homeData.folders) ? homeData.folders : [];
                    ordersCache.folders = Array.isArray(homeData.folder_order) ? homeData.folder_order : [];
                    cachedDeckList = Array.isArray(homeData.decks) ? homeData.decks : [];
                    // Cache the fresh data
                    saveToCache(CACHE_KEY_HOME, {
                        folders: cachedFolderList,
                        decks: cachedDeckList,
                        folder_order: ordersCache.folders,
                    });
                    updateDeckCounts();
                    renderFolderGrid();
                    renderDeckGrid();
                    populateDeckSelect();
                } finally {
                    hideLoader();
                }
            } catch (e) {
                alert(String(e && e.message ? e.message : e || 'Failed to delete deck'));
            }
        });

















        if (pdfBtn) pdfBtn.addEventListener('click', () => {
            window.location.href = '/pdf';
        });

        // Video mode: navigate to video page
        const videoBtn = document.getElementById('videoBtn');
        if (videoBtn) videoBtn.addEventListener('click', () => {
            showLoader();
            window.location.href = '/video';
        });

        // Story mode: navigate to story page
        const storyBtn = document.getElementById('storyBtn');
        if (storyBtn) storyBtn.addEventListener('click', () => {
            showLoader();
            window.location.href = '/story';
        });



        function showCardData(cardData) {
            if (!cardData) return;
            currentCard = cardData;
            card.innerText = currentCard[frontLang];
            showingFront = true;

            // Update pending word count badge
            const remaining = words.length + 1;
            const badge = document.createElement('span');
            badge.textContent = remaining;
            badge.style.cssText = 'text-align:center;font-size:8px;font-weight:600;color:var(--muted,#64748b);pointer-events:none;line-height:1.4;letter-spacing:0.3px;margin-top:12px;';
            badge.className = 'card-count-badge';
            // Remove any previous badge
            const oldBadge = document.querySelector('.card-count-badge');
            if (oldBadge) oldBadge.remove();
            // Append after all elements in the app container
            const appContainer = card.closest('.app') || card.parentElement;
            appContainer.appendChild(badge);

            // Show line hint if available
            updateLineHint();

            // Windowed line audio preloading: trigger next batch every 4 cards
            lineAudioCardsSeen++;
            if (lineAudioCardsSeen === 1 || lineAudioCardsSeen % LINE_AUDIO_TRIGGER_AT === 0) {
                preloadNextLineAudios();
                preloadNextWordAudios();
            }

            // Auto-play audio when showing front (German) side
            if (frontLang === 'de') {
                // Small delay to ensure card text is rendered
                setTimeout(() => {
                    autoPlayAudio();
                }, 100);
            }
        }

        function updateLineHint() {
            if (!lineHint || !lineHintText || !currentCard) {
                if (lineHint) lineHint.style.display = 'none';
                return;
            }

            const lineData = lineDataCache.get(currentCard.de);
            if (lineData && lineData.line_de) {
                const isRevealed = revealedLineWords.has(currentCard.de);
                const isEnglishFront = frontLang === 'en';

                if (showingFront) {
                    // Front side (Question)
                    if (isEnglishFront) {
                        // English Front: show English line (blurred unless revealed)
                        lineHintText.textContent = lineData.line_en || lineData.line_de;
                    } else {
                        // German Front: show German line (blurred unless revealed)
                        lineHintText.textContent = lineData.line_de;
                    }

                    if (isRevealed) {
                        lineHintText.classList.remove('is-blurred');
                    } else {
                        lineHintText.classList.add('is-blurred');
                    }
                } else {
                    // Back side (Answer) - always unblurred
                    if (isEnglishFront) {
                        // English Front -> Back is German
                        lineHintText.textContent = lineData.line_de;
                    } else {
                        // German Front -> Back is English
                        lineHintText.textContent = lineData.line_en || lineData.line_de;
                    }
                    lineHintText.classList.remove('is-blurred');
                }
                lineHint.style.display = 'flex';
            } else {
                lineHint.style.display = 'none';
            }
        }

        // Click on line hint - unblur if blurred, otherwise flip card
        if (lineHint) {
            lineHint.addEventListener('click', (e) => {
                // Don't do anything if clicking audio button
                if (e.target === lineHintAudio || e.target.closest('.line-hint-audio')) {
                    return;
                }

                // If line is blurred (front side and not revealed), unblur it instead of flipping
                if (currentCard && showingFront && !revealedLineWords.has(currentCard.de)) {
                    revealedLineWords.add(currentCard.de);
                    updateLineHint();
                    return;
                }

                // Otherwise flip the card
                flipCard();
            });
        }

        // Audio button to play line audio
        if (lineHintAudio) {
            lineHintAudio.addEventListener('click', async (e) => {
                e.stopPropagation();
                if (!currentCard) return;

                const lineData = lineDataCache.get(currentCard.de);
                let textToSpeak = null;
                const isEnglishFront = frontLang === 'en';

                if (showingFront) {
                    if (isEnglishFront) {
                        textToSpeak = (lineData && lineData.line_en) || (lineData && lineData.line_de);
                    } else {
                        textToSpeak = lineData && lineData.line_de;
                    }
                } else {
                    if (isEnglishFront) {
                        textToSpeak = lineData && lineData.line_de;
                    } else {
                        textToSpeak = (lineData && lineData.line_en) || (lineData && lineData.line_de);
                    }
                }

                let lang = 'de';
                if (showingFront) {
                    lang = isEnglishFront ? 'en' : 'de';
                } else {
                    lang = isEnglishFront ? 'de' : 'en';
                }

                if (textToSpeak) {
                    try {
                        // Use preloaded blob cache first (instant playback)
                        const cachedUrl = lineAudioCache.get(textToSpeak);
                        if (cachedUrl) {
                            const audio = new Audio(cachedUrl);
                            await audio.play();
                            return;
                        }
                        // Fall back to /tts endpoint
                        const resp = await fetch(`/tts?text=${encodeURIComponent(textToSpeak)}&lang=${lang}`);
                        if (resp.ok) {
                            const blob = await resp.blob();
                            const url = URL.createObjectURL(blob);
                            lineAudioCache.set(textToSpeak, url);
                            const audio = new Audio(url);
                            await audio.play();
                        }
                    } catch { }
                }
            });
        }

        // Load line data for a deck in background
        async function loadLineDataForDeck(deckName) {
            if (!deckName) return;
            try {
                const resp = await fetch(`/lines/generate?deck=${encodeURIComponent(deckName)}`);
                if (resp.ok) {
                    const data = await resp.json();
                    const items = data.items || data || [];
                    items.forEach(item => {
                        if (item.de && item.line_de) {
                            lineDataCache.set(item.de, {
                                line_de: item.line_de,
                                line_en: item.line_en || ''
                            });
                        }
                    });
                    // Update current card's line hint if it now has data
                    updateLineHint();
                    // Preload line audio in background after line data is ready
                    preloadLineAudioForDeck(deckName);
                }
            } catch (err) {
                console.warn('Failed to load line data:', err);
            }
        }

        // Preload line audio URLs from R2 (lightweight, only stores URL strings)
        async function preloadLineAudioForDeck(deckName) {
            try {
                const resp = await fetch(`/preload_lines_audio?deck=${encodeURIComponent(deckName)}`);
                if (!resp.ok) return;
                const data = await resp.json();
                const map = (data && data.audio_urls) ? data.audio_urls : {};
                Object.entries(map).forEach(([text, url]) => lineAudioURLMap.set(text, url));
                // Trigger initial batch of blob downloads
                preloadNextLineAudios();
            } catch { }
        }

        // Download blobs for only the next N upcoming cards' line audio
        async function preloadNextLineAudios() {
            // Collect line_de texts for upcoming cards that need audio
            const textsToLoad = [];
            // Include current card
            if (currentCard) {
                const ld = lineDataCache.get(currentCard.de);
                if (ld && ld.line_de && !lineAudioCache.has(ld.line_de)) {
                    textsToLoad.push(ld.line_de);
                }
            }
            // Include upcoming cards from words array
            for (let i = 0; i < words.length && textsToLoad.length < LINE_AUDIO_BATCH_SIZE; i++) {
                const w = words[i];
                if (!w || !w.de) continue;
                const ld = lineDataCache.get(w.de);
                if (ld && ld.line_de && !lineAudioCache.has(ld.line_de) && !textsToLoad.includes(ld.line_de)) {
                    textsToLoad.push(ld.line_de);
                }
            }
            if (!textsToLoad.length) return;
            const tasks = textsToLoad.map(text => (async () => {
                try {
                    if (lineAudioCache.has(text)) return;
                    const url = lineAudioURLMap.get(text);
                    if (!url) return;
                    const r = await fetch(url);
                    if (!r.ok) return;
                    const blob = await r.blob();
                    lineAudioCache.set(text, URL.createObjectURL(blob));
                } catch { }
            })());
            await Promise.allSettled(tasks);
        }

        function getNewCard() {
            if (currentCard) {
                cardHistory.push(currentCard);
            }
            if (words.length === 0) {
                card.innerText = 'You have learned all the words!';
                return;
            }
            const next = words.shift();
            showCardData(next);
        }

        let wordListExpanded = false;
        const WORD_LIST_LIMIT = 5;

        function renderWordList() {
            if (!wordListGrid) return;

            const wrongItems = practicedWords.filter(item => item && item.isWrong);

            if (practicedCountEl) {
                practicedCountEl.textContent = wrongItems.length;
            }

            if (!wrongItems.length) {
                wordListGrid.innerHTML = '<div class="word-list-empty">Wrong words will appear here</div>';
                return;
            }

            const visibleItems = wordListExpanded ? wrongItems : wrongItems.slice(0, WORD_LIST_LIMIT);

            const table = document.createElement('table');
            table.className = 'word-list-table';
            const tbody = document.createElement('tbody');
            table.appendChild(tbody);

            const isEnglishFront = frontLang === 'en';

            visibleItems.forEach((item, idx) => {
                const tr = document.createElement('tr');
                tr.dataset.text = item.word.de || '';
                tr.className = item.isWrong ? 'is-wrong' : 'is-right';

                const tdDe = document.createElement('td');
                tdDe.textContent = item.word.de || '';

                const tdEn = document.createElement('td');
                tdEn.textContent = item.word.en || '';

                if (isEnglishFront) {
                    // English visible (left), German blurred (right)
                    tr.appendChild(tdEn);
                    tr.appendChild(tdDe);
                    tdDe.className = 'word-list-en-cell word-list-en-blurred';

                    // Clicking German (hidden) only reveals it (no audio)
                    tdDe.addEventListener('click', (e) => {
                        e.stopPropagation();
                        tdDe.classList.remove('word-list-en-blurred');
                        const existing = wordRevealTimers.get(tdDe);
                        if (existing) {
                            clearTimeout(existing);
                        }
                        const timerId = setTimeout(() => {
                            tdDe.classList.add('word-list-en-blurred');
                            wordRevealTimers.delete(tdDe);
                        }, 5000);
                        wordRevealTimers.set(tdDe, timerId);
                    });
                } else {
                    // German visible (left), English blurred (right)
                    tr.appendChild(tdDe);
                    tr.appendChild(tdEn);
                    tdEn.className = 'word-list-en-cell word-list-en-blurred';

                    tdDe.addEventListener('click', () => {
                        if (item.word.de) {
                            speakText(item.word.de);
                        }
                    });

                    tdEn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        tdEn.classList.remove('word-list-en-blurred');
                        const existing = wordRevealTimers.get(tdEn);
                        if (existing) {
                            clearTimeout(existing);
                        }
                        const timerId = setTimeout(() => {
                            tdEn.classList.add('word-list-en-blurred');
                            wordRevealTimers.delete(tdEn);
                        }, 5000);
                        wordRevealTimers.set(tdEn, timerId);
                    });
                }

                tbody.appendChild(tr);
            });

            wordListGrid.innerHTML = '';
            wordListGrid.appendChild(table);

            // Show "View More" / "Show Less" button if there are more than 5 items
            if (wrongItems.length > WORD_LIST_LIMIT) {
                const toggleBtn = document.createElement('button');
                toggleBtn.type = 'button';
                toggleBtn.textContent = wordListExpanded ? `Show Less` : `View More (${wrongItems.length - WORD_LIST_LIMIT} more)`;
                toggleBtn.style.cssText = 'width:100%;padding:10px;background:var(--card-bg);border:none;border-top:1px solid var(--card-border);color:var(--text);opacity:0.55;font-size:13px;font-weight:600;cursor:pointer;height:auto;box-shadow:none;position:sticky;bottom:0;z-index:1;';
                toggleBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    wordListExpanded = !wordListExpanded;
                    renderWordList();
                });
                wordListGrid.appendChild(toggleBtn);
            }
        }

        function addPracticedWord(word, isWrong) {
            if (!word) return;
            // Add to front of array (newest first)
            practicedWords.unshift({
                word,
                isWrong
            });
            renderWordList();
        }

        function clearPracticedWords() {
            practicedWords = [];
            renderWordList();
        }

        // Helper function to speak text
        async function speakText(text) {
            if (!text) return;

            const cached = audioCache.get(text);
            if (cached) {
                try {
                    const audio = new Audio(cached);
                    await audio.play();
                    return;
                } catch { }
            }

            try {
                const resp = await fetch(`/tts?text=${encodeURIComponent(text)}&lang=de`);
                if (resp.ok) {
                    const blob = await resp.blob();
                    const url = URL.createObjectURL(blob);
                    const audio = new Audio(url);
                    await audio.play();
                }
            } catch { }
        }

        let recentlyTouched = false;

        function flipCard() {
            if (currentCard[backLang]) {
                card.innerText = showingFront ? currentCard[backLang] : currentCard[frontLang];
                showingFront = !showingFront;

                // Auto-reveal line when flipping (user saw the meaning)
                if (currentCard) {
                    revealedLineWords.add(currentCard.de);
                }

                // Update line hint for back/front
                updateLineHint();
            }
        }
        // Touch-first: flip immediately and suppress synthetic click
        card.addEventListener('touchstart', (e) => {
            recentlyTouched = true;
            // prevent synthetic click and any default touch feedback
            e.preventDefault();

            // If no deck selected, open multi-deck modal
            if (!originalWords || originalWords.length === 0) {
                openMultiDeckModal();
                setTimeout(() => {
                    recentlyTouched = false;
                }, 400);
                return;
            }

            card.classList.add('is-pressed');
            card.classList.add('tap-pop');
            flipCard();
            // reset the flag shortly after touch sequence completes
            setTimeout(() => {
                recentlyTouched = false;
            }, 400);
        }, {
            passive: false
        });
        card.addEventListener('touchend', () => {
            card.classList.remove('is-pressed');
            card.classList.remove('tap-pop');
        });
        card.addEventListener('touchcancel', () => {
            card.classList.remove('is-pressed');
            card.classList.remove('tap-pop');
        });
        // Click for mouse/non-touch; ignore if a touch just happened
        card.addEventListener('click', (e) => {
            if (recentlyTouched) return;

            // If no deck selected, open multi-deck modal
            if (!originalWords || originalWords.length === 0) {
                openMultiDeckModal();
                return;
            }

            flipCard();
        });

        wrongBtn.addEventListener('click', () => {
            if (currentCard) {
                // Add to practiced words list (wrong)
                addPracticedWord(currentCard, true);

                // In random mode, don't repeat words - just track for adding at end
                if (isRandomTest) {
                    randomTestWrongWords.push(currentCard);
                    // Mark as practiced so it won't be added again
                    randomTestPracticedKeys.add(currentCard.de + '|' + currentCard.en);
                } else {
                    // In normal mode, add back to queue
                    words.push(currentCard);
                }
            }
            if (isRandomTest) {
                randomTestCompletedCount++;
                checkAndAddMoreRandomWords();
            }
            getNewCard();
        });

        rightBtn.addEventListener('click', () => {
            if (currentCard) {
                // Add to practiced words list (right)
                addPracticedWord(currentCard, false);
                // Mark as practiced in random mode
                if (isRandomTest) {
                    randomTestPracticedKeys.add(currentCard.de + '|' + currentCard.en);
                }
            }
            if (isRandomTest) {
                randomTestCompletedCount++;
                checkAndAddMoreRandomWords();
            }
            getNewCard();
        });

        // Check if we need to add more words to random test (when running low)
        function checkAndAddMoreRandomWords() {
            if (!isRandomTest) return;

            // When only 3 or fewer words left, add more words + wrong words
            if (words.length <= 3) {
                // Get current word keys in queue
                const currentWordKeys = new Set(words.map(w => w.de + '|' + w.en));
                if (currentCard) {
                    currentWordKeys.add(currentCard.de + '|' + currentCard.en);
                }

                // Filter available words that haven't been practiced yet AND aren't in queue
                const availableNew = allAvailableWords.filter(w => {
                    const key = w.de + '|' + w.en;
                    return !randomTestPracticedKeys.has(key) && !currentWordKeys.has(key);
                });

                // Shuffle and pick 10 new words
                for (let i = availableNew.length - 1; i > 0; i--) {
                    const j = Math.floor(Math.random() * (i + 1));
                    [availableNew[i], availableNew[j]] = [availableNew[j], availableNew[i]];
                }
                const newWords = availableNew.slice(0, 10);

                // Get wrong words that aren't currently in queue
                const wrongToAdd = randomTestWrongWords.filter(w => !currentWordKeys.has(w.de + '|' + w.en));

                // Clear wrong words from tracking (they'll be added now)
                randomTestWrongWords = [];

                // Remove wrong words from practiced keys so they can be practiced again
                wrongToAdd.forEach(w => {
                    randomTestPracticedKeys.delete(w.de + '|' + w.en);
                });

                // Combine new words and wrong words
                const toAdd = [...newWords, ...wrongToAdd];

                if (toAdd.length > 0) {
                    // Shuffle the combined batch
                    for (let i = toAdd.length - 1; i > 0; i--) {
                        const j = Math.floor(Math.random() * (i + 1));
                        [toAdd[i], toAdd[j]] = [toAdd[j], toAdd[i]];
                    }

                    // Add to words queue
                    words.push(...toAdd);
                    originalWords.push(...toAdd);

                    // Preload audio for new words
                    preloadRandomTestAudio(toAdd);

                    // Update display
                    const wrongCount = wrongToAdd.length;
                    const newCount = newWords.length;
                    let msg = randomTestCompletedCount + ' done';
                    if (newCount > 0) msg += ', +' + newCount + ' new';
                    if (wrongCount > 0) msg += ', +' + wrongCount + ' wrong';
                    updateSelectedDeckName(['Random Test (' + msg + ')']);
                } else if (words.length === 0) {
                    // No more words available
                    updateSelectedDeckName(['Random Test (' + randomTestCompletedCount + ' done - complete!)']);
                }
            }
        }

        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                if (!cardHistory.length) return;
                // Put current card back to the front of the queue so it isn't lost
                const previous = cardHistory.pop();
                if (currentCard && currentCard !== previous) {
                    words.unshift(currentCard);
                }
                showCardData(previous);
            });
        }

        resetBtn.addEventListener('click', () => {
            words = Array.isArray(originalWords) ? originalWords.slice() : [];
            shuffle(words);
            cardHistory = [];
            clearPracticedWords();
            revealedLineWords.clear();
            getNewCard();
        });

        // Toggle word list collapse
        if (wordListHeader) {
            wordListHeader.addEventListener('click', () => {
                if (wordListContainer) {
                    wordListContainer.classList.toggle('is-collapsed');
                }
            });
        }

        let germanVoice = null;
        const audioCache = new Map();
        const preloadedAudios = new Map();
        const audioBlobUrls = new Map();
        const wordAudioURLMap = new Map();
        const audioPlayer = new Audio();
        audioPlayer.preload = 'none';
        let lastObjectUrl = null;

        function cleanupObjectUrl() {
            if (lastObjectUrl) {
                const isPreloaded = Array.from(audioBlobUrls.values()).includes(lastObjectUrl);
                if (!isPreloaded) {
                    try {
                        URL.revokeObjectURL(lastObjectUrl);
                    } catch { }
                }
                lastObjectUrl = null;
            }
        }
        audioPlayer.addEventListener('ended', () => {
            speakBtn.disabled = false;
            speakBtn.textContent = 'Speak';
            cleanupObjectUrl();
        });
        audioPlayer.addEventListener('error', () => {
            speakBtn.disabled = false;
            speakBtn.textContent = 'Speak';
            cleanupObjectUrl();
        });

        function safeFileName(text) {
            return text.trim().replace(/[^a-zA-Z0-9äöüÄÖÜß]+/g, '_').substring(0, 100);
        }

        function selectGermanVoice() {
            const voices = window.speechSynthesis.getVoices();
            // Try to find a voice with 'de' in lang and 'German' or 'Deutsch' in name
            germanVoice = voices.find(v => v.lang.startsWith('de') && (v.name.toLowerCase().includes('german') || v.name.toLowerCase().includes('deutsch')));
            // Fallback: any voice with 'de' in lang
            if (!germanVoice) {
                germanVoice = voices.find(v => v.lang.startsWith('de'));
            }
        }
        // Ensure voices are loaded
        if (typeof speechSynthesis !== 'undefined') {
            if (speechSynthesis.onvoiceschanged !== undefined) {
                speechSynthesis.onvoiceschanged = selectGermanVoice;
            }
            selectGermanVoice();
        }
        speakBtn.addEventListener('click', async () => {
            if (!currentCard.de) return;
            const text = currentCard.de;
            speakBtn.disabled = true;
            speakBtn.textContent = 'Speaking...';

            // Use cached URL first if available
            const cached = audioCache.get(text);
            if (cached) {
                try {
                    audioPlayer.pause();
                    audioPlayer.src = cached;
                    audioPlayer.currentTime = 0;
                    await audioPlayer.play();
                    return;
                } catch { }
            }

            // Prefer same-origin fetch as blob to avoid re-downloading
            try {
                const resp2 = await fetch(`/r2/tts?text=${encodeURIComponent(text)}&lang=de`);
                if (resp2.ok) {
                    const blob = await resp2.blob();
                    const objUrl = URL.createObjectURL(blob);
                    audioPlayer.pause();
                    audioPlayer.src = objUrl;
                    audioPlayer.currentTime = 0;
                    await audioPlayer.play();
                    lastObjectUrl = objUrl;
                    audioCache.set(text, objUrl);
                    audioBlobUrls.set(text, objUrl);
                    return;
                }
            } catch { }

            // No write fallbacks: if R2 URL unavailable, use browser speechSynthesis
            try {
                const utterance = new SpeechSynthesisUtterance(text);
                utterance.lang = 'de-DE';
                if (germanVoice) utterance.voice = germanVoice;
                speechSynthesis.speak(utterance);
            } finally {
                speakBtn.disabled = false;
                speakBtn.textContent = 'Speak';
            }
        });

        // Initialize auto-play state and load data
        initAutoPlayState();
        // Load folders and decks in parallel for faster startup
        loadAllData();

        // Simple dropdown change handler
        deckSelect.addEventListener('change', (e) => {
            const name = e.target.value;
            if (!name) return;
            isAutoPlayEnabled = false;
            try {
                localStorage.setItem('autoPlayEnabled', 'false');
            } catch { }
            updateAutoPlayToggle();
            loadWordsForDeck(name);
        });

        if (spellingBtn) spellingBtn.addEventListener('click', () => {
            const name = deckSelect && deckSelect.value;
            if (name) window.location.href = `/spelling?deck=${encodeURIComponent(name)}`;
            else window.location.href = '/spelling';
        });

        // Multi-deck modal interactions
        const flashMultiBtn = document.getElementById('flashMultiBtn');
        const multiDeckModal = document.getElementById('multiDeckModal');
        let isOnFlashcardScreen = false;

        function updateFlashMultiBtn() {
            if (!flashMultiBtn) return;
            const primaryText = flashMultiBtn.querySelector('.link__text--primary');
            const hoverText = flashMultiBtn.querySelector('.link__text--hover');
            if (isOnFlashcardScreen) {
                if (primaryText) primaryText.textContent = 'Multi-Deck';
                if (hoverText) hoverText.textContent = 'Multi-Deck';
            } else {
                if (primaryText) primaryText.textContent = 'Flashcard';
                if (hoverText) hoverText.textContent = 'Flashcard';
            }
        }
        const multiDeckList = document.getElementById('multiDeckList');
        const multiDeckCancelBtn = document.getElementById('multiDeckCancelBtn');
        const multiDeckSaveBtn = document.getElementById('multiDeckSaveBtn');
        const multiDeckStatus = document.getElementById('multiDeckStatus');
        const multiDeckBackBtn = document.getElementById('multiDeckBackBtn');

        let multiDeckSelectedFolder = null;
        const multiDeckModalTitle = document.getElementById('multiDeckModalTitle');

        function updateMultiDeckBackBtn(show) {
            if (multiDeckBackBtn) {
                multiDeckBackBtn.style.visibility = show ? 'visible' : 'hidden';
            }
        }
        const multiDeckSelectedNamesEl = document.getElementById('multiDeckSelectedNames');
        let multiDeckSelectedNames = new Set();

        function updateMultiDeckSelectedNames() {
            if (!multiDeckSelectedNamesEl) return;
            const arr = Array.from(multiDeckSelectedNames);
            multiDeckSelectedNamesEl.textContent = arr.length ? arr.join(', ') : '';
        }

        function populateMultiDeckFolders() {
            multiDeckList.innerHTML = '';
            multiDeckSelectedFolder = null;
            if (multiDeckModalTitle) multiDeckModalTitle.textContent = 'Select Folder';
            updateMultiDeckBackBtn(false);

            const allFolders = Array.isArray(cachedFolderList) ? cachedFolderList.slice() : [];

            if (!allFolders.length && !cachedDeckList.length) {
                multiDeckList.innerHTML = '<p style="padding:14px;color:var(--muted);text-align:center;">No folders available.</p>';
                return;
            }

            let rootFolders = allFolders.filter(f => !f.parent);

            if (!rootFolders.length) {
                const folderCounts = {};
                cachedDeckList.forEach(d => {
                    const f = d.folder || 'Uncategorized';
                    folderCounts[f] = (folderCounts[f] || 0) + 1;
                });
                rootFolders = Object.keys(folderCounts).map(name => ({
                    name,
                    count: folderCounts[name]
                }));
            }

            if (!rootFolders.length) {
                multiDeckList.innerHTML = '<p style="padding:14px;color:var(--muted);text-align:center;">No folders available.</p>';
                return;
            }

            const ordered = applyOrder(rootFolders, 'folder');

            ordered.forEach((folder, index) => {
                const name = folder.name || folder;
                const row = document.createElement('div');
                row.className = 'multi-deck-folder';

                const decksInFolder = cachedDeckList.filter(d => (d.folder || 'Uncategorized') === name);
                const hasSelectedDeck = decksInFolder.some(d => multiDeckSelectedNames.has(d.name));
                if (hasSelectedDeck) row.classList.add('is-selected');

                const label = document.createElement('span');
                label.className = 'folder-label';
                label.textContent = name;

                const arrow = document.createElement('span');
                arrow.className = 'folder-arrow material-symbols-outlined';
                arrow.textContent = hasSelectedDeck ? 'check_circle' : 'arrow_forward_ios';

                row.appendChild(label);
                row.appendChild(arrow);
                row.addEventListener('click', () => {
                    populateMultiDeckFolderLevel(name);
                });
                multiDeckList.appendChild(row);

                if (index < ordered.length - 1) {
                    const divider = document.createElement('hr');
                    divider.className = 'multi-deck-divider';
                    multiDeckList.appendChild(divider);
                }
            });

            updateMultiDeckSelectedNames();
        }

        function populateMultiDeckFolderLevel(parentName) {
            multiDeckList.innerHTML = '';
            multiDeckSelectedFolder = parentName;
            if (multiDeckModalTitle) multiDeckModalTitle.textContent = parentName;
            updateMultiDeckBackBtn(true);

            const allFolders = Array.isArray(cachedFolderList) ? cachedFolderList.slice() : [];
            const children = allFolders.filter(f => f.parent === parentName);

            if (!children.length) {
                populateMultiDeckDecks(parentName);
                return;
            }

            const ordered = applyOrder(children, 'folder');

            ordered.forEach(child => {
                const name = child.name || child;
                const row = document.createElement('div');
                row.className = 'multi-deck-folder';

                const decksInFolder = cachedDeckList.filter(d => (d.folder || 'Uncategorized') === name);
                const hasSelectedDeck = decksInFolder.some(d => multiDeckSelectedNames.has(d.name));
                if (hasSelectedDeck) row.classList.add('is-selected');

                const label = document.createElement('span');
                label.className = 'folder-label';
                label.textContent = name;

                const arrow = document.createElement('span');
                arrow.className = 'folder-arrow material-symbols-outlined';
                arrow.textContent = hasSelectedDeck ? 'check_circle' : 'arrow_forward_ios';

                row.appendChild(label);
                row.appendChild(arrow);
                row.addEventListener('click', () => {
                    populateMultiDeckDecks(name);
                });
                multiDeckList.appendChild(row);
            });

            // Also show decks that belong directly to this folder (not just sub-folders)
            const directDecks = cachedDeckList.filter(d => (d.folder || 'Uncategorized') === parentName);
            if (directDecks.length) {
                const baseDecks = directDecks.map(d => ({ name: d.name, _raw: d }));
                const orderedDecks = applyOrder(baseDecks, 'deck', parentName);

                orderedDecks.forEach(entry => {
                    const d = entry._raw;
                    const row = document.createElement('div');
                    row.className = 'multi-deck-item';
                    if (multiDeckSelectedNames.has(d.name)) row.classList.add('is-selected');

                    const label = document.createElement('span');
                    label.textContent = d.name;
                    row.appendChild(label);

                    row.addEventListener('click', () => {
                        if (multiDeckSelectedNames.has(d.name)) {
                            multiDeckSelectedNames.delete(d.name);
                            row.classList.remove('is-selected');
                        } else {
                            multiDeckSelectedNames.add(d.name);
                            row.classList.add('is-selected');
                        }
                        updateMultiDeckSelectedNames();
                    });

                    multiDeckList.appendChild(row);
                });
            }

            updateMultiDeckSelectedNames();
        }

        function populateMultiDeckDecks(folderName) {
            multiDeckList.innerHTML = '';
            multiDeckSelectedFolder = folderName;
            if (multiDeckModalTitle) multiDeckModalTitle.textContent = folderName;
            updateMultiDeckBackBtn(true);

            // Filter decks by folder
            const decksInFolder = cachedDeckList.filter(d => (d.folder || 'Uncategorized') === folderName);

            if (!decksInFolder.length) {
                const empty = document.createElement('p');
                empty.textContent = 'No decks in this folder.';
                empty.style.padding = '14px';
                empty.style.color = 'var(--muted)';
                empty.style.textAlign = 'center';
                multiDeckList.appendChild(empty);
                return;
            }

            // Apply same deck order as folder page (use folder name directly as scope)
            const baseDecks = decksInFolder.map(d => ({
                name: d.name,
                _raw: d
            }));
            const orderedDecks = applyOrder(baseDecks, 'deck', folderName);

            orderedDecks.forEach((entry, index) => {
                const d = entry._raw;
                const row = document.createElement('div');
                row.className = 'multi-deck-item';
                if (multiDeckSelectedNames.has(d.name)) row.classList.add('is-selected');

                const label = document.createElement('span');
                label.textContent = d.name;
                row.appendChild(label);

                // Toggle selection on click
                row.addEventListener('click', () => {
                    if (multiDeckSelectedNames.has(d.name)) {
                        multiDeckSelectedNames.delete(d.name);
                        row.classList.remove('is-selected');
                    } else {
                        multiDeckSelectedNames.add(d.name);
                        row.classList.add('is-selected');
                    }
                    updateMultiDeckSelectedNames();
                });

                multiDeckList.appendChild(row);
            });

            updateMultiDeckSelectedNames();
        }

        function openMultiDeckModal() {
            multiDeckStatus.textContent = '';
            multiDeckSelectedNames = new Set();
            updateMultiDeckSelectedNames();
            populateMultiDeckFolders();
            multiDeckModal.classList.add('is-open');
            multiDeckModal.setAttribute('aria-hidden', 'false');

            // Preload deck orders for all folders in background (no delay when clicking folder)
            const folders = new Set(cachedDeckList.map(d => d.folder || 'Uncategorized'));
            folders.forEach(f => refreshDeckOrder(f));
        }

        function closeMultiDeckModal() {
            multiDeckModal.classList.remove('is-open');
            multiDeckModal.classList.remove('is-closing');
            multiDeckModal.setAttribute('aria-hidden', 'true');
            multiDeckSelectedFolder = null;
            multiDeckSelectedNames = new Set();
            updateMultiDeckSelectedNames();
        }

        async function loadWordsForDecks(deckNames) {
            isAutoPlayEnabled = false;
            isRandomTest = false;
            randomTestWrongWords = [];
            randomTestCompletedCount = 0;
            randomTestPracticedKeys.clear();
            try {
                localStorage.setItem('autoPlayEnabled', 'false');
            } catch { }
            updateAutoPlayToggle();
            const names = Array.isArray(deckNames) ? deckNames.filter(Boolean) : [];
            if (!names.length) return;
            updateSelectedDeckName(names);
            try {
                const promises = names.map((n) => fetch(`/cards?deck=${encodeURIComponent(n)}`).then(r => r.ok ? r.json() : []).catch(() => []));
                const lists = await Promise.all(promises);
                // Cache each deck's cards for instant navigation
                names.forEach((n, i) => {
                    if (Array.isArray(lists[i])) {
                        try {
                            sessionStorage.setItem(`flashcard_cards_${n}`, JSON.stringify(lists[i]));
                        } catch { }
                    }
                });
                const combined = ([]).concat(...lists);
                originalWords = Array.isArray(combined) ? combined : [];
                words = originalWords.slice();
                shuffle(words);
                getNewCard();
                clearDeckPreloads();
                names.forEach((n) => preloadDeckAudio(n));
                // Load line data for all decks in background
                names.forEach((n) => loadLineDataForDeck(n));
                clearPracticedWords();
                revealedLineWords.clear();
            } catch {
                originalWords = [];
                words = [];
                card.innerText = 'Failed to load selected decks.';
            }
        }

        if (flashMultiBtn) flashMultiBtn.addEventListener('click', () => {
            if (isOnFlashcardScreen) {
                // On flashcard screen - open multi-deck modal
                openMultiDeckModal();
            } else {
                // On home screen - show flashcard screen
                if (flashApp) flashApp.style.display = 'block';
                if (homeDeckList) homeDeckList.style.display = 'none';
                if (createFolderBtn) createFolderBtn.style.display = 'none';
                isOnFlashcardScreen = true;
                updateFlashMultiBtn();
            }
        });
        if (multiDeckCancelBtn) multiDeckCancelBtn.addEventListener('click', closeMultiDeckModal);
        if (multiDeckBackBtn) multiDeckBackBtn.addEventListener('click', () => {
            populateMultiDeckFolders();
        });
        multiDeckModal.addEventListener('click', (e) => {
            if (e.target.dataset.closeMulti === 'true') closeMultiDeckModal();
        });

        if (multiDeckSaveBtn) multiDeckSaveBtn.addEventListener('click', () => {
            const selected = Array.from(multiDeckSelectedNames);
            if (!selected.length) {
                multiDeckStatus.textContent = 'Please select at least one deck.';
                return;
            }
            closeMultiDeckModal();
            loadWordsForDecks(selected);
            if (flashApp) flashApp.style.display = 'block';
            if (homeDeckList) homeDeckList.style.display = 'none';
            isOnFlashcardScreen = true;
            updateFlashMultiBtn();
        });

        // Header interactions
        function updateHeaderSolid() {
            if (window.scrollY > 10) header.classList.add('is-solid');
            else header.classList.remove('is-solid');
        }
        updateHeaderSolid();
        window.addEventListener('scroll', updateHeaderSolid, {
            passive: true
        });
        // Mobile menu removed per design request; no toggle handlers.

        // Deck creation navigation
        const createDeckBtn = document.getElementById('createDeckBtn');

        if (createDeckBtn) createDeckBtn.addEventListener('click', () => {
            try {
                showLoader();
            } catch { }
            window.location.href = '/create';
        });
