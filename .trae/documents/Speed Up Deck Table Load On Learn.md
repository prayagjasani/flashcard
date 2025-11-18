         ## Goal
Reduce the time-to-first-render and overall load time when opening the Learn view for a specific deck by optimizing data fetch, rendering strategy, and resource contention.

## Current Bottlenecks
- Synchronous localStorage hydration runs before rendering (`hi.html:355–361`).
- Audio preloading runs concurrently with deck fetch (`hi.html:484–495` calling `preloadDeckAudio`), competing for bandwidth.
- Per-row event handlers are attached during render (`hi.html:345`), increasing setup cost for large decks.
- Full DOM construction runs in one pass (`hi.html:314–353`), which can block the main thread for big decks.

## Changes (Phase 1: Immediate, Low-Risk)
1. Defer audio preloading until after initial render:
   - Call `preloadDeckAudio()` via `requestIdleCallback` (fallback to `setTimeout`) after `renderGrid()` completes.
   - Update `initializeApp` to await `loadDeck()` first; schedule `preloadDeckAudio()` later (`hi.html:484–495`).

2. Remove localStorage hydration from the render path:
   - Stop calling `hydrateAudioFromLocalStorage()` inside `renderGrid` (`hi.html:320`).
   - Trigger hydration lazily: on first `speak()` usage or in an idle task.

3. Use event delegation for row clicks:
   - Set `data-text` on each `<tr>` and attach a single click listener on `#cardGrid` to call `speak(e.target.closest('tr')?.dataset.text)`.
   - Eliminates N listeners and reduces memory/time (`hi.html:345`).

4. Chunked rendering for large decks:
   - Render the first N rows synchronously (e.g., 50) for immediate feedback, then append remaining rows in batches using `requestAnimationFrame` with `DocumentFragment`.
   - Maintain current table markup and styling (`hi.html:314–353`).

## Changes (Phase 2: Perceived Performance & Caching)
5. Show a skeleton table while fetching:
   - Display 8–12 placeholder rows using Tailwind classes; replace with real rows as soon as data arrives.

6. Session caching of deck data:
   - Persist fetched deck JSON in `sessionStorage` keyed by deck name.
   - On navigation to Learn, immediately render from cache (if available), then revalidate in background and update if changed (`hi.html:269–312`).

## Optional (Phase 3: For Very Large Decks)
7. Virtualized table:
   - For decks > 1000 cards, render only visible rows using a fixed row height and a scroll container; update rows on scroll.
   - Keeps DOM size small and interaction smooth.

## Implementation Notes
- Keep `loadDeck` fetch as is but remove parallel audio preload to avoid bandwidth contention (`hi.html:282–299` and `hi.html:484–495`).
- Move `hydrateAudioFromLocalStorage` to an idle task or call inside `speak` if cache-miss (
  `hi.html:355–361`, `hi.html:439–465`).
- Replace per-row listeners with delegation on `cardGrid` (use `dataset`), reducing render overhead.
- Implement batch append: build fragments of 50–200 rows per frame until complete; this prevents long main-thread blocks.

## Expected Impact
- Instant initial render for first rows; overall render completes asynchronously.
- Reduced main-thread blocking from localStorage and event attachment.
- Faster deck data availability by removing concurrent audio preloading.
- Noticeably better perceived performance via skeletons and session cache.

## Next Step
Confirm this plan. Once approved, I will implement Phase 1 fully, verify with local testing, and then proceed with Phase 2 improvements.