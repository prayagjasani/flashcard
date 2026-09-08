# React frontend

The home and folder library are React components. FastAPI still owns all APIs
and serves the built frontend. Study modes, the deck editor, PDF, video, and
story screens currently use their existing HTML/JavaScript implementations.

Every screen shares the same learning-app design system:

- `static/css/app-theme.css`: colours, type, cards, buttons, forms, and responsive layout.
- `static/js/app-shell.js`: common navigation and accessible session progress.
- `frontend/legacy-tailwind.config.cjs`: compiled utility styles for the older screens.

The build also copies Nunito and Material Symbols into `static/fonts/`, with
their licences. Pages no longer depend on external CSS or font services.
Rebuild after editing shared styles so React and the HTML screens stay aligned.

## Build and run

From this directory:

```sh
npm ci
npm run build
```

Then, from the repository root:

```sh
python app.py
```

Open http://localhost:8000. Node.js is required to build the frontend, but not
to run the built app. Rebuild after editing React files. Generated files live
in `static/react/`; edit `frontend/src/`, not the generated files.
The Docker build compiles the frontend automatically in a separate Node stage.

For live frontend editing, run Python on port 8000 and `npm run dev` here.
Open http://127.0.0.1:5173/static/react/. Existing API and page routes are
proxied to Python. Following a library link opens the latest production build;
return to the Vite URL for live editing.

The Python routes fall back to the original templates if the React build is
missing. Links such as `/?mode=flash&deck=...` continue to open the existing
study engine, including its multi-deck control.

## Verification

```sh
npm test
```

Browser tests use mocked API responses and never modify stored decks or folders.
Install a Playwright Chromium browser with `npx playwright install chromium`
if one is not available on your machine.

Python routing tests run from the repository root with
`python -m unittest discover -s tests`. They require `fastapi` and `httpx` and
verify the React bundle, legacy screens, and fallback when no build exists.
