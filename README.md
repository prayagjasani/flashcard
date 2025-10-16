# Flashcards (FastAPI)

Simple flashcards web app built with FastAPI and a small HTML/CSS/JS front end.

## Run locally

- Create and activate a virtual environment (optional).
- Install deps: `pip install -r requirements.txt`
- Start dev server: `uvicorn app:app --host 0.0.0.0 --port 8000 --reload`
- Open `http://localhost:8000`.

Folders:
- `templates/` – HTML front end.
- `audio/` – generated MP3s (ignored in git).
- `csv/` – decks stored as CSV files.

## Docker

Build and run:

```sh
docker build -t flashcards .
docker run -p 8000:8000 -v %CD%/csv:/app/csv -v %CD%/audio:/app/audio flashcards
```

## Deploy options

### Railway
- Link repo and set Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`.
- Add a Volume for persistence and mount to `/app/audio` and `/app/csv` or `/data`.

### Render
- Web Service: Build `pip install -r requirements.txt`.
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`.
- Add a Disk for persistence and mount paths (adjust `app.py` if using `/data`).

### Fly.io
- Use the provided Dockerfile, `flyctl launch`.
- Add a Volume and mount to `/data`, then point `app.py` to read/write there if needed.

## Notes
- The `audio/` directory is ignored to keep the repo small; the server will generate files as needed.
- Ensure `requirements.txt` includes FastAPI, Uvicorn and Jinja2.