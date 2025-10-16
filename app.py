import csv
import uvicorn
import io
import os
import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from gtts import gTTS

app = FastAPI()

# Serve cached audio files directly from /audio
app.mount("/audio", StaticFiles(directory="audio"), name="audio")

# Legacy loader removed: decks are loaded on demand via /cards

@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.get("/decks")
def list_decks():
    # Collect deck names from csv/ and project root for backward compatibility
    names = set()
    csv_dir = os.path.join('.', 'csv')
    try:
        for f in os.listdir(csv_dir):
            if f.lower().endswith('.csv'):
                names.add(os.path.splitext(f)[0])
    except FileNotFoundError:
        pass
    for f in os.listdir('.'):
        if f.lower().endswith('.csv'):
            names.add(os.path.splitext(f)[0])

    return [{"name": n, "file": f"{n}.csv"} for n in sorted(names)]

@app.get("/cards")
def get_cards(deck: str | None = None):
    # Determine target CSV
    target = 'list.csv'
    if deck:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", deck).strip()
        if not safe:
            raise HTTPException(status_code=400, detail="Invalid deck name")
        target = safe if safe.lower().endswith('.csv') else f"{safe}.csv"

    # Prefer csv/ folder; fallback to project root for legacy decks
    csv_dir = os.path.join('.', 'csv')
    target_in_csv = os.path.join(csv_dir, target)
    path_to_open = None
    if os.path.exists(target_in_csv):
        path_to_open = target_in_csv
    elif os.path.exists(target):
        path_to_open = target
    else:
        raise HTTPException(status_code=404, detail="Deck not found")

    # Read words from target CSV
    result = []
    with open(path_to_open, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                result.append({'en': row[0].strip(), 'de': row[1].strip()})
    return result

class DeckCreate(BaseModel):
    name: str
    data: str

@app.post("/deck/create")
def create_deck(payload: DeckCreate):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")
    # Allow letters, numbers, underscore and dash
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", name)[:50]
    # Ensure csv directory exists and write deck CSV inside it
    csv_dir = os.path.join('.', 'csv')
    os.makedirs(csv_dir, exist_ok=True)
    file_path = os.path.join(csv_dir, f"{safe_name}.csv")

    lines = (payload.data or "").splitlines()
    rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",", 1)
        if len(parts) < 2:
            # skip malformed lines without a comma
            continue
        en = parts[0].strip()
        de = parts[1].strip()
        if en and de:
            rows.append([en, de])

    if not rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    try:
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        # Prefetch audio for German words so Speak is instant next time
        try:
            cache_dir = os.path.join("audio", "de")
            os.makedirs(cache_dir, exist_ok=True)
            # Use a set to avoid duplicate generation
            unique_de = set(de for _, de in rows)
            generated = 0
            skipped = 0
            for text in unique_de:
                # Sanitize filename similar to /tts endpoint
                safe_text = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß]+", "_", text.strip())[:100]
                target_path = os.path.join(cache_dir, f"{safe_text}.mp3")
                if os.path.exists(target_path):
                    skipped += 1
                    continue
                try:
                    tts = gTTS(text=text, lang="de", slow=False)
                    tts.save(target_path)
                    generated += 1
                except Exception:
                    # Best-effort: skip failures and continue
                    pass
        except Exception:
            # Do not fail deck creation if audio prefetch has issues
            generated = 0
            skipped = 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"ok": True, "file": file_path, "rows": len(rows), "audio_generated": generated, "audio_skipped": skipped}

@app.get("/tts")
def tts(text: str, lang: str = "de", slow: bool = False):
    try:
        # Ensure cache directory exists
        cache_dir = os.path.join("audio", lang)
        os.makedirs(cache_dir, exist_ok=True)

        # Sanitize filename (keep German characters)
        safe_text = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß]+", "_", text.strip())[:100]
        file_path = os.path.join(cache_dir, f"{safe_text}.mp3")

        # Serve cached file if present
        if os.path.exists(file_path):
            return FileResponse(file_path, media_type="audio/mpeg")

        # Generate and cache audio
        tts = gTTS(text=text, lang=lang, slow=slow)
        tts.save(file_path)
        return FileResponse(file_path, media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)