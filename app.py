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

words = []
def load_words():
    words.clear()
    with open('list.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                words.append({'en': row[0].strip(), 'de': row[1].strip()})

@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.get("/decks")
def list_decks():
    files = [f for f in os.listdir('.') if f.lower().endswith('.csv')]
    files.sort()
    return [{"name": os.path.splitext(f)[0], "file": f} for f in files]

@app.get("/cards")
def get_cards(deck: str | None = None):
    # Determine target CSV
    target = 'list.csv'
    if deck:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", deck).strip()
        if not safe:
            raise HTTPException(status_code=400, detail="Invalid deck name")
        target = safe if safe.lower().endswith('.csv') else f"{safe}.csv"

    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail="Deck not found")

    # Read words from target CSV
    result = []
    with open(target, 'r', encoding='utf-8') as f:
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
    file_path = f"{safe_name}.csv"

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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"ok": True, "file": file_path, "rows": len(rows)}

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
    load_words()
    uvicorn.run(app, host="0.0.0.0", port=8000)