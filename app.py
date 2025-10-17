import csv
import os
import re
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from gtts import gTTS
import uvicorn

app = FastAPI()

# Serve cached audio files
app.mount("/audio", StaticFiles(directory="audio"), name="audio")

@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.get("/decks")
def list_decks():
    """List all deck CSV files from /csv directory."""
    csv_dir = "csv"
    if not os.path.exists(csv_dir):
        return []
    return [
        {"name": os.path.splitext(f)[0], "file": f}
        for f in os.listdir(csv_dir)
        if f.lower().endswith(".csv")
    ]

@app.get("/cards")
def get_cards(deck: str = "list"):
    """Return all cards (EN–DE pairs) from a CSV deck."""
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", deck).strip()
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    path = os.path.join("csv", f"{safe}.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Deck not found")

    result = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                en, de = row[0].strip(), row[1].strip()
                if en and de:
                    result.append({"en": en, "de": de})
    return result

class DeckCreate(BaseModel):
    name: str
    data: str

@app.post("/deck/create")
def create_deck(payload: DeckCreate):
    """Create a new deck and pre-generate German audio."""
    name = re.sub(r"[^a-zA-Z0-9_-]+", "_", payload.name.strip())[:50]
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")

    os.makedirs("csv", exist_ok=True)
    file_path = os.path.join("csv", f"{name}.csv")

    rows = []
    for line in payload.data.splitlines():
        parts = [p.strip() for p in line.split(",", 1)]
        if len(parts) == 2 and all(parts):
            rows.append(parts)

    if not rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    with open(file_path, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)

    os.makedirs("audio/de", exist_ok=True)
    generated = 0
    for _, de in rows:
        safe_text = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß]+", "_", de)[:100]
        audio_path = os.path.join("audio/de", f"{safe_text}.mp3")
        if not os.path.exists(audio_path):
            try:
                gTTS(text=de, lang="de").save(audio_path)
                generated += 1
            except Exception:
                pass

    return {"ok": True, "file": file_path, "rows": len(rows), "audio_generated": generated}

@app.get("/tts")
def tts(text: str, lang: str = "de", slow: bool = False):
    """Generate or serve cached text-to-speech audio."""
    os.makedirs(f"audio/{lang}", exist_ok=True)
    safe_text = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß]+", "_", text.strip())[:100]
    file_path = os.path.join("audio", lang, f"{safe_text}.mp3")

    if not os.path.exists(file_path):
        try:
            gTTS(text=text, lang=lang, slow=slow).save(file_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return FileResponse(file_path, media_type="audio/mpeg")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
