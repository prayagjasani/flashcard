from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from gtts import gTTS
import os, io, json, csv, boto3, re
from typing import Optional

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# CORS setup (you can add specific origins if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# CONFIG & R2 CLIENT SETUP
# -------------------------------
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")

s3_client = None
if all([R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET, R2_ACCOUNT_ID]):
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
    except Exception as e:
        print(f"⚠️ Failed to connect R2: {e}")
else:
    print("⚠️ R2 credentials missing — R2 features disabled.")

# -------------------------------
# HELPERS
# -------------------------------
def safe_key(key: str) -> str:
    """Clean filename or key for safe R2 usage."""
    return re.sub(r"[^a-zA-Z0-9._/-]", "_", key)

def upload_to_r2(key: str, data: bytes, content_type: str = "application/octet-stream"):
    """Upload bytes to R2 safely."""
    if not s3_client:
        raise HTTPException(status_code=503, detail="R2 not configured")
    s3_client.put_object(Bucket=R2_BUCKET, Key=key, Body=data, ContentType=content_type)
    return f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com/{R2_BUCKET}/{key}"

# -------------------------------
# ROUTES
# -------------------------------
@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    """Serve the index page if it exists."""
    if not os.path.exists("templates/index.html"):
        return {"message": "Index not found"}
    return templates.TemplateResponse("index.html", {"request": request})

# ----- Simple CSV Upload -----
@app.post("/upload_csv")
async def upload_csv(file: bytes = Form(...), filename: str = Form(...)):
    if not s3_client:
        raise HTTPException(status_code=503, detail="R2 not configured")
    key = safe_key(filename)
    upload_to_r2(key, file, "text/csv")
    return {"message": "CSV uploaded", "key": key}

# ----- TTS (Text to Speech) -----
@app.get("/tts_url")
async def tts_url(text: str, lang: Optional[str] = "en"):
    """Generate TTS audio and upload to R2."""
    try:
        tts = gTTS(text=text, lang=lang)
        buf = io.BytesIO()
        tts.write_to_fp(buf)
        buf.seek(0)
        key = f"tts/{safe_key(text[:30])}.mp3"
        url = upload_to_r2(key, buf.read(), "audio/mpeg")
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tts")
async def tts_direct(text: str, lang: Optional[str] = "en"):
    """Generate and return audio directly."""
    try:
        tts = gTTS(text=text, lang=lang)
        buf = io.BytesIO()
        tts.write_to_fp(buf)
        buf.seek(0)
        return StreamingResponse(buf, media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----- R2 File Retrieval -----
@app.get("/r2/get")
def get_r2_file(key: str):
    """Fetch file from R2 by key (restricted to safe prefixes)."""
    if not s3_client:
        raise HTTPException(status_code=503, detail="R2 not configured")
    if not key.startswith(("tts/", "csv/")):
        raise HTTPException(status_code=403, detail="Access denied")
    try:
        obj = s3_client.get_object(Bucket=R2_BUCKET, Key=key)
        return StreamingResponse(obj["Body"], media_type=obj["ContentType"])
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"File not found: {e}")

# ----- Deck & Card Routes -----
@app.get("/cards")
async def get_cards(deck: Optional[str] = None):
    """Fetch cards from a local CSV file or from R2."""
    # Default to a local CSV file if no deck specified
    deck_file = deck if deck else "list"
    
    # First try local file
    local_path = f"csv/{deck_file}.csv" if not deck_file.endswith('.csv') else f"csv/{deck_file}"
    if os.path.exists(local_path):
        try:
            cards = []
            with open(local_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, fieldnames=['en', 'de'])
                for row in reader:
                    if row.get('en') and row.get('de'):
                        cards.append({"en": row['en'].strip(), "de": row['de'].strip()})
            return cards
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading CSV: {e}")
    
    # If local file doesn't exist and R2 is configured, try R2
    if s3_client:
        try:
            key = f"csv/{deck_file}.csv" if not deck_file.endswith('.csv') else f"csv/{deck_file}"
            obj = s3_client.get_object(Bucket=R2_BUCKET, Key=key)
            content = obj["Body"].read().decode('utf-8')
            cards = []
            reader = csv.DictReader(io.StringIO(content), fieldnames=['en', 'de'])
            for row in reader:
                if row.get('en') and row.get('de'):
                    cards.append({"en": row['en'].strip(), "de": row['de'].strip()})
            return cards
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Deck not found: {e}")
    
    raise HTTPException(status_code=404, detail="Deck not found")

@app.post("/register_deck")
async def register_deck(name: str = Form(...), csv_data: bytes = Form(...)):
    """Register new deck and upload to R2."""
    key = f"csv/{safe_key(name)}.csv"
    upload_to_r2(key, csv_data, "text/csv")
    return {"message": "Deck registered", "key": key}

@app.post("/deck/create")
async def create_deck(request: Request):
    """Create a new deck from JSON data (name + CSV text)."""
    try:
        body = await request.json()
        name = body.get("name", "").strip()
        data = body.get("data", "").strip()
        
        if not name:
            raise HTTPException(status_code=400, detail="Deck name is required")
        if not data:
            raise HTTPException(status_code=400, detail="Deck data is required")
        
        # Sanitize the deck name
        safe_name = safe_key(name)
        
        # Save to local CSV file first
        os.makedirs("csv", exist_ok=True)
        local_path = f"csv/{safe_name}.csv"
        with open(local_path, 'w', encoding='utf-8', newline='') as f:
            f.write(data)
        
        # Also try to upload to R2 if configured
        if s3_client:
            try:
                key = f"csv/{safe_name}.csv"
                upload_to_r2(key, data.encode('utf-8'), "text/csv")
            except Exception as e:
                print(f"⚠️ Failed to upload to R2: {e}")
        
        return {"ok": True, "message": "Deck created", "name": safe_name}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/decks")
async def list_decks():
    """List all available deck files from local storage and R2."""
    decks = []
    deck_names = set()
    
    # First, check local CSV files
    if os.path.exists("csv"):
        try:
            for filename in os.listdir("csv"):
                if filename.endswith(".csv"):
                    deck_name = filename[:-4]  # Remove .csv extension
                    deck_names.add(deck_name)
        except Exception as e:
            print(f"⚠️ Error listing local CSVs: {e}")
    
    # Then check R2 if configured
    if s3_client:
        try:
            objects = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix="csv/")
            for obj in objects.get("Contents", []):
                filename = obj["Key"].split("/")[-1]
                if filename.endswith(".csv"):
                    deck_name = filename[:-4]  # Remove .csv extension
                    deck_names.add(deck_name)
        except Exception as e:
            print(f"⚠️ Error listing R2 decks: {e}")
    
    # Convert set to list of objects with 'name' property
    decks = [{"name": name} for name in sorted(deck_names)]
    
    return decks

# -------------------------------
# LOCAL UTIL ROUTES (OPTIONAL)
# -------------------------------
@app.get("/local/tts")
def local_tts(text: str, lang: str = "en"):
    """Generate audio locally (without uploading to R2)."""
    tts = gTTS(text=text, lang=lang)
    path = f"temp_{safe_key(text[:15])}.mp3"
    tts.save(path)
    return FileResponse(path, media_type="audio/mpeg", filename=os.path.basename(path))

# -------------------------------
# RUN LOCALLY (for testing)
# -------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
