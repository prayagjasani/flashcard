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
@app.post("/register_deck")
async def register_deck(name: str = Form(...), csv_data: bytes = Form(...)):
    """Register new deck and upload to R2."""
    key = f"csv/{safe_key(name)}.csv"
    upload_to_r2(key, csv_data, "text/csv")
    return {"message": "Deck registered", "key": key}

@app.get("/decks")
async def list_decks():
    """List all deck files in R2."""
    if not s3_client:
        raise HTTPException(status_code=503, detail="R2 not configured")
    try:
        objects = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix="csv/")
        decks = [obj["Key"].split("/")[-1] for obj in objects.get("Contents", [])]
        return {"decks": decks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
