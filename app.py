import csv
import os
import re
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from gtts import gTTS
import uvicorn
import io
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json
from botocore.config import Config
import base64
import urllib.request
import urllib.error
import traceback

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Load environment variables from a local .env file if present
load_dotenv()

# Cloudflare R2 (S3 API) configuration via environment variables
R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET") or os.getenv("R2_BUCKET")
R2_PUBLIC_URL_BASE = os.getenv("R2_PUBLIC_URL_BASE")

# Endpoint can be provided directly or derived from account id
R2_ENDPOINT = (
    os.getenv("CLOUDFLARE_R2_ENDPOINT")
    or os.getenv("R2_ENDPOINT_URL")
    or (f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None)
)

r2_client = None
if R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT:
    try:
        r2_client = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            endpoint_url=R2_ENDPOINT,
            region_name="auto",
            config=Config(s3={"addressing_style": "path"}),
        )
    except Exception:
        r2_client = None

# Gemini API - Force load from .env file to override system env vars
from dotenv import load_dotenv
load_dotenv(override=True)  # Force override system environment variables
GEMINI_API_KEY = os.getenv("gemini_api_key") or os.getenv("GEMINI_API_KEY")
print(f"DEBUG: Loading Gemini API key: {GEMINI_API_KEY[:20]}..." if GEMINI_API_KEY else "DEBUG: No Gemini API key found!")

def _gemini_generate_lines(cards):
    """
    Generate real-life example sentences for German–English vocabulary pairs.
    """

    # No fallback when no API key
    if not GEMINI_API_KEY:
        return []

    # Model + endpoint
    model = "gemini-2.5-flash"  # Faster, more reliable model
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )

    # Vocabulary list for the prompt
    vocab_list = "\n".join([f'- {{ "de": "{c["de"]}", "en": "{c["en"]}" }}' for c in cards])

    # ----------------------------------------------------------------------
    # ⭐ Strong prompt — prevents useless sentences like “This is body.”
    # ----------------------------------------------------------------------
    prompt = f"""
You are an expert German language teacher.

TASK:
Generate PRACTICAL, REAL-LIFE example sentences for A1–B1 learners.

### Output rules:
- Output ONLY a JSON array, no text before or after it.
- Each element must match:
  {{
    "de": "<German word>",
    "en": "<English word>",
    "line_de": "<real-life German sentence>",
    "line_en": "<real-life English sentence>"
  }}

IMPORTANT:
- Echo the input values for fields "de" and "en" exactly as provided.
- Do not change, translate, normalize, or shorten these field values.

### Sentence rules:
- 8–14 words each.
- Use daily-life contexts: doctor visit, sports, work, family, morning routine.
- German must use correct grammar (cases, articles, verb placement).
- English and German sentences must NOT be literal translations.
- Use realistic daily actions and contexts.

### Vocabulary:
{vocab_list}
"""

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"},
    }

    # ----------------------------------------------------------------------
    # ⭐ API REQUEST
    # ----------------------------------------------------------------------
    try:
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")

        parsed = json.loads(raw)

        # Standard: result inside candidates → content → parts → text
        candidates = parsed.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                p0 = parts[0]
                if isinstance(p0, dict) and "text" in p0:
                    return json.loads(p0["text"])   # JSON array directly
                if isinstance(p0, dict) and "inlineData" in p0:
                    data_b64 = p0["inlineData"].get("data", "")
                    if data_b64:
                        raw_json = base64.b64decode(data_b64).decode("utf-8")
                        return json.loads(raw_json)

        # Some Gemini variants directly return JSON arrays
        if isinstance(parsed, list):
            return parsed

        raise ValueError("Could not extract JSON output from Gemini response.")

    except Exception as e:
        print("Gemini error:", e)
        traceback.print_exc()
        return []

# -------------------------------
# MODELS
# -------------------------------
class DeckCreate(BaseModel):
    name: str
    data: str
    folder: str | None = None

class DeckUpdate(BaseModel):
    name: str
    content: str
class DeckDelete(BaseModel):
    name: str
class DeckRename(BaseModel):
    old_name: str
    new_name: str

class AudioRebuildRequest(BaseModel):
    text: str
    lang: str = "de"
    old_text: str | None = None
class FolderCreate(BaseModel):
    name: str
class FolderRename(BaseModel):
    old_name: str
    new_name: str
class FolderDelete(BaseModel):
    name: str
class DeckMove(BaseModel):
    name: str
    folder: str | None = None
class FolderOrderUpdate(BaseModel):
    order: list[str]
class DeckOrderUpdate(BaseModel):
    scope: str | None = None
    order: list[str]

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def _safe_deck_name(name: str) -> str:
    """Sanitize deck name for file/key usage."""
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip())[:50]

def _safe_tts_key(text: str, lang: str = "de") -> str:
    """Generate safe R2 key for TTS audio."""
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe:
        safe = "tts"
    return f"{R2_BUCKET_NAME}/tts/{lang}/{safe}.mp3"

def _order_folders_key() -> str:
    return f"{R2_BUCKET_NAME}/order/folders.json"

def _order_decks_key(scope: str | None) -> str:
    s = _safe_deck_name(scope or "root") or "root"
    return f"{R2_BUCKET_NAME}/order/decks/{s}.json"

# -------------------------------
# BASIC ROUTES
# -------------------------------
@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.get("/learn")
def learn_screen():
    return FileResponse('hi.html')

@app.get("/match")
def match_screen():
    return FileResponse('match.html')

@app.get("/spelling")
def spelling_screen():
    return FileResponse('spelling.html')

@app.get("/line")
def line_screen():
    return FileResponse('line.html')

@app.get("/folder")
def folder_screen():
    return FileResponse('folder.html')

@app.get("/edit")
def edit_screen():
    return FileResponse('edit.html')

@app.get("/create")
def create_screen():
    return FileResponse('templates/create.html')

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/favicon.ico")
def favicon():
    return FileResponse('static/favicon.png')

@app.get("/r2/health")
def r2_health():
    """Simple health endpoint for R2 configuration diagnostics (no secrets)."""
    return {
        "configured": bool(r2_client and R2_BUCKET_NAME),
        "client_initialized": bool(r2_client is not None),
        "has_access_key": bool(R2_ACCESS_KEY_ID),
        "has_secret_key": bool(R2_SECRET_ACCESS_KEY),
        "has_endpoint": bool(R2_ENDPOINT),
        "has_bucket": bool(R2_BUCKET_NAME),
        "endpoint": R2_ENDPOINT,
        "bucket": R2_BUCKET_NAME,
        "public_url_base": R2_PUBLIC_URL_BASE,
    }

# -------------------------------
# DECK MANAGEMENT
# -------------------------------
@app.get("/decks")
def list_decks():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=f"{R2_BUCKET_NAME}/csv/index.json")
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = []
            for d in parsed:
                if isinstance(d, dict):
                    name = d.get("name")
                    file = d.get("file")
                    # Accept both relative keys like "csv/foo.csv" and full keys like "<bucket>/csv/foo.csv"
                    if name and file and file.lower().endswith(".csv") and (file.startswith("csv/") or "/csv/" in file):
                        # carry over optional last_modified if present in index.json
                        lm = d.get("last_modified")
                        folder = d.get("folder")
                        items.append({"name": name, "file": file, "last_modified": lm, "folder": folder})

            # Fallback: if index entries lack last_modified, compute it from R2 listing
            try:
                if not any(it.get("last_modified") for it in items):
                    lm_map = {}
                    continuation = None
                    while True:
                        kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": f"{R2_BUCKET_NAME}/csv/"}
                        if continuation:
                            kwargs["ContinuationToken"] = continuation
                        resp = r2_client.list_objects_v2(**kwargs)
                        for o in resp.get("Contents", []):
                            lm = o.get("LastModified")
                            lm_map[o.get("Key", "")] = lm.isoformat() if lm else ""
                        if resp.get("IsTruncated"):
                            continuation = resp.get("NextContinuationToken")
                        else:
                            break
                    for it in items:
                        file = it.get("file", "")
                        full_key = file if "/csv/" in file else f"{R2_BUCKET_NAME}/{file}"
                        it["last_modified"] = lm_map.get(full_key, it.get("last_modified", ""))
            except Exception:
                # keep items unsorted if last_modified mapping fails
                pass

            # Sort newest-first by last_modified when available
            items.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
            return items
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cards")
def get_cards(deck: str = "list"):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    if r2_client and R2_BUCKET_NAME:
        key = f"{R2_BUCKET_NAME}/csv/{safe}.csv"
        try:
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
            data = obj["Body"].read().decode("utf-8")
            result = []
            reader = csv.reader(io.StringIO(data))
            for row in reader:
                if len(row) >= 2:
                    en, de = row[0].strip(), row[1].strip()
                    if en and de:
                        result.append({"en": en, "de": de})
            return result
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise HTTPException(status_code=404, detail="Deck not found")
            raise HTTPException(status_code=500, detail=str(e))

    # R2 required; no local file fallback
    raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")

@app.get("/deck/csv")
def get_deck_csv(deck: str):
    """Return raw CSV content for an existing deck from R2."""
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    key = f"{R2_BUCKET_NAME}/csv/{safe}.csv"
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        return {"name": safe, "file": key, "csv": data}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Deck not found")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/deck/update")
def update_deck(payload: DeckUpdate):
    """Update an existing deck's CSV content in R2."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")
    content = payload.content or ""
    key = f"{R2_BUCKET_NAME}/csv/{name}.csv"

    # Read old CSV to compute changes
    old_csv = ""
    try:
        obj_old = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        old_csv = obj_old["Body"].read().decode("utf-8")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))

    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/csv",
        )
        # Compute German word changes and sync audio
        def parse_de_words(csv_text: str):
            words = set()
            try:
                reader = csv.reader(io.StringIO(csv_text))
                for row in reader:
                    if len(row) >= 2:
                        de = row[1].strip()
                        if de:
                            words.add(de)
            except Exception:
                pass
            return words

        old_de = parse_de_words(old_csv)
        new_de = parse_de_words(content)
        to_delete = old_de - new_de
        to_generate = new_de - old_de

        audios_deleted = 0
        audios_generated = 0
        audio_errors = 0

        for w in to_delete:
            try:
                r2_key = _safe_tts_key(w, "de")
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
                audios_deleted += 1
            except Exception:
                audio_errors += 1

        for w in to_generate:
            try:
                buf_mp3 = io.BytesIO()
                gTTS(text=w, lang="de").write_to_fp(buf_mp3)
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=_safe_tts_key(w, "de"),
                    Body=buf_mp3.getvalue(),
                    ContentType="audio/mpeg",
                )
                audios_generated += 1
            except Exception:
                audio_errors += 1

        rows_count = sum(1 for line in content.splitlines() if "," in line)
        return {
            "ok": True,
            "r2_bucket": R2_BUCKET_NAME,
            "r2_csv_key": key,
            "rows": rows_count,
            "audios_deleted": audios_deleted,
            "audios_generated": audios_generated,
            "audio_errors": audio_errors,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update deck CSV: {e}")

@app.post("/deck/create")
def create_deck(payload: DeckCreate):
    """Create a new deck and ensure audio exists in Cloudflare R2 only."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")

    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")

    rows = []
    for line in payload.data.splitlines():
        parts = [p.strip() for p in line.split(",", 1)]
        if len(parts) == 2 and all(parts):
            rows.append(parts)

    if not rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    # Upload CSV to R2
    r2_csv_key = f"{R2_BUCKET_NAME}/csv/{name}.csv"
    try:
        buf = io.StringIO()
        csv.writer(buf).writerows(rows)
        data_bytes = buf.getvalue().encode("utf-8")
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_csv_key,
            Body=data_bytes,
            ContentType="text/csv",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload deck CSV: {e}")

    # Update R2 deck index
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    index_list = []
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            index_list = parsed
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=f"Failed to read index: {e}")
    except Exception:
        pass

    updated = False
    for d in index_list:
        if isinstance(d, dict) and d.get("name") == name:
            d["file"] = r2_csv_key
            if payload.folder:
                d["folder"] = _safe_deck_name(payload.folder)
            updated = True
            break
    if not updated:
        entry = {"name": name, "file": r2_csv_key}
        if payload.folder:
            entry["folder"] = _safe_deck_name(payload.folder)
        index_list.append(entry)

    index_updated = False
    index_error = None
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(index_list).encode("utf-8"),
            ContentType="application/json",
        )
        index_updated = True
    except Exception as e:
        index_error = str(e)

    # Pre-generate TTS audio for German words
    r2_uploaded = 0
    r2_skipped = 0
    r2_errors = 0

    for _, de in rows:
        r2_key = _safe_tts_key(de, "de")
        try:
            # Check if exists
            exists = True
            try:
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                exists = code not in ("404", "NoSuchKey", "NotFound")

            if exists:
                r2_skipped += 1
                continue

            # Generate and upload
            buf_mp3 = io.BytesIO()
            gTTS(text=de, lang="de").write_to_fp(buf_mp3)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=r2_key,
                Body=buf_mp3.getvalue(),
                ContentType="audio/mpeg",
            )
            r2_uploaded += 1
        except Exception:
            r2_errors += 1

    # Rebuild the deck index at the end so dropdowns stay in sync
    index_rebuild = None
    try:
        index_rebuild = rebuild_deck_index()
    except Exception as e:
        index_rebuild = {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "r2_bucket": R2_BUCKET_NAME,
        "r2_csv_key": r2_csv_key,
        "rows": len(rows),
        "r2_uploaded": r2_uploaded,
        "r2_skipped": r2_skipped,
        "r2_errors": r2_errors,
        "index_updated": index_updated,
        "index_error": index_error,
        "index_rebuild": index_rebuild,
    }

@app.post("/deck/delete")
def delete_deck(payload: DeckDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")
    csv_key = f"{R2_BUCKET_NAME}/csv/{name}.csv"
    de_words = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=csv_key)
        data = obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(data))
        for row in reader:
            if len(row) >= 2:
                de = row[1].strip()
                if de:
                    de_words.append(de)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    audio_deleted = 0
    audio_errors = 0
    for w in de_words:
        try:
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=_safe_tts_key(w, "de"))
            audio_deleted += 1
        except Exception:
            audio_errors += 1
    csv_deleted = False
    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=csv_key)
        csv_deleted = True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            csv_deleted = False
        else:
            raise HTTPException(status_code=500, detail=str(e))
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    index_updated = False
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            new_list = [d for d in parsed if not (isinstance(d, dict) and (d.get("name") == name or d.get("file") == csv_key))]
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=index_key,
                Body=json.dumps(new_list).encode("utf-8"),
                ContentType="application/json",
            )
            index_updated = True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    index_rebuild = None
    try:
        index_rebuild = rebuild_deck_index()
    except Exception as e:
        index_rebuild = {"ok": False, "error": str(e)}
    return {
        "ok": True,
        "csv_deleted": csv_deleted,
        "audio_deleted": audio_deleted,
        "audio_errors": audio_errors,
        "index_updated": index_updated,
        "index_rebuild": index_rebuild,
    }

@app.post("/deck/rename")
def rename_deck(payload: DeckRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_deck_name(payload.old_name)
    new = _safe_deck_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Deck name required")
    if old == new:
        raise HTTPException(status_code=400, detail="New name must be different")
    old_key = f"{R2_BUCKET_NAME}/csv/{old}.csv"
    new_key = f"{R2_BUCKET_NAME}/csv/{new}.csv"
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=new_key)
        raise HTTPException(status_code=400, detail="Target deck already exists")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=old_key)
        content = obj["Body"].read()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Deck not found")
        raise HTTPException(status_code=500, detail=str(e))
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=new_key, Body=content, ContentType="text/csv")
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rename: {e}")
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    index_updated = False
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and d.get("name") == old:
                    d["name"] = new
                    d["file"] = new_key
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=index_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
            index_updated = True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    index_rebuild = None
    try:
        index_rebuild = rebuild_deck_index()
    except Exception as e:
        index_rebuild = {"ok": False, "error": str(e)}
    return {"ok": True, "old_name": old, "new_name": new, "index_updated": index_updated, "index_rebuild": index_rebuild}

@app.get("/folders")
def get_folders():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    folders_key = f"{R2_BUCKET_NAME}/folders/index.json"
    names = set()
    counts = {}
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict):
                    f = d.get("folder") or "Uncategorized"
                    names.add(f)
                    counts[f] = counts.get(f, 0) + 1
    except Exception:
        pass
    extra = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=folders_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            extra = [str(x) for x in parsed]
    except Exception:
        pass
    for f in extra:
        names.add(f)
        counts.setdefault(f, 0)
    base = [{"name": n, "count": counts.get(n, 0)} for n in names]
    ordered = base
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            name_to_item = {x["name"]: x for x in base}
            ordered = [name_to_item[n] for n in arr if n in name_to_item]
            for x in base:
                if x["name"] not in arr:
                    ordered.append(x)
    except Exception:
        ordered = sorted(base, key=lambda x: x["name"].lower())
    return {"folders": ordered}

@app.post("/folder/create")
def folder_create(payload: FolderCreate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if name not in items:
        items.append(name)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    return {"ok": True, "name": name}

@app.post("/folder/rename")
def folder_rename(payload: FolderRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_deck_name(payload.old_name)
    new = _safe_deck_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if old in items:
        items = [new if x == old else x for x in items]
    if new not in items:
        items.append(new)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    # Update folders order list if present
    try:
        ok = False
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if old in oitems:
            oitems = [new if x == old else x for x in oitems]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
            ok = True
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == old:
                    d["folder"] = new
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    return {"ok": True, "old_name": old, "new_name": new}

@app.post("/folder/delete")
def folder_delete(payload: FolderDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if x != name]
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    # Remove from folders order if present
    try:
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if name in oitems:
            oitems = [x for x in oitems if x != name]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == name:
                    d.pop("folder", None)
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    return {"ok": True, "deleted": name}

@app.post("/deck/move")
def deck_move(payload: DeckMove):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Deck name required")
    folder = _safe_deck_name(payload.folder) if payload.folder else None
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            prev_folder = None
            for d in parsed:
                if isinstance(d, dict) and d.get("name") == name:
                    prev_folder = d.get("folder") or None
                    if folder:
                        d["folder"] = folder
                    else:
                        d.pop("folder", None)
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
            # Update deck order lists: remove from previous, append to target
            try:
                if prev_folder:
                    pkey = _order_decks_key(prev_folder)
                    plist = []
                    try:
                        pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=pkey)
                        pdata = pobj["Body"].read().decode("utf-8")
                        parsed_p = json.loads(pdata)
                        if isinstance(parsed_p, list):
                            plist = parsed_p
                    except Exception:
                        pass
                    if name in plist:
                        plist = [x for x in plist if x != name]
                        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=pkey, Body=json.dumps(plist).encode("utf-8"), ContentType="application/json")
                tkey = _order_decks_key(folder or "root")
                tlist = []
                try:
                    tobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=tkey)
                    tdata = tobj["Body"].read().decode("utf-8")
                    parsed_t = json.loads(tdata)
                    if isinstance(parsed_t, list):
                        tlist = parsed_t
                except Exception:
                    pass
                if name not in tlist:
                    tlist.append(name)
                    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=tkey, Body=json.dumps(tlist).encode("utf-8"), ContentType="application/json")
            except Exception:
                pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"ok": True, "name": name, "folder": folder or None}

@app.post("/decks/index/rebuild")
def rebuild_deck_index():
    """Scan R2 for csv/*.csv and rebuild csv/index.json accordingly."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        items = []
        keep = {}
        try:
            prev = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=f"{R2_BUCKET_NAME}/csv/index.json")
            prev_data = prev["Body"].read().decode("utf-8")
            parsed_prev = json.loads(prev_data)
            if isinstance(parsed_prev, list):
                for d in parsed_prev:
                    if isinstance(d, dict) and d.get("name"):
                        keep[d["name"]] = d.get("folder")
        except Exception:
            pass
        continuation = None
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": f"{R2_BUCKET_NAME}/csv/"}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj.get("Key", "")
                if key.endswith(".csv") and key != f"{R2_BUCKET_NAME}/csv/index.json":
                    base = key.split("/")[-1]
                    name = _safe_deck_name(base[:-4])
                    if name:
                        lm = obj.get("LastModified")
                        items.append({
                            "name": name,
                            "file": key,
                            "last_modified": lm.isoformat() if lm else None,
                            "folder": keep.get(name),
                        })
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break
        # Sort newest-first
        items.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=f"{R2_BUCKET_NAME}/csv/index.json",
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
        return {"ok": True, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rebuild index: {e}")

# -------------------------------
# TTS ROUTES
# -------------------------------
@app.get("/tts")
def tts(text: str, lang: str = "de", slow: bool = False):
    """Stream from R2 if available; otherwise generate in-memory and upload when configured."""
    try:
        if r2_client and R2_BUCKET_NAME:
            key = _safe_tts_key(text, lang)
            
            # Check if exists
            exists = True
            try:
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                exists = code not in ("404", "NoSuchKey", "NotFound")
            
            if exists:
                obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
                return StreamingResponse(obj["Body"], media_type="audio/mpeg")
            
            # Generate and upload
            buf = io.BytesIO()
            gTTS(text=text, lang=lang, slow=slow).write_to_fp(buf)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=buf.getvalue(),
                ContentType="audio/mpeg"
            )
            return StreamingResponse(io.BytesIO(buf.getvalue()), media_type="audio/mpeg")
        
        # No R2: just generate and stream
        buf = io.BytesIO()
        gTTS(text=text, lang=lang, slow=slow).write_to_fp(buf)
        return StreamingResponse(io.BytesIO(buf.getvalue()), media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lines/generate")
def generate_lines(deck: str, limit: int = 100):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    try:
        cards = get_cards(deck)
        cards = cards[: max(1, limit)]
        items = _gemini_generate_lines(cards)
        cleaned = []
        # Build index by German term only (AI may normalize English)
        by_de = {}
        for it in items or []:
            k = (it.get('de') or '').strip().lower()
            if k and k not in by_de:
                by_de[k] = it
        for c in cards:
            de = c.get('de', '')
            en = c.get('en', '')
            chosen = by_de.get((de or '').strip().lower())
            en_raw = en.strip()
            en_clean = re.sub(r"\(.*?\)", "", en_raw).strip()
            if ':' in en_clean:
                en_clean = en_clean.split(':', 1)[0].strip()
            is_verb = en_clean.lower().startswith('to ')
            base = en_clean[3:].strip() if is_verb else en_clean
            bad_en = False
            if chosen:
                le = (chosen.get('line_en') or '').strip().lower()
                bad_en = (not le) or le.startswith('this is') or le.startswith('that is') or le.startswith('i the') or (' to ' in le)
            if chosen:
                cleaned.append({
                    "de": de,
                    "en": en,
                    "line_en": (chosen.get('line_en') or '').strip(),
                    "line_de": (chosen.get('line_de') or '').strip(),
                })
            else:
                cleaned.append({"de": de, "en": en, "line_en": '', "line_de": ''})
        return {"deck": deck, "count": len(cleaned), "items": cleaned}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lines/debug")
def lines_debug(deck: str, limit: int = 10):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    cards = get_cards(deck)[: max(1, limit)]
    
    # Debug: show what key we're using
    print(f"DEBUG: In lines_debug, GEMINI_API_KEY: {GEMINI_API_KEY[:20]}...")
    print(f"DEBUG: Key length: {len(GEMINI_API_KEY) if GEMINI_API_KEY else 'None'}")
    
    try:
        # Make the same request but capture raw response too
        model = "gemini-2.5-flash"
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent?key={GEMINI_API_KEY}"
        )
        print(f"DEBUG: Using endpoint: {endpoint[:100]}...")
        print(f"DEBUG: API key length: {len(GEMINI_API_KEY) if GEMINI_API_KEY else 'None'}")
        vocab_list = "\n".join([f'- {{ "de": "{c["de"]}", "en": "{c["en"]}" }}' for c in cards])
        prompt = f"Generate practical sentences and return ONLY JSON array with fields de,en,line_de,line_en for these pairs:\n{vocab_list}"
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"response_mime_type": "application/json"},
        }
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        items = _gemini_generate_lines(cards)
        return {"deck": deck, "raw": parsed, "items": items}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode()
        except Exception:
            body = ""
        return {"error": f"HTTP {e.code} {e.reason}", "body": body}
    except Exception as e:
        return {"error": str(e), "items": _gemini_generate_lines(cards)}

@app.get("/preload_deck_audio")
async def preload_deck_audio(deck: str, lang: str = "de"):
    """Preload all audio files for a deck and return URLs with concurrent processing."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Get deck data
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    try:
        # Get deck cards
        cards = get_cards(deck)
        
        # Process all audio files concurrently
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        async def process_audio_file(card):
            """Process a single audio file asynchronously."""
            text = card["de"]
            key = _safe_tts_key(text, lang)
            
            def check_and_generate():
                try:
                    # Check if exists
                    r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
                    return text, f"/r2/get?key={key}"
                except ClientError:
                    # Generate and upload if not exists
                    try:
                        buf = io.BytesIO()
                        gTTS(text=text, lang=lang).write_to_fp(buf)
                        buf.seek(0)
                        r2_client.put_object(
                            Bucket=R2_BUCKET_NAME,
                            Key=key,
                            Body=buf.getvalue(),
                            ContentType="audio/mpeg",
                        )
                        return text, f"/r2/get?key={key}"
                    except Exception:
                        # Skip this audio if generation fails
                        return None, None
            
            # Run the blocking operation in a thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=10) as executor:
                result = await loop.run_in_executor(executor, check_and_generate)
                return result
        
        # Process all cards concurrently (limit to 10 concurrent operations)
        semaphore = asyncio.Semaphore(10)
        
        async def process_with_semaphore(card):
            async with semaphore:
                return await process_audio_file(card)
        
        # Execute all tasks concurrently
        tasks = [process_with_semaphore(card) for card in cards]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build audio_urls from results
        audio_urls = {}
        for result in results:
            if isinstance(result, Exception):
                continue  # Skip failed operations
            text, url = result
            if text and url:
                audio_urls[text] = url
        
        return {"audio_urls": audio_urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preload deck audio: {str(e)}")

# -------------------------------
# R2 UTILITIES
# -------------------------------

@app.get("/debug/r2-config")
def debug_r2_config():
    """Debug endpoint to check R2 configuration in deployment."""
    config_info = {
        "r2_configured": bool(r2_client and R2_BUCKET_NAME),
        "bucket_name": R2_BUCKET_NAME,
        "endpoint": R2_ENDPOINT,
        "has_credentials": bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY),
        "account_id": R2_ACCOUNT_ID[:8] + "..." if R2_ACCOUNT_ID else None,
    }
    
    # Test basic R2 connection
    if r2_client and R2_BUCKET_NAME:
        try:
            # Try to list objects with the bucket prefix
            response = r2_client.list_objects_v2(
                Bucket=R2_BUCKET_NAME,
                Prefix=f"{R2_BUCKET_NAME}/csv/",
                MaxKeys=5
            )
            config_info["r2_connection"] = "success"
            config_info["objects_found"] = len(response.get("Contents", []))
            config_info["sample_keys"] = [obj["Key"] for obj in response.get("Contents", [])[:3]]
        except Exception as e:
            config_info["r2_connection"] = "failed"
            config_info["r2_error"] = str(e)
    else:
        config_info["r2_connection"] = "not_configured"
    
    return config_info

@app.get("/r2/get")
def r2_get(key: str):
    """Stream an object from Cloudflare R2 by key."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        stream = obj["Body"]
        content_type = obj.get("ContentType", "application/octet-stream")
        return StreamingResponse(stream, media_type=content_type)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Object not found")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/audio/rebuild")
def audio_rebuild(req: AudioRebuildRequest):
    """Delete old audio (if provided) and regenerate new audio for given text/lang."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    text = (req.text or "").strip()
    lang = (req.lang or "de").strip() or "de"
    old_text = (req.old_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text required")

    try:
        # If old_text differs, remove its audio
        if old_text and old_text != text:
            try:
                old_key = _safe_tts_key(old_text, lang)
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_key)
            except Exception:
                pass

        # Remove current audio (ignore if not exists)
        try:
            cur_key = _safe_tts_key(text, lang)
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=cur_key)
        except Exception:
            pass

        # Generate fresh audio
        buf = io.BytesIO()
        gTTS(text=text, lang=lang).write_to_fp(buf)
        key = _safe_tts_key(text, lang)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=buf.getvalue(),
            ContentType="audio/mpeg",
        )
        return {"ok": True, "key": key, "url": f"/r2/get?key={key}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# ORDER ROUTES
# -------------------------------
@app.get("/order/folders")
def order_folders_get():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            return arr
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order/folders")
def order_folders_set(payload: FolderOrderUpdate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    names = [ _safe_deck_name(x) for x in (payload.order or []) if _safe_deck_name(x) ]
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key(), Body=json.dumps(names).encode("utf-8"), ContentType="application/json")
        return {"ok": True, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/order/decks")
def order_decks_get(scope: str | None = None):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_decks_key(scope))
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            return arr
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order/decks")
def order_decks_set(payload: DeckOrderUpdate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    scope = _safe_deck_name((payload.scope or "root")) or "root"
    names = [ _safe_deck_name(x) for x in (payload.order or []) if _safe_deck_name(x) ]
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=_order_decks_key(scope), Body=json.dumps(names).encode("utf-8"), ContentType="application/json")
        return {"ok": True, "scope": scope, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
