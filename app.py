import csv
import os
import re
from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from gtts import gTTS
import uvicorn
import io
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json
from botocore.config import Config

app = FastAPI()

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

# -------------------------------
# MODELS
# -------------------------------
class DeckCreate(BaseModel):
    name: str
    data: str

class FolderCreate(BaseModel):
    prefix: str = "csv/"

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
    return f"tts/{lang}/{safe}.mp3"

# -------------------------------
# BASIC ROUTES
# -------------------------------
@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.head("/")
def head_root():
    return Response(status_code=200)

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
    """List decks from R2 csv/index.json only (no local fallback)."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key="csv/index.json")
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = []
            for d in parsed:
                if isinstance(d, dict):
                    name = d.get("name")
                    file = d.get("file")
                    if name and file and file.startswith("csv/") and file.lower().endswith(".csv"):
                        items.append({"name": name, "file": file})
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
    """Return all cards (EN–DE pairs) from a CSV deck."""
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    if r2_client and R2_BUCKET_NAME:
        key = f"csv/{safe}.csv"
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

    # Local fallback
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
    r2_csv_key = f"csv/{name}.csv"
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
    index_key = "csv/index.json"
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
            updated = True
            break
    if not updated:
        index_list.append({"name": name, "file": r2_csv_key})

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
    }

@app.post("/decks/index/register")
def register_deck(name: str):
    """Register an existing R2 CSV deck in csv/index.json."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    safe = _safe_deck_name(name)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    r2_csv_key = f"csv/{safe}.csv"
    
    # Ensure the CSV exists
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_csv_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Deck CSV not found in R2")
        raise HTTPException(status_code=500, detail=str(e))

    # Load and update index
    index_key = "csv/index.json"
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
        if isinstance(d, dict) and d.get("name") == safe:
            d["file"] = r2_csv_key
            updated = True
            break
    if not updated:
        index_list.append({"name": safe, "file": r2_csv_key})

    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(index_list).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update index: {e}")

    return {"ok": True, "registered": safe, "file": r2_csv_key}

@app.post("/decks/index/rebuild")
def rebuild_deck_index():
    """Scan R2 for csv/*.csv and rebuild csv/index.json accordingly."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        items = []
        continuation = None
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": "csv/"}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj.get("Key", "")
                if key.endswith(".csv") and key != "csv/index.json":
                    base = key.split("/")[-1]
                    name = _safe_deck_name(base[:-4])
                    if name:
                        items.append({"name": name, "file": key})
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key="csv/index.json",
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
        return {"ok": True, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rebuild index: {e}")

@app.post("/decks/ingest_local")
def ingest_local(name: str):
    """Upload local csv/<name>.csv into R2 and update index."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    safe = _safe_deck_name(name)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    local_path = os.path.join("csv", f"{safe}.csv")
    if not os.path.exists(local_path):
        raise HTTPException(status_code=404, detail="Local CSV not found")

    # Upload to R2
    r2_csv_key = f"csv/{safe}.csv"
    try:
        with open(local_path, "rb") as f:
            data_bytes = f.read()
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_csv_key,
            Body=data_bytes,
            ContentType="text/csv",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload CSV to R2: {e}")

    # Update index
    index_key = "csv/index.json"
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
        if isinstance(d, dict) and d.get("name") == safe:
            d["file"] = r2_csv_key
            updated = True
            break
    if not updated:
        index_list.append({"name": safe, "file": r2_csv_key})

    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(index_list).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update index: {e}")

    return {"ok": True, "uploaded": True, "registered": safe, "file": r2_csv_key}

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

@app.get("/r2/tts")
def r2_tts(text: str, lang: str = "de", slow: bool = False):
    """Stream TTS from R2, generating if needed."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")

    key = _safe_tts_key(text, lang)

    # Check if exists
    exists = True
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        exists = code not in ("404", "NoSuchKey", "NotFound")

    if exists:
        try:
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
            return StreamingResponse(obj["Body"], media_type="audio/mpeg")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch object: {e}")

    # Generate and upload
    try:
        buf = io.BytesIO()
        gTTS(text=text, lang=lang, slow=slow).write_to_fp(buf)
        buf.seek(0)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=buf.getvalue(),
            ContentType="audio/mpeg"
        )
        buf.seek(0)
        return StreamingResponse(buf, media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate/upload TTS: {e}")

@app.get("/r2/tts_url")
def r2_tts_url(text: str, lang: str = "de", slow: bool = False, expires: int = 3600):
    """Return presigned URL for TTS audio, generating if needed."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")

    key = _safe_tts_key(text, lang)

    # Check if exists
    exists = True
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            exists = False
        else:
            raise HTTPException(status_code=500, detail=f"Failed to check object: {e}")

    # Generate and upload if missing
    if not exists:
        try:
            buf = io.BytesIO()
            gTTS(text=text, lang=lang, slow=slow).write_to_fp(buf)
            buf.seek(0)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=buf.getvalue(),
                ContentType="audio/mpeg"
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate/upload TTS: {e}")

    # Return presigned URL
    try:
        presigned = r2_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET_NAME, "Key": key},
            ExpiresIn=expires,
        )
        public_url = f"{R2_PUBLIC_URL_BASE.rstrip('/')}/{key}" if R2_PUBLIC_URL_BASE else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate URL: {e}")

    return {"key": key, "url": presigned, "public_url": public_url}

@app.get("/preload_deck_audio")
def preload_deck_audio(deck: str, lang: str = "de", expires: int = 3600):
    """Preload all audio files for a deck and return URLs."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Get deck data
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    try:
        # Get deck cards
        cards = get_cards(deck)
        audio_urls = {}
        
        # Generate or get presigned URLs for each German word
        for card in cards:
            text = card["de"]
            key = _safe_tts_key(text, lang)
            
            try:
                # Check if exists
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
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
                except Exception as e:
                    # Skip this audio if generation fails
                    continue
            
            # Use proxied same-origin URL to avoid CORS
            url = f"/r2/get?key={key}"
            audio_urls[text] = url
            
        return {"audio_urls": audio_urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preload deck audio: {str(e)}")

# -------------------------------
# R2 UTILITIES
# -------------------------------
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

@app.post("/r2/folder/create")
def r2_folder_create(payload: FolderCreate):
    """Create a folder marker in R2."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    prefix = payload.prefix.strip()
    if not prefix:
        raise HTTPException(status_code=400, detail="Prefix required")
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    
    marker_key = f"{prefix}.keep"
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=marker_key,
            Body=b"",
            ContentType="application/octet-stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create folder marker: {e}")
    
    # Verify
    exists = True
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=marker_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        exists = code not in ("404", "NoSuchKey", "NotFound")
    
    return {
        "ok": True,
        "bucket": R2_BUCKET_NAME,
        "prefix": prefix,
        "marker_key": marker_key,
        "created": exists,
    }

@app.get("/r2/folder/status")
def r2_folder_status(prefix: str = "csv/"):
    """Check folder status in R2."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    
    marker_key = f"{prefix}.keep"
    exists = True
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=marker_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        exists = code not in ("404", "NoSuchKey", "NotFound")
    
    # Count keys under prefix
    key_count = 0
    try:
        resp = r2_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix, MaxKeys=1000)
        key_count = resp.get("KeyCount", 0)
    except Exception:
        pass
    
    return {"prefix": prefix, "exists": exists, "key_count": key_count}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)