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

# -------------------------------
# MODELS
# -------------------------------
class DeckCreate(BaseModel):
    name: str
    data: str

class DeckUpdate(BaseModel):
    name: str
    content: str

class AudioRebuildRequest(BaseModel):
    text: str
    lang: str = "de"
    old_text: str | None = None

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

# -------------------------------
# BASIC ROUTES
# -------------------------------
@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

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
                        items.append({"name": name, "file": file, "last_modified": lm})

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

@app.post("/decks/index/rebuild")
def rebuild_deck_index():
    """Scan R2 for csv/*.csv and rebuild csv/index.json accordingly."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        items = []
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

if __name__ == "__main__":
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)