import io
import re
import json
import csv
import asyncio
import urllib.request
import urllib.error

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from gtts import gTTS
from botocore.exceptions import ClientError

from models import AudioRebuildRequest
from services.storage import (
    r2_client, R2_BUCKET_NAME, 
    lines_key as _lines_key
)
from services.ai import generate_lines as _gemini_generate_lines, GEMINI_API_KEY
from services.executor import get_executor
from services.deck_service import get_cards_silent
from utils import safe_deck_name as _safe_deck_name, safe_tts_key as _safe_tts_key_util

router = APIRouter()

# TTS configuration
MAX_TTS_TEXT_LENGTH = 500  # Maximum characters for TTS input

# Helper access to tts key
def _safe_tts_key(text: str, lang: str = "de") -> str:
    return _safe_tts_key_util(text, R2_BUCKET_NAME, lang)

@router.get("/tts")
def tts(text: str, lang: str = "de", slow: bool = False):
    """Stream from R2 if available; otherwise generate in-memory and upload when configured."""
    # Validate text length to prevent abuse
    if not text or not text.strip():
        raise HTTPException(status_code=400, detail="Text is required")
    if len(text) > MAX_TTS_TEXT_LENGTH:
        raise HTTPException(
            status_code=400, 
            detail=f"Text too long. Maximum {MAX_TTS_TEXT_LENGTH} characters allowed."
        )
    
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

@router.get("/lines/generate")
async def generate_lines(deck: str, limit: int | None = None, refresh: bool = False):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    # Serve cached lines if available (unless explicit refresh requested)
    if not refresh and r2_client and R2_BUCKET_NAME:
        try:
            key = _lines_key(deck)
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
            data = obj["Body"].read().decode("utf-8")
            parsed = json.loads(data)
            if isinstance(parsed, list):
                items = parsed
            elif isinstance(parsed, dict):
                items = parsed.get("items") or []
            else:
                items = []
            if isinstance(limit, int) and limit > 0:
                items = items[:limit]
            return {"deck": deck, "count": len(items), "items": items, "cached": True}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                raise HTTPException(status_code=500, detail=str(e))
        except Exception:
            pass

    try:
        # Use shared deck service instead of duplicating logic
        cards = get_cards_silent(deck)
        if not cards:
            # Try inline fallback if service returns empty
            if r2_client and R2_BUCKET_NAME:
                csv_key = f"{R2_BUCKET_NAME}/csv/{safe}.csv"
                try:
                    obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=csv_key)
                    data = obj["Body"].read().decode("utf-8")
                    reader = csv.reader(io.StringIO(data))
                    for row in reader:
                        if len(row) >= 2:
                            en, de = row[0].strip(), row[1].strip()
                            if en and de:
                                cards.append({"en": en, "de": de})
                except Exception:
                    pass
        
        items = _gemini_generate_lines(cards)
        cleaned = []
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
            # base = en_clean[3:].strip() if is_verb else en_clean
            bad_en = False
            bad_de = False
            if chosen:
                le = (chosen.get('line_en') or '').strip().lower()
                ld = (chosen.get('line_de') or '').strip()
                bad_en = (
                    (not le)
                    or le.startswith('this is')
                    or le.startswith('that is')
                    or le.startswith('i the')
                    or le.startswith('"')
                    or le.startswith('",')
                    or (' to ' in le and is_verb)
                )
                bad_de = (not ld) or ld.startswith('"') or ld.startswith('",')
            if chosen:
                cleaned.append({
                    "de": de,
                    "en": en,
                    "line_en": '' if bad_en else (chosen.get('line_en') or '').strip(),
                    "line_de": '' if bad_de else (chosen.get('line_de') or '').strip(),
                })
            else:
                cleaned.append({"de": de, "en": en, "line_en": '', "line_de": ''})

        # Save to R2 for caching
        saved = False
        if r2_client and R2_BUCKET_NAME:
            try:
                key = _lines_key(deck)
                payload = json.dumps({"deck": deck, "items": cleaned}).encode("utf-8")
                r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=payload, ContentType="application/json")
                saved = True
            except Exception:
                saved = False

        if r2_client and R2_BUCKET_NAME:
            try:
                def process_one_sync(it):
                    text = (it.get("line_de") or "").strip()
                    if not text:
                        return None
                    r2_key = _safe_tts_key(text, "de")
                    try:
                        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
                        return True
                    except ClientError:
                        try:
                            buf = io.BytesIO()
                            gTTS(text=text, lang="de").write_to_fp(buf)
                            buf.seek(0)
                            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=r2_key, Body=buf.getvalue(), ContentType="audio/mpeg")
                            return True
                        except Exception:
                            return None
                
                # Use shared executor instead of creating new one per request
                executor = get_executor()
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(executor, lambda: [process_one_sync(it) for it in cleaned])
            except Exception:
                pass
                
        if isinstance(limit, int) and limit > 0:
            cleaned = cleaned[:limit]
            
        return {"deck": deck, "count": len(cleaned), "items": cleaned, "cached": False, "saved": saved}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lines/debug")
def lines_debug(deck: str, limit: int | None = None):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    # Inline get_cards logic again to avoid circular dependeny
    cards = []
    if r2_client and R2_BUCKET_NAME:
        csv_key = f"{R2_BUCKET_NAME}/csv/{safe}.csv"
        try:
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=csv_key)
            data = obj["Body"].read().decode("utf-8")
            reader = csv.reader(io.StringIO(data))
            for row in reader:
                if len(row) >= 2:
                    en, de = row[0].strip(), row[1].strip()
                    if de:
                        cards.append({"de": de, "en": en})
        except Exception:
            pass

    if isinstance(limit, int) and limit > 0:
        cards = cards[:limit]
    
    try:
        # Make the same request but capture raw response too
        model = "gemini-2.5-flash"
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent?key={GEMINI_API_KEY}"
        )
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

@router.get("/preload_lines_audio")
async def preload_lines_audio(deck: str, lang: str = "de"):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    try:
        key = _lines_key(deck)
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        items = parsed.get("items") if isinstance(parsed, dict) else parsed
        items = items or []
        async def process_one(it):
            text = (it.get("line_de") or "").strip()
            if not text:
                return None, None
            r2_key = _safe_tts_key(text, lang)
            def check_and_generate():
                try:
                    r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
                    return text, f"/r2/get?key={r2_key}"
                except ClientError:
                    try:
                        buf = io.BytesIO()
                        gTTS(text=text, lang=lang).write_to_fp(buf)
                        buf.seek(0)
                        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=r2_key, Body=buf.getvalue(), ContentType="audio/mpeg")
                        return text, f"/r2/get?key={r2_key}"
                    except Exception:
                        return None, None
            # Use shared executor
            executor = get_executor()
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(executor, check_and_generate)
        sem = asyncio.Semaphore(10)
        async def with_sem(it):
            async with sem:
                return await process_one(it)
        tasks = [with_sem(it) for it in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        audio_urls = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            t, u = r
            if t and u:
                audio_urls[t] = u
        return {"audio_urls": audio_urls}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return {"audio_urls": {}}
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/audio/rebuild")
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
