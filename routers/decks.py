import csv
import io
import json
import threading
import asyncio
from typing import List
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException
from gtts import gTTS
from botocore.exceptions import ClientError

from models import DeckCreate, DeckUpdate, DeckDelete, DeckRename, DeckMove, DeckOrderUpdate
from services.storage import (
    r2_client, R2_BUCKET_NAME, 
    order_decks_key as _order_decks_key
)
from services.audio import background_audio_generation, background_audio_cleanup_and_generate, _safe_tts_key_helper, _safe_tts_key_helper as _safe_tts_key
from services.cache import invalidate_cache
from utils import safe_deck_name as _safe_deck_name

router = APIRouter()

@router.get("/decks")
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
            items.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
            return items
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cards")
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

@router.get("/deck/csv")
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

@router.post("/deck/create")
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

    # Start background audio generation (non-blocking)
    de_words = [de for _, de in rows]
    background_audio_generation(de_words)

    # Return immediately - audio will be generated in background
    return {
        "ok": True,
        "r2_bucket": R2_BUCKET_NAME,
        "r2_csv_key": r2_csv_key,
        "rows": len(rows),
        "audio_status": "generating_in_background",
        "index_updated": index_updated,
        "index_error": index_error,
    }

@router.post("/deck/update")
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
        # Compute German word changes
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

        # Start background audio cleanup and generation (non-blocking)
        if to_delete or to_generate:
            thread = threading.Thread(
                target=background_audio_cleanup_and_generate, 
                args=(to_delete, to_generate), 
                daemon=True
            )
            thread.start()

        rows_count = sum(1 for line in content.splitlines() if "," in line)
        return {
            "ok": True,
            "r2_bucket": R2_BUCKET_NAME,
            "r2_csv_key": key,
            "rows": rows_count,
            "audio_status": "processing_in_background",
            "words_to_delete": len(to_delete),
            "words_to_generate": len(to_generate),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update deck CSV: {e}")

@router.post("/deck/delete")
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
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=_safe_tts_key_helper(w, "de"))
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

@router.post("/deck/rename")
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

@router.post("/deck/move")
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

@router.post("/decks/index/rebuild")
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
        items.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=f"{R2_BUCKET_NAME}/csv/index.json",
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
        return {"ok": True, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rebuild index: {e}")

@router.get("/preload_deck_audio")
async def preload_deck_audio(deck: str, lang: str = "de"):
    """Preload all audio files for a deck and return URLs with concurrent processing."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Get deck data
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    try:
        # Get deck cards - duplicated local logic to avoid circular import issues with get_cards
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

        # Process all audio files concurrently
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

@router.get("/order/decks")
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

@router.post("/order/decks")
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
