import json
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from gtts import gTTS
from botocore.exceptions import ClientError

from models import CustomStoryRequest
from services.storage import (
    r2_client, R2_BUCKET_NAME, 
    story_key as _story_key, 
    story_audio_key as _story_audio_key,
    story_audio_prefix as _story_audio_prefix,
    get_stories_index,
    update_stories_index,
    remove_from_stories_index,
    stories_index_key
)
from services.ai import (
    generate_story as _gemini_generate_story, 
    generate_custom_story as _gemini_generate_custom_story
)
from services.audio import generate_story_audio_background
from services.cache import get_cached, set_cached, invalidate_cache
from routers.decks import get_cards # We need get_cards logic. It's in new router. 
# Problem: circular import or just duplicating logic? 
# Better to import get_cards from a common place. 
# But get_cards is a route handler. 
# I should extract get_cards logic to a helper in services or utils.
# For now, I will duplicate the simple get_cards logic or re-implement it to avoid dependency on routers.decks
from utils import safe_deck_name as _safe_deck_name

router = APIRouter()

import csv
import io

# Helper specific for stories.py to avoid circular import with routers.decks
def _get_cards_helper(deck: str):
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
    raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")

def _rebuild_stories_index_internal():
    if not r2_client or not R2_BUCKET_NAME:
        return []
    
    try:
        story_keys = []
        prefix = f"{R2_BUCKET_NAME}/stories/"
        continuation = None
        
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            
            for obj in resp.get("Contents", []):
                key = obj.get("Key", "")
                last_modified = obj.get("LastModified").isoformat() if obj.get("LastModified") else None
                
                # New structure: stories/{deck}/story.json
                if key.endswith("/story.json"):
                    parts = key.split("/")
                    if len(parts) >= 3:
                        name = parts[-2]
                        story_keys.append({"key": key, "deck": name, "last_modified": last_modified})
                # Old structure: stories/{deck}.json
                elif key.endswith(".json") and "/audio/" not in key:
                    parts = key.split("/")
                    if len(parts) == 3:
                        name = parts[-1].replace(".json", "")
                        story_keys.append({"key": key, "deck": name, "last_modified": last_modified})
            
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break
        
        def fetch_story_metadata(item):
            story_info = {
                "deck": item["deck"],
                "last_modified": item["last_modified"],
                "title_de": None,
                "title_en": None,
                "level": None,
            }
            try:
                story_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=item["key"])
                story_data = json.loads(story_obj["Body"].read().decode("utf-8"))
                story_info["title_de"] = story_data.get("title_de")
                story_info["title_en"] = story_data.get("title_en")
                story_info["level"] = story_data.get("level")
            except Exception:
                pass
            return story_info
        
        stories = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            stories = list(executor.map(fetch_story_metadata, story_keys))
        
        # Sort by last_modified desc
        stories.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
        
        # Update index
        try:
             key = stories_index_key()
             
             r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(stories).encode("utf-8"),
                ContentType="application/json"
             )
        except Exception:
             pass
             
        return stories
    except Exception:
        return []

@router.post("/stories/rebuild-index")
def rebuild_stories_index():
    stories = _rebuild_stories_index_internal()
    invalidate_cache("stories_list")
    return {"ok": True, "count": len(stories)}

@router.get("/stories/list")
def list_stories():
    """List all available generated stories using index."""
    cached = get_cached("stories_list", 60)
    if cached:
        return {"stories": cached}
    
    stories = get_stories_index()
    if not stories:
        stories = _rebuild_stories_index_internal()
    
    set_cached("stories_list", stories)
    return {"stories": stories}

@router.get("/story/generate")
def generate_story(deck: str, refresh: bool = False):
    """Generate or retrieve a narrative story for a deck."""
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    # Check cache first (unless refresh requested)
    if not refresh and r2_client and R2_BUCKET_NAME:
        # Try new structure first: stories/{deck}/story.json
        try:
            key = _story_key(deck)
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
            data = obj["Body"].read().decode("utf-8")
            cached = json.loads(data)
            if cached and cached.get("segments"):
                return {"story": cached, "cached": True}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                raise HTTPException(status_code=500, detail=str(e))
        except Exception:
            pass
        
        # Try old structure for backwards compatibility: stories/{deck}.json
        try:
            old_key = f"{R2_BUCKET_NAME}/stories/{safe}.json"
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=old_key)
            data = obj["Body"].read().decode("utf-8")
            cached = json.loads(data)
            if cached and cached.get("segments"):
                return {"story": cached, "cached": True}
        except ClientError:
            pass
        except Exception:
            pass

    # Get deck cards
    try:
        cards = _get_cards_helper(deck)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not cards:
        raise HTTPException(status_code=400, detail="Deck is empty")

    # If refreshing, delete old audio files first
    if refresh and r2_client and R2_BUCKET_NAME:
        try:
            prefix = _story_audio_prefix(deck)
            continuation = None
            while True:
                kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
                if continuation:
                    kwargs["ContinuationToken"] = continuation
                resp = r2_client.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []):
                    try:
                        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=obj["Key"])
                    except Exception:
                        pass
                if resp.get("IsTruncated"):
                    continuation = resp.get("NextContinuationToken")
                else:
                    break
        except Exception:
            pass

    # Generate story
    story = _gemini_generate_story(cards, deck)
    if not story:
        raise HTTPException(status_code=500, detail="Failed to generate story")

    # For deck-based stories, mark an approximate level so UI can label it
    if isinstance(story, dict):
        story.setdefault("level", "A1-B1")

    # Cache the story
    if r2_client and R2_BUCKET_NAME:
        try:
            key = _story_key(deck)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(story).encode("utf-8"),
                ContentType="application/json"
            )
        except Exception:
            pass
            
        # Update stories index
        if story and isinstance(story, dict):
             meta = {
                 "key": key,
                 "deck": _safe_deck_name(deck),
                 "last_modified": datetime.now().isoformat(),
                 "title_de": story.get("title_de"),
                 "title_en": story.get("title_en"),
                 "level": story.get("level")
             }
             update_stories_index(meta)

    # Generate audio in background
    if story and story.get("segments"):
        thread = threading.Thread(
            target=generate_story_audio_background,
            args=(deck, story["segments"]),
            daemon=True
        )
        thread.start()

    return {"story": story, "cached": False}

@router.post("/story/generate/custom")
def generate_custom_story(payload: CustomStoryRequest):
    """Generate a story based on a custom topic."""
    topic = (payload.topic or "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="Topic is required")

    # Normalise and validate level (CEFR A1–C2)
    level = (payload.level or "A2").upper()
    valid_levels = {"A1", "A2", "B1", "B2", "C1", "C2"}
    if level not in valid_levels:
        level = "A2"
    
    # Generate a unique story ID
    import time
    story_id = payload.story_id or f"custom_{int(time.time())}"
    safe_id = _safe_deck_name(story_id)
    
    # Generate story with custom topic
    story = _gemini_generate_custom_story(topic, level=level)
    if not story:
        raise HTTPException(status_code=500, detail="Failed to generate story")

    # Attach level metadata so it can be shown in the UI
    if isinstance(story, dict):
        story.setdefault("level", level)
    
    # Cache the story
    if r2_client and R2_BUCKET_NAME:
        try:
            key = f"{R2_BUCKET_NAME}/stories/{safe_id}/story.json"
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(story).encode("utf-8"),
                ContentType="application/json"
            )
        except Exception:
            pass
            
        # Update stories index
        if story and isinstance(story, dict):
             meta = {
                 "key": f"{R2_BUCKET_NAME}/stories/{safe_id}/story.json",
                 "deck": safe_id,
                 "last_modified": datetime.now().isoformat(),
                 "title_de": story.get("title_de"),
                 "title_en": story.get("title_en"),
                 "level": story.get("level")
             }
             update_stories_index(meta)

    # Generate audio in background
    if story and story.get("segments"):
        thread = threading.Thread(
            target=generate_story_audio_background,
            args=(safe_id, story["segments"]),
            daemon=True
        )
        thread.start()
    
    return {"story": story, "story_id": safe_id}

@router.delete("/story/delete")
def delete_story(deck: str):
    """Delete a generated story and all its audio files."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    deleted_files = 0
    errors = 0
    
    # Delete NEW structure: all files in the story folder (stories/{deck}/)
    story_prefix = f"{R2_BUCKET_NAME}/stories/{safe}/"
    try:
        continuation = None
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": story_prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            
            for obj in resp.get("Contents", []):
                try:
                    r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=obj["Key"])
                    deleted_files += 1
                except Exception:
                    errors += 1
            
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break
    except Exception:
        pass
    
    # Also delete OLD structure: stories/{deck}.json (for backwards compatibility)
    old_key = f"{R2_BUCKET_NAME}/stories/{safe}.json"
    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_key)
        deleted_files += 1
    except Exception:
        pass
    
    remove_from_stories_index(deck)
    invalidate_cache("stories_list")
    
    return {
        "ok": True,
        "deleted": deck,
        "files_deleted": deleted_files,
        "errors": errors
    }

@router.get("/story/audio")
def get_story_audio(deck: str, text: str):
    """Get or generate audio for a story segment."""
    if not r2_client or not R2_BUCKET_NAME:
        # Fallback to regular TTS
        try:
            buf = io.BytesIO()
            gTTS(text=text, lang="de").write_to_fp(buf)
            return StreamingResponse(io.BytesIO(buf.getvalue()), media_type="audio/mpeg")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    key = _story_audio_key(deck, text)
    
    # Try to get from cache
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return StreamingResponse(obj["Body"], media_type="audio/mpeg")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    
    # Generate and cache
    try:
        buf = io.BytesIO()
        gTTS(text=text, lang="de").write_to_fp(buf)
        audio_data = buf.getvalue()
        
        # Save to R2
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=audio_data,
            ContentType="audio/mpeg"
        )
        
        return StreamingResponse(io.BytesIO(audio_data), media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
