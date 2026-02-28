import json
import io
import csv
import threading
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel

from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from gtts import gTTS
from botocore.exceptions import ClientError

from models import CustomStoryRequest, TextStoryRequest
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
    generate_custom_story as _gemini_generate_custom_story,
    generate_subtitle_story as _gemini_generate_subtitle_story,
)
from services.audio import generate_story_audio_background
from services.cache import get_cached, set_cached, invalidate_cache
from services.deck_service import get_cards as _get_cards_from_service
from utils import safe_deck_name as _safe_deck_name


class YoutubeStoryRequest(BaseModel):
    url: str
    level: str | None = "A2"
    story_id: str | None = None

router = APIRouter()
def _get_cards_helper(deck: str):
    """Get cards for a deck using the shared deck service."""
    return _get_cards_from_service(deck)

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
        
        # Sort by last_modified content
        stories.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
        
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

    # Normalise and validate level (CEFR A1ΓÇôC2)
    level = (payload.level or "A2").upper()
    valid_levels = {"A1", "A2", "B1", "B2", "C1", "C2"}
    if level not in valid_levels:
        level = "A2"
    
    # Generate a unique story ID
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


@router.post("/story/from_text")
def story_from_text(payload: TextStoryRequest):
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required")

    level = (payload.level or "A2").upper()
    valid_levels = {"A1", "A2", "B1", "B2", "C1", "C2"}
    if level not in valid_levels:
        level = "A2"

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="Text has no content")

    story = _gemini_generate_subtitle_story(lines, level=level)
    if not isinstance(story, dict):
        story = {}
    story.setdefault("title_de", "Eigene Geschichte")
    story.setdefault("title_en", "Custom Story");
    story.setdefault("characters", [])
    story.setdefault("level", level)
    story.setdefault("vocabulary", {})
    story.setdefault(
        "segments",
        [
            {
                "type": "narration",
                "speaker": "narrator",
                "text_de": text_line,
                "text_en": "",
                "highlight_pairs": [],
            }
            for text_line in lines
        ],
    )

    segments = story.get("segments") or []
    count = min(len(segments), len(lines))
    segments = segments[:count]
    cleaned_segments = []
    for idx, seg in enumerate(segments):
        if not isinstance(seg, dict):
            seg = {}
        text_de = lines[idx]
        seg.setdefault("type", "narration")
        seg.setdefault("speaker", "narrator")
        seg.setdefault("text_de", text_de)
        seg.setdefault("text_en", "")
        seg.setdefault("highlight_pairs", [])
        cleaned_segments.append(seg)
    story["segments"] = cleaned_segments

    raw_id = payload.story_id or f"text_{int(time.time())}"
    safe_id = _safe_deck_name(raw_id)

    if r2_client and R2_BUCKET_NAME:
        try:
            key = _story_key(safe_id)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(story).encode("utf-8"),
                ContentType="application/json",
            )
            meta = {
                "key": key,
                "deck": safe_id,
                "last_modified": datetime.now().isoformat(),
                "title_de": story.get("title_de"),
                "title_en": story.get("title_en"),
                "level": story.get("level"),
            }
            update_stories_index(meta)
        except Exception:
            pass

        if story.get("segments"):
            thread = threading.Thread(
                target=generate_story_audio_background,
                args=(safe_id, story["segments"]),
                daemon=True,
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


def _timestamp_to_ms(ts: str) -> int | None:
    m = re.match(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})", ts.strip())
    if not m:
        return None
    h, m_, s, ms = map(int, m.groups())
    return ((h * 60 + m_) * 60 + s) * 1000 + ms


def _normalize_subtitle_text(text: str) -> str:
    text = text.strip()
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return text
    upper = sum(1 for ch in letters if ch.isupper())
    # If most letters are uppercase, treat as "all caps" subtitles and normalize
    if upper / len(letters) < 0.6:
        return text
    lowered = text.lower()
    chars = list(lowered)
    for idx, ch in enumerate(chars):
        if ch.isalpha():
            chars[idx] = ch.upper()
            break
    return "".join(chars).strip()


def _parse_srt(content: str):
    blocks = []
    current_lines = []
    for line in content.splitlines():
        line = line.strip("\ufeff")
        if line.strip() == "":
            if current_lines:
                blocks.append(current_lines)
                current_lines = []
            continue
        current_lines.append(line)
    if current_lines:
        blocks.append(current_lines)

    subtitles = []
    for block in blocks:
        start_ms = None
        end_ms = None
        text_lines = []

        if len(block) >= 2 and "-->" in block[1]:
            parts = block[1].split("-->")
            if len(parts) == 2:
                start_ms = _timestamp_to_ms(parts[0])
                end_ms = _timestamp_to_ms(parts[1])
            text_lines = block[2:]
        elif len(block) >= 1:
            text_lines = block[1:]

        text = " ".join(text_lines).strip()
        if text:
            subtitles.append(
                {
                    "text": _normalize_subtitle_text(text),
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            )
    return subtitles


@router.post("/story/upload_srt")
async def upload_srt(file: UploadFile = File(...), level: str = "A2"):
    try:
        raw = await file.read()
        try:
            content = raw.decode("utf-8")
        except UnicodeDecodeError:
            content = raw.decode("latin-1", errors="ignore")
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read subtitle file")

    subtitles = _parse_srt(content)
    if not subtitles:
        raise HTTPException(status_code=400, detail="Subtitle file is empty or invalid")
    norm_level = (level or "A2").upper()

    lines = [s["text"] for s in subtitles]
    story = _gemini_generate_subtitle_story(lines, level=norm_level)
    if not isinstance(story, dict):
        story = {}
    story.setdefault("title_de", file.filename or "Untertitel")
    story.setdefault("title_en", "Subtitles")
    story.setdefault("characters", [])
    story.setdefault("level", norm_level)
    story.setdefault("vocabulary", {})
    story.setdefault(
        "segments",
        [
            {
                "type": "narration",
                "speaker": "narrator",
                "text_de": text,
                "text_en": "",
                "highlight_pairs": [],
            }
            for text in lines
        ],
    )

    # generate_subtitle_story now returns exactly one segment per input line,
    # with text_de already set to the original. Just ensure every subtitle is
    # represented, padding with empty translations if the AI missed any.
    ai_segments = (story.get("segments") or [])
    final_segments = []
    for idx, sub in enumerate(subtitles):
        if idx < len(ai_segments) and isinstance(ai_segments[idx], dict):
            seg = dict(ai_segments[idx])
        else:
            seg = {}
        seg["type"] = seg.get("type") or "narration"
        seg["speaker"] = seg.get("speaker") or "narrator"
        seg["text_de"] = sub["text"]          # always the original SRT text
        seg["text_en"] = seg.get("text_en") or ""
        seg["highlight_pairs"] = seg.get("highlight_pairs") or []
        if sub.get("start_ms") is not None:
            seg["start_ms"] = sub["start_ms"]
        if sub.get("end_ms") is not None:
            seg["end_ms"] = sub["end_ms"]
        final_segments.append(seg)

    story["segments"] = final_segments

    story.setdefault("title_de", file.filename or "Untertitel")
    story.setdefault("title_en", "Subtitles")
    story.setdefault("level", norm_level)

    story_id_raw = (file.filename or "episode").rsplit("/", 1)[-1]
    story_id_base = _safe_deck_name(story_id_raw.rsplit(".", 1)[0]) or "episode"
    story_id = f"{story_id_base}_{int(time.time())}"

    if r2_client and R2_BUCKET_NAME:
        try:
            key = _story_key(story_id)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(story).encode("utf-8"),
                ContentType="application/json",
            )
            meta = {
                "key": key,
                "deck": story_id,
                "last_modified": datetime.now().isoformat(),
                "title_de": story.get("title_de"),
                "title_en": story.get("title_en"),
                "level": story.get("level"),
            }
            update_stories_index(meta)
        except Exception:
            pass

        if story.get("segments"):
            thread = threading.Thread(
                target=generate_story_audio_background,
                args=(story_id, story["segments"]),
                daemon=True,
            )
            thread.start()

    return {"story": story, "story_id": story_id}


# ---------------------------------------------------------------------------
# Retranslate (refresh translations for an existing story)
# ---------------------------------------------------------------------------

@router.post("/story/retranslate")
def story_retranslate(payload: dict):
    """Re-run subtitle AI translation on an existing story using its German lines."""
    story_id = (payload.get("story_id") or "").strip()
    level = (payload.get("level") or "A2").upper()
    if level not in {"A1", "A2", "B1", "B2", "C1", "C2"}:
        level = "A2"
    if not story_id:
        raise HTTPException(status_code=400, detail="story_id is required")

    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=503, detail="R2 storage not configured")

    # Load the existing story
    key = _story_key(story_id)
    try:
        resp = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        existing = json.loads(resp["Body"].read())
    except Exception:
        raise HTTPException(status_code=404, detail="Story not found")

    # Extract German lines in original order
    segments = existing.get("segments") or []
    lines = [(s.get("text_de") or "").strip() for s in segments]
    lines = [l for l in lines if l]
    if not lines:
        raise HTTPException(status_code=422, detail="Story has no German text to retranslate")

    # Re-run AI (now with batched processing that preserves 1:1 mapping)
    refreshed = _gemini_generate_subtitle_story(lines, level=level)
    if not isinstance(refreshed, dict):
        raise HTTPException(status_code=500, detail="AI translation failed")

    # Align regenerated segments back to original German lines
    ai_segs = refreshed.get("segments") or []
    new_segments = []
    for idx, line in enumerate(lines):
        seg = ai_segs[idx] if idx < len(ai_segs) and isinstance(ai_segs[idx], dict) else {}
        # Carry over timing from original if present
        orig = segments[idx] if idx < len(segments) else {}
        new_seg = {
            "type": seg.get("type") or orig.get("type") or "narration",
            "speaker": seg.get("speaker") or orig.get("speaker") or "narrator",
            "text_de": line,                              # always original German
            "text_en": (seg.get("text_en") or "").strip(),
            "highlight_pairs": seg.get("highlight_pairs") or [],
        }
        if orig.get("start_ms") is not None:
            new_seg["start_ms"] = orig["start_ms"]
        if orig.get("end_ms") is not None:
            new_seg["end_ms"] = orig["end_ms"]
        new_segments.append(new_seg)

    existing["segments"] = new_segments
    existing["vocabulary"] = refreshed.get("vocabulary") or {}
    if refreshed.get("title_de"):
        existing.setdefault("title_de", refreshed["title_de"])
    if refreshed.get("title_en"):
        existing.setdefault("title_en", refreshed["title_en"])

    # Save updated story back to R2
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=json.dumps(existing).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:
        pass  # Return the refreshed data even if save fails

    return {"story": existing, "story_id": story_id}



def _extract_video_id(url: str) -> str | None:
    """Extract YouTube video ID from various URL formats."""
    patterns = [
        r"(?:v=|youtu\.be/|/embed/|/shorts/)([A-Za-z0-9_-]{11})",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


# Public Invidious instances ΓÇö these route around YouTube's cloud IP blocks.
# Multiple instances provide redundancy if one is down.
_INVIDIOUS_INSTANCES = [
    "https://invidious.nerdvpn.de",
    "https://inv.tux.pizza",
    "https://iv.ggtyler.dev",
    "https://invidious.privacydev.net",
    "https://yt.artemislena.eu",
]


def _parse_vtt(vtt_text: str) -> list[dict]:
    """Parse WebVTT subtitle format into transcript chunks."""
    chunks = []
    for line in vtt_text.splitlines():
        line = line.strip()
        # Skip header, timestamps, blank lines, cue identifiers
        if not line or "-->" in line or line.startswith("WEBVTT") or line.isdigit():
            continue
        # Strip VTT inline tags like <c>, <00:01:02.000>
        text = re.sub(r"<[^>]+>", "", line).strip()
        if text:
            chunks.append({"text": text})
    return chunks


def _get_transcript_invidious(video_id: str) -> list[dict] | None:
    """Try to get German captions via public Invidious instances."""
    import urllib.request as _req
    headers = {"User-Agent": "Mozilla/5.0 (compatible; flashcard-app/1.0)"}

    for instance in _INVIDIOUS_INSTANCES:
        try:
            # 1. Get caption list
            list_url = f"{instance}/api/v1/captions/{video_id}"
            r1 = _req.Request(list_url, headers=headers)
            with _req.urlopen(r1, timeout=8) as resp:
                caps_data = json.loads(resp.read())

            # 2. Find a German caption track
            german = None
            for cap in caps_data.get("captions", []):
                lang = (cap.get("languageCode") or "").lower()
                label = (cap.get("label") or "").lower()
                if lang.startswith("de") or "german" in label or "deutsch" in label:
                    german = cap
                    break
            if not german:
                continue

            # 3. Fetch the VTT/SRT content
            cap_url = german.get("url") or ""
            if cap_url.startswith("/"):
                cap_url = f"{instance}{cap_url}"
            r2 = _req.Request(cap_url, headers=headers)
            with _req.urlopen(r2, timeout=12) as resp:
                vtt_text = resp.read().decode("utf-8", errors="replace")

            chunks = _parse_vtt(vtt_text)
            if chunks:
                return chunks

        except Exception:
            continue  # Try next Invidious instance

    return None


def _merge_transcript_chunks(chunks: list[dict], max_chars: int = 120) -> list[str]:
    """Merge short transcript chunks into sentence-length lines."""
    lines = []
    buf = ""
    for c in chunks:
        text = (c.get("text") or "").replace("\n", " ").strip()
        if not text:
            continue
        if buf:
            candidate = buf + " " + text
        else:
            candidate = text
        # Break on sentence-ending punctuation or when buffer is long enough
        if len(candidate) >= max_chars or text[-1] in ".!?":
            lines.append(candidate.strip())
            buf = ""
        else:
            buf = candidate
    if buf.strip():
        lines.append(buf.strip())
    return [l for l in lines if l]


@router.post("/story/from_youtube")
def story_from_youtube(payload: YoutubeStoryRequest):
    """Extract German subtitles from a YouTube video and generate a story."""
    url = (payload.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="YouTube URL is required")

    level = (payload.level or "A2").upper()
    if level not in {"A1", "A2", "B1", "B2", "C1", "C2"}:
        level = "A2"

    # Extract video ID
    video_id = _extract_video_id(url)
    if not video_id:
        raise HTTPException(status_code=400, detail="Could not extract video ID from URL")

    # Fetch transcript ΓÇö try Invidious proxies first (bypasses cloud IP blocks),
    # then fall back to youtube-transcript-api.
    transcript = _get_transcript_invidious(video_id)

    if not transcript:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            ytt = YouTubeTranscriptApi()
            try:
                transcript = ytt.fetch(video_id, languages=["de"])
            except Exception:
                transcript_list = ytt.list(video_id)
                transcript = transcript_list.find_generated_transcript(["de"]).fetch()
        except Exception:
            transcript = None

    if not transcript:
        raise HTTPException(
            status_code=422,
            detail=(
                "Could not fetch German subtitles ΓÇö YouTube is blocking requests from this server's IP. "
                "Workaround: download the .srt subtitle file from YouTube "
                "(Video ΓåÆ More ΓåÆ Open transcript ΓåÆ Γï« ΓåÆ Download) and upload it using the SRT button."
            ),
        )

    if not transcript:
        raise HTTPException(status_code=422, detail="No German subtitles found for this video")

    # Merge chunks into readable lines (max ~120 chars)
    lines = _merge_transcript_chunks(transcript, max_chars=120)
    if not lines:
        raise HTTPException(status_code=422, detail="Subtitle text is empty after processing")

    # Cap at 200 lines to keep AI response manageable
    lines = lines[:200]

    # Run through AI subtitle pipeline (same as SRT upload)
    story = _gemini_generate_subtitle_story(lines, level=level)
    if not isinstance(story, dict):
        story = {}

    story.setdefault("title_de", f"YouTube: {video_id}")
    story.setdefault("title_en", "YouTube Video")
    story.setdefault("characters", [])
    story.setdefault("level", level)
    story.setdefault("vocabulary", {})
    story.setdefault(
        "segments",
        [
            {
                "type": "narration",
                "speaker": "narrator",
                "text_de": line,
                "text_en": "",
                "highlight_pairs": [],
            }
            for line in lines
        ],
    )

    # Align segments to lines
    segments = story.get("segments") or []
    count = min(len(segments), len(lines))
    segments = segments[:count]
    cleaned_segments = []
    for idx, seg in enumerate(segments):
        if not isinstance(seg, dict):
            seg = {}
        seg.setdefault("type", "narration")
        seg.setdefault("speaker", "narrator")
        seg.setdefault("text_de", lines[idx])
        seg.setdefault("text_en", "")
        seg.setdefault("highlight_pairs", [])
        cleaned_segments.append(seg)
    story["segments"] = cleaned_segments

    # Build story ID
    raw_id = payload.story_id or f"yt_{video_id}_{int(time.time())}"
    safe_id = _safe_deck_name(raw_id)

    # Save to R2
    if r2_client and R2_BUCKET_NAME:
        try:
            key = _story_key(safe_id)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(story).encode("utf-8"),
                ContentType="application/json",
            )
            meta = {
                "key": key,
                "deck": safe_id,
                "last_modified": datetime.now().isoformat(),
                "title_de": story.get("title_de"),
                "title_en": story.get("title_en"),
                "level": story.get("level"),
            }
            update_stories_index(meta)
        except Exception:
            pass

        if story.get("segments"):
            thread = threading.Thread(
                target=generate_story_audio_background,
                args=(safe_id, story["segments"]),
                daemon=True,
            )
            thread.start()

    return {"story": story, "story_id": safe_id}

