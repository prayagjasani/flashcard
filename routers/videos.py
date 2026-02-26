import json
import re
import uuid
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, BackgroundTasks
from models import VideoCreate
from services.storage import r2_client, R2_BUCKET_NAME
from services.ai import _generate

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Storage helpers ──────────────────────────────────────────────

VIDEOS_INDEX_KEY = "videos/index.json"


def _get_index() -> list[dict]:
    if not r2_client or not R2_BUCKET_NAME:
        return []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=VIDEOS_INDEX_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return []


def _save_index(index: list[dict]):
    if not r2_client or not R2_BUCKET_NAME:
        return
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=VIDEOS_INDEX_KEY,
        Body=json.dumps(index).encode("utf-8"),
        ContentType="application/json",
    )


def _video_key(video_id: str) -> str:
    return f"videos/{video_id}.json"


# ── SRT parsing ──────────────────────────────────────────────────

_TS_RE = re.compile(
    r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2})[,.](\d{3})"
)


def _ts_to_sec(h, m, s, ms):
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000


def parse_srt(srt_text: str) -> list[dict]:
    """Parse SRT content into list of {start, end, text_de}."""
    blocks = re.split(r"\n\s*\n", srt_text.strip())
    subs = []
    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue
        m = _TS_RE.search(block)
        if not m:
            continue
        start = _ts_to_sec(m.group(1), m.group(2), m.group(3), m.group(4))
        end = _ts_to_sec(m.group(5), m.group(6), m.group(7), m.group(8))
        # Text is everything after the timestamp line
        ts_line_idx = next(
            (i for i, l in enumerate(lines) if _TS_RE.search(l)), 1
        )
        text = " ".join(lines[ts_line_idx + 1:]).strip()
        text = re.sub(r"<[^>]+>", "", text)  # strip HTML tags
        if text:
            subs.append({"start": round(start, 3), "end": round(end, 3), "text_de": text})
    return subs


# ── AI translation ───────────────────────────────────────────────

def translate_subtitles(subs: list[dict]) -> list[dict]:
    """Translate German subtitle lines to English using Gemini AI."""
    if not subs:
        return subs

    BATCH = 30
    for i in range(0, len(subs), BATCH):
        batch = subs[i: i + BATCH]
        lines = [s["text_de"] for s in batch]
        
        # We number from 1 to len(batch) to ensure the AI follows the correct structure.
        numbered = "\n".join(f"{j+1}. {l}" for j, l in enumerate(lines))
        prompt = (
            "Translate each numbered German line to English. "
            "Return a JSON array of objects with keys \"n\" (line number) and \"en\" (English translation). "
            "Preserve the order and ensure you translate every single line provided. Only return the JSON array, nothing else.\n\n"
            f"{numbered}"
        )
        raw = _generate(prompt, timeout=120)
        
        if raw:
            try:
                # Clean up any potential markdown or trailing text from the AI response
                clean_raw = raw.strip()
                if clean_raw.startswith("```json"):
                    clean_raw = clean_raw.replace("```json\n", "", 1)
                if clean_raw.startswith("```"):
                    clean_raw = clean_raw.replace("```\n", "", 1)
                if clean_raw.endswith("```"):
                    clean_raw = clean_raw[:-3].strip()
                    
                # Find the first [ and last ] in case there is conversation text
                start_idx = clean_raw.find("[")
                end_idx = clean_raw.rfind("]")
                if start_idx != -1 and end_idx != -1:
                    clean_raw = clean_raw[start_idx:end_idx+1]

                arr = json.loads(clean_raw)
                for item in arr:
                    # Parse correctly whether the AI returned an int or string for n
                    idx = int(item.get("n", 0)) - 1
                    if 0 <= idx < len(batch):
                        batch[idx]["text_en"] = item.get("en", "")
            except Exception as e:
                logger.error(f"Translation parse error for batch {i//BATCH}: {e}. Raw: {raw[:200]}...")
                
        # Fill any missing translations for this batch
        for s in batch:
            if "text_en" not in s:
                s["text_en"] = ""
    return subs


# ── YouTube helpers ──────────────────────────────────────────────

_YT_RE = re.compile(
    r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([A-Za-z0-9_-]{11})"
)


def extract_youtube_id(url: str) -> str | None:
    m = _YT_RE.search(url)
    return m.group(1) if m else None


# ── Endpoints ────────────────────────────────────────────────────

@router.get("/videos")
def list_videos():
    return {"videos": _get_index()}


@router.post("/videos")
def create_video(req: VideoCreate, background_tasks: BackgroundTasks):
    yt_id = extract_youtube_id(req.youtube_url)
    if not yt_id:
        raise HTTPException(400, "Invalid YouTube URL")

    subs = parse_srt(req.srt_content)
    if not subs:
        raise HTTPException(400, "Could not parse any subtitles from SRT")

    video_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()

    video = {
        "id": video_id,
        "title": req.title.strip(),
        "youtube_url": req.youtube_url.strip(),
        "youtube_id": yt_id,
        "subtitles": subs, # Will be translated in background
        "created_at": now,
        "translating": True # Flag to show UI that it's still translating
    }

    # Save initial video JSON
    if r2_client and R2_BUCKET_NAME:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=_video_key(video_id),
            Body=json.dumps(video).encode("utf-8"),
            ContentType="application/json",
        )

    # Update index
    meta = {
        "id": video_id,
        "title": video["title"],
        "youtube_id": yt_id,
        "subtitle_count": len(subs),
        "created_at": now,
        "translating": True
    }
    idx = _get_index()
    idx.insert(0, meta)
    _save_index(idx)

    # Run translation in background
    background_tasks.add_task(_background_translate, video_id, video)

    return {"ok": True, "video": meta}

def _background_translate(video_id: str, video_data: dict):
    """Translates subtitles in the background and updates storage."""
    try:
        translated_subs = translate_subtitles(video_data["subtitles"])
        video_data["subtitles"] = translated_subs
        video_data["translating"] = False
        
        # Save updated video JSON
        if r2_client and R2_BUCKET_NAME:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=_video_key(video_id),
                Body=json.dumps(video_data).encode("utf-8"),
                ContentType="application/json",
            )
            
        # Update index flag
        idx = _get_index()
        for i, meta in enumerate(idx):
            if meta["id"] == video_id:
                idx[i]["translating"] = False
                break
        _save_index(idx)
        logger.info(f"Successfully background translated video {video_id}")
    except Exception as e:
        logger.error(f"Failed to background translate video {video_id}: {e}")


@router.get("/videos/{video_id}")
def get_video(video_id: str):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(500, "Storage not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_video_key(video_id))
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data
    except Exception:
        raise HTTPException(404, "Video not found")


@router.delete("/videos/{video_id}")
def delete_video(video_id: str):
    # Remove from storage
    if r2_client and R2_BUCKET_NAME:
        try:
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=_video_key(video_id))
        except Exception:
            pass

    # Update index
    idx = _get_index()
    idx = [v for v in idx if v.get("id") != video_id]
    _save_index(idx)

    return {"ok": True}
