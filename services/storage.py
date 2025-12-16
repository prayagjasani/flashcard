import os
import re
import logging
import threading
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from utils import safe_deck_name

# Logger for storage operations
logger = logging.getLogger(__name__)

load_dotenv()

R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET") or os.getenv("R2_BUCKET")
R2_PUBLIC_URL_BASE = os.getenv("R2_PUBLIC_URL_BASE")

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


def order_decks_key(scope: str | None) -> str:
    s = safe_deck_name(scope or "root") or "root"
    return f"{R2_BUCKET_NAME}/order/decks/{s}.json"

def lines_key(deck: str) -> str:
    safe = safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/lines/{safe}.json"

def story_key(deck: str) -> str:
    safe = safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/stories/{safe}/story.json"

def story_audio_key(deck: str, text: str) -> str:
    """Generate R2 key for story-specific audio file."""
    safe_deck = safe_deck_name(deck)
    safe_text = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe_text:
        safe_text = "audio"
    return f"{R2_BUCKET_NAME}/stories/{safe_deck}/audio/{safe_text}.mp3"

def story_audio_prefix(deck: str) -> str:
    """Get the prefix for all audio files of a story."""
    safe_deck = safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/stories/{safe_deck}/audio/"

# -----------------
# INDEX HELPERS
# -----------------
import json

# Lock for stories index operations to prevent race conditions
_stories_index_lock = threading.Lock()


def stories_index_key() -> str:
    return f"{R2_BUCKET_NAME}/stories/index.json"

def get_stories_index():
    if not r2_client or not R2_BUCKET_NAME:
        return []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=stories_index_key())
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return []

def update_stories_index(new_story_meta: dict):
    """Update the stories index with new story metadata (thread-safe)."""
    if not r2_client or not R2_BUCKET_NAME:
        return
    
    # Use lock to prevent concurrent read-modify-write race conditions
    with _stories_index_lock:
        current = get_stories_index()
        
        # Remove existing entry if any (by deck name which is unique ID here)
        filtered = [s for s in current if s.get("deck") != new_story_meta.get("deck")]
        filtered.append(new_story_meta)
        
        # Sort by last_modified desc
        try:
            filtered.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
        except Exception as e:
            logger.warning(f"Failed to sort stories index: {e}")
        
        try:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=stories_index_key(),
                Body=json.dumps(filtered).encode("utf-8"),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Failed to update stories index: {e}")

def remove_from_stories_index(deck: str):
    """Remove a story from the index (thread-safe)."""
    if not r2_client or not R2_BUCKET_NAME:
        return
    
    # Use lock to prevent concurrent modifications
    with _stories_index_lock:
        current = get_stories_index()
        filtered = [s for s in current if s.get("deck") != deck]
        
        if len(filtered) != len(current):
            try:
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=stories_index_key(),
                    Body=json.dumps(filtered).encode("utf-8"),
                    ContentType="application/json"
                )
            except Exception as e:
                logger.error(f"Failed to remove from stories index: {e}")
