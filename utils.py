import re
import hashlib

def safe_deck_name(name: str) -> str:
    """Sanitize deck name for file/key usage."""
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip())[:50]

def safe_tts_key(text: str, bucket_name: str, lang: str = "de") -> str:
    """Generate safe R2 key for TTS audio using prefix routing."""
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe:
        safe = "tts"
        
    safe_hash = hashlib.md5(safe.encode("utf-8")).hexdigest()
    prefix = safe_hash[0:2]
    # Cap string length and append hash back to keep it unique
    short_safe = safe[:30]
    
    return f"{bucket_name}/tts/{lang}/{prefix}/{short_safe}_{safe_hash[-8:]}.mp3"
