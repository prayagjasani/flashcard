import re

def safe_deck_name(name: str) -> str:
    """Sanitize deck name for file/key usage."""
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip())[:50]

def safe_tts_key(text: str, bucker_name: str, lang: str = "de") -> str:
    """Generate safe R2 key for TTS audio."""
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe:
        safe = "tts"
    return f"{bucker_name}/tts/{lang}/{safe}.mp3"
