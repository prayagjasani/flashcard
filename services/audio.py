import io
import threading
from concurrent.futures import ThreadPoolExecutor
from gtts import gTTS
from botocore.exceptions import ClientError

from services.storage import r2_client, R2_BUCKET_NAME, story_audio_key
from utils import safe_tts_key

# Background task queue for audio generation
_audio_generation_executor = None

def _get_audio_executor():
    global _audio_generation_executor
    if _audio_generation_executor is None:
        _audio_generation_executor = ThreadPoolExecutor(max_workers=4)
    return _audio_generation_executor

def _safe_tts_key_helper(text: str, lang: str = "de") -> str:
    return safe_tts_key(text, R2_BUCKET_NAME, lang)

def generate_audio_for_word(de_word: str):
    """Generate TTS audio for a single word (background task)."""
    if not r2_client or not R2_BUCKET_NAME or not de_word:
        return
    try:
        r2_key = _safe_tts_key_helper(de_word, "de")
        # Check if exists
        try:
            r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
            return  # Already exists
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                return
        # Generate and upload
        buf_mp3 = io.BytesIO()
        gTTS(text=de_word, lang="de").write_to_fp(buf_mp3)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            Body=buf_mp3.getvalue(),
            ContentType="audio/mpeg",
        )
    except Exception:
        pass  # Silently fail in background

def background_audio_generation(words: list):
    """Generate audio for all words in background with parallel processing."""
    if not words:
        return
    executor = _get_audio_executor()
    # Submit all words for parallel processing
    futures = [executor.submit(generate_audio_for_word, w) for w in words]
    # Don't wait - let them complete in background

def background_audio_cleanup_and_generate(to_delete: set, to_generate: set):
    """Delete old audio and generate new audio in background."""
    # Delete old audio files
    for w in to_delete:
        try:
            r2_key = _safe_tts_key_helper(w, "de")
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        except Exception:
            pass
    # Generate new audio files in parallel
    if to_generate:
        background_audio_generation(list(to_generate))

def generate_story_audio_background(deck: str, segments: list):
    """Generate all audio files for a story in background."""
    if not r2_client or not R2_BUCKET_NAME:
        return
    
    texts_to_generate = set()
    for seg in segments:
        text = (seg.get("text_de") or "").strip()
        if text:
            texts_to_generate.add(text)
    
    for text in texts_to_generate:
        try:
            key = story_audio_key(deck, text)
            # Check if already exists
            try:
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
                continue  # Already exists
            except ClientError:
                pass
            
            # Generate and upload
            buf = io.BytesIO()
            gTTS(text=text, lang="de").write_to_fp(buf)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=buf.getvalue(),
                ContentType="audio/mpeg"
            )
        except Exception:
            pass
