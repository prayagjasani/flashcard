import io
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from gtts import gTTS
from botocore.exceptions import ClientError

from services.storage import (
    r2_client,
    R2_BUCKET_NAME,
    story_audio_key,
    story_audio_prefix,
)
from utils import safe_tts_key

_audio_generation_executor = None


def _get_audio_executor():
    global _audio_generation_executor
    if _audio_generation_executor is None:
        _audio_generation_executor = ThreadPoolExecutor(max_workers=4)
    return _audio_generation_executor


def _safe_tts_key_helper(text: str, lang: str = "de") -> str:
    return safe_tts_key(text, R2_BUCKET_NAME, lang)


def generate_audio_for_word(de_word: str):
    if not r2_client or not R2_BUCKET_NAME or not de_word:
        return
    try:
        r2_key = _safe_tts_key_helper(de_word, "de")
        try:
            r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                return
        buf_mp3 = io.BytesIO()
        gTTS(text=de_word, lang="de").write_to_fp(buf_mp3)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            Body=buf_mp3.getvalue(),
            ContentType="audio/mpeg",
        )
    except Exception:
        pass


def background_audio_generation(words: list):
    if not words:
        return
    executor = _get_audio_executor()
    for w in words:
        executor.submit(generate_audio_for_word, w)


def background_audio_cleanup_and_generate(to_delete: set, to_generate: set):
    for w in to_delete:
        try:
            r2_key = _safe_tts_key_helper(w, "de")
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        except Exception:
            pass
    if to_generate:
        background_audio_generation(list(to_generate))


def _delete_story_audio_prefix(deck: str):
    if not r2_client or not R2_BUCKET_NAME:
        return
    prefix = story_audio_prefix(deck)
    try:
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
            if not resp.get("IsTruncated"):
                break
            continuation = resp.get("NextContinuationToken")
    except Exception:
        pass


def generate_story_audio_background(deck: str, segments: list):
    if not r2_client or not R2_BUCKET_NAME:
        return

    _delete_story_audio_prefix(deck)

    texts_to_generate = set()
    for seg in segments:
        text = (seg.get("text_de") or "").strip()
        if not text:
            continue
        parts = re.split(r"(?<=[.!?])\s+", text)
        for part in parts:
            sentence = part.strip()
            if sentence:
                texts_to_generate.add(sentence)

    for text in texts_to_generate:
        try:
            key = story_audio_key(deck, text)
            try:
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
                continue
            except ClientError:
                pass

            buf = io.BytesIO()
            gTTS(text=text, lang="de").write_to_fp(buf)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=buf.getvalue(),
                ContentType="audio/mpeg",
            )
        except Exception:
            pass
