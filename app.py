import csv
import os
import re
import io
import json
import base64
import urllib.request
import urllib.error

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from gtts import gTTS
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import uvicorn

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Load environment variables from a local .env file if present
load_dotenv()

# Cloudflare R2 (S3 API) configuration via environment variables
R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET") or os.getenv("R2_BUCKET")
R2_PUBLIC_URL_BASE = os.getenv("R2_PUBLIC_URL_BASE")

# Endpoint can be provided directly or derived from account id
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

# Gemini API - Force load from .env file to override system env vars
load_dotenv(override=True)  # Force override system environment variables
GEMINI_API_KEY = os.getenv("gemini_api_key") or os.getenv("GEMINI_API_KEY")

def _gemini_generate_lines(cards):
    if not GEMINI_API_KEY:
        return []

    model = "gemini-2.5-flash"
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )

    def run_chunk(chunk):
        vocab_list = "\n".join([f'- {{ "de": "{c["de"]}", "en": "{c["en"]}" }}' for c in chunk])
        prompt = f"""
You are an expert German language teacher.

Generate PRACTICAL, REAL-LIFE example sentences for A1–B1 learners.

Output ONLY a JSON array with objects of fields: de,en,line_de,line_en.

Echo the input values for fields de and en exactly as provided.

Sentences 8–14 words; daily-life contexts; not literal translations; correct German grammar.

Vocabulary:
{vocab_list}
"""
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
        candidates = parsed.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                p0 = parts[0]
                if isinstance(p0, dict) and "text" in p0:
                    return json.loads(p0["text"])  
                if isinstance(p0, dict) and "inlineData" in p0:
                    data_b64 = p0["inlineData"].get("data", "")
                    if data_b64:
                        raw_json = base64.b64decode(data_b64).decode("utf-8")
                        return json.loads(raw_json)
        if isinstance(parsed, list):
            return parsed
        return []

    all_items = []
    CHUNK_SIZE = 30
    i = 0
    while i < len(cards):
        chunk = cards[i:i+CHUNK_SIZE]
        try:
            res = run_chunk(chunk) or []
            if isinstance(res, list):
                all_items.extend(res)
        except Exception:
            pass  # Skip failed chunks silently
        i += CHUNK_SIZE
    return all_items

# -------------------------------
# MODELS
# -------------------------------
class DeckCreate(BaseModel):
    name: str
    data: str
    folder: str | None = None

class DeckUpdate(BaseModel):
    name: str
    content: str
class DeckDelete(BaseModel):
    name: str
class DeckRename(BaseModel):
    old_name: str
    new_name: str

class AudioRebuildRequest(BaseModel):
    text: str
    lang: str = "de"
    old_text: str | None = None
class FolderCreate(BaseModel):
    name: str
class FolderRename(BaseModel):
    old_name: str
    new_name: str
class FolderDelete(BaseModel):
    name: str
class FolderMove(BaseModel):
    name: str
    parent: str | None = None
class DeckMove(BaseModel):
    name: str
    folder: str | None = None
class FolderOrderUpdate(BaseModel):
    order: list[str]
class DeckOrderUpdate(BaseModel):
    scope: str | None = None
    order: list[str]

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def _safe_deck_name(name: str) -> str:
    """Sanitize deck name for file/key usage."""
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip())[:50]

def _safe_tts_key(text: str, lang: str = "de") -> str:
    """Generate safe R2 key for TTS audio."""
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe:
        safe = "tts"
    return f"{R2_BUCKET_NAME}/tts/{lang}/{safe}.mp3"

def _order_folders_key() -> str:
    return f"{R2_BUCKET_NAME}/order/folders.json"

def _order_decks_key(scope: str | None) -> str:
    s = _safe_deck_name(scope or "root") or "root"
    return f"{R2_BUCKET_NAME}/order/decks/{s}.json"

def _lines_key(deck: str) -> str:
    safe = _safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/lines/{safe}.json"

# -------------------------------
# BASIC ROUTES
# -------------------------------
@app.get("/")
def read_root():
    return FileResponse('templates/index.html')

@app.get("/learn")
def learn_screen():
    return FileResponse('templates/hi.html')

@app.get("/match")
def match_screen():
    return FileResponse('templates/match.html')

@app.get("/spelling")
def spelling_screen():
    return FileResponse('templates/spelling.html')

@app.get("/line")
def line_screen():
    return FileResponse('templates/line.html')

@app.get("/story")
def story_screen():
    return FileResponse('templates/story.html')

def _story_key(deck: str) -> str:
    safe = _safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/stories/{safe}/story.json"

def _story_audio_key(deck: str, text: str) -> str:
    """Generate R2 key for story-specific audio file."""
    safe_deck = _safe_deck_name(deck)
    safe_text = re.sub(r"[^A-Za-z0-9_\-]", "_", text).strip("_")
    if not safe_text:
        safe_text = "audio"
    return f"{R2_BUCKET_NAME}/stories/{safe_deck}/audio/{safe_text}.mp3"

def _story_audio_prefix(deck: str) -> str:
    """Get the prefix for all audio files of a story."""
    safe_deck = _safe_deck_name(deck)
    return f"{R2_BUCKET_NAME}/stories/{safe_deck}/audio/"

def _gemini_generate_story(cards, deck_name: str):
    """Generate an actual narrative story using vocabulary from the deck."""
    if not GEMINI_API_KEY:
        return None

    model = "gemini-2.5-flash"
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )

    # Pick 8-12 words for a short story
    import random
    selected = cards[:12] if len(cards) <= 12 else random.sample(cards, 12)
    vocab_list = "\n".join([f'- {c["de"]} ({c["en"]})' for c in selected])

    # Pick a random story theme for variety
    story_themes = [
        "a hilarious misunderstanding at a café where someone orders completely the wrong thing",
        "a mini mystery where something goes missing and friends must find it",
        "an awkward first date with unexpected surprises",
        "a chaotic day where everything goes wrong but ends well",
        "a funny competition between friends or neighbors",
        "a surprise party with last-minute disasters",
        "a mix-up that leads to an unexpected adventure",
        "a bet between friends with silly consequences",
        "someone trying to impress someone else but failing hilariously",
        "a day trip that doesn't go as planned at all",
    ]
    theme = random.choice(story_themes)

    prompt = f"""You are a comedy writer creating SHORT, PUNCHY stories for German learners. Think sitcom vibes!

Create a funny, memorable story using these vocabulary words:
{vocab_list}

STORY THEME: {theme}

CRITICAL RULES FOR ENGAGING STORIES:
1. START with action or dialogue - NO boring intros like "Anna is a student" or "It is a sunny day"
2. Create 2-3 characters with DISTINCT personalities (one nervous, one confident, one sarcastic, etc.)
3. By segment 2 or 3, introduce a CLEAR PROBLEM or goal (e.g. something is lost, a plan goes wrong, someone makes a mistake, someone wants to impress another person)
4. Make the problem WORSE or more complicated before it gets better
5. Include at least ONE unexpected twist or surprise
6. Show how the characters FEEL (embarrassed, excited, stressed, relieved, etc.) and let this affect what they say
7. End with a punchline, callback, or satisfying resolution where something has CHANGED (a decision, a relationship, a plan, etc.)
8. Keep dialogue snappy - like how real people talk!

STRUCTURE (8-12 segments):
- Hook: Start in the middle of action or with intriguing dialogue
- Problem: The situation becomes difficult, awkward, or risky
- Escalation: Complications and misunderstandings
- Twist: Something unexpected happens
- Resolution: Funny or heartwarming ending

STYLE:
- At least half of the segments should be DIALOGUE
- The remaining segments should be NARRATION that adds tension, emotion, or humor (not just describing the weather)
- Use the given theme directly in the plot

AVOID:
- Generic openings ("Today is a nice day", "Anna wakes up")
- Simple "perfect day" stories where nothing really goes wrong or changes
- Characters just listing what they are doing
- Stories that only describe the location (beach, park, home) without a real problem
- Predictable storylines
- Flat, emotionless dialogue

Use simple German (A1-B1), but make it DRAMATIC, FUNNY, and MEMORABLE!

Output ONLY a JSON object with this exact structure:
{{
  "title_de": "Catchy German title",
  "title_en": "Catchy English title",
  "characters": ["Name1", "Name2"],
  "vocabulary": {{
    "german_word": "english meaning",
    "Flughafen": "airport",
    "sind": "are",
    "am": "at the",
    "angekommen": "arrived"
  }},
  "segments": [
    {{
      "type": "narration" or "dialogue",
      "speaker": "narrator" or character name,
      "text_de": "German text",
      "text_en": "English translation",
      "highlight_words": ["word1", "word2"]
    }}
  ]
}}

The "vocabulary" object MUST contain EVERY German word used in all segments with its English translation.
Include common words like articles (der, die, das = the), verbs (ist = is, sind = are), etc.
The highlight_words should contain vocabulary words from the input list that appear in that segment.

Remember: The best language learning happens when students are entertained and want to know what happens next!"""

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"},
    }
    
    try:
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        candidates = parsed.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                p0 = parts[0]
                if isinstance(p0, dict) and "text" in p0:
                    return json.loads(p0["text"])
        return None
    except Exception as e:
        print(f"Story generation error: {e}")
        return None

def _generate_story_audio_background(deck: str, segments: list):
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
            key = _story_audio_key(deck, text)
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

@app.get("/story/generate")
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
        cards = get_cards(deck)
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
    try:
        if isinstance(story, dict):
            story.setdefault("level", "A1-B1")
    except Exception:
        pass

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

    # Generate audio in background
    if story.get("segments"):
        thread = threading.Thread(
            target=_generate_story_audio_background,
            args=(deck, story["segments"]),
            daemon=True
        )
        thread.start()

    return {"story": story, "cached": False}

class CustomStoryRequest(BaseModel):
    topic: str
    story_id: str | None = None
    level: str | None = "A2"

@app.post("/story/generate/custom")
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
    try:
        if isinstance(story, dict):
            story.setdefault("level", level)
    except Exception:
        pass
    
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
    
    # Generate audio in background
    if story.get("segments"):
        thread = threading.Thread(
            target=_generate_story_audio_background,
            args=(safe_id, story["segments"]),
            daemon=True
        )
        thread.start()
    
    return {"story": story, "story_id": safe_id}

def _gemini_generate_custom_story(topic: str, level: str = "A2"):
    """Generate a story based on a custom topic using Gemini."""
    if not GEMINI_API_KEY:
        return None
    
    model = "gemini-2.5-flash"
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )
    
    prompt = f"""You are a comedy writer creating SHORT, PUNCHY stories for German learners.
The target CEFR level is {level}. Adjust the vocabulary and grammar to match this level
(A1 = very simple everyday language, C2 = very advanced, natural native-like language).

Create a funny, memorable story about: {topic}

CRITICAL RULES FOR ENGAGING STORIES:
1. START with action or dialogue - NO boring intros like "Anna is a student" or "It is a sunny day"
2. Create 2-3 characters with DISTINCT personalities (one nervous, one confident, one sarcastic, etc.)
3. By segment 2 or 3, introduce a CLEAR PROBLEM or goal (e.g. something is lost, a plan goes wrong, someone makes a mistake, someone wants to impress another person)
4. Make the problem WORSE or more complicated before it gets better
5. Include at least ONE unexpected twist or surprise
6. Show how the characters FEEL (embarrassed, excited, stressed, relieved, etc.) and let this affect what they say
7. End with a punchline, callback, or satisfying resolution where something has CHANGED (a decision, a relationship, a plan, etc.)
8. Keep dialogue snappy - like how real people talk!

STRUCTURE (8-12 segments):
- Hook: Start in the middle of action or with intriguing dialogue
- Problem: The situation becomes difficult, awkward, or risky
- Escalation: Complications and misunderstandings
- Twist: Something unexpected happens
- Resolution: Funny or heartwarming ending

STYLE:
- At least half of the segments should be DIALOGUE
- The remaining segments should be NARRATION that adds tension, emotion, or humor (not just describing the weather)

AVOID:
- Generic openings ("Today is a nice day", "Anna wakes up")
- Simple "perfect day" stories where nothing really goes wrong or changes
- Characters just listing what they are doing
- Stories that only describe the location (beach, park, home) without a real problem
- Predictable storylines
- Flat, emotionless dialogue

Use German that is mostly at level {level}, but make it DRAMATIC, FUNNY, and MEMORABLE!

Output ONLY a JSON object with this exact structure:
{{
  "title_de": "Catchy German title",
  "title_en": "Catchy English title",
  "characters": ["Name1", "Name2"],
  "vocabulary": {{
    "german_word": "english meaning",
    "der": "the",
    "ist": "is",
    "und": "and"
  }},
  "segments": [
    {{
      "type": "narration" or "dialogue",
      "speaker": "narrator" or character name,
      "text_de": "German text",
      "text_en": "English translation",
      "highlight_words": ["key", "vocabulary", "words"]
    }}
  ]
}}

The "vocabulary" object MUST contain EVERY German word used in all segments with its English translation.
Include common words like articles (der, die, das = the), verbs (ist = is, sind = are), prepositions, etc.

Remember: The best language learning happens when students are entertained and want to know what happens next!"""

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"},
    }
    
    try:
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        candidates = parsed.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                p0 = parts[0]
                if isinstance(p0, dict) and "text" in p0:
                    return json.loads(p0["text"])
        return None
    except Exception as e:
        print(f"Custom story generation error: {e}")
        return None

@app.get("/story/audio")
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

@app.get("/stories/list")
def list_stories():
    """List all available generated stories with their titles."""
    if not r2_client or not R2_BUCKET_NAME:
        return {"stories": []}
    
    try:
        # First pass: collect all story keys
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
        
        # Second pass: fetch story metadata in parallel
        import concurrent.futures
        
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            stories = list(executor.map(fetch_story_metadata, story_keys))
        
        return {"stories": stories}
    except Exception as e:
        return {"stories": [], "error": str(e)}

@app.delete("/story/delete")
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
    
    return {
        "ok": True,
        "deleted": deck,
        "files_deleted": deleted_files,
        "errors": errors
    }

@app.get("/folder")
def folder_screen():
    return FileResponse('templates/folder.html')

@app.get("/edit")
def edit_screen():
    return FileResponse('templates/edit.html')

@app.get("/create")
def create_screen():
    return FileResponse('templates/create.html')

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/favicon.ico")
def favicon():
    return FileResponse('static/favicon.png')

@app.get("/r2/health")
def r2_health():
    """Simple health endpoint for R2 configuration diagnostics (no secrets)."""
    return {
        "configured": bool(r2_client and R2_BUCKET_NAME),
        "client_initialized": bool(r2_client is not None),
        "has_access_key": bool(R2_ACCESS_KEY_ID),
        "has_secret_key": bool(R2_SECRET_ACCESS_KEY),
        "has_endpoint": bool(R2_ENDPOINT),
        "has_bucket": bool(R2_BUCKET_NAME),
        "endpoint": R2_ENDPOINT,
        "bucket": R2_BUCKET_NAME,
        "public_url_base": R2_PUBLIC_URL_BASE,
    }

# -------------------------------
# DECK MANAGEMENT
# -------------------------------
@app.get("/decks")
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
            items.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
            return items
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cards")
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

@app.get("/deck/csv")
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

def _background_audio_cleanup_and_generate(to_delete: set, to_generate: set):
    """Delete old audio and generate new audio in background."""
    # Delete old audio files
    for w in to_delete:
        try:
            r2_key = _safe_tts_key(w, "de")
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        except Exception:
            pass
    # Generate new audio files in parallel
    if to_generate:
        _background_audio_generation(list(to_generate))

@app.post("/deck/update")
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
                target=_background_audio_cleanup_and_generate, 
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

import threading

# Background task queue for audio generation
_audio_generation_executor = None

def _get_audio_executor():
    global _audio_generation_executor
    if _audio_generation_executor is None:
        from concurrent.futures import ThreadPoolExecutor
        _audio_generation_executor = ThreadPoolExecutor(max_workers=4)
    return _audio_generation_executor

def _generate_audio_for_word(de_word: str):
    """Generate TTS audio for a single word (background task)."""
    if not r2_client or not R2_BUCKET_NAME or not de_word:
        return
    try:
        r2_key = _safe_tts_key(de_word, "de")
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

def _background_audio_generation(words: list):
    """Generate audio for all words in background with parallel processing."""
    if not words:
        return
    executor = _get_audio_executor()
    # Submit all words for parallel processing
    futures = [executor.submit(_generate_audio_for_word, w) for w in words]
    # Don't wait - let them complete in background

@app.post("/deck/create")
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
    thread = threading.Thread(target=_background_audio_generation, args=(de_words,), daemon=True)
    thread.start()

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

@app.post("/deck/delete")
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
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=_safe_tts_key(w, "de"))
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

@app.post("/deck/rename")
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

@app.get("/folders")
def get_folders():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    folders_key = f"{R2_BUCKET_NAME}/folders/index.json"
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    names = set()
    counts = {}
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict):
                    f = d.get("folder") or "Uncategorized"
                    names.add(f)
                    counts[f] = counts.get(f, 0) + 1
    except Exception:
        pass
    extra = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=folders_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            extra = [str(x) for x in parsed]
    except Exception:
        pass
    for f in extra:
        names.add(f)
        counts.setdefault(f, 0)
    # Load parent relationships
    parents_data = {}
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, dict):
            parents_data = parsed
    except Exception:
        pass
    base = [{"name": n, "count": counts.get(n, 0), "parent": parents_data.get(n)} for n in names]
    ordered = base
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            name_to_item = {x["name"]: x for x in base}
            ordered = [name_to_item[n] for n in arr if n in name_to_item]
            for x in base:
                if x["name"] not in arr:
                    ordered.append(x)
    except Exception:
        ordered = sorted(base, key=lambda x: x["name"].lower())
    return {"folders": ordered}

@app.post("/folder/create")
def folder_create(payload: FolderCreate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if name not in items:
        items.append(name)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    return {"ok": True, "name": name}

@app.post("/folder/rename")
def folder_rename(payload: FolderRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_deck_name(payload.old_name)
    new = _safe_deck_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if old in items:
        items = [new if x == old else x for x in items]
    if new not in items:
        items.append(new)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    # Update folders order list if present
    try:
        ok = False
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if old in oitems:
            oitems = [new if x == old else x for x in oitems]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
            ok = True
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == old:
                    d["folder"] = new
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    # Update folder parents when renaming
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    try:
        parents_data = {}
        try:
            pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
            pdata = pobj["Body"].read().decode("utf-8")
            parsed_p = json.loads(pdata)
            if isinstance(parsed_p, dict):
                parents_data = parsed_p
        except Exception:
            pass
        updated = False
        # If the renamed folder had a parent, update its key
        if old in parents_data:
            parents_data[new] = parents_data.pop(old)
            updated = True
        # If any folder had old as parent, update to new
        for k, v in list(parents_data.items()):
            if v == old:
                parents_data[k] = new
                updated = True
        if updated:
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=parents_key, Body=json.dumps(parents_data).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    return {"ok": True, "old_name": old, "new_name": new}

@app.post("/folder/delete")
def folder_delete(payload: FolderDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if x != name]
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    # Remove from folders order if present
    try:
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if name in oitems:
            oitems = [x for x in oitems if x != name]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == name:
                    d.pop("folder", None)
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    # Clean up folder parents when deleting
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    try:
        parents_data = {}
        try:
            pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
            pdata = pobj["Body"].read().decode("utf-8")
            parsed_p = json.loads(pdata)
            if isinstance(parsed_p, dict):
                parents_data = parsed_p
        except Exception:
            pass
        updated = False
        # Remove the deleted folder's parent entry
        if name in parents_data:
            del parents_data[name]
            updated = True
        # Remove parent reference for any child folders (move them to root)
        for k, v in list(parents_data.items()):
            if v == name:
                del parents_data[k]
                updated = True
        if updated:
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=parents_key, Body=json.dumps(parents_data).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    return {"ok": True, "deleted": name}

@app.post("/folder/move")
def folder_move(payload: FolderMove):
    """Move a folder to be a child of another folder (nested folders)."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    parent = _safe_deck_name(payload.parent) if payload.parent else None
    
    # Prevent moving folder into itself or its descendants
    if parent and parent == name:
        raise HTTPException(status_code=400, detail="Cannot move folder into itself")
    
    # Read folder parents data
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    parents_data = {}
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, dict):
            parents_data = parsed
    except Exception:
        pass
    
    # Check for circular reference: walk up from parent to ensure name is not an ancestor
    if parent:
        current = parent
        visited = set()
        while current:
            if current == name:
                raise HTTPException(status_code=400, detail="Cannot move folder into its own descendant")
            if current in visited:
                break
            visited.add(current)
            current = parents_data.get(current)
    
    # Update parent
    if parent:
        parents_data[name] = parent
    else:
        parents_data.pop(name, None)
    
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=parents_key,
        Body=json.dumps(parents_data).encode("utf-8"),
        ContentType="application/json"
    )
    
    return {"ok": True, "name": name, "parent": parent}

@app.post("/deck/move")
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

@app.post("/decks/index/rebuild")
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
        items.sort(key=lambda x: x.get("last_modified", ""), reverse=True)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=f"{R2_BUCKET_NAME}/csv/index.json",
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
        return {"ok": True, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rebuild index: {e}")

# -------------------------------
# TTS ROUTES
# -------------------------------
@app.get("/tts")
def tts(text: str, lang: str = "de", slow: bool = False):
    """Stream from R2 if available; otherwise generate in-memory and upload when configured."""
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

@app.get("/lines/generate")
def generate_lines(deck: str, limit: int | None = None, refresh: bool = False):
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
        cards = get_cards(deck)
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
            base = en_clean[3:].strip() if is_verb else en_clean
            bad_en = False
            if chosen:
                le = (chosen.get('line_en') or '').strip().lower()
                bad_en = (not le) or le.startswith('this is') or le.startswith('that is') or le.startswith('i the') or (' to ' in le)
            if chosen:
                cleaned.append({
                    "de": de,
                    "en": en,
                    "line_en": (chosen.get('line_en') or '').strip(),
                    "line_de": (chosen.get('line_de') or '').strip(),
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
                import asyncio
                from concurrent.futures import ThreadPoolExecutor
                async def process_one(it):
                    text = (it.get("line_de") or "").strip()
                    if not text:
                        return None
                    r2_key = _safe_tts_key(text, "de")
                    def check_and_generate():
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
                    loop = asyncio.get_event_loop()
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        return await loop.run_in_executor(executor, check_and_generate)
                sem = asyncio.Semaphore(10)
                async def with_sem(it):
                    async with sem:
                        return await process_one(it)
                tasks = [with_sem(it) for it in cleaned]
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            except Exception:
                pass

        if isinstance(limit, int) and limit > 0:
            cleaned = cleaned[:limit]
        return {"deck": deck, "count": len(cleaned), "items": cleaned, "cached": False, "saved": saved}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lines/debug")
def lines_debug(deck: str, limit: int | None = None):
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    cards = get_cards(deck)
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

@app.get("/preload_deck_audio")
async def preload_deck_audio(deck: str, lang: str = "de"):
    """Preload all audio files for a deck and return URLs with concurrent processing."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Get deck data
    safe = _safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")
    
    try:
        # Get deck cards
        cards = get_cards(deck)
        
        # Process all audio files concurrently
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
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

@app.get("/preload_lines_audio")
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
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
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
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=10) as executor:
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

# -------------------------------
# R2 UTILITIES
# -------------------------------

@app.get("/debug/r2-config")
def debug_r2_config():
    """Debug endpoint to check R2 configuration in deployment."""
    config_info = {
        "r2_configured": bool(r2_client and R2_BUCKET_NAME),
        "bucket_name": R2_BUCKET_NAME,
        "endpoint": R2_ENDPOINT,
        "has_credentials": bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY),
        "account_id": R2_ACCOUNT_ID[:8] + "..." if R2_ACCOUNT_ID else None,
    }
    
    # Test basic R2 connection
    if r2_client and R2_BUCKET_NAME:
        try:
            # Try to list objects with the bucket prefix
            response = r2_client.list_objects_v2(
                Bucket=R2_BUCKET_NAME,
                Prefix=f"{R2_BUCKET_NAME}/csv/",
                MaxKeys=5
            )
            config_info["r2_connection"] = "success"
            config_info["objects_found"] = len(response.get("Contents", []))
            config_info["sample_keys"] = [obj["Key"] for obj in response.get("Contents", [])[:3]]
        except Exception as e:
            config_info["r2_connection"] = "failed"
            config_info["r2_error"] = str(e)
    else:
        config_info["r2_connection"] = "not_configured"
    
    return config_info

@app.get("/r2/get")
def r2_get(key: str):
    """Stream an object from Cloudflare R2 by key."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        stream = obj["Body"]
        content_type = obj.get("ContentType", "application/octet-stream")
        return StreamingResponse(stream, media_type=content_type)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Object not found")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/audio/rebuild")
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

@app.post("/audio/cleanup")
def audio_cleanup(dry_run: bool = False):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        valid_texts = set()
        idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
        decks = []
        try:
            idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
            idx_data = idx_obj["Body"].read().decode("utf-8")
            parsed = json.loads(idx_data)
            if isinstance(parsed, list):
                decks = parsed
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                raise HTTPException(status_code=500, detail=str(e))
        except Exception:
            decks = []

        for d in decks:
            if not isinstance(d, dict):
                continue
            name = d.get("name") or ""
            file_key = d.get("file") or f"{R2_BUCKET_NAME}/csv/{_safe_deck_name(name)}.csv"
            try:
                obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=file_key)
                data = obj["Body"].read().decode("utf-8")
                reader = csv.reader(io.StringIO(data))
                for row in reader:
                    if len(row) >= 2:
                        de = (row[1] or "").strip()
                        if de:
                            valid_texts.add(de)
            except Exception:
                pass
            try:
                lkey = _lines_key(name)
                lobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=lkey)
                ldata = lobj["Body"].read().decode("utf-8")
                lparsed = json.loads(ldata)
                items = lparsed.get("items") if isinstance(lparsed, dict) else lparsed
                items = items or []
                for it in items:
                    if isinstance(it, dict):
                        t = (it.get("line_de") or "").strip()
                        if t:
                            valid_texts.add(t)
            except Exception:
                pass

        valid_keys = set(_safe_tts_key(t, "de") for t in valid_texts)
        prefix = f"{R2_BUCKET_NAME}/tts/de/"
        continuation = None
        total = 0
        deleted = 0
        kept = 0
        errors = 0
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            contents = resp.get("Contents", [])
            for obj in contents:
                key = obj.get("Key", "")
                if not key.endswith(".mp3"):
                    continue
                total += 1
                if key in valid_keys:
                    kept += 1
                else:
                    if dry_run:
                        deleted += 1
                    else:
                        try:
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
                            deleted += 1
                        except Exception:
                            errors += 1
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break

        return {
            "ok": True,
            "dry_run": dry_run,
            "tts_total": total,
            "kept": kept,
            "deleted": deleted,
            "errors": errors,
            "valid_texts": len(valid_texts),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup audio: {e}")

# -------------------------------
# ORDER ROUTES
# -------------------------------
@app.get("/order/folders")
def order_folders_get():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
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

@app.post("/order/folders")
def order_folders_set(payload: FolderOrderUpdate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    names = [ _safe_deck_name(x) for x in (payload.order or []) if _safe_deck_name(x) ]
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key(), Body=json.dumps(names).encode("utf-8"), ContentType="application/json")
        return {"ok": True, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/order/decks")
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

@app.post("/order/decks")
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

if __name__ == "__main__":
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
