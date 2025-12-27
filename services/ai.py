import os
import json
import base64
import urllib.request
import urllib.error
import random
import time
from typing import Optional
from dotenv import load_dotenv

import services.metrics as metrics

# Force load from .env file
load_dotenv(override=True)
GEMINI_API_KEY = os.getenv("gemini_api_key") or os.getenv("GEMINI_API_KEY")
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
MAX_GEMINI_ATTEMPTS = int(os.getenv("GEMINI_MAX_ATTEMPTS", "3"))
GEMINI_TIMEOUT_SECONDS = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "60"))
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


def _gemini_request(
    body: dict,
    endpoint: str,
    label: str,
    timeout: int = GEMINI_TIMEOUT_SECONDS,
    max_attempts: int = MAX_GEMINI_ATTEMPTS,
) -> Optional[str]:
    """Call Gemini with retries and basic timing/error logging."""
    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            req = urllib.request.Request(
                endpoint,
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
            duration = _ms(start)
            metrics.record_timing(label, duration)
            print(f"[AI] {label} attempt {attempt}/{max_attempts} ok in {duration:.1f} ms")
            return raw
        except urllib.error.HTTPError as e:
            duration = _ms(start)
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass
            metrics.record_error(label, f"HTTP {e.code}: {e.reason}")
            print(f"[AI] HTTP {e.code} on {label} attempt {attempt}/{max_attempts} after {duration:.1f} ms: {e.reason}")
            if error_body:
                print(f"[AI] {label} error body: {error_body[:400]}")
            if e.code not in RETRYABLE_HTTP_CODES or attempt == max_attempts:
                return None
            time.sleep(min(2 ** (attempt - 1), 8))
        except urllib.error.URLError as e:
            duration = _ms(start)
            metrics.record_error(label, f"URL error: {e.reason}")
            print(f"[AI] URL error on {label} attempt {attempt}/{max_attempts} after {duration:.1f} ms: {e.reason}")
            if attempt == max_attempts:
                return None
            time.sleep(min(2 ** (attempt - 1), 8))
        except Exception as e:
            duration = _ms(start)
            metrics.record_error(label, f"{type(e).__name__}: {e}")
            print(f"[AI] Error on {label} attempt {attempt}/{max_attempts} after {duration:.1f} ms: {type(e).__name__}: {e}")
            if attempt == max_attempts:
                return None
            time.sleep(min(2 ** (attempt - 1), 8))
    return None

def generate_lines(cards):
    if not GEMINI_API_KEY:
        return []

    model = DEFAULT_GEMINI_MODEL
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
        raw = _gemini_request(body, endpoint, label="generate_lines", timeout=30)
        if not raw:
            return []
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
        except Exception as e:
            metrics.record_error("generate_lines", f"chunk_error:{type(e).__name__}")
        i += CHUNK_SIZE
    return all_items

def generate_story(cards, deck_name: str):
    """Generate an actual narrative story using vocabulary from the deck."""
    if not GEMINI_API_KEY:
        return None

    model = DEFAULT_GEMINI_MODEL
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )

    # Pick 8-12 words for a short story
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
  "segments": [
    {{
      "type": "narration" or "dialogue",
      "speaker": "narrator" or character name,
      "text_de": "German text",
      "text_en": "English translation",
      "highlight_pairs": [
        {{"de": "Frage", "en": "question", "color": 0}},
        {{"de": "Taxi", "en": "taxi", "color": 1}}
      ]
    }}
  ]
}}

IMPORTANT: Each segment MUST include "highlight_pairs" array with vocabulary word pairs.
- "de": The exact German word as it appears in text_de
- "en": The exact English word as it appears in text_en  
- "color": SEQUENTIAL number starting from 0. First word pair = 0, second = 1, third = 2, etc. Each word pair in the segment MUST have a unique color number (0-15).

CRITICAL: Highlight EVERY word in the sentence EXCEPT these common words: der, die, das, ein, eine, und, oder.
Include ALL other words: verbs (bin, ist, war, habe, gehe, etc.), pronouns (ich, du, er, sie, wir, etc.), 
nouns, adjectives, adverbs, prepositions (in, auf, mit, zu, etc.), and ALL other vocabulary.
Do NOT skip words just because they seem simple - learners need to see ALL translations.
Make sure the German and English words are EXACTLY as they appear in the text (same case, same form).

Remember: The best language learning happens when students are entertained and want to know what happens next!"""

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"},
    }
    
    raw = _gemini_request(body, endpoint, label="generate_story", timeout=GEMINI_TIMEOUT_SECONDS)
    if not raw:
        metrics.record_error("generate_story", "no_response")
        return None
    try:
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
        metrics.record_error("generate_story", f"parse_error:{type(e).__name__}")
        return None

def generate_custom_story(topic: str, level: str = "A2"):
    """Generate a story based on a custom topic using Gemini."""
    if not GEMINI_API_KEY:
        return None
    
    model = DEFAULT_GEMINI_MODEL
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
    
    raw = _gemini_request(body, endpoint, label="generate_custom_story", timeout=GEMINI_TIMEOUT_SECONDS)
    if not raw:
        metrics.record_error("generate_custom_story", "no_response")
        return None
    try:
        parsed = json.loads(raw)
        candidates = parsed.get("candidates") or []
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                p0 = parts[0]
                if isinstance(p0, dict) and "text" in p0:
                    return json.loads(p0["text"])
        print(f"[AI] No valid response from Gemini: {parsed}")
        return None
    except Exception as e:
        metrics.record_error("generate_custom_story", f"parse_error:{type(e).__name__}")
        print(f"[AI] Error generating custom story: {type(e).__name__}: {e}")
        return None
