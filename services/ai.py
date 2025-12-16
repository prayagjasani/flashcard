import os
import json
import base64
import urllib.request
import urllib.error
import random
from dotenv import load_dotenv

# Force load from .env file
load_dotenv(override=True)
GEMINI_API_KEY = os.getenv("gemini_api_key") or os.getenv("GEMINI_API_KEY")

def generate_lines(cards):
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

def generate_story(cards, deck_name: str):
    """Generate an actual narrative story using vocabulary from the deck."""
    if not GEMINI_API_KEY:
        return None

    model = "gemini-2.5-flash"
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
- "color": A number 0-7, use different colors for different word pairs within the same segment

The highlight_pairs should contain 2-4 vocabulary words from the input list that appear in that segment.
Make sure the German and English words are EXACTLY as they appear in the text (same case, same form).

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
    except Exception:
        return None

def generate_custom_story(topic: str, level: str = "A2"):
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
  "segments": [
    {{
      "type": "narration" or "dialogue",
      "speaker": "narrator" or character name,
      "text_de": "German text",
      "text_en": "English translation",
      "highlight_pairs": [
        {{"de": "Flughafen", "en": "airport", "color": 0}},
        {{"de": "nervös", "en": "nervous", "color": 1}},
        {{"de": "Koffer", "en": "suitcase", "color": 2}}
      ]
    }}
  ]
}}

IMPORTANT: Each segment MUST include "highlight_pairs" array with 2-4 vocabulary word pairs.
- "de": The exact German word as it appears in text_de (same case, same form)
- "en": The exact English word as it appears in text_en (same case, same form)
- "color": A number 0-7, use different colors for different word pairs within the same segment

This creates visual links between German words and their English translations with matching colors.

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
        print(f"[AI] No valid response from Gemini: {parsed}")
        return None
    except Exception as e:
        print(f"[AI] Error generating custom story: {e}")
        return None
