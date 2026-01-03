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

IMPORTANT: Each segment MUST include a "highlight_pairs" array with vocabulary word pairs.
- "de": The exact German word or SHORT PHRASE as it appears in text_de (same case, same form)
- "en": The exact English word or SHORT PHRASE as it appears in text_en (same case, same form)
- "color": SEQUENTIAL number starting from 0. First word pair = 0, second = 1, third = 2, etc. Each word pair in the segment MUST have a unique color number (0-15).

HIGHLIGHTING STRATEGY (YOU decide what is most useful for A2–B1 learners):
- Focus on meaningful vocabulary and chunks: verbs, nouns, adjectives, adverbs, prepositions, and short phrases that carry real meaning.
- DO NOT highlight extremely basic function words such as articles (der, die, das, ein, eine), conjunctions (und, oder, aber), or very common pronouns (ich, du, er, sie, wir, ihr, Sie, es).
- Also avoid highlighting very basic helper verbs like "sein", "haben", "werden" and modal verbs in their most frequent forms, unless they are part of an interesting phrase.
- Aim for roughly 6–14 highlighted items per segment (fewer for short sentences, more for long ones). It is OK if not every word is highlighted.
- Ensure that every "de" and "en" value actually appears in the corresponding text.

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
The target CEFR level is {level}. Adjust the vocabulary and grammar strictly to this level
(A1 = very simple everyday language, C2 = very advanced, natural native-like language).

CEFR GRAMMAR AND VOCABULARY RULES (follow the ones for level {level}):
- A1: very short sentences, present tense only, high-frequency everyday words, almost no subordinate clauses, simple word order (Subject–Verb–Object).
- A2: mostly present tense with occasional perfect tense, simple connectors like "weil", "aber", "dann", still straightforward word order, limited idioms.
- B1: mix of present, perfect, and simple past where natural, more connectors and subordinate clauses, some idiomatic everyday expressions, but still learner-friendly.
- B2: natural variety of tenses, frequent subordinate clauses, richer vocabulary, more idiomatic expressions, but still clear and structured.
- C1–C2: near-native grammar and vocabulary, complex sentences, natural idioms, nuanced expressions.

Do not drift above the requested level: if {level} is A1 or A2, avoid B2/C1-style long, complex sentences or advanced vocabulary.

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

IMPORTANT: Each segment MUST include a "highlight_pairs" array with vocabulary word pairs.
- "de": The exact German word or SHORT PHRASE as it appears in text_de (same case, same form)
- "en": The exact English word or SHORT PHRASE as it appears in text_en (same case, same form)
- "color": SEQUENTIAL number starting from 0. First word pair = 0, second = 1, third = 2, etc. Each word pair in the segment MUST have a unique color number (0-15).

HIGHLIGHTING STRATEGY (ADAPT TO CEFR LEVEL {level}):
- A1: highlight the most important content words (nouns, main verbs, adjectives, useful adverbs and prepositions). It is fine to highlight simpler words if they are central to understanding the story.
- A2–B1: treat basic A1 vocabulary as already known. DO NOT highlight very frequent function words or pronouns (ich, du, er, sie, wir, ihr, Sie, es) or helper verbs like "sein", "haben", "werden", "können", "müssen", "wollen" unless they are part of an interesting phrase. Focus on slightly more complex or topic-specific words and short phrases.
- B2–C2: focus on advanced, nuanced vocabulary, idiomatic expressions, and less common phrases. Avoid highlighting simple A1/A2 words.
- In all levels, aim for a reasonable number of highlights (roughly 5–15 per segment depending on length) and make them feel intentional, not random.
- Ensure every highlighted "de" and "en" actually appears in the corresponding text.

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
