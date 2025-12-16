from pydantic import BaseModel

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

class CustomStoryRequest(BaseModel):
    topic: str
    story_id: str | None = None
    level: str | None = "A2"


# AI Response Models for validation
class StorySegment(BaseModel):
    """A single segment of a story (dialogue or narration)."""
    type: str  # "narration" or "dialogue"
    speaker: str  # "narrator" or character name
    text_de: str
    text_en: str
    highlight_words: list[str] = []


class StoryResponse(BaseModel):
    """AI-generated story response structure."""
    title_de: str
    title_en: str
    characters: list[str] = []
    vocabulary: dict[str, str] = {}
    segments: list[StorySegment] = []
    level: str | None = None


class LineItem(BaseModel):
    """A single vocabulary line with example sentence."""
    de: str
    en: str
    line_de: str = ""
    line_en: str = ""


class LinesResponse(BaseModel):
    """Response containing generated example sentences."""
    deck: str
    count: int
    items: list[LineItem]
    cached: bool = False
    saved: bool = False
