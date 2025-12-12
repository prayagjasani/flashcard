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
