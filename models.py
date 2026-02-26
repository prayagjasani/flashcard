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


class PdfRename(BaseModel):
    old_name: str
    new_name: str


class PdfDelete(BaseModel):
    name: str


class PdfMove(BaseModel):
    name: str
    folder: str | None = None


class PdfOrderUpdate(BaseModel):
    scope: str | None = None
    order: list[str]


class PdfFolderCreate(BaseModel):
    name: str


class PdfFolderRename(BaseModel):
    old_name: str
    new_name: str


class PdfFolderDelete(BaseModel):
    name: str


class PdfFolderMove(BaseModel):
    source: str
    target: str | None = None





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
