from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse, Response

router = APIRouter()

REACT_INDEX = Path(__file__).resolve().parent.parent / 'static' / 'react' / 'index.html'
REACT_ASSETS = Path(__file__).resolve().parent.parent / 'static' / 'react' / 'assets'

@router.get("/static/react/assets/{filename}")
def serve_react_asset(filename: str):
    target = REACT_ASSETS / filename
    if target.is_file():
        return FileResponse(target)
    # Stale asset requested by a cached HTML from a previous deployment
    if filename.startswith("index-") and filename.endswith(".js"):
        current = next(REACT_ASSETS.glob("index-*.js"), None)
        if current and current.is_file():
            return FileResponse(current, headers={'Cache-Control': 'no-cache'})
    elif filename.startswith("index-") and filename.endswith(".css"):
        current = next(REACT_ASSETS.glob("index-*.css"), None)
        if current and current.is_file():
            return FileResponse(current, headers={'Cache-Control': 'no-cache'})
    elif filename.startswith("dialogs-") and filename.endswith(".js"):
        current = next(REACT_ASSETS.glob("dialogs-*.js"), None)
        if current and current.is_file():
            return FileResponse(current, headers={'Cache-Control': 'no-cache'})
    return Response(status_code=404)

@router.get("/")
def read_root(mode: str = '', deck: str = ''):
    # Existing links open the established flashcard engine using these parameters.
    if not mode and not deck and REACT_INDEX.is_file():
        return FileResponse(REACT_INDEX, headers={'Cache-Control': 'no-cache'})
    return FileResponse('templates/index.html')

@router.get("/learn")
def learn_screen():
    return FileResponse('templates/hi.html')

@router.get("/match")
def match_screen():
    return FileResponse('templates/match.html')

@router.get("/spelling")
def spelling_screen():
    return FileResponse('templates/spelling.html')

@router.get("/line")
def line_screen():
    return FileResponse('templates/line.html')

@router.get("/video")
def video_screen():
    return FileResponse('templates/video.html')

@router.get("/story")
def story_screen():
    return FileResponse('templates/story.html')

@router.get("/folder")
def folder_screen(legacy: bool = False):
    if not legacy and REACT_INDEX.is_file():
        return FileResponse(REACT_INDEX, headers={'Cache-Control': 'no-cache'})
    return FileResponse('templates/folder.html')

@router.get("/edit")
def edit_screen():
    return FileResponse('templates/edit.html')

@router.get("/pdf")
def pdf_screen():
    return FileResponse('templates/pdf.html')

@router.get("/create")
def create_screen():
    return FileResponse('templates/create.html')

@router.head("/")
def head_root():
    return Response(status_code=200)

@router.get("/favicon.ico")
def favicon():
    return FileResponse('static/favicon.png')
