from fastapi import APIRouter
from fastapi.responses import FileResponse, Response

router = APIRouter()

@router.get("/")
def read_root():
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

@router.get("/folder")
def folder_screen():
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
