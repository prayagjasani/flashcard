import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

from services.storage import r2_client, R2_BUCKET_NAME
from routers import screens, decks, folders, stories, cards, system

# Load env
load_dotenv(override=True)

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(screens.router)
app.include_router(decks.router)
app.include_router(folders.router)
app.include_router(stories.router)
app.include_router(cards.router)
app.include_router(system.router)

# Mount Static
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
