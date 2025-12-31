import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

from services.storage import r2_client, R2_BUCKET_NAME
from services.executor import shutdown_executor
from routers import screens, decks, folders, stories, cards, system, pdfs

# Load env
load_dotenv(override=True)

# Configuration
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() in ("true", "1", "yes")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup
    yield
    # Shutdown
    shutdown_executor(wait=True)


app = FastAPI(lifespan=lifespan)

# CORS Configuration
# Parse origins from environment variable (comma-separated)
if CORS_ORIGINS == "*":
    origins = ["*"]
else:
    origins = [origin.strip() for origin in CORS_ORIGINS.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True if CORS_ORIGINS != "*" else False,  # Don't allow credentials with wildcard
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
app.include_router(pdfs.router)

# Mount Static
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
