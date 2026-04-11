# main.py — NBAEdge Backend
# Runs on Render.com (free tier)
# Pulls from nba_api (NBA.com official data) + official injury report PDF

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from routes import router
from cache import cache

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-warm cache on startup
    logger.info("NBAEdge backend starting — warming cache...")
    try:
        await asyncio.to_thread(cache.refresh_all)
        logger.info("Cache warmed successfully")
    except Exception as e:
        logger.warning(f"Cache warm failed (will retry on first request): {e}")
    yield

app = FastAPI(title="NBAEdge API", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten this to your GitHub Pages URL in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/")
def root():
    return {"status": "ok", "service": "NBAEdge API v2.0"}

@app.get("/health")
def health():
    return {"status": "ok"}
