# main.py — NBAEdge Backend
# Cache warms lazily on first request, not at startup (prevents OOM on free tier)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from routes import router

app = FastAPI(title="NBAEdge API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
