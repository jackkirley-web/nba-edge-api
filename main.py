# main.py — FastAPI entry point with scheduler and HEAD health support

import logging

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from routes import router

logger = logging.getLogger(__name__)

app = FastAPI(title="NBAEdge API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
async def startup_event():
    """Start the background cache prefetch scheduler when the server boots."""
    try:
        from data_store import scheduler
        from cache import cache

        scheduler.start(cache.get)
    except Exception as e:
        logger.warning("Scheduler failed to start: %s", e)


@app.get("/")
def root():
    return {"status": "ok", "service": "NBAEdge API"}


@app.head("/")
def root_head():
    return Response(status_code=200)


@app.get("/health")
def health():
    return {"status": "ok"}
