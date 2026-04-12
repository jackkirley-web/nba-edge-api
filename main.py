# main.py — FastAPI entry point with daily scheduler

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router

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
    """Start the daily prefetch scheduler when the server boots."""
    try:
        from data_store import scheduler
        from cache import cache
        scheduler.start(cache.get)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Scheduler failed to start: %s", e)


@app.get("/")
def root():
    return {"status": "ok", "service": "NBAEdge API"}


@app.get("/health")
def health():
    return {"status": "ok"}
