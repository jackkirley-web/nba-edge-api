# routes.py
from fastapi import APIRouter, Query
from cache import cache

router = APIRouter()

@router.get("/api/picks")
def get_picks(refresh: bool = Query(False)):
    data = cache.get(force_refresh=refresh)
    return {
        "picks": data.get("picks", {}),
        "last_updated": data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
    }

@router.get("/api/slate")
def get_slate(refresh: bool = Query(False)):
    data = cache.get(force_refresh=refresh)
    return {
        "games": data.get("games", []),
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/injuries")
def get_injuries(refresh: bool = Query(False)):
    data = cache.get(force_refresh=refresh)
    return {
        "injuries": data.get("injuries", {}),
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/legs")
def get_legs():
    data = cache.get()
    return {
        "legs": data.get("legs", []),
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {"status": "refreshed", "last_updated": data.get("last_updated")}
