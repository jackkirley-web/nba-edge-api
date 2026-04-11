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
        "legs_scored": len(data.get("legs", [])),
    }

@router.get("/api/slate")
def get_slate():
    data = cache.get()
    return {
        "games": data.get("games", []),
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/injuries")
def get_injuries():
    data = cache.get()
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

@router.get("/api/debug")
def debug():
    """Shows raw game list and odds matching — use to diagnose issues."""
    data = cache.get()
    games = data.get("games", [])
    return {
        "games_found": len(games),
        "legs_scored": len(data.get("legs", [])),
        "game_list": [
            {
                "away": g.get("away_team_abbrev"),
                "home": g.get("home_team_abbrev"),
                "has_spread": g.get("spread_line") is not None,
                "has_total": g.get("total_line") is not None,
                "spread": g.get("spread_line"),
                "total": g.get("total_line"),
            }
            for g in games
        ],
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status": "refreshed",
        "last_updated": data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored": len(data.get("legs", [])),
    }
