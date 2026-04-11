# routes.py
from fastapi import APIRouter, Query
import requests
from cache import cache

router = APIRouter()

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"

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
    return {"games": data.get("games", []), "last_updated": data.get("last_updated")}

@router.get("/api/injuries")
def get_injuries():
    data = cache.get()
    return {"injuries": data.get("injuries", {}), "last_updated": data.get("last_updated")}

@router.get("/api/legs")
def get_legs():
    data = cache.get()
    return {"legs": data.get("legs", []), "last_updated": data.get("last_updated")}

@router.get("/api/debug")
def debug():
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
                "spread": g.get("spread_line"),
                "total": g.get("total_line"),
            }
            for g in games
        ],
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/odds-raw")
def odds_raw():
    """Shows exactly what The Odds API returns — team names and available games."""
    try:
        r = requests.get(
            f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "spreads",
                "oddsFormat": "decimal",
            },
            timeout=15,
        )
        data = r.json()
        # Return just team names and count — easy to read
        return {
            "status": r.status_code,
            "events_count": len(data) if isinstance(data, list) else 0,
            "events": [
                {
                    "home_team": e.get("home_team"),
                    "away_team": e.get("away_team"),
                    "commence_time": e.get("commence_time"),
                    "bookmakers_count": len(e.get("bookmakers", [])),
                }
                for e in (data if isinstance(data, list) else [])
            ],
            "raw_response": data if not isinstance(data, list) else None,
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status": "refreshed",
        "last_updated": data.get("last_updated"),
        "legs_scored": len(data.get("legs", [])),
    }
