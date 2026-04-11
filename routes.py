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
        "picks":          data.get("picks", {}),
        "last_updated":   data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored":    len(data.get("legs", [])),
        "props_scored":   len(data.get("props", [])),
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

@router.get("/api/props")
def get_props(
    game: str = Query(None, description="Filter by game e.g. 'BOS @ MIA'"),
    stat: str = Query(None, description="Filter by stat e.g. 'pts'"),
    min_conf: int = Query(55, description="Minimum confidence score"),
    limit: int = Query(50, description="Max results"),
):
    data = cache.get()
    props = data.get("props", [])

    if game:
        props = [p for p in props if game.upper() in p.get("game", "").upper()]
    if stat:
        props = [p for p in props if p.get("stat", "").lower() == stat.lower()]

    props = [p for p in props if p.get("confidence", 0) >= min_conf]
    props = props[:limit]

    return {
        "props":        props,
        "total":        len(props),
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/debug")
def debug():
    data = cache.get()
    games = data.get("games", [])
    return {
        "games_found":  len(games),
        "legs_scored":  len(data.get("legs", [])),
        "props_scored": len(data.get("props", [])),
        "game_list": [
            {
                "away":       g.get("away_team_abbrev"),
                "home":       g.get("home_team_abbrev"),
                "has_spread": g.get("spread_line") is not None,
                "spread":     g.get("spread_line"),
                "total":      g.get("total_line"),
            }
            for g in games
        ],
        "top_props": [
            {
                "player":     p.get("player"),
                "game":       p.get("game"),
                "stat":       p.get("stat_label"),
                "direction":  p.get("direction"),
                "line":       p.get("est_line"),
                "projection": p.get("projection"),
                "confidence": p.get("confidence"),
            }
            for p in sorted(data.get("props", []), key=lambda x: x.get("confidence", 0), reverse=True)[:10]
        ],
        "last_updated": data.get("last_updated"),
    }

@router.get("/api/odds-raw")
def odds_raw():
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us", "markets": "spreads", "oddsFormat": "decimal"},
            timeout=15,
        )
        data = r.json()
        return {
            "status": r.status_code,
            "events_count": len(data) if isinstance(data, list) else 0,
            "events": [
                {"home_team": e.get("home_team"), "away_team": e.get("away_team"),
                 "commence_time": e.get("commence_time")}
                for e in (data if isinstance(data, list) else [])
            ],
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status":       "refreshed",
        "last_updated": data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored":  len(data.get("legs", [])),
        "props_scored": len(data.get("props", [])),
    }
