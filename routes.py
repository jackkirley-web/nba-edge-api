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
        "legs_scored":    data.get("legs_scored", 0),
        "props_scored":   data.get("props_scored", 0),
        "streaks_found":  data.get("streaks_found", 0),
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
    game: str = Query(None),
    stat: str = Query(None),
    min_conf: int = Query(55),
    limit: int = Query(100),
):
    data = cache.get()
    props = data.get("props", [])
    if game:
        props = [p for p in props if game.upper() in p.get("game","").upper()]
    if stat:
        props = [p for p in props if p.get("stat","").lower() == stat.lower()]
    props = [p for p in props if p.get("confidence",0) >= min_conf]
    return {"props": props[:limit], "total": len(props), "last_updated": data.get("last_updated")}


@router.get("/api/streaks")
def get_streaks(
    window: int = Query(10, description="5, 10, or 15"),
    min_rate: float = Query(0.6, description="Minimum hit rate 0.0-1.0"),
    stat: str = Query(None, description="Filter by stat: pts, reb, ast, 3pm, stl, blk"),
    team: str = Query(None, description="Filter by team abbreviation e.g. BOS"),
    perfect_only: bool = Query(False, description="Only show 100% hit rate streaks"),
    limit: int = Query(50),
):
    """
    Returns real streak data calculated from NBA.com game logs.
    Each entry shows how often a player has hit a threshold in the last N games.
    """
    data = cache.get()
    streaks = data.get("streaks", [])

    # Filter by window — return the window-specific data
    filtered = []
    for s in streaks:
        window_data = s.get("windows", {}).get(window)
        if not window_data:
            continue
        hit_rate = window_data["hit_rate"]
        if hit_rate < min_rate:
            continue
        if stat and s.get("stat") != stat:
            continue
        if team and s.get("team","").upper() != team.upper():
            continue
        if perfect_only and window_data["hits"] != window:
            continue

        filtered.append({
            "player":      s["player"],
            "team":        s["team"],
            "position":    s["position"],
            "stat":        s["stat"],
            "stat_label":  s["stat_label"],
            "threshold":   s["threshold"],
            "label":       s["label"],
            "season_avg":  s["season_avg"],
            "recent_avg":  s["recent_avg"],
            "trend":       s["trend"],
            "hits":        window_data["hits"],
            "games":       window,
            "hit_rate":    hit_rate,
            "pct":         window_data["pct"],
            "is_perfect":  window_data["hits"] == window,
            "last_5_vals": s["last_5_vals"],
            "mins":        s["mins"],
            # Include all windows for the toggle
            "all_windows": s["windows"],
        })

    # Sort perfect streaks first, then by hit rate
    filtered.sort(key=lambda x: (-int(x["is_perfect"]), -x["hit_rate"], -x["threshold"]))

    return {
        "streaks":       filtered[:limit],
        "total":         len(filtered),
        "window":        window,
        "last_updated":  data.get("last_updated"),
    }


@router.get("/api/debug")
def debug():
    data = cache.get()
    return {
        "games_found":   len(data.get("games", [])),
        "legs_scored":   data.get("legs_scored", 0),
        "props_scored":  data.get("props_scored", 0),
        "streaks_found": data.get("streaks_found", 0),
        "game_list": [
            {"away": g.get("away_team_abbrev"), "home": g.get("home_team_abbrev"),
             "spread": g.get("spread_line"), "total": g.get("total_line")}
            for g in data.get("games", [])
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
            "events": [{"home": e.get("home_team"), "away": e.get("away_team")} for e in (data if isinstance(data, list) else [])],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status":        "refreshed",
        "last_updated":  data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored":   data.get("legs_scored", 0),
        "props_scored":  data.get("props_scored", 0),
        "streaks_found": data.get("streaks_found", 0),
    }
