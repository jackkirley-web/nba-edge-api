# routes.py
from fastapi import APIRouter, Query
import requests
from cache import cache, streak_cache

router = APIRouter()
ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"


@router.get("/api/picks")
def get_picks(refresh: bool = Query(False)):
    data = cache.get(force_refresh=refresh)
    return {
        "picks":          data.get("picks", {}),
        "last_updated":   data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored":    data.get("legs_scored", len(data.get("legs", []))),
        "props_scored":   data.get("props_scored", len(data.get("props", []))),
        "is_stale":       False,
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
        props = [p for p in props if game.upper() in p.get("game", "").upper()]
    if stat:
        props = [p for p in props if p.get("stat", "").lower() == stat.lower()]
    props = [p for p in props if p.get("confidence", 0) >= min_conf]
    return {
        "props":        props[:limit],
        "total":        len(props),
        "last_updated": data.get("last_updated"),
    }


@router.get("/api/streaks")
def get_streaks(
    window: int = Query(10, description="Primary window: 5, 10, or 15"),
    stat: str = Query(None, description="Filter by stat e.g. 'pts'"),
    team: str = Query(None, description="Filter by team abbrev e.g. 'BOS'"),
    perfect_only: bool = Query(False, description="Only show perfect hit-rate streaks"),
    force_refresh: bool = Query(False),
):
    result = streak_cache.get(force_refresh=force_refresh)
    streaks = result.get("streaks", [])

    # Filter
    if stat:
        streaks = [s for s in streaks if s.get("stat", "").lower() == stat.lower()]
    if team:
        streaks = [s for s in streaks if s.get("team", "").upper() == team.upper()]
    if perfect_only:
        streaks = [s for s in streaks if s.get("best_hit_rate", 0) >= 1.0]

    # Sort by the requested window's hit rate if available
    def sort_key(s):
        window_data = s.get("windows", {}).get(window, {})
        return window_data.get("hit_rate", 0)

    streaks.sort(key=sort_key, reverse=True)

    return {
        "streaks":      streaks,
        "total":        len(streaks),
        "loading":      result.get("loading", False),
        "last_updated": result.get("last_updated"),
        "window":       window,
    }


@router.get("/api/streak-force-refresh")
def streak_force_refresh():
    result = streak_cache.get(force_refresh=True)
    return {
        "status":       "refresh triggered",
        "loading":      result.get("loading"),
        "streaks_count": len(result.get("streaks", [])),
    }


@router.get("/api/debug")
def debug():
    data = cache.get()
    streak_data = streak_cache.get()
    games = data.get("games", [])
    return {
        "games_found":    len(games),
        "legs_scored":    len(data.get("legs", [])),
        "props_scored":   len(data.get("props", [])),
        "streaks_found":  len(streak_data.get("streaks", [])),
        "streaks_loading": streak_data.get("loading", False),
        "game_list": [
            {
                "away":   g.get("away_team_abbrev"),
                "home":   g.get("home_team_abbrev"),
                "spread": g.get("spread_line"),
                "total":  g.get("total_line"),
            }
            for g in games
        ],
        "last_updated": data.get("last_updated"),
    }


@router.get("/api/player-logs-debug")
def player_logs_debug(name: str = Query(..., description="Player name to look up")):
    """Debug endpoint to check a specific player's game logs."""
    try:
        from player_logs import get_all_player_base_stats, get_player_game_logs_batch
        players = get_all_player_base_stats()
        # Find matching player
        matches = {
            pid: p for pid, p in players.items()
            if name.lower() in p.get("name", "").lower()
        }
        if not matches:
            return {"error": f"No player found matching '{name}'"}

        pid, pdata = next(iter(matches.items()))
        logs = get_player_game_logs_batch([pid], last_n=5)
        player_logs = logs.get(pid, [])

        return {
            "player":     pdata.get("name"),
            "team":       pdata.get("team_abbrev"),
            "season_avg": {k: pdata.get(k) for k in ["pts", "reb", "ast", "3pm", "mins"]},
            "last_5_games": player_logs[:5],
        }
    except Exception as e:
        return {"error": str(e)}


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
                {"home_team": e.get("home_team"), "away_team": e.get("away_team")}
                for e in (data if isinstance(data, list) else [])
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status":         "refreshed",
        "last_updated":   data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed", 0),
        "legs_scored":    data.get("legs_scored", 0),
        "props_scored":   data.get("props_scored", 0),
    }
