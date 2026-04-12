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
        "legs_scored":    data.get("legs_scored", 0),
        "props_scored":   data.get("props_scored", 0),
    }


@router.get("/api/slate")
def get_slate():
    data = cache.get()
    return {"games": data.get("games", []), "last_updated": data.get("last_updated")}


@router.get("/api/injuries")
def get_injuries():
    data = cache.get()
    return {"injuries": data.get("injuries", {}), "last_updated": data.get("last_updated")}


@router.get("/api/props")
def get_props(
    game: str = Query(None),
    stat: str = Query(None),
    min_conf: int = Query(50),
    limit: int = Query(100),
):
    data = cache.get()
    props = data.get("props", [])
    if game:
        props = [p for p in props if game.upper() in p.get("game", "").upper()]
    if stat:
        props = [p for p in props if p.get("stat", "").lower() == stat.lower()]
    props = [p for p in props if p.get("confidence", 0) >= min_conf]
    return {"props": props[:limit], "total": len(props), "last_updated": data.get("last_updated")}


@router.get("/api/streaks")
def get_streaks(
    window: int = Query(10),
    min_rate: float = Query(0.5),
    stat: str = Query(None),
    team: str = Query(None),
    perfect_only: bool = Query(False),
    limit: int = Query(100),
):
    result = streak_cache.get()
    streaks = result.get("streaks", [])
    loading = result.get("loading", False)

    filtered = []
    for s in streaks:
        wd = s.get("windows", {}).get(window)
        if not wd:
            continue
        if wd["hit_rate"] < min_rate:
            continue
        if stat and s.get("stat") != stat:
            continue
        if team and s.get("team", "").upper() != team.upper():
            continue
        if perfect_only and wd["hits"] != window:
            continue

        filtered.append({
            "player":       s["player"],
            "team":         s["team"],
            "position":     s.get("position", ""),
            "stat":         s["stat"],
            "stat_label":   s["stat_label"],
            "threshold":    s["threshold"],
            "label":        s["label"],
            "season_avg":   s["season_avg"],
            "recent_avg":   s.get("recent_avg", s["season_avg"]),
            "trend":        s.get("trend", "stable"),
            "hits":         wd["hits"],
            "games":        window,
            "hit_rate":     wd["hit_rate"],
            "pct":          wd["pct"],
            "is_perfect":   wd["hits"] == window,
            "last_5_vals":  s.get("last_5_vals", []),
            "last_5_dates": s.get("last_5_dates", []),
            "all_windows":  s.get("windows", {}),
        })

    filtered.sort(key=lambda x: (-int(x["is_perfect"]), -x["hit_rate"], -x["threshold"]))

    return {
        "streaks":      filtered[:limit],
        "total":        len(filtered),
        "window":       window,
        "loading":      loading,
        "last_updated": result.get("last_updated"),
    }


@router.get("/api/player-logs-debug")
def player_logs_debug(name: str = Query(..., description="Partial player name e.g. 'Wagner'")):
    """
    Debug endpoint: fetch raw game logs for a specific player by name.
    Shows exactly what dates and stat values the streak engine sees.
    Usage: /api/player-logs-debug?name=Wagner
    """
    import time
    from player_logs import get_all_player_base_stats, get_player_game_logs_batch

    # Find matching players in base stats
    player_base = get_all_player_base_stats()
    matches = [
        (pid, p) for pid, p in player_base.items()
        if name.lower() in p.get("name", "").lower()
    ]

    if not matches:
        return {"error": f"No player found matching '{name}'", "available": []}

    results = []
    for pid, pdata in matches[:3]:  # Limit to 3 matches
        logs = get_player_game_logs_batch([pid], last_n=15)
        player_logs = logs.get(pid, [])

        results.append({
            "player_id":   pid,
            "name":        pdata["name"],
            "team":        pdata["team_abbrev"],
            "season_avgs": {
                "pts": pdata["pts"],
                "reb": pdata["reb"],
                "ast": pdata["ast"],
                "3pm": pdata["3pm"],
                "stl": pdata["stl"],
                "blk": pdata["blk"],
                "mins": pdata["mins"],
            },
            "logs_fetched": len(player_logs),
            "games": [
                {
                    "date":  g.get("game_date", "")[:10],
                    "matchup": g.get("matchup", ""),
                    "pts":   g.get("pts"),
                    "reb":   g.get("reb"),
                    "ast":   g.get("ast"),
                    "3pm":   g.get("3pm"),
                    "stl":   g.get("stl"),
                    "blk":   g.get("blk"),
                    "mins":  g.get("mins"),
                }
                for g in player_logs
            ],
        })

    return {"query": name, "matches": results}


@router.get("/api/streak-force-refresh")
def streak_force_refresh():
    """Force the streak cache to recalculate immediately."""
    result = streak_cache.get(force_refresh=True)
    return {
        "status": "refresh triggered",
        "loading": result.get("loading", False),
        "streaks_cached": len(result.get("streaks", [])),
        "last_updated": result.get("last_updated"),
    }


@router.get("/api/debug")
def debug():
    data = cache.get()
    streak_data = streak_cache.get()
    return {
        "games_found":     len(data.get("games", [])),
        "legs_scored":     data.get("legs_scored", 0),
        "props_scored":    data.get("props_scored", 0),
        "streaks_found":   len(streak_data.get("streaks", [])),
        "streaks_loading": streak_data.get("loading", False),
        "streaks_updated": streak_data.get("last_updated"),
        "game_list": [
            {"away": g.get("away_team_abbrev"), "home": g.get("home_team_abbrev"),
             "spread": g.get("spread_line"), "total": g.get("total_line")}
            for g in data.get("games", [])
        ],
        "last_updated": data.get("last_updated"),
    }


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


@router.get("/api/odds-raw")
def odds_raw():
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "spreads", "oddsFormat": "decimal"},
            timeout=15,
        )
        data = r.json()
        return {
            "status": r.status_code,
            "events": [{"home": e.get("home_team"), "away": e.get("away_team")}
                       for e in (data if isinstance(data, list) else [])],
        }
    except Exception as e:
        return {"error": str(e)}
