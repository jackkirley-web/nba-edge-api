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
        # Stale data flags — shown as banner in app
        "is_stale":       data.get("_stale", False),
        "stale_reason":   data.get("_stale_reason", ""),
    }


@router.get("/api/slate")
def get_slate():
    data = cache.get()
    return {
        "games":        data.get("games", []),
        "last_updated": data.get("last_updated"),
        "is_stale":     data.get("_stale", False),
    }


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
    data  = cache.get()
    props = data.get("props", [])
    if game:
        props = [p for p in props if game.upper() in p.get("game","").upper()]
    if stat:
        props = [p for p in props if p.get("stat","").lower() == stat.lower()]
    props = [p for p in props if p.get("confidence", 0) >= min_conf]
    return {
        "props":      props[:limit],
        "total":      len(props),
        "last_updated": data.get("last_updated"),
        "is_stale":   data.get("_stale", False),
        "stale_reason": data.get("_stale_reason", ""),
    }


@router.get("/api/streaks")
def get_streaks(
    window: int = Query(10),
    min_rate: float = Query(0.5),
    stat: str = Query(None),
    team: str = Query(None),
    perfect_only: bool = Query(False),
    limit: int = Query(100),
):
    result  = streak_cache.get()
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
        if team and s.get("team","").upper() != team.upper():
            continue
        if perfect_only and wd["hits"] != window:
            continue

        filtered.append({
            "player":       s["player"],
            "team":         s["team"],
            "position":     s.get("position",""),
            "stat":         s["stat"],
            "stat_label":   s["stat_label"],
            "threshold":    s["threshold"],
            "label":        s["label"],
            "season_avg":   s["season_avg"],
            "recent_avg":   s.get("recent_avg", s["season_avg"]),
            "trend":        s.get("trend","stable"),
            "hits":         wd["hits"],
            "games":        window,
            "hit_rate":     wd["hit_rate"],
            "pct":          wd["pct"],
            "is_perfect":   wd["hits"] == window,
            "last_5_vals":  s.get("last_5_vals",[]),
            "last_5_dates": s.get("last_5_dates",[]),
            "all_windows":  s.get("windows",{}),
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
def player_logs_debug(name: str = Query(...)):
    from player_logs import get_all_player_base_stats, get_player_game_logs_batch
    player_base = get_all_player_base_stats()
    matches = [(pid, p) for pid, p in player_base.items()
               if name.lower() in p.get("name","").lower()]
    results = []
    for pid, pdata in matches[:3]:
        logs = get_player_game_logs_batch([pid], last_n=15)
        results.append({
            "player_id":   pid,
            "name":        pdata["name"],
            "team":        pdata["team_abbrev"],
            "source":      pdata.get("source","unknown"),
            "season_avgs": {s: pdata[s] for s in ["pts","reb","ast","3pm","stl","blk","mins"]},
            "logs_fetched": len(logs.get(pid,[])),
            "games": logs.get(pid,[]),
        })
    return {"query": name, "matches": results}


@router.get("/api/streak-force-refresh")
def streak_force_refresh():
    result = streak_cache.get(force_refresh=True)
    return {
        "status":         "refresh triggered",
        "loading":        result.get("loading", False),
        "streaks_cached": len(result.get("streaks",[])),
        "last_updated":   result.get("last_updated"),
    }


@router.get("/api/debug")
def debug():
    data        = cache.get()
    streak_data = streak_cache.get()
    try:
        from data_store import get_disk_meta, get_data_age_str
        disk_meta = get_disk_meta()
        disk_age  = get_data_age_str()
    except Exception:
        disk_meta = {}
        disk_age  = "unknown"

    return {
        "games_found":     len(data.get("games",[])),
        "legs_scored":     data.get("legs_scored",0),
        "props_scored":    data.get("props_scored",0),
        "streaks_found":   len(streak_data.get("streaks",[])),
        "streaks_loading": streak_data.get("loading",False),
        "is_stale":        data.get("_stale",False),
        "stale_reason":    data.get("_stale_reason",""),
        "disk_age":        disk_age,
        "disk_meta":       disk_meta,
        "game_list": [
            {"away": g.get("away_team_abbrev"), "home": g.get("home_team_abbrev"),
             "spread": g.get("spread_line"), "total": g.get("total_line")}
            for g in data.get("games",[])
        ],
        "last_updated": data.get("last_updated"),
    }


@router.get("/api/refresh")
def force_refresh():
    data = cache.get(force_refresh=True)
    return {
        "status":         "refreshed",
        "last_updated":   data.get("last_updated"),
        "games_analyzed": data.get("games_analyzed",0),
        "legs_scored":    data.get("legs_scored",0),
        "props_scored":   data.get("props_scored",0),
        "is_stale":       data.get("_stale",False),
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
