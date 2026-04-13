# routes_afl.py -- AFL API endpoints
# All routes prefixed /api/afl/
# Mirrors NBA route structure for frontend consistency

from fastapi import APIRouter, Query
from cache_afl import afl_cache, afl_streak_cache

router = APIRouter(prefix="/api/afl")


@router.get("/picks")
def afl_picks(refresh: bool = Query(False)):
    """AFL multis (Safe/Mid/Lotto) for the upcoming round."""
    data = afl_cache.get(force_refresh=refresh)
    return {
        "picks":        data.get("picks", {}),
        "round":        data.get("round"),
        "year":         data.get("year"),
        "last_updated": data.get("last_updated"),
        "legs_scored":  data.get("legs_scored", 0),
        "props_scored": data.get("props_scored", 0),
        "sport":        "AFL",
    }


@router.get("/games")
def afl_games():
    """Upcoming round fixtures with odds, ladder positions, and tips."""
    data = afl_cache.get()
    return {
        "games":        data.get("games", []),
        "round":        data.get("round"),
        "year":         data.get("year"),
        "last_updated": data.get("last_updated"),
        "sport":        "AFL",
    }


@router.get("/ladder")
def afl_ladder():
    """Current AFL ladder standings."""
    data = afl_cache.get()
    return {
        "ladder":       data.get("ladder", []),
        "round":        data.get("round"),
        "last_updated": data.get("last_updated"),
    }


@router.get("/props")
def afl_props(
    game:     str = Query(None, description="Filter by game e.g. 'GEE @ COL'"),
    stat:     str = Query(None, description="Filter by stat e.g. 'disposals'"),
    team:     str = Query(None, description="Filter by team abbrev e.g. 'GEE'"),
    real_only: bool = Query(False, description="Only show props with real bookmaker lines"),
    min_conf: int  = Query(55, description="Minimum confidence score"),
    limit:    int  = Query(100),
):
    """AFL player props with projections and bookmaker lines."""
    data  = afl_cache.get()
    props = data.get("props", [])

    if game:
        props = [p for p in props if game.upper() in p.get("game", "").upper()]
    if stat:
        props = [p for p in props if p.get("stat", "").lower() == stat.lower()]
    if team:
        props = [p for p in props if p.get("team", "").upper() == team.upper()]
    if real_only:
        props = [p for p in props if p.get("has_real_line")]
    props = [p for p in props if p.get("confidence", 0) >= min_conf]

    return {
        "props":        props[:limit],
        "total":        len(props),
        "last_updated": data.get("last_updated"),
        "sport":        "AFL",
    }


@router.get("/streaks")
def afl_streaks(
    window:        int  = Query(10),
    stat:          str  = Query(None),
    team:          str  = Query(None),
    perfect_only:  bool = Query(False),
    force_refresh: bool = Query(False),
):
    """AFL player stat streaks."""
    result  = afl_streak_cache.get(force_refresh=force_refresh)
    streaks = result.get("streaks", [])

    if stat:
        streaks = [s for s in streaks if s.get("stat", "").lower() == stat.lower()]
    if team:
        streaks = [s for s in streaks if s.get("team", "").upper() == team.upper()]
    if perfect_only:
        streaks = [s for s in streaks if s.get("is_perfect")]

    # Sort by selected window hit rate
    def sort_key(s):
        wd = s.get("windows", {}).get(window, {})
        return wd.get("hit_rate", 0)
    streaks.sort(key=sort_key, reverse=True)

    return {
        "streaks":      streaks,
        "total":        len(streaks),
        "loading":      result.get("loading", False),
        "last_updated": result.get("last_updated"),
        "window":       window,
        "sport":        "AFL",
    }


@router.get("/streak-force-refresh")
def afl_streak_refresh():
    """Force a refresh of AFL streak data."""
    result = afl_streak_cache.get(force_refresh=True)
    return {
        "status":        "refresh triggered",
        "loading":       result.get("loading"),
        "streaks_count": len(result.get("streaks", [])),
        "sport":         "AFL",
    }


@router.get("/debug")
def afl_debug():
    """Debug endpoint for AFL data."""
    data   = afl_cache.get()
    streak = afl_streak_cache.get()
    return {
        "round":          data.get("round"),
        "games_found":    len(data.get("games", [])),
        "legs_scored":    len(data.get("legs", [])),
        "props_scored":   len(data.get("props", [])),
        "streaks_found":  len(streak.get("streaks", [])),
        "streaks_loading": streak.get("loading", False),
        "last_updated":   data.get("last_updated"),
        "games": [
            {
                "home":   g.get("home_team"),
                "away":   g.get("away_team"),
                "time":   g.get("game_time"),
                "venue":  g.get("venue"),
                "h2h_odds":    g.get("home_odds"),
                "spread": g.get("spread_line"),
                "total":  g.get("total_line"),
            }
            for g in data.get("games", [])
        ],
        "sport": "AFL",
    }


@router.get("/refresh")
def afl_refresh():
    """Force a full AFL data refresh."""
    data = afl_cache.get(force_refresh=True)
    return {
        "status":       "refreshed",
        "round":        data.get("round"),
        "last_updated": data.get("last_updated"),
        "legs_scored":  data.get("legs_scored", 0),
        "props_scored": data.get("props_scored", 0),
        "sport":        "AFL",
    }
