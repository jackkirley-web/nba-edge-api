# routes_greyhound.py -- Greyhound racing API endpoints
# All routes prefixed /api/grey/

from fastapi import APIRouter, Query
from cache_greyhound import greyhound_cache

router = APIRouter(prefix="/api/grey")


@router.get("/meetings")
def grey_meetings(refresh: bool = Query(False)):
    """All today's AU greyhound meetings with scored races."""
    data = greyhound_cache.get(force_refresh=refresh)
    return {
        "meetings":      data.get("meetings", []),
        "total_races":   data.get("total_races", 0),
        "total_runners": data.get("total_runners", 0),
        "has_odds":      data.get("has_odds", False),
        "last_updated":  data.get("last_updated"),
        "sport":         "Greyhounds",
    }


@router.get("/races")
def grey_races(
    track: str   = Query(None, description="Filter by track name"),
    state: str   = Query(None, description="Filter by state e.g. VIC"),
    limit: int   = Query(200),
):
    """All scored races, optionally filtered by track or state."""
    data     = greyhound_cache.get()
    meetings = data.get("meetings", [])

    all_races = []
    for meeting in meetings:
        if track and track.lower() not in meeting.get("track", "").lower():
            continue
        if state and state.upper() != meeting.get("state", "").upper():
            continue
        for race in meeting.get("races", []):
            all_races.append({
                **race,
                "meeting_track": meeting.get("track"),
                "meeting_state": meeting.get("state"),
                "condition":     meeting.get("condition"),
            })

    return {
        "races":        all_races[:limit],
        "total":        len(all_races),
        "last_updated": data.get("last_updated"),
        "sport":        "Greyhounds",
    }


@router.get("/top4")
def grey_top4(
    track:  str  = Query(None),
    state:  str  = Query(None),
    limit:  int  = Query(100),
):
    """Top 4 picks for every race across all meetings today."""
    data     = greyhound_cache.get()
    meetings = data.get("meetings", [])

    picks = []
    for meeting in meetings:
        if track and track.lower() not in meeting.get("track", "").lower():
            continue
        if state and state.upper() != meeting.get("state", "").upper():
            continue
        for race in meeting.get("races", []):
            if not race.get("top_4"):
                continue
            picks.append({
                "track":      meeting.get("track"),
                "state":      meeting.get("state"),
                "condition":  meeting.get("condition"),
                "race_num":   race.get("race_num"),
                "race_time":  race.get("race_time"),
                "distance":   race.get("distance"),
                "grade":      race.get("grade"),
                "has_odds":   race.get("has_odds"),
                "top_4":      race.get("top_4", []),
            })

    return {
        "picks":        picks[:limit],
        "total_races":  len(picks),
        "last_updated": data.get("last_updated"),
        "sport":        "Greyhounds",
    }


@router.get("/debug")
def grey_debug():
    """Debug info for greyhound cache."""
    data = greyhound_cache.get()
    return {
        "meetings":       len(data.get("meetings", [])),
        "total_races":    data.get("total_races", 0),
        "has_odds":       data.get("has_odds", False),
        "last_updated":   data.get("last_updated"),
        "meeting_list": [
            {
                "track":    m.get("track"),
                "state":    m.get("state"),
                "races":    len(m.get("races", [])),
                "condition":m.get("condition"),
            }
            for m in data.get("meetings", [])
        ],
    }


@router.get("/refresh")
def grey_refresh():
    """Force a full greyhound data refresh."""
    data = greyhound_cache.get(force_refresh=True)
    return {
        "status":       "refreshed",
        "meetings":     len(data.get("meetings", [])),
        "total_races":  data.get("total_races", 0),
        "last_updated": data.get("last_updated"),
    }
