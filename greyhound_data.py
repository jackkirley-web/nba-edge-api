# greyhound_data.py -- Australian greyhound data
# Primary: TAB.com.au internal API (JSON, no auth required, cloud-friendly)
# The TAB website uses this API itself - returns races, runners, form, odds

import logging
import time
import random
import requests
from datetime import datetime, date, timezone, timedelta

logger = logging.getLogger(__name__)

# TAB uses Venue-based API endpoints that are publicly accessible
TAB_BASE = "https://api.tab.com.au/v1/tab-info-service"
TAB_RACING = "https://api.tab.com.au/v1/tab-info-service/racing"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.tab.com.au/",
    "Origin": "https://www.tab.com.au",
}

# State jurisdictions for TAB
JURISDICTIONS = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "ACT", "NT"]


def _get(url, params=None, timeout=20):
    for attempt in range(3):
        try:
            time.sleep(0.6 + random.uniform(0, 0.3) + attempt * 1.0)
            r = requests.get(url, params=params or {}, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.json()
            logger.warning("TAB API %s returned %d (attempt %d)", url, r.status_code, attempt + 1)
        except Exception as e:
            logger.warning("TAB API %s failed attempt %d: %s", url, attempt + 1, e)
    return None


def get_today_meetings() -> list:
    """
    Fetch all today's AU greyhound meetings from TAB API.
    Returns list of meeting dicts with races and runners.
    """
    # Get AEST date (UTC+10)
    aest = datetime.now(timezone(timedelta(hours=10)))
    date_str = aest.strftime("%Y-%m-%d")

    logger.info("Fetching greyhound meetings for %s AEST", date_str)

    # TAB meetings endpoint
    data = _get(f"{TAB_RACING}/meetings/greyhound/{date_str}", params={"jurisdiction": "NSW"})

    if not data:
        # Try alternate endpoint format
        data = _get(f"{TAB_BASE}/racing/meetings", params={
            "raceType": "G",
            "date": date_str,
        })

    if not data:
        # Try the race card endpoint
        data = _get("https://api.tab.com.au/v1/tab-info-service/racing/meetings/G", params={
            "date": date_str,
            "jurisdiction": "NSW",
        })

    if not data:
        logger.warning("TAB API unavailable, trying alternative sources")
        return _get_meetings_alternative(date_str)

    return _parse_tab_meetings(data, date_str)


def _get_meetings_alternative(date_str: str) -> list:
    """
    Alternative sources when TAB API fails.
    Tries Sportsbet and Racing.com public APIs.
    """
    # Try Racing.com (Australian racing aggregator - publicly accessible JSON)
    try:
        r = requests.get(
            "https://www.racing.com/racing/api/meetings",
            params={"date": date_str, "type": "G"},
            headers=HEADERS,
            timeout=15,
        )
        if r.status_code == 200 and r.content:
            return _parse_racingcom_meetings(r.json(), date_str)
    except Exception as e:
        logger.warning("Racing.com failed: %s", e)

    # Try Sportsbet internal API
    try:
        r = requests.get(
            "https://www.sportsbet.com.au/apigw/racing-service/v2/meetings",
            params={"date": date_str, "type": "Greyhound Racing"},
            headers={**HEADERS, "Referer": "https://www.sportsbet.com.au/"},
            timeout=15,
        )
        if r.status_code == 200 and r.content:
            return _parse_sportsbet_meetings(r.json(), date_str)
    except Exception as e:
        logger.warning("Sportsbet API failed: %s", e)

    # Last resort - try Ladbrokes
    try:
        r = requests.get(
            "https://www.ladbrokes.com.au/api/racing/meetings",
            params={"date": date_str, "sportCode": "G"},
            headers={**HEADERS, "Referer": "https://www.ladbrokes.com.au/"},
            timeout=15,
        )
        if r.status_code == 200 and r.content:
            return _parse_generic_meetings(r.json(), date_str, "Ladbrokes")
    except Exception as e:
        logger.warning("Ladbrokes API failed: %s", e)

    logger.error("All greyhound data sources failed for %s", date_str)
    return []


def _parse_tab_meetings(data, date_str: str) -> list:
    """Parse TAB API meeting response."""
    meetings = []

    # TAB returns meetings in various structures
    raw_meetings = (
        data if isinstance(data, list) else
        data.get("meetings", []) or
        data.get("data", {}).get("meetings", []) or
        []
    )

    for m in raw_meetings:
        try:
            track = (
                m.get("meetingName") or
                m.get("venueName") or
                m.get("name") or ""
            ).strip()

            state = (
                m.get("location") or
                m.get("state") or
                m.get("jurisdiction") or
                _guess_state(track)
            ).upper()

            condition = _parse_condition(
                m.get("trackCondition") or m.get("condition") or "Good"
            )

            # Get races
            raw_races = m.get("races", []) or m.get("events", []) or []
            races = []
            for race_data in raw_races:
                race = _parse_tab_race(race_data, track, condition)
                if race and race.get("runners"):
                    races.append(race)

            if not races:
                # Fetch races separately if not included
                meeting_id = m.get("meetingId") or m.get("id")
                if meeting_id:
                    races = _fetch_meeting_races(meeting_id, track, condition, date_str)

            if races:
                meetings.append({
                    "track":           track,
                    "state":           state,
                    "condition":       condition,
                    "date":            date_str,
                    "races":           races,
                    "meeting_id":      m.get("meetingId") or m.get("id"),
                })

        except Exception as e:
            logger.warning("Failed to parse TAB meeting: %s", e)

    logger.info("Parsed %d meetings from TAB API", len(meetings))
    return meetings


def _parse_tab_race(race_data: dict, track: str, condition: str) -> dict:
    """Parse a single race from TAB data."""
    try:
        race_num = int(race_data.get("raceNumber") or race_data.get("number") or 0)
        if race_num == 0:
            return {}

        # Race time
        start_time = race_data.get("raceStartTime") or race_data.get("startTime") or ""
        race_time = ""
        if start_time:
            try:
                dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                aest = dt.astimezone(timezone(timedelta(hours=10)))
                race_time = aest.strftime("%-I:%M %p")
            except Exception:
                race_time = start_time[:5] if len(start_time) >= 5 else start_time

        # Distance
        distance = None
        dist_raw = race_data.get("distance") or race_data.get("raceDistance")
        if dist_raw:
            try:
                distance = int(str(dist_raw).replace("m", "").strip())
            except Exception:
                pass

        # Grade
        grade = (
            race_data.get("raceClassConditions") or
            race_data.get("grade") or
            race_data.get("raceClass") or ""
        ).strip()

        # Parse runners
        runners = []
        raw_runners = race_data.get("runners") or race_data.get("entrants") or []
        for r in raw_runners:
            runner = _parse_tab_runner(r)
            if runner:
                runners.append(runner)

        if not runners:
            return {}

        return {
            "race_num":  race_num,
            "race_time": race_time,
            "distance":  distance,
            "grade":     grade,
            "condition": condition,
            "track":     track,
            "runners":   runners,
        }

    except Exception as e:
        logger.warning("Failed to parse TAB race: %s", e)
        return {}


def _parse_tab_runner(r: dict) -> dict:
    """Parse a single runner from TAB data."""
    try:
        name = (
            r.get("runnerName") or
            r.get("name") or
            r.get("dogName") or ""
        ).strip()

        if not name:
            return {}

        box = None
        box_raw = r.get("barrierNumber") or r.get("tabNo") or r.get("number")
        if box_raw:
            try:
                box = int(box_raw)
            except Exception:
                pass

        if not box:
            return {}

        trainer = (
            r.get("trainerName") or
            r.get("trainer", {}).get("name") or ""
        ).strip()

        # Scratchings
        scratched = (
            r.get("isScratched") or
            r.get("scratched") or
            r.get("status", "").lower() in ("scratched", "scr", "s")
        )

        # Form string (last 5 results)
        form_str = r.get("form") or r.get("last5Starts") or r.get("formSummary") or ""

        # Odds
        odds = None
        odds_raw = (
            r.get("fixedOdds", {}).get("returnWin") or
            r.get("winPrice") or
            r.get("price")
        )
        if odds_raw:
            try:
                odds = float(odds_raw)
            except Exception:
                pass

        # Parse last 5 positions from form string
        last_5 = _parse_form_string(form_str)

        # Track/distance stats
        track_wins    = _safe_int(r.get("trackWins") or r.get("trackWin"))
        track_starts  = _safe_int(r.get("trackStarts") or r.get("trackStart"))
        dist_wins     = _safe_int(r.get("distanceWins") or r.get("distanceWin"))
        dist_starts   = _safe_int(r.get("distanceStarts") or r.get("distanceStart"))
        career_wins   = _safe_int(r.get("careerWins") or r.get("wins"))
        career_starts = _safe_int(r.get("careerStarts") or r.get("starts"))

        return {
            "box":            box,
            "name":           name,
            "trainer":        trainer,
            "form_str":       form_str,
            "last_5":         last_5,
            "odds":           odds,
            "track_wins":     track_wins,
            "track_starts":   track_starts,
            "dist_wins":      dist_wins,
            "dist_starts":    dist_starts,
            "career_wins":    career_wins,
            "career_starts":  career_starts,
            "scratched":      bool(scratched),
        }

    except Exception as e:
        logger.warning("Failed to parse TAB runner: %s", e)
        return {}


def _fetch_meeting_races(meeting_id, track, condition, date_str) -> list:
    """Fetch races for a specific meeting ID."""
    data = _get(f"{TAB_RACING}/meetings/G/{meeting_id}/races", params={"date": date_str})
    if not data:
        return []
    raw_races = data if isinstance(data, list) else data.get("races", [])
    races = []
    for race_data in raw_races:
        race = _parse_tab_race(race_data, track, condition)
        if race and race.get("runners"):
            races.append(race)
    return races


def _parse_racingcom_meetings(data, date_str) -> list:
    """Parse Racing.com API response."""
    meetings = []
    raw = data if isinstance(data, list) else data.get("meetings", [])
    for m in raw:
        try:
            track = m.get("name") or m.get("venue") or ""
            state = _guess_state(track)
            condition = _parse_condition(m.get("condition") or "Good")
            races = []
            for rd in m.get("races", []):
                race = _parse_tab_race(rd, track, condition)
                if race and race.get("runners"):
                    races.append(race)
            if races:
                meetings.append({
                    "track": track, "state": state, "condition": condition,
                    "date": date_str, "races": races,
                })
        except Exception as e:
            logger.warning("Racing.com parse error: %s", e)
    return meetings


def _parse_sportsbet_meetings(data, date_str) -> list:
    """Parse Sportsbet API response."""
    meetings = []
    raw = data if isinstance(data, list) else data.get("meetings", []) or data.get("data", [])
    for m in raw:
        try:
            track = m.get("meetingName") or m.get("name") or ""
            state = m.get("state") or _guess_state(track)
            condition = _parse_condition(m.get("trackCondition") or "Good")
            races = []
            for rd in m.get("races", []) or m.get("events", []):
                race = _parse_tab_race(rd, track, condition)
                if race and race.get("runners"):
                    races.append(race)
            if races:
                meetings.append({
                    "track": track, "state": state, "condition": condition,
                    "date": date_str, "races": races,
                })
        except Exception as e:
            logger.warning("Sportsbet parse error: %s", e)
    return meetings


def _parse_generic_meetings(data, date_str, source) -> list:
    """Generic parser for any bookmaker API structure."""
    meetings = []
    raw = data if isinstance(data, list) else (
        data.get("meetings") or data.get("data") or data.get("results") or []
    )
    for m in raw:
        try:
            track = (m.get("meetingName") or m.get("name") or m.get("venue") or "").strip()
            if not track:
                continue
            state = _guess_state(track)
            condition = _parse_condition(m.get("trackCondition") or m.get("condition") or "Good")
            raw_races = m.get("races") or m.get("events") or []
            races = []
            for rd in raw_races:
                race = _parse_tab_race(rd, track, condition)
                if race and race.get("runners"):
                    races.append(race)
            if races:
                meetings.append({
                    "track": track, "state": state, "condition": condition,
                    "date": date_str, "races": races, "source": source,
                })
        except Exception as e:
            logger.warning("%s parse error: %s", source, e)
    return meetings


# -- Form parsing -----------------------------------------------------------

def _parse_form_string(form_str: str) -> list:
    """Parse form string like '1-2-4-1-3' into list of int positions."""
    if not form_str:
        return []
    import re
    parts = re.split(r"[-.\s,]", str(form_str))
    if len(parts) == 1:
        parts = list(str(form_str))
    positions = []
    for p in parts:
        p = str(p).strip().upper()
        if p in ("F", "D", "N", "X", "S"):
            positions.append(8)
        elif p.isdigit():
            positions.append(int(p))
    return positions[:5]


def _parse_condition(raw: str) -> str:
    raw = str(raw).strip().title()
    mapping = {
        "Good": "Good", "Good 4": "Good", "Firm": "Good", "Fast": "Good",
        "Soft": "Soft", "Soft 5": "Soft", "Soft 6": "Soft",
        "Heavy": "Heavy", "Heavy 8": "Heavy", "Heavy 9": "Heavy", "Heavy 10": "Heavy",
        "Wet": "Wet", "Rain Affected": "Wet",
    }
    return mapping.get(raw, "Good")


def _guess_state(track: str) -> str:
    track_lower = track.lower()
    vic = ["meadows", "sandown", "ballarat", "geelong", "warragul", "cranbourne",
           "shepparton", "horsham", "bendigo", "traralgon", "sale", "healesville",
           "hamilton", "mildura", "kilmore", "pakenham"]
    nsw = ["wentworth", "richmond", "bathurst", "gosford", "tamworth", "gunnedah",
           "nowra", "grafton", "lismore", "Newcastle", "dapto", "penrith"]
    qld = ["albion", "ipswich", "capalaba", "townsville", "rockhampton", "logan",
           "bundaberg", "toowoomba", "cairns", "mackay"]
    sa  = ["angle park", "gawler", "murray bridge", "mount gambier"]
    wa  = ["cannington", "mandurah", "northam", "albany"]
    tas = ["launceston", "devonport", "hobart"]
    nt  = ["darwin", "alice springs"]

    for t in vic:
        if t in track_lower: return "VIC"
    for t in nsw:
        if t in track_lower: return "NSW"
    for t in qld:
        if t in track_lower: return "QLD"
    for t in sa:
        if t in track_lower: return "SA"
    for t in wa:
        if t in track_lower: return "WA"
    for t in tas:
        if t in track_lower: return "TAS"
    for t in nt:
        if t in track_lower: return "NT"
    return "AU"


# -- Box advantage database -----------------------------------------------
# Historical win % by box position for major AU greyhound tracks.

BOX_WIN_PCT = {
    "the meadows":  {525:{1:17.8,2:14.2,3:13.1,4:12.0,5:11.8,6:11.5,7:10.8,8:8.8}, 600:{1:16.5,2:13.5,3:12.5,4:12.0,5:12.0,6:11.5,7:11.0,8:11.0}},
    "sandown park": {515:{1:18.2,2:14.5,3:13.0,4:12.0,5:11.5,6:11.0,7:10.5,8:9.3}, 595:{1:16.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:11.0,8:10.0}},
    "ballarat":     {450:{1:18.5,2:14.8,3:13.2,4:12.0,5:11.0,6:10.5,7:10.0,8:10.0}, 520:{1:17.0,2:14.0,3:12.5,4:12.0,5:11.5,6:11.5,7:11.0,8:10.5}},
    "geelong":      {400:{1:19.0,2:15.0,3:13.5,4:12.0,5:11.0,6:10.0,7:9.5,8:10.0},  520:{1:17.5,2:14.5,3:13.0,4:12.0,5:11.5,6:11.0,7:10.5,8:10.0}},
    "warragul":     {390:{1:20.5,2:15.5,3:13.0,4:12.0,5:10.5,6:10.0,7:9.5,8:9.0},   450:{1:18.0,2:14.5,3:13.0,4:12.0,5:11.0,6:10.5,7:11.0,8:10.0}},
    "cranbourne":   {311:{1:22.0,2:16.0,3:13.0,4:11.0,5:10.5,6:10.0,7:9.5,8:8.0},   520:{1:17.0,2:14.0,3:12.5,4:12.0,5:12.0,6:11.5,7:11.0,8:10.0}},
    "wentworth park":{520:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.0,7:10.5,8:9.5}, 720:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0}},
    "albion park":  {520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5},  600:{1:16.0,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:9.5}},
    "angle park":   {520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5},  595:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0}},
    "cannington":   {520:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.0},  642:{1:15.5,2:13.5,3:13.0,4:13.0,5:12.5,6:12.0,7:11.0,8:9.5}},
    "ipswich":      {431:{1:19.0,2:15.0,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:9.5},  520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5}},
    "launceston":   {461:{1:19.0,2:15.0,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:9.5},  553:{1:16.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:10.0}},
    "richmond":     {410:{1:19.5,2:15.5,3:13.0,4:12.0,5:11.0,6:10.0,7:9.5,8:9.5},   525:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5}},
    "default":      {300:{1:22.0,2:16.0,3:13.0,4:11.0,5:10.0,6:9.5,7:9.0,8:9.5},
                     400:{1:19.0,2:14.5,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:10.0},
                     500:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.0},
                     600:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0},
                     700:{1:14.5,2:13.5,3:13.0,4:13.0,5:12.5,6:12.5,7:11.5,8:9.5}},
}


def get_box_win_pct(track: str, distance: int, box: int) -> float:
    track_key = track.lower().strip()
    track_data = BOX_WIN_PCT.get(track_key)
    if not track_data:
        for key in BOX_WIN_PCT:
            if key != "default" and (key in track_key or track_key in key):
                track_data = BOX_WIN_PCT[key]
                break
    if not track_data:
        track_data = BOX_WIN_PCT["default"]
    distances = sorted(track_data.keys())
    closest = min(distances, key=lambda d: abs(d - (distance or 520)))
    return track_data.get(closest, {}).get(box, 12.5)


WET_FACTOR = {"good":1.0,"fast":1.0,"soft":1.05,"heavy":1.12,"wet":1.10}

def get_condition_factor(condition: str) -> float:
    return WET_FACTOR.get(condition.lower(), 1.0)


def _safe_int(v) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0
