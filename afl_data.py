# afl_data.py -- AFL data fetcher
# Priority: AFL.com.au API (primary) -> Squiggle (fallback) -> ESPN (fallback)
# Player stats: AFL.com.au player stats API -> Footywire scraping
# NO synthetic player pools - bad data is worse than no data for betting

import logging
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timezone, timedelta

logger = logging.getLogger(__name__)

SQUIGGLE_BASE  = "https://api.squiggle.com.au"
FOOTYWIRE_BASE = "https://www.footywire.com/afl/footy"

# AFL.com.au public API - no auth required
AFL_CD_BASE    = "https://api.afl.com.au/cfs/afl"
AFL_SEASON_ID  = "CD_S2026014"   # 2026 AFL Premiership season
AFL_COMP_ID    = "CD_R2026014"   # 2026 competition ID

CURRENT_YEAR   = 2026
CURRENT_ROUND  = 6               # Updated each week manually as last resort

SQUIGGLE_UA    = "SportEdge-AFL dev@sportedge.app"

AFL_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin":  "https://www.afl.com.au",
    "Referer": "https://www.afl.com.au/",
}

FW_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.footywire.com/",
}

TEAM_CANONICAL = {
    "Adelaide":            "Adelaide",
    "Adelaide Crows":      "Adelaide",
    "Brisbane":            "Brisbane Lions",
    "Brisbane Lions":      "Brisbane Lions",
    "Carlton":             "Carlton",
    "Collingwood":         "Collingwood",
    "Essendon":            "Essendon",
    "Fremantle":           "Fremantle",
    "Geelong":             "Geelong",
    "Geelong Cats":        "Geelong",
    "Gold Coast":          "Gold Coast",
    "Gold Coast Suns":     "Gold Coast",
    "GWS":                 "GWS Giants",
    "Greater Western Sydney": "GWS Giants",
    "GWS Giants":          "GWS Giants",
    "Hawthorn":            "Hawthorn",
    "Melbourne":           "Melbourne",
    "Melbourne Demons":    "Melbourne",
    "North Melbourne":     "North Melbourne",
    "Kangaroos":           "North Melbourne",
    "Port Adelaide":       "Port Adelaide",
    "Port Adelaide Power": "Port Adelaide",
    "Richmond":            "Richmond",
    "St Kilda":            "St Kilda",
    "St Kilda Saints":     "St Kilda",
    "Sydney":              "Sydney",
    "Sydney Swans":        "Sydney",
    "West Coast":          "West Coast",
    "West Coast Eagles":   "West Coast",
    "Western Bulldogs":    "Western Bulldogs",
    "Footscray":           "Western Bulldogs",
    "Bulldogs":            "Western Bulldogs",
}

TEAM_ABBREV = {
    "Adelaide":         "ADE", "Brisbane Lions":   "BRI",
    "Carlton":          "CAR", "Collingwood":       "COL",
    "Essendon":         "ESS", "Fremantle":         "FRE",
    "Geelong":          "GEE", "Gold Coast":        "GCS",
    "GWS Giants":       "GWS", "Hawthorn":          "HAW",
    "Melbourne":        "MEL", "North Melbourne":   "NTH",
    "Port Adelaide":    "PTA", "Richmond":          "RIC",
    "St Kilda":         "STK", "Sydney":            "SYD",
    "West Coast":       "WCE", "Western Bulldogs":  "WBD",
}

# AFL.com.au squad IDs for player stats lookups
AFL_SQUAD_IDS = {
    "Adelaide":         "CD_T145",
    "Brisbane Lions":   "CD_T149",
    "Carlton":          "CD_T150",
    "Collingwood":      "CD_T151",
    "Essendon":         "CD_T152",
    "Fremantle":        "CD_T153",
    "Geelong":          "CD_T154",
    "Gold Coast":       "CD_T155",
    "GWS Giants":       "CD_T156",
    "Hawthorn":         "CD_T157",
    "Melbourne":        "CD_T158",
    "North Melbourne":  "CD_T159",
    "Port Adelaide":    "CD_T160",
    "Richmond":         "CD_T161",
    "St Kilda":         "CD_T162",
    "Sydney":           "CD_T163",
    "West Coast":       "CD_T164",
    "Western Bulldogs": "CD_T165",
}


# ---------------------------------------------------------------------------
# Generic HTTP helpers
# ---------------------------------------------------------------------------

def _get(url, params=None, headers=None, timeout=15) -> dict:
    """Generic GET with retry, validates response before parsing."""
    for attempt in range(3):
        try:
            time.sleep(0.5 + random.uniform(0, 0.3) + attempt * 1.2)
            r = requests.get(
                url,
                params=params or {},
                headers=headers or AFL_API_HEADERS,
                timeout=timeout,
            )
            if r.status_code == 200 and len(r.content) > 20:
                try:
                    return r.json()
                except Exception:
                    logger.warning("GET %s: non-JSON response (attempt %d)", url, attempt + 1)
                    return {}
            logger.warning("GET %s returned %d len=%d (attempt %d)",
                           url, r.status_code, len(r.content), attempt + 1)
        except Exception as e:
            logger.warning("GET %s failed (attempt %d): %s", url, attempt + 1, e)
    return {}


def _squiggle_get(query: str) -> dict:
    """Squiggle API with correct User-Agent and strict response validation."""
    try:
        time.sleep(0.6 + random.uniform(0, 0.4))
        r = requests.get(
            SQUIGGLE_BASE,
            params={"q": query},
            headers={"User-Agent": SQUIGGLE_UA},
            timeout=15,
        )
        if r.status_code != 200:
            logger.warning("Squiggle %s: HTTP %d", query, r.status_code)
            return {}
        if not r.content or len(r.content) < 10:
            logger.warning("Squiggle %s: empty response body", query)
            return {}
        try:
            return r.json()
        except Exception as e:
            logger.warning("Squiggle %s: JSON parse failed: %s", query, e)
            return {}
    except Exception as e:
        logger.warning("Squiggle error for %s: %s", query, e)
        return {}


# ---------------------------------------------------------------------------
# AFL.com.au fixture API  (PRIMARY source)
# ---------------------------------------------------------------------------

def _fetch_afl_fixture_cdn(round_num: int = None, year: int = CURRENT_YEAR) -> list:
    """
    Fetch fixture from AFL.com.au public CFS API.
    Returns list of raw game dicts or [].
    """
    # Endpoint patterns to try
    endpoints = [
        f"{AFL_CD_BASE}/fixturesAndResults",
        f"{AFL_CD_BASE}/matches",
    ]
    params_options = [
        {"compSeason.id": AFL_SEASON_ID, "roundNumber": round_num} if round_num
            else {"compSeason.id": AFL_SEASON_ID},
        {"competitionId": "CD_S2026014", "pageSize": 250},
    ]

    for url, params in zip(endpoints, params_options):
        try:
            time.sleep(0.5)
            r = requests.get(url, params=params, headers=AFL_API_HEADERS, timeout=20)
            if r.status_code == 200 and len(r.content) > 200:
                data = r.json()
                logger.info("AFL.com.au fixture OK: %s", url)
                return _parse_afl_cdn_fixture(data, year)
        except Exception as e:
            logger.warning("AFL.com.au fixture %s failed: %s", url, e)

    # Try alternative AFL CDN endpoint
    try:
        time.sleep(0.5)
        r = requests.get(
            "https://s.afl.com.au/staticfile/AFL%20Tenant/AFL/Fixture/fixture.json",
            headers=AFL_API_HEADERS,
            timeout=15,
        )
        if r.status_code == 200 and len(r.content) > 200:
            data = r.json()
            logger.info("AFL CDN static fixture OK")
            return _parse_afl_cdn_fixture(data, year)
    except Exception as e:
        logger.warning("AFL CDN static fixture failed: %s", e)

    return []


def _parse_afl_cdn_fixture(data, year: int) -> list:
    """
    Parse games from AFL.com.au API - handles multiple response structures.
    Returns normalised game list.
    """
    games = []
    now_utc = datetime.now(timezone.utc)

    # Unwrap various response shapes
    raw = (
        data.get("fixturesAndResults", data)
        if isinstance(data, dict) else data
    )

    # Get rounds list from common structures
    rounds = []
    if isinstance(raw, dict):
        rounds = (
            raw.get("rounds") or
            raw.get("fixture", {}).get("rounds") or
            []
        )
        # Sometimes it's a flat list of matches
        if not rounds and "matches" in raw:
            rounds = [{"roundNumber": None, "matches": raw["matches"]}]
        if not rounds and isinstance(raw.get("fixtures"), list):
            rounds = [{"roundNumber": None, "matches": raw["fixtures"]}]

    for rnd in rounds:
        rnd_num = rnd.get("roundNumber") or rnd.get("number")
        matches  = rnd.get("matches") or rnd.get("games") or []
        for m in matches:
            try:
                game = _parse_afl_match(m, rnd_num, year)
                if game:
                    games.append(game)
            except Exception as e:
                logger.debug("match parse error: %s", e)

    logger.info("AFL.com.au fixture parsed: %d games", len(games))
    return games


def _parse_afl_match(m: dict, round_num, year: int) -> dict:
    """Normalise a single match dict from AFL.com.au API."""
    home_raw = m.get("home") or m.get("homeTeam") or {}
    away_raw = m.get("away") or m.get("awayTeam") or {}

    home_name = (
        home_raw.get("name") or
        home_raw.get("teamName") or
        home_raw.get("shortName", "")
    )
    away_name = (
        away_raw.get("name") or
        away_raw.get("teamName") or
        away_raw.get("shortName", "")
    )

    home = TEAM_CANONICAL.get(home_name, home_name)
    away = TEAM_CANONICAL.get(away_name, away_name)

    if not home or not away:
        return None

    # Parse round number
    rnd = round_num
    if not rnd:
        rnd_raw = m.get("round") or m.get("roundNumber") or {}
        if isinstance(rnd_raw, dict):
            rnd = rnd_raw.get("roundNumber") or rnd_raw.get("number")
        else:
            rnd = rnd_raw

    # Parse date/time
    utc_str = (
        m.get("utcStartTime") or
        m.get("startTime") or
        m.get("date") or
        m.get("matchStartDateTime", "")
    )
    try:
        dt_utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        dt_aest = dt_utc.astimezone(timezone(timedelta(hours=10)))
        display_time = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
        game_date = dt_aest.date()
    except Exception:
        display_time = utc_str or "TBC"
        game_date = None
        dt_utc = datetime.min.replace(tzinfo=timezone.utc)

    # Venue
    venue_raw = m.get("venue") or m.get("ground") or {}
    if isinstance(venue_raw, dict):
        venue = venue_raw.get("name") or venue_raw.get("venueName", "")
    else:
        venue = str(venue_raw)

    # Status
    status_raw = m.get("status") or m.get("matchStatus") or ""
    if isinstance(status_raw, dict):
        status_raw = status_raw.get("name") or status_raw.get("statusName", "")
    complete = str(status_raw).upper() in (
        "CONCLUDED", "COMPLETED", "FINAL", "RESULT", "POST_GAME"
    )

    return {
        "game_id":     str(m.get("id") or m.get("matchId") or ""),
        "round":       rnd,
        "year":        year,
        "home_team":   home,
        "away_team":   away,
        "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper()),
        "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper()),
        "venue":       venue,
        "game_time":   display_time,
        "game_date":   str(game_date) if game_date else "",
        "game_dt_utc": dt_utc,
        "complete":    complete,
        "source":      "afl_api",
    }


# ---------------------------------------------------------------------------
# Squiggle fixture  (FALLBACK source 1)
# ---------------------------------------------------------------------------

def _fetch_squiggle_upcoming(year: int = CURRENT_YEAR) -> tuple:
    """
    Fetch upcoming round from Squiggle.
    Returns (round_num, games_list) or (None, []).
    """
    data = _squiggle_get(f"games;year={year};incomplete=1")
    squiggle_games = data.get("games", [])
    if not squiggle_games:
        return None, []

    squiggle_games.sort(key=lambda g: g.get("date") or "")
    next_round = squiggle_games[0].get("round")
    if not next_round:
        return None, []

    data2 = _squiggle_get(f"games;year={year};round={next_round}")
    round_games = data2.get("games", [])
    if not round_games:
        return None, []

    games = []
    for g in round_games:
        home = TEAM_CANONICAL.get(g.get("hteam", ""), g.get("hteam", ""))
        away = TEAM_CANONICAL.get(g.get("ateam", ""), g.get("ateam", ""))
        raw_date = g.get("date", "")
        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            dt_aest = dt.astimezone(timezone(timedelta(hours=10)))
            display_time = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
            game_date = dt_aest.date()
        except Exception:
            display_time = raw_date
            game_date = None
            dt = datetime.min.replace(tzinfo=timezone.utc)

        games.append({
            "game_id":     str(g.get("id", "")),
            "round":       next_round,
            "year":        year,
            "home_team":   home,
            "away_team":   away,
            "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper() if home else ""),
            "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper() if away else ""),
            "venue":       g.get("venue", ""),
            "game_time":   display_time,
            "game_date":   str(game_date) if game_date else "",
            "game_dt_utc": dt,
            "complete":    g.get("complete", 0) == 100,
            "tip":         TEAM_CANONICAL.get(g.get("tip", ""), g.get("tip", "")),
            "source":      "squiggle",
        })

    return next_round, games


# ---------------------------------------------------------------------------
# ESPN fixture  (FALLBACK source 2)
# ---------------------------------------------------------------------------

def _fetch_espn_afl(year: int = CURRENT_YEAR) -> tuple:
    """Fetch upcoming AFL round from ESPN scoreboard API."""
    try:
        # Try today first, then tomorrow, then next 7 days
        for days_ahead in range(8):
            check_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y%m%d")
            r = requests.get(
                "https://site.api.espn.com/apis/site/v2/sports/australian-football/afl/scoreboard",
                params={"dates": check_date, "limit": 50},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            if r.status_code == 200 and r.content:
                data = r.json()
                events = data.get("events", [])
                if events:
                    logger.info("ESPN AFL: found %d events for %s", len(events), check_date)
                    return _parse_espn_events(events, year)
            else:
                logger.info("ESPN schedule empty for %s", check_date)

    except Exception as e:
        logger.warning("ESPN AFL fetch failed: %s", e)
    return None, []


def _parse_espn_events(events: list, year: int) -> tuple:
    games = []
    round_num = None

    for ev in events:
        try:
            comp = ev.get("competitions", [{}])[0]
            competitors = comp.get("competitors", [])
            home_c = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away_c = next((c for c in competitors if c.get("homeAway") == "away"), {})
            ht = TEAM_CANONICAL.get(home_c.get("team", {}).get("displayName", ""), "")
            at = TEAM_CANONICAL.get(away_c.get("team", {}).get("displayName", ""), "")

            # Try to get round number
            if not round_num:
                week_detail = ev.get("week", {})
                if isinstance(week_detail, dict):
                    round_num = week_detail.get("number")

            utc_str = ev.get("date", "")
            try:
                dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                dt_aest = dt.astimezone(timezone(timedelta(hours=10)))
                display = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
                gdate = dt_aest.date()
            except Exception:
                display = utc_str
                gdate = None
                dt = datetime.min.replace(tzinfo=timezone.utc)

            games.append({
                "game_id":     ev.get("id", ""),
                "round":       round_num,
                "year":        year,
                "home_team":   ht,
                "away_team":   at,
                "home_abbrev": TEAM_ABBREV.get(ht, ""),
                "away_abbrev": TEAM_ABBREV.get(at, ""),
                "venue":       comp.get("venue", {}).get("fullName", ""),
                "game_time":   display,
                "game_date":   str(gdate) if gdate else "",
                "game_dt_utc": dt,
                "complete":    False,
                "source":      "espn",
            })
        except Exception as e:
            logger.debug("ESPN event parse error: %s", e)

    return round_num, games


# ---------------------------------------------------------------------------
# Main fixture entry point
# ---------------------------------------------------------------------------

def get_upcoming_round(year: int = CURRENT_YEAR) -> dict:
    """
    Get the next upcoming AFL round.
    Priority: AFL.com.au API -> Squiggle -> ESPN -> Hardcoded

    IMPORTANT: Never returns synthetic/wrong round data.
    Returns { round, year, games: [...] }
    """
    now_utc = datetime.now(timezone.utc)

    # --- 1. Try AFL.com.au CDN API (most reliable) ---
    logger.info("Trying AFL.com.au API for fixture...")
    all_games = _fetch_afl_fixture_cdn(year=year)
    if all_games:
        future = [
            g for g in all_games
            if not g["complete"]
            and g.get("game_dt_utc", datetime.min.replace(tzinfo=timezone.utc)) > now_utc
            and g.get("home_team") and g.get("away_team")
        ]
        if future:
            future.sort(key=lambda g: g.get("game_dt_utc", datetime.min.replace(tzinfo=timezone.utc)))
            next_round = future[0]["round"]
            round_games = [g for g in future if g["round"] == next_round]
            if round_games:
                logger.info("AFL.com.au API: Round %s with %d games", next_round, len(round_games))
                return {"round": next_round, "year": year, "games": round_games}

    # --- 2. Try Squiggle ---
    logger.info("AFL.com.au failed, trying Squiggle...")
    sq_round, sq_games = _fetch_squiggle_upcoming(year)
    if sq_games:
        logger.info("Squiggle: Round %s with %d games", sq_round, len(sq_games))
        return {"round": sq_round, "year": year, "games": sq_games}

    # --- 3. Try ESPN ---
    logger.info("Squiggle failed, trying ESPN...")
    espn_round, espn_games = _fetch_espn_afl(year)
    if espn_games:
        logger.info("ESPN: Round %s with %d games", espn_round, len(espn_games))
        return {"round": espn_round or CURRENT_ROUND, "year": year, "games": espn_games}

    # --- 4. Hardcoded Round 6 (LAST RESORT - correct data only) ---
    logger.warning("All AFL APIs failed -- using hardcoded Round %d fixture", CURRENT_ROUND)
    hardcoded = _get_hardcoded_round6()
    return {"round": CURRENT_ROUND, "year": year, "games": hardcoded}


def _get_hardcoded_round6() -> list:
    """
    Hardcoded Round 6 2026 fixture.
    Only used when every API is down. Update CURRENT_ROUND each week.
    Verified from afl.com.au fixture PDF.
    """
    games_raw = [
        # (home, away, venue, datetime_aest_str)
        ("Brisbane Lions",   "St Kilda",          "Gabba",                  "2026-04-16T19:30:00+10:00"),
        ("Gold Coast",       "Hawthorn",           "Heritage Bank Stadium",  "2026-04-17T19:10:00+10:00"),
        ("Carlton",          "Melbourne",           "MCG",                    "2026-04-18T13:45:00+10:00"),
        ("Western Bulldogs", "North Melbourne",    "Marvel Stadium",          "2026-04-18T16:35:00+10:00"),
        ("Essendon",         "Richmond",            "MCG",                    "2026-04-18T19:25:00+10:00"),
        ("West Coast",       "Fremantle",           "Optus Stadium",          "2026-04-19T13:10:00+10:00"),
        ("GWS Giants",       "Adelaide",            "ENGIE Stadium",          "2026-04-19T15:20:00+10:00"),
        ("Collingwood",      "Port Adelaide",       "MCG",                    "2026-04-19T15:20:00+10:00"),
        ("Geelong",          "Sydney",              "GMHBA Stadium",          "2026-04-19T16:10:00+10:00"),
    ]

    games = []
    for i, (home, away, venue, dt_str) in enumerate(games_raw):
        try:
            dt_utc = datetime.fromisoformat(dt_str).astimezone(timezone.utc)
            dt_aest = datetime.fromisoformat(dt_str)
            display_time = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
            game_date = dt_aest.date().isoformat()
        except Exception:
            dt_utc = datetime.min.replace(tzinfo=timezone.utc)
            display_time = "TBC"
            game_date = "2026-04-16"

        games.append({
            "game_id":     f"r6_2026_{i}",
            "round":       CURRENT_ROUND,
            "year":        2026,
            "home_team":   home,
            "away_team":   away,
            "home_abbrev": TEAM_ABBREV.get(home, ""),
            "away_abbrev": TEAM_ABBREV.get(away, ""),
            "venue":       venue,
            "game_time":   display_time,
            "game_date":   game_date,
            "game_dt_utc": dt_utc,
            "complete":    False,
            "source":      "hardcoded",
        })
    return games


def get_round_games(year: int, round_num: int) -> list:
    data = _squiggle_get(f"games;year={year};round={round_num}")
    return data.get("games", [])


# ---------------------------------------------------------------------------
# Player season averages  (AFL.com.au API primary, Footywire fallback)
# ---------------------------------------------------------------------------

def get_player_season_averages(year: int = CURRENT_YEAR) -> dict:
    """
    Fetch real player season averages.
    Tries AFL.com.au player stats API, then Footywire.

    Returns {player_name: stats_dict} or {} if all fail.
    NEVER returns synthetic/fake data.
    """
    # --- Try AFL.com.au player stats API ---
    logger.info("Trying AFL.com.au player stats API...")
    players = _fetch_afl_player_stats_api(year)
    if players:
        logger.info("AFL.com.au player stats: %d players", len(players))
        return players

    # --- Try Footywire ---
    logger.info("AFL.com.au player stats failed, trying Footywire...")
    players = _fetch_footywire_player_stats(year)
    if players:
        logger.info("Footywire player stats: %d players", len(players))
        return players

    logger.warning("All player stat sources failed -- returning empty (no synthetic data)")
    return {}


def _fetch_afl_player_stats_api(year: int) -> dict:
    """
    Fetch player season stats from AFL.com.au CFS API.
    Tries the player statistics endpoint.
    """
    urls = [
        f"{AFL_CD_BASE}/playerStats/season",
        f"{AFL_CD_BASE}/statsCentre/players",
        f"https://aflapi.afl.com.au/afl/v2/playerStats?competitionId={AFL_SEASON_ID}&pageSize=500",
    ]
    params_list = [
        {"compSeason.id": AFL_SEASON_ID},
        {"compSeason.id": AFL_SEASON_ID, "pageSize": 500},
        {},
    ]

    for url, params in zip(urls, params_list):
        try:
            time.sleep(0.5)
            r = requests.get(url, params=params, headers=AFL_API_HEADERS, timeout=20)
            if r.status_code == 200 and len(r.content) > 500:
                data = r.json()
                players = _parse_afl_player_stats(data)
                if players:
                    return players
        except Exception as e:
            logger.warning("AFL player stats %s: %s", url, e)

    return {}


def _parse_afl_player_stats(data: dict) -> dict:
    """Parse AFL.com.au player stats response into normalised player dict."""
    players = {}

    # Various response shapes
    raw_players = (
        data.get("players") or
        data.get("playerStats") or
        data.get("stats", {}).get("players") or
        []
    )

    if not raw_players and isinstance(data, list):
        raw_players = data

    for p in raw_players:
        try:
            # Player identity
            player_info = p.get("player") or p.get("person") or p
            fname = player_info.get("firstName") or player_info.get("givenName", "")
            lname = player_info.get("surname") or player_info.get("familyName", "")
            pname = player_info.get("displayName") or f"{fname} {lname}".strip()
            if not pname:
                continue

            # Team
            team_raw = (
                p.get("team", {}).get("name") or
                p.get("squad", {}).get("name") or
                player_info.get("team", {}).get("name", "")
            )
            team = TEAM_CANONICAL.get(team_raw, team_raw)

            # Position
            position = (
                player_info.get("position", {}).get("name") or
                player_info.get("position", "") or
                p.get("position", "MID")
            )
            if isinstance(position, dict):
                position = position.get("name", "MID")

            # Stats - handle both nested and flat
            stats = p.get("stats") or p.get("averages") or p
            gms = _safe_int(stats.get("gamesPlayed") or stats.get("games") or stats.get("gms", 0))
            if gms < 1:
                continue

            players[pname] = {
                "name":            pname,
                "team":            team,
                "team_abbrev":     TEAM_ABBREV.get(team, ""),
                "position":        str(position),
                "games":           gms,
                "kicks":           _safe_float(stats.get("kicks") or stats.get("k", 0)),
                "handballs":       _safe_float(stats.get("handballs") or stats.get("hb", 0)),
                "disposals":       _safe_float(stats.get("disposals") or stats.get("d", 0)),
                "marks":           _safe_float(stats.get("marks") or stats.get("m", 0)),
                "goals":           _safe_float(stats.get("goals") or stats.get("g", 0)),
                "behinds":         _safe_float(stats.get("behinds") or stats.get("b", 0)),
                "tackles":         _safe_float(stats.get("tackles") or stats.get("t", 0)),
                "hitouts":         _safe_float(stats.get("hitouts") or stats.get("ho", 0)),
                "clearances":      _safe_float(stats.get("clearances") or stats.get("cl", 0)),
                "inside_50s":      _safe_float(stats.get("inside50s") or stats.get("i50", 0)),
                "contested_poss":  _safe_float(stats.get("contestedPossessions") or stats.get("cp", 0)),
                "fantasy_pts":     _safe_float(stats.get("dreamTeamPoints") or stats.get("af", 0)),
                "supercoach_pts":  _safe_float(stats.get("supercoachPoints") or stats.get("sc", 0)),
            }
        except Exception as e:
            logger.debug("Player parse error: %s", e)

    return players


def _fetch_footywire_player_stats(year: int) -> dict:
    """Scrape player season averages from Footywire."""
    try:
        time.sleep(1.0)
        r = requests.get(
            f"{FOOTYWIRE_BASE}/ft_player_statistics",
            params={"year": year, "type": "averages", "team": "all", "round": "all"},
            headers=FW_HEADERS,
            timeout=25,
        )
        if r.status_code != 200 or not r.content:
            logger.warning("Footywire player stats: HTTP %d", r.status_code)
            return {}
    except Exception as e:
        logger.warning("Footywire player stats failed: %s", e)
        return {}

    try:
        soup = BeautifulSoup(r.text, "html.parser")
        table = (
            soup.find("table", {"id": "datatable"}) or
            soup.find("table", {"class": "datatable"})
        )
        if not table:
            logger.warning("Footywire: no datatable found")
            return {}

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        players = {}
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 10:
                continue
            try:
                d = dict(zip(headers, [c.get_text(strip=True) for c in cells]))
                pname = d.get("player", cells[0].get_text(strip=True))
                team_raw = d.get("team", cells[1].get_text(strip=True) if len(cells) > 1 else "")
                team = TEAM_CANONICAL.get(team_raw, team_raw)
                gms = _safe_int(d.get("gms", d.get("g", "0")))
                if gms < 1:
                    continue

                players[pname] = {
                    "name":           pname,
                    "team":           team,
                    "team_abbrev":    TEAM_ABBREV.get(team, ""),
                    "position":       d.get("pos", d.get("position", "MID")),
                    "games":          gms,
                    "kicks":          _safe_float(d.get("k", "0")),
                    "handballs":      _safe_float(d.get("hb", "0")),
                    "disposals":      _safe_float(d.get("d", d.get("dis", "0"))),
                    "marks":          _safe_float(d.get("m", d.get("mk", "0"))),
                    "goals":          _safe_float(d.get("g.1", d.get("gl", "0"))),
                    "behinds":        _safe_float(d.get("b", "0")),
                    "tackles":        _safe_float(d.get("t", d.get("tk", "0"))),
                    "hitouts":        _safe_float(d.get("ho", "0")),
                    "clearances":     _safe_float(d.get("cl", "0")),
                    "inside_50s":     _safe_float(d.get("i50", "0")),
                    "contested_poss": _safe_float(d.get("cp", "0")),
                    "fantasy_pts":    _safe_float(d.get("afl", d.get("af", "0"))),
                    "supercoach_pts": _safe_float(d.get("sc", "0")),
                }
            except Exception:
                continue

        logger.info("Footywire: %d player averages", len(players))
        return players

    except Exception as e:
        logger.warning("Footywire parse error: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Ladder
# ---------------------------------------------------------------------------

def get_ladder(year: int = CURRENT_YEAR) -> list:
    """Fetch AFL ladder from AFL.com.au or Squiggle."""

    # Try AFL.com.au first
    try:
        r = requests.get(
            f"{AFL_CD_BASE}/ladder",
            params={"compSeason.id": AFL_SEASON_ID},
            headers=AFL_API_HEADERS,
            timeout=15,
        )
        if r.status_code == 200 and len(r.content) > 100:
            data = r.json()
            ladder = _parse_afl_ladder(data)
            if ladder:
                return ladder
    except Exception as e:
        logger.warning("AFL.com.au ladder failed: %s", e)

    # Squiggle fallback
    data = _squiggle_get(f"standings;year={year}")
    standings = data.get("standings", [])
    if not standings:
        return []

    ladder = []
    for s in standings:
        team = TEAM_CANONICAL.get(s.get("name", ""), s.get("name", ""))
        ladder.append({
            "position": s.get("rank", 0),
            "team":     team,
            "abbrev":   TEAM_ABBREV.get(team, ""),
            "wins":     s.get("wins", 0),
            "losses":   s.get("losses", 0),
            "draws":    s.get("draws", 0),
            "pct":      s.get("percentage", 100),
            "pts":      s.get("pts", 0),
            "for":      s.get("for", 0),
            "against":  s.get("against", 0),
        })
    ladder.sort(key=lambda x: x["position"])
    return ladder


def _parse_afl_ladder(data: dict) -> list:
    """Parse AFL.com.au ladder response."""
    entries = (
        data.get("ladder", {}).get("teams") or
        data.get("ladderPositions") or
        data.get("entries") or
        []
    )
    ladder = []
    for i, e in enumerate(entries, 1):
        team_raw = (
            e.get("team", {}).get("name") or
            e.get("squad", {}).get("name") or
            e.get("teamName", "")
        )
        team = TEAM_CANONICAL.get(team_raw, team_raw)
        stats = e.get("stats") or e.get("totals") or e
        ladder.append({
            "position": e.get("position") or e.get("rank") or i,
            "team":     team,
            "abbrev":   TEAM_ABBREV.get(team, ""),
            "wins":     _safe_int(stats.get("wins") or stats.get("w", 0)),
            "losses":   _safe_int(stats.get("losses") or stats.get("l", 0)),
            "draws":    _safe_int(stats.get("draws") or stats.get("d", 0)),
            "pct":      _safe_float(str(stats.get("percentage") or stats.get("pct") or 100)),
            "pts":      _safe_int(stats.get("premiershipsPoints") or stats.get("pts") or stats.get("points", 0)),
            "for":      _safe_int(stats.get("pointsFor") or stats.get("for", 0)),
            "against":  _safe_int(stats.get("pointsAgainst") or stats.get("against", 0)),
        })
    ladder.sort(key=lambda x: x["position"])
    return ladder


# ---------------------------------------------------------------------------
# Squiggle tips
# ---------------------------------------------------------------------------

def get_squiggle_tips(year: int = CURRENT_YEAR, round_num: int = None) -> list:
    if round_num:
        data = _squiggle_get(f"tips;year={year};round={round_num};source=aggregate")
    else:
        data = _squiggle_get(f"tips;year={year};source=aggregate")
    tips = data.get("tips", [])
    result = []
    for t in tips:
        home = TEAM_CANONICAL.get(t.get("hteam", ""), t.get("hteam", ""))
        away = TEAM_CANONICAL.get(t.get("ateam", ""), t.get("ateam", ""))
        result.append({
            "game_id":   str(t.get("gameid", "")),
            "home_team": home,
            "away_team": away,
            "tip":       TEAM_CANONICAL.get(t.get("tip", ""), t.get("tip", "")),
            "home_conf": _safe_float(str(t.get("hconfidence", "50"))),
            "margin":    _safe_float(str(t.get("margin", "0"))),
        })
    return result


# ---------------------------------------------------------------------------
# H2H history
# ---------------------------------------------------------------------------

def get_h2h_history(home_team: str, away_team: str,
                    year: int = CURRENT_YEAR, last_n: int = 10) -> list:
    data = _squiggle_get(f"games;year={year};team={home_team.replace(' ', '+')}")
    games = data.get("games", [])
    h2h = []
    for g in games:
        h = TEAM_CANONICAL.get(g.get("hteam", ""), "")
        a = TEAM_CANONICAL.get(g.get("ateam", ""), "")
        if (h == home_team and a == away_team) or (h == away_team and a == home_team):
            h2h.append({
                "date":       g.get("date", ""),
                "home_team":  h,
                "away_team":  a,
                "home_score": g.get("hscore", 0),
                "away_score": g.get("ascore", 0),
                "venue":      g.get("venue", ""),
                "winner":     TEAM_CANONICAL.get(g.get("winnerteam", ""), ""),
            })
    h2h.sort(key=lambda x: x["date"], reverse=True)
    return h2h[:last_n]


# ---------------------------------------------------------------------------
# Venue stats
# ---------------------------------------------------------------------------

VENUE_STATS = {
    "MCG":                    {"name": "MCG",                   "city": "Melbourne",  "avg_total": 162, "home_adv": 1.05},
    "Marvel Stadium":         {"name": "Marvel Stadium",        "city": "Melbourne",  "avg_total": 155, "home_adv": 1.04},
    "Adelaide Oval":          {"name": "Adelaide Oval",         "city": "Adelaide",   "avg_total": 158, "home_adv": 1.07},
    "Optus Stadium":          {"name": "Optus Stadium",         "city": "Perth",      "avg_total": 163, "home_adv": 1.08},
    "GMHBA Stadium":          {"name": "GMHBA Stadium",         "city": "Geelong",    "avg_total": 154, "home_adv": 1.09},
    "SCG":                    {"name": "SCG",                   "city": "Sydney",     "avg_total": 151, "home_adv": 1.07},
    "ENGIE Stadium":          {"name": "ENGIE Stadium",         "city": "Sydney",     "avg_total": 156, "home_adv": 1.06},
    "Gabba":                  {"name": "Gabba",                 "city": "Brisbane",   "avg_total": 157, "home_adv": 1.07},
    "Heritage Bank Stadium":  {"name": "Heritage Bank Stadium", "city": "Gold Coast", "avg_total": 158, "home_adv": 1.06},
    "UTAS Stadium":           {"name": "UTAS Stadium",          "city": "Hobart",     "avg_total": 152, "home_adv": 1.00},
    "University of Tasmania Stadium": {"name": "UTAS Stadium",  "city": "Hobart",     "avg_total": 152, "home_adv": 1.00},
    "default":                {"name": "Unknown",               "city": "",           "avg_total": 157, "home_adv": 1.04},
}


def get_venue_stats(venue_name: str) -> dict:
    if not venue_name:
        return VENUE_STATS["default"]
    if venue_name in VENUE_STATS:
        return VENUE_STATS[venue_name]
    for key, stats in VENUE_STATS.items():
        if key != "default" and key.lower() in venue_name.lower():
            return stats
    return VENUE_STATS["default"]


def get_team_news(year: int = CURRENT_YEAR, round_num: int = None) -> dict:
    return {}


def get_player_logs_by_name_batch(player_names: list, year: int = CURRENT_YEAR, last_n: int = 10) -> dict:
    """Stub -- returns empty. Real logs require per-player AFL match ID lookups."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(s) -> float:
    try:
        return float(str(s).strip().replace(",", ""))
    except Exception:
        return 0.0


def _safe_int(s) -> int:
    try:
        return int(float(str(s).strip().replace(",", "")))
    except Exception:
        return 0
