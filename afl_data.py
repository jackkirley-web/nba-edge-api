# afl_data.py -- AFL data fetcher
# Fixtures:     AFL.com.au API (primary) + Squiggle (fallback)
# Player stats: Footywire scraping
# Ladder/tips:  Squiggle API

import logging
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timezone, timedelta

logger = logging.getLogger(__name__)

SQUIGGLE_BASE = "https://api.squiggle.com.au"
FOOTYWIRE_BASE = "https://www.footywire.com/afl/footy"
AFL_API_BASE = "https://aflapi.afl.com.au/afl/v2"
AFL_CD_BASE = "https://api.afl.com.au/cfs/afl"

CURRENT_YEAR = 2026

SQUIGGLE_UA = "SportEdge-AFL dev@sportedge.app"

FW_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.footywire.com/",
}

AFL_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.afl.com.au",
    "Referer": "https://www.afl.com.au/",
}

TEAM_CANONICAL = {
    "Adelaide": "Adelaide", "Adelaide Crows": "Adelaide",
    "Brisbane": "Brisbane Lions", "Brisbane Lions": "Brisbane Lions",
    "Carlton": "Carlton", "Collingwood": "Collingwood",
    "Essendon": "Essendon", "Fremantle": "Fremantle",
    "Geelong": "Geelong", "Geelong Cats": "Geelong",
    "Gold Coast": "Gold Coast", "Gold Coast Suns": "Gold Coast",
    "GWS": "GWS Giants", "Greater Western Sydney": "GWS Giants", "GWS Giants": "GWS Giants",
    "Hawthorn": "Hawthorn",
    "Melbourne": "Melbourne", "Melbourne Demons": "Melbourne",
    "North Melbourne": "North Melbourne", "Kangaroos": "North Melbourne",
    "Port Adelaide": "Port Adelaide", "Port Adelaide Power": "Port Adelaide",
    "Richmond": "Richmond",
    "St Kilda": "St Kilda", "St Kilda Saints": "St Kilda",
    "Sydney": "Sydney", "Sydney Swans": "Sydney",
    "West Coast": "West Coast", "West Coast Eagles": "West Coast",
    "Western Bulldogs": "Western Bulldogs", "Footscray": "Western Bulldogs", "Bulldogs": "Western Bulldogs",
}

TEAM_ABBREV = {
    "Adelaide": "ADE", "Brisbane Lions": "BRI", "Carlton": "CAR",
    "Collingwood": "COL", "Essendon": "ESS", "Fremantle": "FRE",
    "Geelong": "GEE", "Gold Coast": "GCS", "GWS Giants": "GWS",
    "Hawthorn": "HAW", "Melbourne": "MEL", "North Melbourne": "NTH",
    "Port Adelaide": "PTA", "Richmond": "RIC", "St Kilda": "STK",
    "Sydney": "SYD", "West Coast": "WCE", "Western Bulldogs": "WBD",
}

TEAM_FW_IDS = {
    "Adelaide": 1, "Brisbane Lions": 2, "Carlton": 3, "Collingwood": 4,
    "Essendon": 5, "Fremantle": 6, "Geelong": 7, "Gold Coast": 8,
    "GWS Giants": 9, "Hawthorn": 10, "Melbourne": 11, "North Melbourne": 12,
    "Port Adelaide": 13, "Richmond": 14, "St Kilda": 15, "Sydney": 16,
    "West Coast": 17, "Western Bulldogs": 18,
}


def _get(url, params=None, headers=None, timeout=15) -> dict:
    """Generic GET with retry."""
    for attempt in range(3):
        try:
            time.sleep(0.5 + random.uniform(0, 0.3) + attempt * 1.0)
            r = requests.get(url, params=params or {}, headers=headers or AFL_API_HEADERS, timeout=timeout)
            if r.status_code == 200 and len(r.content) > 20:
                try:
                    return r.json()
                except Exception:
                    return {}
            logger.warning("GET %s returned %d (attempt %d)", url, r.status_code, attempt+1)
        except Exception as e:
            logger.warning("GET %s failed (attempt %d): %s", url, attempt+1, e)
    return {}


def _squiggle_get(query: str) -> dict:
    """Squiggle API with correct User-Agent."""
    try:
        time.sleep(0.6 + random.uniform(0, 0.3))
        r = requests.get(
            SQUIGGLE_BASE,
            params={"q": query},
            headers={"User-Agent": SQUIGGLE_UA},
            timeout=15,
        )
        if r.status_code == 200 and len(r.content) > 10:
            return r.json()
        logger.warning("Squiggle %s returned %d", query, r.status_code)
    except Exception as e:
        logger.warning("Squiggle error for %s: %s", query, e)
    return {}


# -- AFL.com.au fixture fetching -------------------------------------------

def _get_afl_fixture_api(year: int = CURRENT_YEAR) -> list:
    """
    Fetch upcoming fixtures from AFL.com.au public API.
    Returns raw game list or empty.
    """
    # Try the public AFL fixture endpoint
    urls_to_try = [
        f"https://api.afl.com.au/cfs/afl/fixturesAndResults?competitionId=1&seasonId={year}",
        f"https://www.afl.com.au/api/cfs/afl/fixturesAndResults?competitionId=1&seasonId={year}",
        f"https://aflapi.afl.com.au/afl/v2/matches?competitionId=1&seasonId={year}&pageSize=200",
    ]
    for url in urls_to_try:
        try:
            time.sleep(0.5)
            r = requests.get(url, headers=AFL_API_HEADERS, timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                data = r.json()
                logger.info("AFL.com.au fixture API success: %s", url)
                return data
        except Exception as e:
            logger.warning("AFL fixture API failed %s: %s", url, e)
    return {}


def _parse_afl_api_games(data: dict, year: int) -> list:
    """Parse games from AFL.com.au API response."""
    games = []
    # The API can return different structures
    raw_games = (
        data.get("fixtures", []) or
        data.get("matches", []) or
        data.get("games", []) or
        (data.get("fixturesAndResults", {}) or {}).get("fixtures", [])
    )
    for g in raw_games:
        try:
            home_raw = g.get("home", {}) or g.get("homeTeam", {}) or {}
            away_raw = g.get("away", {}) or g.get("awayTeam", {}) or {}
            home = TEAM_CANONICAL.get(home_raw.get("name", ""), home_raw.get("name", ""))
            away = TEAM_CANONICAL.get(away_raw.get("name", ""), away_raw.get("name", ""))
            round_num = g.get("round", {})
            if isinstance(round_num, dict):
                round_num = round_num.get("roundNumber") or round_num.get("number")
            utc_str = g.get("utcStartTime") or g.get("startTime") or g.get("date", "")
            try:
                dt_utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                dt_aest = dt_utc.astimezone(timezone(timedelta(hours=10)))
                display_time = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
                game_date = dt_aest.date()
            except Exception:
                display_time = utc_str
                game_date = None
                dt_utc = datetime.min.replace(tzinfo=timezone.utc)

            venue = g.get("venue", {})
            if isinstance(venue, dict):
                venue = venue.get("name", "")

            games.append({
                "game_id": str(g.get("id") or g.get("matchId") or ""),
                "round": round_num,
                "year": year,
                "home_team": home,
                "away_team": away,
                "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper() if home else ""),
                "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper() if away else ""),
                "venue": venue,
                "game_time": display_time,
                "game_date": str(game_date) if game_date else "",
                "game_dt_utc": dt_utc,
                "complete": g.get("status", "") in ("CONCLUDED", "COMPLETED", "FINAL"),
                "source": "afl_api",
            })
        except Exception as e:
            logger.warning("Failed to parse AFL API game: %s", e)
    return games


def _get_upcoming_from_espn(year: int = CURRENT_YEAR) -> list:
    """Fetch upcoming AFL games from ESPN as additional fallback."""
    try:
        r = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/australian-football/afl/scoreboard",
            params={"dates": datetime.now().strftime("%Y%m%d"), "limit": 50},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        if r.status_code != 200 or not r.content:
            return []
        data = r.json()
        games = []
        for ev in data.get("events", []):
            comp = ev.get("competitions", [{}])[0]
            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})
            ht = TEAM_CANONICAL.get(home.get("team", {}).get("displayName", ""), "")
            at = TEAM_CANONICAL.get(away.get("team", {}).get("displayName", ""), "")
            game_time_str = ev.get("date", "")
            try:
                dt = datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
                dt_aest = dt.astimezone(timezone(timedelta(hours=10)))
                display = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
                gdate = dt_aest.date()
            except Exception:
                display = game_time_str
                gdate = None
                dt = datetime.min.replace(tzinfo=timezone.utc)
            games.append({
                "game_id": ev.get("id", ""),
                "round": None,
                "year": year,
                "home_team": ht,
                "away_team": at,
                "home_abbrev": TEAM_ABBREV.get(ht, ""),
                "away_abbrev": TEAM_ABBREV.get(at, ""),
                "venue": comp.get("venue", {}).get("fullName", ""),
                "game_time": display,
                "game_date": str(gdate) if gdate else "",
                "game_dt_utc": dt,
                "complete": False,
                "source": "espn",
            })
        logger.info("ESPN AFL: %d games", len(games))
        return games
    except Exception as e:
        logger.warning("ESPN AFL fetch failed: %s", e)
        return []


def get_upcoming_round(year: int = CURRENT_YEAR) -> dict:
    """
    Get the next upcoming round of AFL fixtures.
    Tries: AFL.com.au API -> Squiggle -> ESPN
    Returns: { round, year, games: [...] }
    """
    now_utc = datetime.now(timezone.utc)
    all_games = []

    # --- Try Squiggle first (most reliable when it works) ---
    data = _squiggle_get(f"games;year={year};incomplete=1")
    squiggle_games = data.get("games", [])

    if squiggle_games:
        squiggle_games.sort(key=lambda g: g.get("date") or "")
        next_round = squiggle_games[0].get("round")
        data2 = _squiggle_get(f"games;year={year};round={next_round}")
        round_games = data2.get("games", [])

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
            all_games.append({
                "game_id": str(g.get("id", "")),
                "round": next_round,
                "year": year,
                "home_team": home,
                "away_team": away,
                "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper() if home else ""),
                "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper() if away else ""),
                "venue": g.get("venue", ""),
                "game_time": display_time,
                "game_date": str(game_date) if game_date else "",
                "game_dt_utc": dt,
                "complete": g.get("complete", 0) == 100,
                "tip": TEAM_CANONICAL.get(g.get("tip", ""), g.get("tip", "")),
                "source": "squiggle",
            })
        if all_games:
            logger.info("Squiggle: Round %s with %d games", next_round, len(all_games))
            return {"round": next_round, "year": year, "games": all_games}

    # --- Fallback: AFL.com.au API ---
    logger.warning("Squiggle failed, trying AFL.com.au API...")
    afl_data = _get_afl_fixture_api(year)
    if afl_data:
        parsed = _parse_afl_api_games(afl_data, year)
        future = [g for g in parsed if not g["complete"] and g.get("game_dt_utc", datetime.min.replace(tzinfo=timezone.utc)) > now_utc]
        if future:
            future.sort(key=lambda g: g.get("game_dt_utc", datetime.min.replace(tzinfo=timezone.utc)))
            next_round = future[0]["round"]
            round_games = [g for g in future if g["round"] == next_round]
            logger.info("AFL.com.au API: Round %s with %d games", next_round, len(round_games))
            return {"round": next_round, "year": year, "games": round_games}

    # --- Final fallback: hardcode Round 5 if all APIs fail ---
    # This is a safety net so the app is never empty
    logger.warning("All APIs failed -- using hardcoded Round 5 fixture")
    hardcoded = _get_hardcoded_round5()
    return {"round": 5, "year": year, "games": hardcoded}


def _get_hardcoded_round5() -> list:
    """
    Hardcoded Round 5 2026 fixture as a last resort fallback.
    Prevents the app from showing nothing when all APIs are down.
    """
    games_raw = [
        ("Brisbane Lions", "Collingwood", "Gabba", "Thu 16 Apr, 7:30 PM AEST"),
        ("Gold Coast", "Melbourne", "Heritage Bank Stadium", "Fri 17 Apr, 4:30 PM AEST"),
        ("North Melbourne", "Essendon", "Marvel Stadium", "Fri 17 Apr, 7:30 PM AEST"),
        ("Port Adelaide", "Richmond", "Adelaide Oval", "Sat 18 Apr, 1:45 PM AEST"),
        ("Carlton", "Fremantle", "MCG", "Sat 18 Apr, 4:35 PM AEST"),
        ("Western Bulldogs", "Adelaide", "Marvel Stadium", "Sat 18 Apr, 7:25 PM AEST"),
        ("Sydney", "West Coast", "SCG", "Sun 19 Apr, 1:10 PM AEST"),
        ("Hawthorn", "GWS Giants", "UTAS Stadium", "Sun 19 Apr, 3:20 PM AEST"),
    ]
    games = []
    for i, (home, away, venue, game_time) in enumerate(games_raw):
        games.append({
            "game_id": f"r5_2026_{i}",
            "round": 5,
            "year": 2026,
            "home_team": home,
            "away_team": away,
            "home_abbrev": TEAM_ABBREV.get(home, ""),
            "away_abbrev": TEAM_ABBREV.get(away, ""),
            "venue": venue,
            "game_time": game_time,
            "game_date": "2026-04-16",
            "complete": False,
            "source": "hardcoded",
        })
    return games


def get_round_games(year: int, round_num: int) -> list:
    data = _squiggle_get(f"games;year={year};round={round_num}")
    return data.get("games", [])


# -- Player season averages --------------------------------------------------

def get_player_season_averages(year: int = CURRENT_YEAR) -> dict:
    """
    Scrape player season averages from Footywire.
    Returns {player_name: stats_dict}
    """
    resp = None
    try:
        time.sleep(0.8)
        r = requests.get(
            f"{FOOTYWIRE_BASE}/ft_player_statistics",
            params={"year": year, "type": "averages", "team": "all", "round": "all"},
            headers=FW_HEADERS, timeout=20
        )
        if r.status_code == 200:
            resp = r
    except Exception as e:
        logger.warning("Footywire player stats failed: %s", e)

    if not resp:
        logger.warning("Could not fetch Footywire player stats -- using empty dict")
        return {}

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        players = {}
        table = (soup.find("table", {"id": "datatable"}) or
                 soup.find("table", {"class": "datatable"}))
        if not table:
            logger.warning("Footywire: no datatable found")
            return {}

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th","td"])]

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td","th"])
            if len(cells) < 10:
                continue
            try:
                d = dict(zip(headers, [c.get_text(strip=True) for c in cells]))
                pname = d.get("player", cells[0].get_text(strip=True))
                team_raw = d.get("team", cells[1].get_text(strip=True) if len(cells)>1 else "")
                team = TEAM_CANONICAL.get(team_raw, team_raw)
                players[pname] = {
                    "name":          pname,
                    "team":          team,
                    "team_abbrev":   TEAM_ABBREV.get(team, ""),
                    "position":      d.get("pos", d.get("position", "MID")),
                    "games":         _safe_int(d.get("gms", d.get("g","0"))),
                    "kicks":         _safe_float(d.get("k","0")),
                    "handballs":     _safe_float(d.get("hb","0")),
                    "disposals":     _safe_float(d.get("d", d.get("dis","0"))),
                    "marks":         _safe_float(d.get("m", d.get("mk","0"))),
                    "goals":         _safe_float(d.get("g.1", d.get("gl","0"))),
                    "behinds":       _safe_float(d.get("b","0")),
                    "tackles":       _safe_float(d.get("t", d.get("tk","0"))),
                    "hitouts":       _safe_float(d.get("ho","0")),
                    "clearances":    _safe_float(d.get("cl","0")),
                    "inside_50s":    _safe_float(d.get("i50","0")),
                    "contested_poss":_safe_float(d.get("cp","0")),
                    "fantasy_pts":   _safe_float(d.get("afl", d.get("af","0"))),
                    "supercoach_pts":_safe_float(d.get("sc","0")),
                }
            except Exception:
                continue

        logger.info("Footywire: %d player averages", len(players))
        return players
    except Exception as e:
        logger.warning("Footywire parse error: %s", e)
        return {}


# -- Synthetic player averages when Footywire fails -------------------------

def get_synthetic_player_pool(games: list) -> dict:
    """
    Generate a synthetic player pool from known AFL star players
    when Footywire is unavailable. Ensures props/streaks always have data.
    """
    # Key players per team with realistic 2026 season averages
    PLAYER_POOL = {
        "Collingwood": [
            {"name":"Nick Daicos","pos":"MID","disp":33,"kicks":18,"hb":15,"marks":6,"goals":0.7,"tackles":4,"clear":5,"hitouts":0,"fantasy":115},
            {"name":"Jordan De Goey","pos":"MID","disp":22,"kicks":13,"hb":9,"marks":5,"goals":1.2,"tackles":4,"clear":3,"hitouts":0,"fantasy":95},
            {"name":"Scott Pendlebury","pos":"MID","disp":26,"kicks":14,"hb":12,"marks":5,"goals":0.4,"tackles":3,"clear":4,"hitouts":0,"fantasy":100},
            {"name":"Mason Cox","pos":"RUC","disp":8,"kicks":5,"hb":3,"marks":4,"goals":1.5,"tackles":2,"clear":2,"hitouts":25,"fantasy":75},
        ],
        "Brisbane Lions": [
            {"name":"Lachie Neale","pos":"MID","disp":30,"kicks":16,"hb":14,"marks":5,"goals":0.6,"tackles":5,"clear":6,"hitouts":0,"fantasy":112},
            {"name":"Joe Daniher","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":6,"goals":2.2,"tackles":2,"clear":1,"hitouts":0,"fantasy":85},
            {"name":"Harris Andrews","pos":"DEF","disp":18,"kicks":12,"hb":6,"marks":7,"goals":0.1,"tackles":3,"clear":1,"hitouts":0,"fantasy":80},
            {"name":"Oscar McInerney","pos":"RUC","disp":9,"kicks":5,"hb":4,"marks":3,"goals":0.4,"tackles":3,"clear":4,"hitouts":35,"fantasy":80},
        ],
        "Carlton": [
            {"name":"Patrick Cripps","pos":"MID","disp":28,"kicks":14,"hb":14,"marks":5,"goals":0.8,"tackles":6,"clear":7,"hitouts":0,"fantasy":110},
            {"name":"Charlie Curnow","pos":"FWD","disp":14,"kicks":9,"hb":5,"marks":7,"goals":2.5,"tackles":2,"clear":1,"hitouts":0,"fantasy":95},
            {"name":"Adam Cerra","pos":"MID","disp":24,"kicks":13,"hb":11,"marks":4,"goals":0.4,"tackles":4,"clear":4,"hitouts":0,"fantasy":95},
            {"name":"Tom De Koning","pos":"RUC","disp":10,"kicks":6,"hb":4,"marks":4,"goals":0.5,"tackles":3,"clear":4,"hitouts":30,"fantasy":80},
        ],
        "Geelong": [
            {"name":"Patrick Dangerfield","pos":"MID","disp":30,"kicks":15,"hb":15,"marks":5,"goals":0.6,"tackles":5,"clear":6,"hitouts":0,"fantasy":112},
            {"name":"Jeremy Cameron","pos":"FWD","disp":14,"kicks":9,"hb":5,"marks":7,"goals":2.8,"tackles":2,"clear":1,"hitouts":0,"fantasy":100},
            {"name":"Tom Stewart","pos":"DEF","disp":22,"kicks":15,"hb":7,"marks":8,"goals":0.1,"tackles":3,"clear":1,"hitouts":0,"fantasy":88},
            {"name":"Rhys Stanley","pos":"RUC","disp":8,"kicks":5,"hb":3,"marks":3,"goals":0.3,"tackles":2,"clear":3,"hitouts":32,"fantasy":72},
        ],
        "Sydney": [
            {"name":"Callum Mills","pos":"MID","disp":28,"kicks":14,"hb":14,"marks":5,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":108},
            {"name":"Tom Papley","pos":"FWD","disp":16,"kicks":10,"hb":6,"marks":5,"goals":1.8,"tackles":3,"clear":2,"hitouts":0,"fantasy":88},
            {"name":"Errol Gulden","pos":"MID","disp":26,"kicks":14,"hb":12,"marks":5,"goals":0.6,"tackles":4,"clear":4,"hitouts":0,"fantasy":100},
        ],
        "Melbourne": [
            {"name":"Clayton Oliver","pos":"MID","disp":30,"kicks":14,"hb":16,"marks":4,"goals":0.5,"tackles":6,"clear":7,"hitouts":0,"fantasy":112},
            {"name":"Christian Petracca","pos":"MID","disp":28,"kicks":15,"hb":13,"marks":5,"goals":0.8,"tackles":5,"clear":5,"hitouts":0,"fantasy":108},
            {"name":"Max Gawn","pos":"RUC","disp":14,"kicks":8,"hb":6,"marks":5,"goals":0.4,"tackles":3,"clear":5,"hitouts":40,"fantasy":100},
        ],
        "Essendon": [
            {"name":"Zach Merrett","pos":"MID","disp":29,"kicks":15,"hb":14,"marks":5,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":110},
            {"name":"Peter Wright","pos":"FWD","disp":10,"kicks":7,"hb":3,"marks":5,"goals":2.0,"tackles":2,"clear":1,"hitouts":0,"fantasy":78},
            {"name":"Sam Draper","pos":"RUC","disp":9,"kicks":5,"hb":4,"marks":3,"goals":0.3,"tackles":3,"clear":4,"hitouts":35,"fantasy":78},
        ],
        "Fremantle": [
            {"name":"Caleb Serong","pos":"MID","disp":30,"kicks":15,"hb":15,"marks":5,"goals":0.5,"tackles":5,"clear":6,"hitouts":0,"fantasy":110},
            {"name":"Andrew Brayshaw","pos":"MID","disp":28,"kicks":15,"hb":13,"marks":5,"goals":0.4,"tackles":5,"clear":5,"hitouts":0,"fantasy":105},
            {"name":"Josh Treacy","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":5,"goals":1.5,"tackles":2,"clear":1,"hitouts":0,"fantasy":78},
        ],
        "Gold Coast": [
            {"name":"Noah Anderson","pos":"MID","disp":27,"kicks":14,"hb":13,"marks":4,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":102},
            {"name":"Matt Rowell","pos":"MID","disp":26,"kicks":13,"hb":13,"marks":4,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":100},
            {"name":"Ben King","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":6,"goals":2.2,"tackles":2,"clear":1,"hitouts":0,"fantasy":85},
        ],
        "Hawthorn": [
            {"name":"Jai Newcombe","pos":"MID","disp":27,"kicks":13,"hb":14,"marks":4,"goals":0.5,"tackles":6,"clear":5,"hitouts":0,"fantasy":105},
            {"name":"Mitch Lewis","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":7,"goals":2.0,"tackles":2,"clear":1,"hitouts":0,"fantasy":85},
            {"name":"James Sicily","pos":"DEF","disp":20,"kicks":13,"hb":7,"marks":7,"goals":0.2,"tackles":3,"clear":1,"hitouts":0,"fantasy":82},
        ],
        "North Melbourne": [
            {"name":"Luke Davies-Uniacke","pos":"MID","disp":26,"kicks":13,"hb":13,"marks":4,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":100},
            {"name":"Tristan Xerri","pos":"RUC","disp":10,"kicks":6,"hb":4,"marks":3,"goals":0.3,"tackles":3,"clear":4,"hitouts":32,"fantasy":78},
        ],
        "Port Adelaide": [
            {"name":"Zak Butters","pos":"MID","disp":28,"kicks":14,"hb":14,"marks":5,"goals":0.6,"tackles":5,"clear":5,"hitouts":0,"fantasy":108},
            {"name":"Connor Rozee","pos":"MID","disp":26,"kicks":14,"hb":12,"marks":5,"goals":0.8,"tackles":4,"clear":4,"hitouts":0,"fantasy":100},
            {"name":"Todd Marshall","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":5,"goals":1.8,"tackles":2,"clear":1,"hitouts":0,"fantasy":80},
        ],
        "Richmond": [
            {"name":"Shai Bolton","pos":"MID","disp":22,"kicks":12,"hb":10,"marks":4,"goals":0.8,"tackles":4,"clear":3,"hitouts":0,"fantasy":90},
            {"name":"Tom Lynch","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":7,"goals":1.8,"tackles":2,"clear":1,"hitouts":0,"fantasy":82},
        ],
        "St Kilda": [
            {"name":"Jack Steele","pos":"MID","disp":27,"kicks":13,"hb":14,"marks":4,"goals":0.5,"tackles":7,"clear":5,"hitouts":0,"fantasy":108},
            {"name":"Max King","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":7,"goals":2.0,"tackles":2,"clear":1,"hitouts":0,"fantasy":82},
        ],
        "West Coast": [
            {"name":"Tim Kelly","pos":"MID","disp":24,"kicks":13,"hb":11,"marks":4,"goals":0.5,"tackles":4,"clear":4,"hitouts":0,"fantasy":92},
            {"name":"Oscar Allen","pos":"FWD","disp":14,"kicks":9,"hb":5,"marks":6,"goals":1.5,"tackles":3,"clear":1,"hitouts":0,"fantasy":80},
        ],
        "Western Bulldogs": [
            {"name":"Adam Treloar","pos":"MID","disp":31,"kicks":16,"hb":15,"marks":5,"goals":0.5,"tackles":5,"clear":5,"hitouts":0,"fantasy":112},
            {"name":"Marcus Bontempelli","pos":"MID","disp":27,"kicks":14,"hb":13,"marks":5,"goals":0.7,"tackles":5,"clear":5,"hitouts":0,"fantasy":108},
            {"name":"Aaron Naughton","pos":"FWD","disp":14,"kicks":9,"hb":5,"marks":8,"goals":2.2,"tackles":2,"clear":1,"hitouts":0,"fantasy":90},
        ],
        "GWS Giants": [
            {"name":"Tom Green","pos":"MID","disp":30,"kicks":15,"hb":15,"marks":5,"goals":0.5,"tackles":5,"clear":6,"hitouts":0,"fantasy":110},
            {"name":"Jesse Hogan","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":6,"goals":2.5,"tackles":2,"clear":1,"hitouts":0,"fantasy":90},
            {"name":"Toby Greene","pos":"FWD","disp":18,"kicks":11,"hb":7,"marks":5,"goals":1.5,"tackles":3,"clear":2,"hitouts":0,"fantasy":88},
        ],
        "Adelaide": [
            {"name":"Rory Laird","pos":"MID","disp":30,"kicks":16,"hb":14,"marks":6,"goals":0.3,"tackles":4,"clear":4,"hitouts":0,"fantasy":108},
            {"name":"Izak Rankine","pos":"FWD","disp":18,"kicks":11,"hb":7,"marks":4,"goals":1.5,"tackles":3,"clear":2,"hitouts":0,"fantasy":88},
            {"name":"Taylor Walker","pos":"FWD","disp":12,"kicks":8,"hb":4,"marks":5,"goals":1.8,"tackles":2,"clear":1,"hitouts":0,"fantasy":78},
        ],
    }

    # Figure out which teams are playing
    playing_teams = set()
    for g in games:
        playing_teams.add(g.get("home_team", ""))
        playing_teams.add(g.get("away_team", ""))

    players = {}
    for team, team_players in PLAYER_POOL.items():
        if team not in playing_teams:
            continue
        for p in team_players:
            disp = p["disp"]
            pname = p["name"]
            players[pname] = {
                "name":          pname,
                "team":          team,
                "team_abbrev":   TEAM_ABBREV.get(team, ""),
                "position":      p["pos"],
                "games":         12,
                "kicks":         float(p["kicks"]),
                "handballs":     float(p["hb"]),
                "disposals":     float(disp),
                "marks":         float(p["marks"]),
                "goals":         float(p["goals"]),
                "behinds":       0.5,
                "tackles":       float(p["tackles"]),
                "hitouts":       float(p["hitouts"]),
                "clearances":    float(p["clear"]),
                "inside_50s":    float(p.get("inside_50s", disp * 0.12)),
                "contested_poss": float(disp * 0.35),
                "fantasy_pts":   float(p["fantasy"]),
                "supercoach_pts": float(p["fantasy"] * 0.9),
            }
    logger.info("Synthetic player pool: %d players for %d teams", len(players), len(playing_teams))
    return players


# -- Ladder ------------------------------------------------------------------

def get_ladder(year: int = CURRENT_YEAR) -> list:
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


# -- Squiggle tips -----------------------------------------------------------

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


# -- H2H history -------------------------------------------------------------

def get_h2h_history(home_team: str, away_team: str, year: int = CURRENT_YEAR, last_n: int = 10) -> list:
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


# -- Venue stats -------------------------------------------------------------

VENUE_STATS = {
    "MCG":            {"name":"MCG",             "city":"Melbourne", "avg_total":162, "home_adv":1.05},
    "Marvel Stadium": {"name":"Marvel Stadium",  "city":"Melbourne", "avg_total":155, "home_adv":1.04},
    "Adelaide Oval":  {"name":"Adelaide Oval",   "city":"Adelaide",  "avg_total":158, "home_adv":1.07},
    "Optus Stadium":  {"name":"Optus Stadium",   "city":"Perth",     "avg_total":163, "home_adv":1.08},
    "GMHBA Stadium":  {"name":"GMHBA Stadium",   "city":"Geelong",   "avg_total":154, "home_adv":1.09},
    "SCG":            {"name":"SCG",             "city":"Sydney",    "avg_total":151, "home_adv":1.07},
    "ENGIE Stadium":  {"name":"ENGIE Stadium",   "city":"Sydney",    "avg_total":156, "home_adv":1.06},
    "Gabba":          {"name":"Gabba",           "city":"Brisbane",  "avg_total":157, "home_adv":1.07},
    "Heritage Bank Stadium":{"name":"Heritage Bank Stadium","city":"Gold Coast","avg_total":158,"home_adv":1.06},
    "UTAS Stadium":   {"name":"UTAS Stadium",    "city":"Hobart",    "avg_total":152, "home_adv":1.00},
    "Norwood Oval":   {"name":"Norwood Oval",    "city":"Adelaide",  "avg_total":145, "home_adv":1.02},
    "default":        {"name":"Unknown",         "city":"",          "avg_total":157, "home_adv":1.04},
}

def get_venue_stats(venue_name: str) -> dict:
    if venue_name in VENUE_STATS:
        return VENUE_STATS[venue_name]
    for key, stats in VENUE_STATS.items():
        if key != "default" and key.lower() in venue_name.lower():
            return stats
    return VENUE_STATS["default"]


def get_team_news(year: int = CURRENT_YEAR, round_num: int = None) -> dict:
    return {}


# -- Player game logs --------------------------------------------------------

def get_player_logs_by_name_batch(player_names: list, year: int = CURRENT_YEAR, last_n: int = 10) -> dict:
    """Stub -- Footywire per-player logs require IDs. Returns empty for now."""
    return {}


# -- Helpers -----------------------------------------------------------------

def _safe_float(s: str) -> float:
    try:
        return float(str(s).strip().replace(",", ""))
    except Exception:
        return 0.0

def _safe_int(s) -> int:
    try:
        return int(float(str(s).strip().replace(",", "")))
    except Exception:
        return 0
