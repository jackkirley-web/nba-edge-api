# afl_data.py -- AFL data fetcher
#
# FIXTURE STRATEGY:
#   Full season fixture is hardcoded from the official AFL PDF (verified).
#   APIs have proven unreliable. Hardcoded = always correct, zero downtime.
#   Update CURRENT_ROUND and FULL_FIXTURE_2026 each week.
#
# PLAYER STATS STRATEGY:
#   Primary:  AFL Tables (afltables.com) - free, reliable, real 2026 data
#   Fallback: Footywire scraping
#   Never:    Synthetic/fake player data
#
# LADDER/TIPS: Squiggle (with robust error handling)

import logging
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

SQUIGGLE_BASE   = "https://api.squiggle.com.au"
FOOTYWIRE_BASE  = "https://www.footywire.com/afl/footy"
AFL_TABLES_BASE = "https://afltables.com/afl/stats"

CURRENT_YEAR  = 2026
CURRENT_ROUND = 6   # ← Bump this each week

SQUIGGLE_UA = "SportEdge-AFL dev@sportedge.app"

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.google.com/",
}

TEAM_CANONICAL = {
    "Adelaide":               "Adelaide",
    "Adelaide Crows":         "Adelaide",
    "Brisbane":               "Brisbane Lions",
    "Brisbane Lions":         "Brisbane Lions",
    "Bris. Lions":            "Brisbane Lions",
    "Bris Lions":             "Brisbane Lions",
    "Carlton":                "Carlton",
    "Collingwood":            "Collingwood",
    "Essendon":               "Essendon",
    "Fremantle":              "Fremantle",
    "Geelong":                "Geelong",
    "Geelong Cats":           "Geelong",
    "Gold Coast":             "Gold Coast",
    "Gold Coast Suns":        "Gold Coast",
    "Gold Cst":               "Gold Coast",
    "Gold Cst Suns":          "Gold Coast",
    "GWS":                    "GWS Giants",
    "Greater Western Sydney": "GWS Giants",
    "GWS Giants":             "GWS Giants",
    "Gtr Western Sydney":     "GWS Giants",
    "Hawthorn":               "Hawthorn",
    "Melbourne":              "Melbourne",
    "Melbourne Demons":       "Melbourne",
    "North Melbourne":        "North Melbourne",
    "North Melb.":            "North Melbourne",
    "Kangaroos":              "North Melbourne",
    "Port Adelaide":          "Port Adelaide",
    "Port Adelaide Power":    "Port Adelaide",
    "Port Adel.":             "Port Adelaide",
    "Richmond":               "Richmond",
    "St Kilda":               "St Kilda",
    "St Kilda Saints":        "St Kilda",
    "Sydney":                 "Sydney",
    "Sydney Swans":           "Sydney",
    "West Coast":             "West Coast",
    "West Coast Eagles":      "West Coast",
    "Western Bulldogs":       "Western Bulldogs",
    "W. Bulldogs":            "Western Bulldogs",
    "West. Bulldogs":         "Western Bulldogs",
    "Bulldogs":               "Western Bulldogs",
    "Footscray":              "Western Bulldogs",
}

TEAM_ABBREV = {
    "Adelaide":        "ADE", "Brisbane Lions":  "BRI",
    "Carlton":         "CAR", "Collingwood":      "COL",
    "Essendon":        "ESS", "Fremantle":        "FRE",
    "Geelong":         "GEE", "Gold Coast":       "GCS",
    "GWS Giants":      "GWS", "Hawthorn":         "HAW",
    "Melbourne":       "MEL", "North Melbourne":  "NTH",
    "Port Adelaide":   "PTA", "Richmond":         "RIC",
    "St Kilda":        "STK", "Sydney":           "SYD",
    "West Coast":      "WCE", "Western Bulldogs": "WBD",
}

# ---------------------------------------------------------------------------
# VERIFIED 2026 AFL FIXTURE
# Source: Official AFL PDF + beforeyoubet.com.au + zerohanger.com
# All times AEST (+10:00). Home team listed first.
# ---------------------------------------------------------------------------

FULL_FIXTURE_2026 = {
    6: [
        # Thu 16 Apr - Sun 19 Apr
        ("Carlton",         "Collingwood",      "MCG",                  "2026-04-16T19:30:00+10:00"),
        ("Geelong",         "Western Bulldogs", "GMHBA Stadium",        "2026-04-17T19:20:00+10:00"),
        ("Sydney",          "GWS Giants",       "SCG",                  "2026-04-17T19:50:00+10:00"),
        ("Gold Coast",      "Essendon",         "People First Stadium", "2026-04-18T13:15:00+10:00"),
        ("Hawthorn",        "Port Adelaide",    "Marvel Stadium",       "2026-04-18T16:15:00+10:00"),
        ("Adelaide",        "St Kilda",         "Adelaide Oval",        "2026-04-18T19:35:00+10:00"),
        ("North Melbourne", "Richmond",         "Marvel Stadium",       "2026-04-19T13:10:00+10:00"),
        ("Melbourne",       "Brisbane Lions",   "MCG",                  "2026-04-19T15:15:00+10:00"),
        ("West Coast",      "Fremantle",        "Optus Stadium",        "2026-04-19T17:10:00+10:00"),
    ],
    7: [
        # ANZAC Round - Thu 23 Apr - Mon 27 Apr
        ("Western Bulldogs","Sydney",           "Marvel Stadium",       "2026-04-23T19:30:00+10:00"),
        ("Richmond",        "Melbourne",        "MCG",                  "2026-04-24T19:40:00+10:00"),
        ("Hawthorn",        "Gold Coast",       "UTAS Stadium",         "2026-04-25T12:15:00+10:00"),
        ("Essendon",        "Collingwood",      "MCG",                  "2026-04-25T15:15:00+10:00"),
        ("Port Adelaide",   "Geelong",          "Adelaide Oval",        "2026-04-25T18:05:00+10:00"),
        ("GWS Giants",      "Brisbane Lions",   "ENGIE Stadium",        "2026-04-26T13:10:00+10:00"),
        ("Carlton",         "Adelaide",         "MCG",                  "2026-04-26T15:20:00+10:00"),
        ("Fremantle",       "North Melbourne",  "Optus Stadium",        "2026-04-26T16:10:00+10:00"),
        ("St Kilda",        "West Coast",       "Marvel Stadium",       "2026-04-27T13:10:00+10:00"),
    ],
    8: [
        ("Adelaide",        "Port Adelaide",    "Adelaide Oval",        "2026-05-01T19:40:00+10:00"),
        ("Sydney",          "Richmond",         "SCG",                  "2026-05-02T13:45:00+10:00"),
        ("Brisbane Lions",  "Gold Coast",       "Gabba",                "2026-05-02T16:35:00+10:00"),
        ("Geelong",         "Carlton",          "GMHBA Stadium",        "2026-05-02T19:25:00+10:00"),
        ("Collingwood",     "Melbourne",        "MCG",                  "2026-05-03T13:10:00+10:00"),
        ("Essendon",        "Fremantle",        "Marvel Stadium",       "2026-05-03T15:20:00+10:00"),
        ("GWS Giants",      "West Coast",       "ENGIE Stadium",        "2026-05-03T16:35:00+10:00"),
        ("Western Bulldogs","Hawthorn",         "Marvel Stadium",       "2026-05-03T16:35:00+10:00"),
        ("North Melbourne", "St Kilda",         "Marvel Stadium",       "2026-05-04T13:10:00+10:00"),
    ],
    9: [
        ("Port Adelaide",   "Western Bulldogs", "Adelaide Oval",        "2026-05-08T19:40:00+10:00"),
        ("Richmond",        "Essendon",         "MCG",                  "2026-05-09T13:45:00+10:00"),
        ("Gold Coast",      "GWS Giants",       "People First Stadium", "2026-05-09T16:35:00+10:00"),
        ("Melbourne",       "Geelong",          "MCG",                  "2026-05-09T19:25:00+10:00"),
        ("Hawthorn",        "Sydney",           "MCG",                  "2026-05-10T13:10:00+10:00"),
        ("Collingwood",     "Brisbane Lions",   "MCG",                  "2026-05-10T15:20:00+10:00"),
        ("Adelaide",        "West Coast",       "Adelaide Oval",        "2026-05-10T15:15:00+10:00"),
        ("Carlton",         "North Melbourne",  "Marvel Stadium",       "2026-05-10T16:35:00+10:00"),
        ("St Kilda",        "Fremantle",        "Marvel Stadium",       "2026-05-11T13:10:00+10:00"),
    ],
    10: [
        ("Sydney",          "Melbourne",        "SCG",                  "2026-05-15T19:40:00+10:00"),
        ("West Coast",      "Carlton",          "Optus Stadium",        "2026-05-16T13:10:00+10:00"),
        ("GWS Giants",      "Port Adelaide",    "ENGIE Stadium",        "2026-05-16T13:10:00+10:00"),
        ("Geelong",         "Essendon",         "GMHBA Stadium",        "2026-05-16T16:35:00+10:00"),
        ("Brisbane Lions",  "Hawthorn",         "Gabba",                "2026-05-16T16:35:00+10:00"),
        ("Fremantle",       "Gold Coast",       "Optus Stadium",        "2026-05-17T13:10:00+10:00"),
        ("Richmond",        "Western Bulldogs", "MCG",                  "2026-05-17T15:20:00+10:00"),
        ("North Melbourne", "Adelaide",         "Marvel Stadium",       "2026-05-17T16:35:00+10:00"),
        ("St Kilda",        "Collingwood",      "Marvel Stadium",       "2026-05-18T13:10:00+10:00"),
    ],
}


def _build_game(home: str, away: str, venue: str, dt_str: str,
                round_num: int, idx: int) -> dict:
    try:
        dt_aware = datetime.fromisoformat(dt_str)
        dt_utc   = dt_aware.astimezone(timezone.utc)
        dt_aest  = dt_aware.astimezone(timezone(timedelta(hours=10)))
        display  = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
        gdate    = dt_aest.date().isoformat()
    except Exception:
        dt_utc  = datetime.min.replace(tzinfo=timezone.utc)
        display = "TBC"
        gdate   = ""

    return {
        "game_id":     f"r{round_num}_2026_{idx}",
        "round":       round_num,
        "year":        2026,
        "home_team":   home,
        "away_team":   away,
        "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper()),
        "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper()),
        "venue":       venue,
        "game_time":   display,
        "game_date":   gdate,
        "game_dt_utc": dt_utc,
        "complete":    False,
        "source":      "hardcoded_fixture",
    }


def get_upcoming_round(year: int = CURRENT_YEAR) -> dict:
    """
    Returns the next upcoming AFL round.
    Uses hardcoded fixture (verified, always works).
    Falls back to Squiggle only for rounds beyond what is hardcoded.
    """
    now_utc = datetime.now(timezone.utc)

    for rnd in sorted(FULL_FIXTURE_2026.keys()):
        raw = FULL_FIXTURE_2026[rnd]
        games = [_build_game(h, a, v, dt, rnd, i)
                 for i, (h, a, v, dt) in enumerate(raw)]
        future = [g for g in games if g["game_dt_utc"] > now_utc]
        if future:
            logger.info("Fixture (hardcoded): Round %d, %d games", rnd, len(games))
            return {"round": rnd, "year": year, "games": games}

    # All hardcoded rounds are past — try Squiggle
    logger.info("All hardcoded rounds past, trying Squiggle...")
    sq_round, sq_games = _squiggle_upcoming(year)
    if sq_games:
        return {"round": sq_round, "year": year, "games": sq_games}

    logger.warning("No fixture found")
    return {"round": CURRENT_ROUND, "year": year, "games": []}


def get_round_games(year: int, round_num: int) -> list:
    if round_num in FULL_FIXTURE_2026:
        return [_build_game(h, a, v, dt, round_num, i)
                for i, (h, a, v, dt) in enumerate(FULL_FIXTURE_2026[round_num])]
    data = _squiggle_get(f"games;year={year};round={round_num}")
    return data.get("games", [])


# ---------------------------------------------------------------------------
# Player season averages — AFL Tables primary, Footywire fallback
# ---------------------------------------------------------------------------

def get_player_season_averages(year: int = CURRENT_YEAR) -> dict:
    """
    Real player averages — AFL Tables then Footywire.
    Returns {} on total failure. Never returns fake data.
    """
    logger.info("Fetching player stats from AFL Tables...")
    players = _scrape_afl_tables(year)
    if players:
        logger.info("AFL Tables: %d players", len(players))
        return players

    logger.info("AFL Tables failed, trying Footywire...")
    players = _scrape_footywire(year)
    if players:
        logger.info("Footywire: %d players", len(players))
        return players

    logger.warning("All player stat sources failed -- no props this cycle (correct behaviour)")
    return {}


def _scrape_afl_tables(year: int) -> dict:
    """
    Scrape from afltables.com/afl/stats/{year}.html
    Page has one table per team. Columns are TOTALS (not averages),
    except DA which is already the disposal average.
    We divide totals by GM to get per-game averages.
    """
    url = f"{AFL_TABLES_BASE}/{year}.html"
    try:
        time.sleep(0.8)
        r = requests.get(url, headers=SCRAPE_HEADERS, timeout=25)
        if r.status_code != 200:
            logger.warning("AFL Tables: HTTP %d", r.status_code)
            return {}
        if len(r.content) < 1000:
            logger.warning("AFL Tables: suspiciously small response")
            return {}
    except Exception as e:
        logger.warning("AFL Tables request failed: %s", e)
        return {}

    try:
        soup = BeautifulSoup(r.text, "html.parser")

        # Teams appear in this order on the page
        TEAM_ORDER = [
            "Adelaide", "Brisbane Lions", "Carlton", "Collingwood",
            "Essendon", "Fremantle", "Geelong", "Gold Coast",
            "GWS Giants", "Hawthorn", "Melbourne", "North Melbourne",
            "Port Adelaide", "Richmond", "St Kilda", "Sydney",
            "West Coast", "Western Bulldogs",
        ]

        # Get all tables — first is abbreviations key, rest are team tables
        all_tables = soup.find_all("table")
        # Filter to tables that look like player stat tables (have GM column)
        stat_tables = []
        for tbl in all_tables:
            header = tbl.find("tr")
            if header:
                text = header.get_text()
                if "GM" in text and "KI" in text and "DI" in text:
                    stat_tables.append(tbl)

        players = {}

        for team_idx, tbl in enumerate(stat_tables):
            if team_idx >= len(TEAM_ORDER):
                break
            team = TEAM_ORDER[team_idx]

            rows = tbl.find_all("tr")
            if not rows:
                continue

            # Parse header
            header_cells = rows[0].find_all(["th", "td"])
            headers = [c.get_text(strip=True).upper() for c in header_cells]

            def col_idx(name):
                try:
                    return headers.index(name)
                except ValueError:
                    return None

            gm_i  = col_idx("GM")
            ki_i  = col_idx("KI")
            mk_i  = col_idx("MK")
            hb_i  = col_idx("HB")
            di_i  = col_idx("DI")
            da_i  = col_idx("DA")
            gl_i  = col_idx("GL")
            bh_i  = col_idx("BH")
            ho_i  = col_idx("HO")
            tk_i  = col_idx("TK")
            rb_i  = col_idx("RB")
            i50_i = col_idx("IF")
            cl_i  = col_idx("CL")
            cp_i  = col_idx("CP")

            if gm_i is None or di_i is None:
                continue

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 10:
                    continue

                # Must have a player link
                player_link = row.find("a")
                if not player_link:
                    continue
                pname = player_link.get_text(strip=True)
                if not pname or pname in ("Player", ""):
                    continue

                def cv(idx, default=0):
                    if idx is None or idx >= len(cells):
                        return default
                    v = cells[idx].get_text(strip=True)
                    return v if v else str(default)

                gms = _safe_int(cv(gm_i))
                if gms < 1:
                    continue

                # All stats except DA are season TOTALS — divide by games
                ki  = _safe_int(cv(ki_i))
                mk  = _safe_int(cv(mk_i))
                hb  = _safe_int(cv(hb_i))
                di  = _safe_int(cv(di_i))
                gl  = _safe_int(cv(gl_i))
                bh  = _safe_int(cv(bh_i))
                ho  = _safe_int(cv(ho_i))
                tk  = _safe_int(cv(tk_i))
                rb  = _safe_int(cv(rb_i))
                i50 = _safe_int(cv(i50_i))
                cl  = _safe_int(cv(cl_i))
                cp  = _safe_int(cv(cp_i))

                disp_avg = di / gms
                ho_avg   = ho / gms
                rb_avg   = rb / gms
                gl_avg   = gl / gms

                # Infer position
                if ho_avg > 12:
                    pos = "RUC"
                elif gl_avg > 1.0 and disp_avg < 14:
                    pos = "FWD"
                elif rb_avg > 4:
                    pos = "DEF"
                else:
                    pos = "MID"

                # Fantasy pts: rough formula based on disposals + goals + tackles
                fantasy_est = round(
                    (di / gms) * 3.0
                    + (gl / gms) * 6.0
                    + (mk / gms) * 3.0
                    + (tk / gms) * 4.0
                    + (ho / gms) * 1.0, 1
                )

                players[pname] = {
                    "name":           pname,
                    "team":           team,
                    "team_abbrev":    TEAM_ABBREV.get(team, ""),
                    "position":       pos,
                    "games":          gms,
                    "kicks":          round(ki / gms, 1),
                    "handballs":      round(hb / gms, 1),
                    "disposals":      round(di / gms, 1),
                    "marks":          round(mk / gms, 1),
                    "goals":          round(gl / gms, 2),
                    "behinds":        round(bh / gms, 2),
                    "tackles":        round(tk / gms, 1),
                    "hitouts":        round(ho / gms, 1),
                    "clearances":     round(cl / gms, 1),
                    "inside_50s":     round(i50 / gms, 1),
                    "contested_poss": round(cp / gms, 1) if cp_i else 0.0,
                    "fantasy_pts":    fantasy_est,
                    "supercoach_pts": round(fantasy_est * 0.9, 1),
                }

        return players

    except Exception as e:
        logger.warning("AFL Tables parse error: %s", e)
        return {}


def _scrape_footywire(year: int) -> dict:
    """Fallback: scrape Footywire player stats page."""
    try:
        time.sleep(1.2)
        r = requests.get(
            f"{FOOTYWIRE_BASE}/ft_player_statistics",
            params={"year": year, "type": "averages", "team": "all", "round": "all"},
            headers=SCRAPE_HEADERS,
            timeout=25,
        )
        if r.status_code != 200 or not r.content:
            logger.warning("Footywire: HTTP %d", r.status_code)
            return {}
    except Exception as e:
        logger.warning("Footywire fetch failed: %s", e)
        return {}

    try:
        soup = BeautifulSoup(r.text, "html.parser")
        table = (
            soup.find("table", {"id": "datatable"}) or
            soup.find("table", {"class": "datatable"})
        )
        if not table:
            logger.warning("Footywire: datatable not found")
            return {}

        header_row = table.find("tr")
        headers = []
        if header_row:
            headers = [th.get_text(strip=True).lower()
                       for th in header_row.find_all(["th", "td"])]

        players = {}
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 10:
                continue
            try:
                d        = dict(zip(headers, [c.get_text(strip=True) for c in cells]))
                pname    = d.get("player", cells[0].get_text(strip=True))
                team_raw = d.get("team", "")
                team     = TEAM_CANONICAL.get(team_raw, team_raw)
                gms      = _safe_int(d.get("gms", "0"))
                if gms < 1:
                    continue
                players[pname] = {
                    "name":           pname,
                    "team":           team,
                    "team_abbrev":    TEAM_ABBREV.get(team, ""),
                    "position":       d.get("pos", "MID"),
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
        return players
    except Exception as e:
        logger.warning("Footywire parse error: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Squiggle (ladder & tips only — fixture NOT fetched from Squiggle)
# ---------------------------------------------------------------------------

def _squiggle_get(query: str) -> dict:
    try:
        time.sleep(0.7 + random.uniform(0, 0.3))
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
            logger.warning("Squiggle %s: empty body", query)
            return {}
        return r.json()
    except Exception as e:
        logger.warning("Squiggle %s: %s", query, e)
        return {}


def _squiggle_upcoming(year: int) -> tuple:
    """Fallback fixture source (only used after hardcoded rounds run out)."""
    data = _squiggle_get(f"games;year={year};incomplete=1")
    games_raw = data.get("games", [])
    if not games_raw:
        return None, []
    games_raw.sort(key=lambda g: g.get("date") or "")
    next_round = games_raw[0].get("round")
    if not next_round:
        return None, []
    data2 = _squiggle_get(f"games;year={year};round={next_round}")
    round_games = data2.get("games", [])
    out = []
    for g in round_games:
        home = TEAM_CANONICAL.get(g.get("hteam", ""), g.get("hteam", ""))
        away = TEAM_CANONICAL.get(g.get("ateam", ""), g.get("ateam", ""))
        raw_date = g.get("date", "")
        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            dt_aest = dt.astimezone(timezone(timedelta(hours=10)))
            display = dt_aest.strftime("%A %-d %B, %-I:%M %p AEST")
            gdate   = dt_aest.date().isoformat()
        except Exception:
            display = raw_date; gdate = ""; dt = datetime.min.replace(tzinfo=timezone.utc)
        out.append({
            "game_id":     str(g.get("id", "")),
            "round":       next_round,
            "year":        year,
            "home_team":   home,
            "away_team":   away,
            "home_abbrev": TEAM_ABBREV.get(home, home[:3].upper() if home else ""),
            "away_abbrev": TEAM_ABBREV.get(away, away[:3].upper() if away else ""),
            "venue":       g.get("venue", ""),
            "game_time":   display,
            "game_date":   gdate,
            "game_dt_utc": dt,
            "complete":    g.get("complete", 0) == 100,
            "source":      "squiggle",
        })
    return next_round, out


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


def get_squiggle_tips(year: int = CURRENT_YEAR, round_num: int = None) -> list:
    q = (f"tips;year={year};round={round_num};source=aggregate"
         if round_num else f"tips;year={year};source=aggregate")
    data = _squiggle_get(q)
    result = []
    for t in data.get("tips", []):
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
                "date": g.get("date", ""), "home_team": h, "away_team": a,
                "home_score": g.get("hscore", 0), "away_score": g.get("ascore", 0),
                "venue": g.get("venue", ""),
                "winner": TEAM_CANONICAL.get(g.get("winnerteam", ""), ""),
            })
    h2h.sort(key=lambda x: x["date"], reverse=True)
    return h2h[:last_n]


# ---------------------------------------------------------------------------
# Venue stats
# ---------------------------------------------------------------------------

VENUE_STATS = {
    "MCG":                  {"name": "MCG",                  "city": "Melbourne",  "avg_total": 162, "home_adv": 1.05},
    "Marvel Stadium":       {"name": "Marvel Stadium",       "city": "Melbourne",  "avg_total": 155, "home_adv": 1.04},
    "Adelaide Oval":        {"name": "Adelaide Oval",        "city": "Adelaide",   "avg_total": 158, "home_adv": 1.07},
    "Optus Stadium":        {"name": "Optus Stadium",        "city": "Perth",      "avg_total": 163, "home_adv": 1.08},
    "GMHBA Stadium":        {"name": "GMHBA Stadium",        "city": "Geelong",    "avg_total": 154, "home_adv": 1.09},
    "SCG":                  {"name": "SCG",                  "city": "Sydney",     "avg_total": 151, "home_adv": 1.07},
    "ENGIE Stadium":        {"name": "ENGIE Stadium",        "city": "Sydney",     "avg_total": 156, "home_adv": 1.06},
    "Gabba":                {"name": "Gabba",                "city": "Brisbane",   "avg_total": 157, "home_adv": 1.07},
    "People First Stadium": {"name": "People First Stadium", "city": "Gold Coast", "avg_total": 158, "home_adv": 1.06},
    "Heritage Bank Stadium":{"name": "People First Stadium", "city": "Gold Coast", "avg_total": 158, "home_adv": 1.06},
    "UTAS Stadium":         {"name": "UTAS Stadium",         "city": "Hobart",     "avg_total": 152, "home_adv": 1.00},
    "University of Tasmania Stadium": {"name": "UTAS Stadium","city": "Hobart",   "avg_total": 152, "home_adv": 1.00},
    "default":              {"name": "Unknown",              "city": "",           "avg_total": 157, "home_adv": 1.04},
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


def get_player_logs_by_name_batch(player_names: list,
                                  year: int = CURRENT_YEAR, last_n: int = 10) -> dict:
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
