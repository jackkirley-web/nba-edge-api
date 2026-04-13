# afl_data.py -- AFL data fetcher
# Fixtures:     Squiggle API (free, public, reliable)
# Player stats: Footywire (scraped directly -- used by every AFL analytics tool)
# Team stats:   AFL website + Footywire aggregation

import logging
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date

logger = logging.getLogger(__name__)

SQUIGGLE_BASE = "https://api.squiggle.com.au"
FOOTYWIRE_BASE = "https://www.footywire.com/afl/footy"
AFL_WEBSITE = "https://www.afl.com.au"

CURRENT_YEAR = 2026

# User agent for Squiggle (they require identifying UA)
SQUIGGLE_UA = "NBAEdge-AFL adam@nbaedge.app"

# Browser headers for Footywire scraping
FW_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.footywire.com/",
}

# Footywire team ID mapping (used for player stat lookups)
TEAM_FW_IDS = {
    "Adelaide": 1, "Brisbane Lions": 2, "Carlton": 3, "Collingwood": 4,
    "Essendon": 5, "Fremantle": 6, "Geelong": 7, "Gold Coast": 8,
    "GWS Giants": 9, "Hawthorn": 10, "Melbourne": 11, "North Melbourne": 12,
    "Port Adelaide": 13, "Richmond": 14, "St Kilda": 15, "Sydney": 16,
    "West Coast": 17, "Western Bulldogs": 18,
}

# Squiggle team name -> canonical name
TEAM_CANONICAL = {
    "Adelaide": "Adelaide",
    "Brisbane Lions": "Brisbane Lions",
    "Brisbane": "Brisbane Lions",
    "Carlton": "Carlton",
    "Collingwood": "Collingwood",
    "Essendon": "Essendon",
    "Fremantle": "Fremantle",
    "Geelong": "Geelong",
    "Gold Coast": "Gold Coast",
    "GWS": "GWS Giants",
    "Greater Western Sydney": "GWS Giants",
    "Hawthorn": "Hawthorn",
    "Melbourne": "Melbourne",
    "North Melbourne": "North Melbourne",
    "Kangaroos": "North Melbourne",
    "Port Adelaide": "Port Adelaide",
    "Richmond": "Richmond",
    "St Kilda": "St Kilda",
    "Sydney": "Sydney",
    "West Coast": "West Coast",
    "Western Bulldogs": "Western Bulldogs",
    "Footscray": "Western Bulldogs",
}

# Short abbreviations for display
TEAM_ABBREV = {
    "Adelaide": "ADE", "Brisbane Lions": "BRI", "Carlton": "CAR",
    "Collingwood": "COL", "Essendon": "ESS", "Fremantle": "FRE",
    "Geelong": "GEE", "Gold Coast": "GCS", "GWS Giants": "GWS",
    "Hawthorn": "HAW", "Melbourne": "MEL", "North Melbourne": "NTH",
    "Port Adelaide": "PTA", "Richmond": "RIC", "St Kilda": "STK",
    "Sydney": "SYD", "West Coast": "WCE", "Western Bulldogs": "WBD",
}


def _squiggle_get(query: str) -> dict:
    """Make a request to the Squiggle API."""
    try:
        time.sleep(0.5 + random.uniform(0, 0.3))
        r = requests.get(
            SQUIGGLE_BASE,
            params={"q": query},
            headers={"User-Agent": SQUIGGLE_UA},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("Squiggle API error for %s: %s", query, e)
        return {}


def _fw_get(path: str, params: dict = None) -> requests.Response | None:
    """Make a request to Footywire."""
    try:
        time.sleep(0.8 + random.uniform(0, 0.4))
        url = f"{FOOTYWIRE_BASE}/{path}"
        r = requests.get(url, params=params or {}, headers=FW_HEADERS, timeout=20)
        if r.status_code == 200:
            return r
        logger.warning("Footywire returned %d for %s", r.status_code, path)
        return None
    except Exception as e:
        logger.warning("Footywire error for %s: %s", path, e)
        return None


# -- Fixtures ---------------------------------------------------------------

def get_upcoming_round() -> dict:
    """
    Get the next upcoming round of AFL fixtures from Squiggle.
    Returns: { round, year, games: [...] }
    """
    # First get upcoming games to find the next round number
    data = _squiggle_get(f"games;year={CURRENT_YEAR};incomplete=1")
    games_raw = data.get("games", [])

    if not games_raw:
        logger.warning("No upcoming AFL games found from Squiggle")
        return {"round": None, "year": CURRENT_YEAR, "games": []}

    # Sort by date to find the very next round
    games_raw.sort(key=lambda g: g.get("date", "") or "")
    next_round = games_raw[0].get("round")

    # Get all games for that round
    data2 = _squiggle_get(f"games;year={CURRENT_YEAR};round={next_round}")
    round_games = data2.get("games", [])

    games = []
    for g in round_games:
        home = TEAM_CANONICAL.get(g.get("hteam", ""), g.get("hteam", ""))
        away = TEAM_CANONICAL.get(g.get("ateam", ""), g.get("ateam", ""))

        # Parse game time
        raw_date = g.get("date", "")
        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            # Convert to AEST (UTC+10)
            from datetime import timezone, timedelta
            aest = dt.astimezone(timezone(timedelta(hours=10)))
            display_time = aest.strftime("%A %-d %B, %-I:%M %p AEST")
            game_date = aest.date()
        except Exception:
            display_time = raw_date
            game_date = None

        games.append({
            "game_id":       str(g.get("id", "")),
            "round":         next_round,
            "year":          CURRENT_YEAR,
            "home_team":     home,
            "away_team":     away,
            "home_abbrev":   TEAM_ABBREV.get(home, home[:3].upper()),
            "away_abbrev":   TEAM_ABBREV.get(away, away[:3].upper()),
            "venue":         g.get("venue", ""),
            "game_time":     display_time,
            "game_date":     str(game_date) if game_date else "",
            "home_score":    g.get("hscore"),
            "away_score":    g.get("ascore"),
            "complete":      g.get("complete", 0),
            "tip":           TEAM_CANONICAL.get(g.get("tip", ""), g.get("tip", "")),
            "source":        "squiggle",
        })

    logger.info("Round %s: %d games", next_round, len(games))
    return {"round": next_round, "year": CURRENT_YEAR, "games": games}


def get_round_games(year: int, round_num: int) -> list:
    """Get games for a specific round."""
    data = _squiggle_get(f"games;year={year};round={round_num}")
    return data.get("games", [])


# -- Team season stats -------------------------------------------------------

def get_all_team_stats(year: int = CURRENT_YEAR) -> dict:
    """
    Scrape season team stats from Footywire.
    Returns {team_name: {avg_score, avg_conceded, avg_disposals, ...}}
    """
    resp = _fw_get("ft_match_statistics", {"year": year, "team": "all"})
    if not resp:
        return {}

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        stats = {}
        # Footywire team stats table
        table = soup.find("table", {"class": "datatable"})
        if not table:
            # Try alternate structure
            tables = soup.find_all("table")
            table = tables[0] if tables else None
        if not table:
            logger.warning("Could not find team stats table on Footywire")
            return {}

        rows = table.find_all("tr")[1:]  # skip header
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            team_name = cells[0].get_text(strip=True)
            canonical = TEAM_CANONICAL.get(team_name, team_name)
            try:
                stats[canonical] = {
                    "games_played": _safe_float(cells[1].get_text()),
                    "avg_score":    _safe_float(cells[2].get_text()),
                    "avg_conceded": _safe_float(cells[3].get_text()) if len(cells) > 3 else 0,
                }
            except Exception:
                continue

        logger.info("Team stats scraped: %d teams", len(stats))
        return stats
    except Exception as e:
        logger.warning("Failed to parse team stats: %s", e)
        return {}


# -- Player season averages --------------------------------------------------

def get_player_season_averages(year: int = CURRENT_YEAR) -> dict:
    """
    Scrape player season averages from Footywire.
    Returns {player_name: {team, games, kicks, handballs, disposals,
             marks, goals, behinds, tackles, hitouts, clearances,
             fantasy_pts, supercoach_pts, ...}}
    """
    resp = _fw_get("ft_player_statistics", {
        "year": year,
        "type": "averages",
        "team": "all",
        "round": "all",
    })
    if not resp:
        return {}

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        players = {}
        table = soup.find("table", {"id": "datatable"}) or \
                soup.find("table", {"class": "datatable"})
        if not table:
            logger.warning("Could not find player stats table on Footywire")
            return {}

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        rows = table.find_all("tr")[1:]
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 10:
                continue
            try:
                row_data = dict(zip(headers, [c.get_text(strip=True) for c in cells]))
                player_name = row_data.get("player", cells[0].get_text(strip=True))
                team = TEAM_CANONICAL.get(
                    row_data.get("team", cells[1].get_text(strip=True)),
                    row_data.get("team", "")
                )
                players[player_name] = {
                    "name":          player_name,
                    "team":          team,
                    "team_abbrev":   TEAM_ABBREV.get(team, ""),
                    "games":         _safe_int(row_data.get("gms", row_data.get("g", "0"))),
                    "kicks":         _safe_float(row_data.get("k", "0")),
                    "handballs":     _safe_float(row_data.get("hb", "0")),
                    "disposals":     _safe_float(row_data.get("d", row_data.get("dis", "0"))),
                    "marks":         _safe_float(row_data.get("m", row_data.get("mk", "0"))),
                    "goals":         _safe_float(row_data.get("g.1", row_data.get("gl", "0"))),
                    "behinds":       _safe_float(row_data.get("b", "0")),
                    "tackles":       _safe_float(row_data.get("t", row_data.get("tk", "0"))),
                    "hitouts":       _safe_float(row_data.get("ho", "0")),
                    "clearances":    _safe_float(row_data.get("cl", "0")),
                    "inside_50s":    _safe_float(row_data.get("i50", "0")),
                    "contested_poss": _safe_float(row_data.get("cp", "0")),
                    "fantasy_pts":   _safe_float(row_data.get("afl", row_data.get("af", "0"))),
                    "supercoach_pts": _safe_float(row_data.get("sc", "0")),
                }
            except Exception:
                continue

        logger.info("Player season averages: %d players scraped", len(players))
        return players
    except Exception as e:
        logger.warning("Failed to parse player stats: %s", e)
        return {}


# -- Player game logs --------------------------------------------------------

def get_player_game_logs(player_fw_id: int, year: int = CURRENT_YEAR, last_n: int = 10) -> list:
    """
    Scrape game-by-game logs for a single player from Footywire.
    Returns list of game dicts sorted most-recent first.
    """
    resp = _fw_get("ft_player_profile_tab", {
        "pid": player_fw_id,
        "year": year,
        "tab": "gba",
    })
    if not resp:
        return []

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "player-game-log"}) or \
                soup.find("table", {"class": "datatable"})
        if not table:
            return []

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        logs = []
        rows = table.find_all("tr")[1:]
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 8:
                continue
            try:
                d = dict(zip(headers, [c.get_text(strip=True) for c in cells]))
                logs.append({
                    "round":      d.get("rnd", d.get("round", "")),
                    "opponent":   TEAM_CANONICAL.get(d.get("opponent", d.get("opp", "")), ""),
                    "is_home":    d.get("h/a", "").upper() == "H",
                    "result":     d.get("result", ""),
                    "kicks":      _safe_int(d.get("k", "0")),
                    "handballs":  _safe_int(d.get("hb", "0")),
                    "disposals":  _safe_int(d.get("d", d.get("dis", "0"))),
                    "marks":      _safe_int(d.get("m", d.get("mk", "0"))),
                    "goals":      _safe_int(d.get("g", d.get("gl", "0"))),
                    "behinds":    _safe_int(d.get("b", "0")),
                    "tackles":    _safe_int(d.get("t", d.get("tk", "0"))),
                    "hitouts":    _safe_int(d.get("ho", "0")),
                    "clearances": _safe_int(d.get("cl", "0")),
                    "inside_50s": _safe_int(d.get("i50", "0")),
                    "fantasy_pts": _safe_int(d.get("afl", d.get("af", "0"))),
                    "supercoach_pts": _safe_int(d.get("sc", "0")),
                    "contested_poss": _safe_int(d.get("cp", "0")),
                })
            except Exception:
                continue

        # Most recent first
        logs.reverse()
        return logs[:last_n]
    except Exception as e:
        logger.warning("Failed to parse game logs for player %d: %s", player_fw_id, e)
        return []


def get_player_logs_by_name_batch(player_names: list, year: int = CURRENT_YEAR, last_n: int = 10) -> dict:
    """
    Get game logs for multiple players by searching Footywire.
    Uses the player search to find Footywire IDs, then fetches logs.
    Returns {player_name: [game_log_dicts]}
    """
    logs = {}
    total = len(player_names)
    for i, name in enumerate(player_names):
        if i % 10 == 0:
            logger.info("Fetching AFL logs: %d/%d players", i, total)
        fw_id = _find_player_fw_id(name, year)
        if fw_id:
            player_logs = get_player_game_logs(fw_id, year, last_n)
            if player_logs:
                logs[name] = player_logs
        time.sleep(0.5 + random.uniform(0, 0.3))
    logger.info("AFL logs fetched for %d/%d players", len(logs), total)
    return logs


def _find_player_fw_id(name: str, year: int = CURRENT_YEAR) -> int | None:
    """Search Footywire for a player's ID."""
    try:
        parts = name.strip().split()
        if len(parts) < 2:
            return None
        # Try last name search
        resp = _fw_get("ft_players", {
            "year": year,
            "search": parts[-1],
        })
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Find player links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "pid=" in href and name.lower() in link.get_text().lower():
                import re
                m = re.search(r"pid=(\d+)", href)
                if m:
                    return int(m.group(1))
        return None
    except Exception as e:
        logger.warning("Could not find Footywire ID for %s: %s", name, e)
        return None


# -- Ladder / standings -----------------------------------------------------

def get_ladder(year: int = CURRENT_YEAR) -> list:
    """Get current AFL ladder from Squiggle."""
    data = _squiggle_get(f"standings;year={year}")
    standings = data.get("standings", [])
    ladder = []
    for s in standings:
        team = TEAM_CANONICAL.get(s.get("name", ""), s.get("name", ""))
        ladder.append({
            "position":  s.get("rank", 0),
            "team":      team,
            "abbrev":    TEAM_ABBREV.get(team, ""),
            "wins":      s.get("wins", 0),
            "losses":    s.get("losses", 0),
            "draws":     s.get("draws", 0),
            "pct":       s.get("percentage", 0),
            "pts":       s.get("pts", 0),
            "for":       s.get("for", 0),
            "against":   s.get("against", 0),
        })
    ladder.sort(key=lambda x: x["position"])
    return ladder


# -- Squiggle predictions ---------------------------------------------------

def get_squiggle_tips(year: int = CURRENT_YEAR, round_num: int = None) -> list:
    """
    Get Squiggle model tips for a round.
    Aggregates predictions from multiple models for consensus.
    """
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
            "game_id":      str(t.get("gameid", "")),
            "home_team":    home,
            "away_team":    away,
            "tip":          TEAM_CANONICAL.get(t.get("tip", ""), t.get("tip", "")),
            "home_conf":    _safe_float(str(t.get("hconfidence", "0"))),
            "margin":       _safe_float(str(t.get("margin", "0"))),
            "err":          _safe_float(str(t.get("err", "0"))),
        })
    return result


# -- Head-to-head history ---------------------------------------------------

def get_h2h_history(home_team: str, away_team: str, year: int = CURRENT_YEAR, last_n: int = 10) -> list:
    """Get head-to-head history between two teams from Squiggle."""
    data = _squiggle_get(f"games;year={year};team={home_team.replace(' ', '+')}")
    games = data.get("games", [])
    h2h = []
    for g in games:
        h = TEAM_CANONICAL.get(g.get("hteam", ""), "")
        a = TEAM_CANONICAL.get(g.get("ateam", ""), "")
        if (h == home_team and a == away_team) or (h == away_team and a == home_team):
            h2h.append({
                "date":        g.get("date", ""),
                "home_team":   h,
                "away_team":   a,
                "home_score":  g.get("hscore", 0),
                "away_score":  g.get("ascore", 0),
                "venue":       g.get("venue", ""),
                "winner":      TEAM_CANONICAL.get(g.get("winnerteam", ""), ""),
            })
    h2h.sort(key=lambda x: x["date"], reverse=True)
    return h2h[:last_n]


# -- Venue stats ------------------------------------------------------------

VENUE_STATS = {
    "MCG": {
        "name": "MCG", "city": "Melbourne", "capacity": 100024,
        "avg_total": 162, "home_adv": 1.05,
        "notes": "Biggest ground - suits running teams, high scoring",
    },
    "Marvel Stadium": {
        "name": "Marvel Stadium", "city": "Melbourne", "capacity": 56347,
        "avg_total": 155, "home_adv": 1.04,
        "notes": "Covered roof - neutral weather impact",
    },
    "ENGIE Stadium": {
        "name": "ENGIE Stadium", "city": "Sydney", "capacity": 45500,
        "avg_total": 156, "home_adv": 1.06,
        "notes": "GWS home, tight ground",
    },
    "Adelaide Oval": {
        "name": "Adelaide Oval", "city": "Adelaide", "capacity": 53583,
        "avg_total": 158, "home_adv": 1.07,
        "notes": "Wind can be a factor, strong SA crowd",
    },
    "Optus Stadium": {
        "name": "Optus Stadium", "city": "Perth", "capacity": 60000,
        "avg_total": 163, "home_adv": 1.08,
        "notes": "Interstate travel disadvantage for visitors, high scoring",
    },
    "GMHBA Stadium": {
        "name": "GMHBA Stadium", "city": "Geelong", "capacity": 36000,
        "avg_total": 154, "home_adv": 1.09,
        "notes": "Cats fortress, strong home advantage",
    },
    "SCG": {
        "name": "SCG", "city": "Sydney", "capacity": 47000,
        "avg_total": 151, "home_adv": 1.07,
        "notes": "Swans fortress, smaller ground",
    },
    "TIO Stadium": {
        "name": "TIO Stadium", "city": "Darwin", "capacity": 18000,
        "avg_total": 168, "home_adv": 1.0,
        "notes": "Neutral venue, heat and humidity factor, high scoring",
    },
    "Norwood Oval": {
        "name": "Norwood Oval", "city": "Adelaide", "capacity": 10000,
        "avg_total": 145, "home_adv": 1.02,
        "notes": "Smaller ground",
    },
    "Blundstone Arena": {
        "name": "Blundstone Arena", "city": "Hobart", "capacity": 16000,
        "avg_total": 152, "home_adv": 1.0,
        "notes": "Neutral, cold weather factor",
    },
    "Cazalys Stadium": {
        "name": "Cazalys Stadium", "city": "Cairns", "capacity": 10000,
        "avg_total": 160, "home_adv": 1.0,
        "notes": "Neutral venue, heat and humidity",
    },
    "default": {
        "name": "Unknown", "city": "", "capacity": 40000,
        "avg_total": 157, "home_adv": 1.04, "notes": "",
    }
}

def get_venue_stats(venue_name: str) -> dict:
    """Get venue statistics for model adjustments."""
    # Try exact match first, then partial
    if venue_name in VENUE_STATS:
        return VENUE_STATS[venue_name]
    for key, stats in VENUE_STATS.items():
        if key != "default" and key.lower() in venue_name.lower():
            return stats
    return VENUE_STATS["default"]


# -- Injury / team news -----------------------------------------------------

def get_team_news(year: int = CURRENT_YEAR, round_num: int = None) -> dict:
    """
    Get team news / lineup from AFL website.
    Returns {team_name: {ins: [...], outs: [...], emergencies: [...]}}
    """
    # AFL website team lineups endpoint
    try:
        url = f"https://www.afl.com.au/api/cfs/afl/lineups"
        r = requests.get(url, headers={"User-Agent": FW_HEADERS["User-Agent"]}, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        news = {}
        for team_data in data.get("teams", []):
            team = TEAM_CANONICAL.get(team_data.get("teamName", ""), "")
            if not team:
                continue
            news[team] = {
                "ins":         [p.get("playerName", "") for p in team_data.get("ins", [])],
                "outs":        [p.get("playerName", "") for p in team_data.get("outs", [])],
                "emergencies": [p.get("playerName", "") for p in team_data.get("emergencies", [])],
                "selected":    [p.get("playerName", "") for p in team_data.get("players", [])],
            }
        return news
    except Exception as e:
        logger.warning("Team news fetch failed: %s", e)
        return {}


# -- Helper functions -------------------------------------------------------

def _safe_float(s: str) -> float:
    try:
        return float(str(s).strip().replace(",", ""))
    except Exception:
        return 0.0

def _safe_int(s: str) -> int:
    try:
        return int(float(str(s).strip().replace(",", "")))
    except Exception:
        return 0
