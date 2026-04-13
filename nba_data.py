# nba_data.py - NBA.com data with direct HTTP to bypass cloud IP blocking
#
# stats.nba.com blocks nba_api default headers on AWS/Render.
# We use direct HTTP requests with full browser headers instead.
# nba_api library used as fallback only.

import logging
import time
import random
import requests
from datetime import datetime, date

logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"
SEASON_TYPE    = "Regular Season"

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
NBA_CDN   = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
NBA_STATS = "https://stats.nba.com/stats"

# -- Shared session with browser headers ---------------------------------------
_session = None

def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Host":               "stats.nba.com",
            "Connection":         "keep-alive",
            "Accept":             "application/json, text/plain, */*",
            "Accept-Language":    "en-US,en;q=0.9",
            "Accept-Encoding":    "gzip, deflate, br",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token":  "true",
            "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer":            "https://www.nba.com/",
            "Origin":             "https://www.nba.com",
            "Sec-Fetch-Site":     "same-site",
            "Sec-Fetch-Mode":     "cors",
            "Sec-Fetch-Dest":     "empty",
            "Sec-Ch-Ua":          '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile":   "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        })
    return _session


def _fetch_stats(endpoint: str, params: dict, retries: int = 3) -> dict:
    """Direct HTTP request to stats.nba.com with browser headers."""
    session = _get_session()
    url = f"{NBA_STATS}/{endpoint}"
    for attempt in range(retries):
        try:
            delay = 0.6 + random.uniform(0, 0.3) + (attempt * 1.5)
            time.sleep(delay)
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.json()
            if resp.status_code == 429:
                logger.warning("Rate limited, waiting 10s...")
                time.sleep(10)
            elif resp.status_code in (403, 503):
                logger.warning("NBA.com blocked (%d) attempt %d", resp.status_code, attempt + 1)
                time.sleep(5 + attempt * 5)
        except requests.exceptions.Timeout:
            logger.warning("Timeout on %s attempt %d", endpoint, attempt + 1)
            time.sleep(3 + attempt * 3)
        except Exception as e:
            logger.warning("Error on %s attempt %d: %s", endpoint, attempt + 1, e)
            time.sleep(2)
    return None


def _parse_stats(data: dict, mapper=None) -> list:
    """Parse NBA.com stats API resultSets format."""
    try:
        rs = data.get("resultSets", [{}])[0]
        headers = rs.get("headers", [])
        rows    = rs.get("rowSet", [])
        result  = []
        for row in rows:
            d = dict(zip(headers, row))
            result.append(mapper(d) if mapper else d)
        return [r for r in result if r is not None]
    except Exception as e:
        logger.warning("Failed to parse stats response: %s", e)
        return []


def _safe_nba_api(fn, *args, retries=3, **kwargs):
    """Fallback: call nba_api library with retries."""
    for attempt in range(retries):
        try:
            time.sleep(0.6 + attempt * 1.0)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning("nba_api call failed attempt %d: %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# -- Schedule / Games ----------------------------------------------------------

def get_today_games():
    # Use US Eastern time (UTC-5) since NBA schedule runs on ET.
    # Without this, servers in Australia request April 13 when NBA is still playing April 12.
    from datetime import timezone, timedelta
    et_now = datetime.now(tz=timezone(timedelta(hours=-5)))
    today = et_now.date()
    logger.info("Using ET date: %s (server local: %s)", today, date.today())

    games = _fetch_espn_schedule(today)
    if not games:
        logger.warning("ESPN schedule empty, trying NBA CDN...")
        games = _fetch_nba_cdn_schedule(today)

    # If still nothing, try previous ET day (edge case around midnight)
    if not games:
        yesterday = today - timedelta(days=1)
        logger.warning("No games for %s, trying %s...", today, yesterday)
        games = _fetch_espn_schedule(yesterday)

    logger.info("Found %d games for %s", len(games), today)
    return games


def _fetch_espn_schedule(target_date: date) -> list:
    try:
        date_str = target_date.strftime("%Y%m%d")
        r = requests.get(
            ESPN_BASE + "/scoreboard",
            params={"dates": date_str},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        data   = r.json()
        events = data.get("events", [])
        games  = []
        for ev in events:
            comp        = ev.get("competitions", [{}])[0]
            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})
            home_team   = home.get("team", {})
            away_team   = away.get("team", {})
            home_abbrev = home_team.get("abbreviation", "")
            away_abbrev = away_team.get("abbreviation", "")
            status      = comp.get("status", {}).get("type", {}).get("description", "Scheduled")
            game_time   = ev.get("date", "")
            try:
                dt           = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
                display_time = dt.strftime("%-I:%M %p ET")
            except Exception:
                display_time = "TBD"
            games.append({
                "game_id":          ev.get("id", ""),
                "status":           status,
                "game_time":        display_time,
                "home_team_id":     _espn_to_nba_id(home_abbrev),
                "home_team":        home_team.get("name", ""),
                "home_team_city":   home_team.get("location", ""),
                "home_team_abbrev": home_abbrev,
                "home_score":       int(home.get("score", 0) or 0),
                "away_team_id":     _espn_to_nba_id(away_abbrev),
                "away_team":        away_team.get("name", ""),
                "away_team_city":   away_team.get("location", ""),
                "away_team_abbrev": away_abbrev,
                "away_score":       int(away.get("score", 0) or 0),
                "arena":            comp.get("venue", {}).get("fullName", ""),
                "source":           "espn",
            })
        logger.info("ESPN returned %d games for %s", len(games), target_date)
        return games
    except Exception as e:
        logger.error("ESPN schedule fetch failed: %s", e)
        return []


def _fetch_nba_cdn_schedule(target_date: date) -> list:
    try:
        r = requests.get(NBA_CDN, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data      = r.json()
        today_str = target_date.isoformat()
        games     = []
        for gd in data.get("leagueSchedule", {}).get("gameDates", []):
            if gd.get("gameDate", "")[:10] != today_str:
                continue
            for g in gd.get("games", []):
                home = g.get("homeTeam", {})
                away = g.get("awayTeam", {})
                games.append({
                    "game_id":          str(g.get("gameId", "")),
                    "status":           g.get("gameStatusText", "Scheduled"),
                    "game_time":        g.get("gameDateTimeEst", "")[-8:-3] + " ET",
                    "home_team_id":     home.get("teamId"),
                    "home_team":        home.get("teamName", ""),
                    "home_team_city":   home.get("teamCity", ""),
                    "home_team_abbrev": home.get("teamTricode", ""),
                    "home_score":       home.get("score", 0),
                    "away_team_id":     away.get("teamId"),
                    "away_team":        away.get("teamName", ""),
                    "away_team_city":   away.get("teamCity", ""),
                    "away_team_abbrev": away.get("teamTricode", ""),
                    "away_score":       away.get("score", 0),
                    "arena":            g.get("arenaName", ""),
                    "source":           "nba_cdn",
                })
        logger.info("NBA CDN returned %d games for %s", len(games), target_date)
        return games
    except Exception as e:
        logger.error("NBA CDN schedule fetch failed: %s", e)
        return []


# -- Team stats ----------------------------------------------------------------

def get_all_team_stats_batch(measure_type="Advanced", location=None, last_n=None):
    """Fetch stats for all 30 teams - direct HTTP first, nba_api fallback."""

    params = {
        "Season":          CURRENT_SEASON,
        "SeasonType":      SEASON_TYPE,
        "MeasureType":     measure_type,
        "PerMode":         "PerGame",
        "LeagueID":        "00",
        "GameScope":       "",
        "PlayerExperience": "",
        "PlayerPosition":  "",
        "StarterBench":    "",
        "LastNGames":      last_n or 0,
    }
    if location:
        params["Location"] = location

    data = _fetch_stats("leaguedashteamstats", params)
    if data:
        rows = _parse_stats(data)
        if rows:
            return _map_team_stats(rows, measure_type)

    # Fallback to nba_api
    return _team_stats_nba_api(measure_type, location, last_n)


def _map_team_stats(rows: list, measure_type: str) -> dict:
    stats = {}
    for row in rows:
        team_id = int(row.get("TEAM_ID", 0))
        if not team_id:
            continue
        if measure_type == "Advanced":
            stats[team_id] = {
                "team_name":  row.get("TEAM_NAME", ""),
                "off_rating": float(row.get("OFF_RATING", 110) or 110),
                "def_rating": float(row.get("DEF_RATING", 110) or 110),
                "net_rating": float(row.get("NET_RATING", 0) or 0),
                "pace":       float(row.get("PACE", 100) or 100),
                "ts_pct":     float(row.get("TS_PCT", 0.55) or 0.55),
                "wins":       int(row.get("W", 0) or 0),
                "losses":     int(row.get("L", 0) or 0),
            }
        else:
            stats[team_id] = {
                "pts":        float(row.get("PTS", 0) or 0),
                "fg_pct":     float(row.get("FG_PCT", 0) or 0),
                "three_pct":  float(row.get("FG3_PCT", 0) or 0),
                "rebounds":   float(row.get("REB", 0) or 0),
                "assists":    float(row.get("AST", 0) or 0),
                "turnovers":  float(row.get("TOV", 0) or 0),
                "wins":       int(row.get("W", 0) or 0),
                "losses":     int(row.get("L", 0) or 0),
                "net_rating": float(row.get("PLUS_MINUS", 0) or 0),
            }
    logger.info("Team stats (direct, %s): %d teams", measure_type, len(stats))
    return stats


def _team_stats_nba_api(measure_type, location, last_n):
    from nba_api.stats.endpoints import leaguedashteamstats
    kwargs = dict(
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense=measure_type,
        per_mode_detailed="PerGame",
    )
    if location:
        kwargs["location_nullable"] = location
    if last_n:
        kwargs["last_n_games"] = last_n
    result = _safe_nba_api(leaguedashteamstats.LeagueDashTeamStats, **kwargs)
    if not result:
        return {}
    try:
        df    = result.get_data_frames()[0]
        rows  = df.to_dict("records")
        stats = _map_team_stats(rows, measure_type)
        logger.info("Team stats (nba_api, %s): %d teams", measure_type, len(stats))
        return stats
    except Exception as e:
        logger.warning("_team_stats_nba_api failed: %s", e)
        return {}


def get_all_team_recent_batch(last_n: int):
    return get_all_team_stats_batch("Base", last_n=last_n)


# -- Player advanced stats -----------------------------------------------------

def get_all_player_stats_batch():
    """Get player advanced stats for all players - direct HTTP first."""
    params = {
        "Season":          CURRENT_SEASON,
        "SeasonType":      SEASON_TYPE,
        "MeasureType":     "Advanced",
        "PerMode":         "PerGame",
        "LeagueID":        "00",
        "GameScope":       "",
        "PlayerExperience": "",
        "PlayerPosition":  "",
        "StarterBench":    "",
    }
    data = _fetch_stats("leaguedashplayerstats", params)
    if data:
        rows = _parse_stats(data)
        if rows:
            team_players = {}
            for row in rows:
                team_id = int(row.get("TEAM_ID", 0))
                if not team_id:
                    continue
                if team_id not in team_players:
                    team_players[team_id] = []
                team_players[team_id].append({
                    "name":       row.get("PLAYER_NAME", ""),
                    "usage_rate": float(row.get("USG_PCT", 0) or 0),
                    "minutes":    float(row.get("MIN", 0) or 0),
                    "pie":        float(row.get("PIE", 0) or 0),
                    "net_rating": float(row.get("NET_RATING", 0) or 0),
                })
            for tid in team_players:
                team_players[tid].sort(key=lambda p: p["usage_rate"], reverse=True)
            logger.info("Player adv stats (direct): %d teams", len(team_players))
            return team_players

    # Fallback
    return _player_adv_stats_nba_api()


def _player_adv_stats_nba_api():
    from nba_api.stats.endpoints import leaguedashplayerstats
    result = _safe_nba_api(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
    )
    if not result:
        return {}
    try:
        df = result.get_data_frames()[0]
        team_players = {}
        for _, row in df.iterrows():
            team_id = int(row.get("TEAM_ID", 0))
            if team_id not in team_players:
                team_players[team_id] = []
            team_players[team_id].append({
                "name":       row.get("PLAYER_NAME", ""),
                "usage_rate": float(row.get("USG_PCT", 0) or 0),
                "minutes":    float(row.get("MIN", 0) or 0),
                "pie":        float(row.get("PIE", 0) or 0),
                "net_rating": float(row.get("NET_RATING", 0) or 0),
            })
        for tid in team_players:
            team_players[tid].sort(key=lambda p: p["usage_rate"], reverse=True)
        logger.info("Player adv stats (nba_api): %d teams", len(team_players))
        return team_players
    except Exception as e:
        logger.warning("_player_adv_stats_nba_api failed: %s", e)
        return {}


# -- H2H history ---------------------------------------------------------------

def get_h2h_history(team_id: int, opponent_id: int):
    from nba_api.stats.endpoints import leaguegamefinder
    all_games = []
    for year in [2025, 2024]:
        season_str = str(year) + "-" + str(year + 1)[-2:]
        result = _safe_nba_api(
            leaguegamefinder.LeagueGameFinder,
            team_id_nullable=team_id,
            vs_team_id_nullable=opponent_id,
            season_nullable=season_str,
            season_type_nullable=SEASON_TYPE,
        )
        if not result:
            continue
        try:
            df = result.get_data_frames()[0]
            for _, row in df.iterrows():
                pts = int(row.get("PTS", 0) or 0)
                pm  = float(row.get("PLUS_MINUS", 0) or 0)
                all_games.append({
                    "home_win":  pm > 0 and "vs." in str(row.get("MATCHUP", "")),
                    "total_pts": pts * 2 - pm,
                    "margin":    abs(pm),
                })
        except Exception:
            continue
    return all_games


# -- ID mapping ----------------------------------------------------------------

ABBREV_TO_NBA_ID = {
    "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751,
    "CHA": 1610612766, "CHI": 1610612741, "CLE": 1610612739,
    "DAL": 1610612742, "DEN": 1610612743, "DET": 1610612765,
    "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763,
    "MIA": 1610612748, "MIL": 1610612749, "MIN": 1610612750,
    "NOP": 1610612740, "NYK": 1610612752, "OKC": 1610612760,
    "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759,
    "TOR": 1610612761, "UTA": 1610612762, "WAS": 1610612764,
}

def _espn_to_nba_id(abbrev: str) -> int:
    return ABBREV_TO_NBA_ID.get(abbrev, 0)
