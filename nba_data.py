# nba_data.py — Correct nba_api header patching + graceful fallback

import logging
import time
import requests
from datetime import datetime, date

logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"
SEASON_TYPE    = "Regular Season"
SLEEP          = 0.6
TIMEOUT        = 15

NBA_HEADERS = {
    "Host":               "stats.nba.com",
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Referer":            "https://www.nba.com/",
    "Connection":         "keep-alive",
    "Origin":             "https://www.nba.com",
}


def _patch_nba_api_headers():
    """
    The correct way to set headers in nba_api is to patch the
    NBAStatsHTTP class headers dict directly before any calls.
    Passing headers= as a kwarg to endpoints is ignored.
    """
    try:
        from nba_api.stats.library.http import NBAStatsHTTP
        NBAStatsHTTP.headers = NBA_HEADERS.copy()
        logger.info("nba_api headers patched successfully")
    except Exception as e:
        logger.warning("Could not patch nba_api headers: %s", e)


# Patch immediately at import time
_patch_nba_api_headers()

# Now import endpoints AFTER patching
from nba_api.stats.endpoints import (
    leaguegamefinder,
    leaguedashteamstats,
    leaguedashplayerstats,
)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
NBA_CDN   = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"


def safe_call(fn, *args, retries=2, **kwargs):
    """Call nba_api endpoint. Retries once on failure, fails fast."""
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("NBA API call failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(3)
    return None


# ─── SCHEDULE ─────────────────────────────────────────────────

def get_today_games():
    today = date.today()
    games = _fetch_espn_schedule(today)
    if not games:
        logger.warning("ESPN schedule empty, trying NBA CDN...")
        games = _fetch_nba_cdn_schedule(today)
    logger.info("Found %d games for %s", len(games), today)
    return games


def _fetch_espn_schedule(target_date: date) -> list:
    try:
        date_str = target_date.strftime("%Y%m%d")
        r = requests.get(
            ESPN_BASE + "/scoreboard",
            params={"dates": date_str},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"}
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
                dt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
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
                    "game_time":        "TBD",
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


# ─── TEAM STATS ───────────────────────────────────────────────

def get_all_team_stats_batch(measure_type="Advanced", location=None, last_n=None):
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

    result = safe_call(leaguedashteamstats.LeagueDashTeamStats, **kwargs)
    if not result:
        logger.warning("Team stats batch returned None (%s loc=%s)", measure_type, location)
        return {}
    try:
        df    = result.get_data_frames()[0]
        stats = {}
        for _, row in df.iterrows():
            team_id = int(row.get("TEAM_ID", 0))
            if measure_type == "Advanced":
                stats[team_id] = {
                    "team_name":  row.get("TEAM_NAME", ""),
                    "off_rating": float(row.get("OFF_RATING", 110) or 110),
                    "def_rating": float(row.get("DEF_RATING", 110) or 110),
                    "net_rating": float(row.get("NET_RATING", 0)   or 0),
                    "pace":       float(row.get("PACE", 100)        or 100),
                    "ts_pct":     float(row.get("TS_PCT", 0.55)     or 0.55),
                    "wins":       int(row.get("W", 0)               or 0),
                    "losses":     int(row.get("L", 0)               or 0),
                }
            else:
                stats[team_id] = {
                    "pts":        float(row.get("PTS", 0)        or 0),
                    "fg_pct":     float(row.get("FG_PCT", 0)     or 0),
                    "three_pct":  float(row.get("FG3_PCT", 0)    or 0),
                    "rebounds":   float(row.get("REB", 0)        or 0),
                    "assists":    float(row.get("AST", 0)        or 0),
                    "turnovers":  float(row.get("TOV", 0)        or 0),
                    "wins":       int(row.get("W", 0)            or 0),
                    "losses":     int(row.get("L", 0)            or 0),
                    "net_rating": float(row.get("PLUS_MINUS", 0) or 0),
                }
        logger.info("Batch stats (%s, loc=%s, L%s): %d teams",
                    measure_type, location, last_n, len(stats))
        return stats
    except Exception as e:
        logger.warning("get_all_team_stats_batch parse failed: %s", e)
        return {}


def get_all_team_recent_batch(last_n: int):
    return get_all_team_stats_batch("Base", last_n=last_n)


def get_all_player_stats_batch():
    result = safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
    )
    if not result:
        return {}
    try:
        df           = result.get_data_frames()[0]
        team_players = {}
        for _, row in df.iterrows():
            team_id = int(row.get("TEAM_ID", 0))
            if team_id not in team_players:
                team_players[team_id] = []
            team_players[team_id].append({
                "name":       row.get("PLAYER_NAME", ""),
                "usage_rate": float(row.get("USG_PCT", 0)   or 0),
                "minutes":    float(row.get("MIN", 0)        or 0),
                "pie":        float(row.get("PIE", 0)        or 0),
                "net_rating": float(row.get("NET_RATING", 0) or 0),
            })
        for tid in team_players:
            team_players[tid].sort(key=lambda p: p["usage_rate"], reverse=True)
        logger.info("Player adv stats: %d teams", len(team_players))
        return team_players
    except Exception as e:
        logger.warning("get_all_player_stats_batch failed: %s", e)
        return {}


def get_h2h_history(team_id: int, opponent_id: int):
    all_games = []
    for year in [2025, 2024]:
        season_str = str(year) + "-" + str(year + 1)[-2:]
        result = safe_call(
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
