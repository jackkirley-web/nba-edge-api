# nba_data.py — Batch calls only. No per-team loops.
# 6 total API calls instead of 150. Runs in ~15 seconds.

import logging
import time
from datetime import datetime, date

logger = logging.getLogger(__name__)

from nba_api.stats.endpoints import (
    leaguegamefinder,
    teamgamelogs,
    leaguedashteamstats,
    leaguedashplayerstats,
)
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

SLEEP_BETWEEN_CALLS = 0.6
CURRENT_SEASON = "2024-25"
SEASON_TYPE = "Regular Season"


def safe_call(fn, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP_BETWEEN_CALLS)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"NBA API call failed (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def get_today_games():
    """Get today's games from NBA live scoreboard."""
    try:
        sb = live_scoreboard.ScoreBoard()
        data = sb.get_dict()
        games = data.get("scoreboard", {}).get("games", [])
        result = []
        for g in games:
            result.append({
                "game_id": g["gameId"],
                "status": g["gameStatusText"],
                "home_team_id": g["homeTeam"]["teamId"],
                "home_team": g["homeTeam"]["teamName"],
                "home_team_city": g["homeTeam"]["teamCity"],
                "home_team_abbrev": g["homeTeam"]["teamTricode"],
                "home_score": g["homeTeam"].get("score", 0),
                "away_team_id": g["awayTeam"]["teamId"],
                "away_team": g["awayTeam"]["teamName"],
                "away_team_city": g["awayTeam"]["teamCity"],
                "away_team_abbrev": g["awayTeam"]["teamTricode"],
                "away_score": g["awayTeam"].get("score", 0),
                "game_time": g.get("gameStatusText", ""),
                "arena": g.get("arenaName", ""),
            })
        return result
    except Exception as e:
        logger.error(f"get_today_games failed: {e}")
        return []


def get_all_team_stats_batch(measure_type="Advanced", location=None, last_n=None):
    """
    Fetch stats for ALL 30 teams in ONE API call.
    Returns {team_id: stats_dict}
    """
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
        return {}

    try:
        df = result.get_data_frames()[0]
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
                    "pts":        float(row.get("PTS", 0)     or 0),
                    "fg_pct":     float(row.get("FG_PCT", 0)  or 0),
                    "three_pct":  float(row.get("FG3_PCT", 0) or 0),
                    "rebounds":   float(row.get("REB", 0)     or 0),
                    "assists":    float(row.get("AST", 0)     or 0),
                    "turnovers":  float(row.get("TOV", 0)     or 0),
                    "wins":       int(row.get("W", 0)         or 0),
                    "losses":     int(row.get("L", 0)         or 0),
                    "net_rating": float(row.get("PLUS_MINUS", 0) or 0),
                }
        logger.info(f"Batch stats ({measure_type}, loc={location}, L{last_n}): {len(stats)} teams")
        return stats
    except Exception as e:
        logger.warning(f"get_all_team_stats_batch failed: {e}")
        return {}


def get_all_team_recent_batch(last_n: int):
    """Get rolling window stats for all teams in one call."""
    return get_all_team_stats_batch("Base", last_n=last_n)


def get_all_player_stats_batch():
    """
    Get player stats for ALL players in ONE call.
    Returns {team_id: [player_dicts]}
    """
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
        df = result.get_data_frames()[0]
        team_players = {}
        for _, row in df.iterrows():
            team_id = int(row.get("TEAM_ID", 0))
            if team_id not in team_players:
                team_players[team_id] = []
            team_players[team_id].append({
                "name":       row.get("PLAYER_NAME", ""),
                "usage_rate": float(row.get("USG_PCT", 0)    or 0),
                "minutes":    float(row.get("MIN", 0)         or 0),
                "pie":        float(row.get("PIE", 0)         or 0),
                "net_rating": float(row.get("NET_RATING", 0)  or 0),
            })
        # Sort each team's players by usage rate
        for tid in team_players:
            team_players[tid].sort(key=lambda p: p["usage_rate"], reverse=True)
            team_players[tid] = team_players[tid][:12]
        logger.info(f"Player stats batch: {len(team_players)} teams")
        return team_players
    except Exception as e:
        logger.warning(f"get_all_player_stats_batch failed: {e}")
        return {}


def get_all_game_logs_batch():
    """Placeholder — game logs not used in batch mode to save time."""
    return {}


def get_h2h_history(team_id: int, opponent_id: int):
    """H2H history — called selectively, not for every game."""
    all_games = []
    for year in [2024, 2023]:
        season_str = f"{year}-{str(year+1)[-2:]}"
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
