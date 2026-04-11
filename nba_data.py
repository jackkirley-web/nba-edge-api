# nba_data.py — Deep NBA.com data fetcher
# Uses nba_api to pull the same data NBA front offices use

import logging
import time
from datetime import datetime, date, timedelta
from typing import Optional
import requests

logger = logging.getLogger(__name__)

# nba_api imports — all free, direct from NBA.com
from nba_api.stats.endpoints import (
    leaguegamefinder,
    teamgamelogs,
    leaguedashteamstats,
    leaguedashplayerstats,
    playergamelogs,
    scoreboardv2,
    boxscoresummaryv2,
    leaguedashlineups,
    teamvsplayer,
    leaguedashptteamdefend,
)
from nba_api.stats.static import teams as static_teams
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

# NBA.com rate limits — be respectful
SLEEP_BETWEEN_CALLS = 0.7  # seconds

CURRENT_SEASON = "2024-25"
SEASON_TYPE = "Regular Season"


def safe_call(fn, *args, retries=3, **kwargs):
    """Call an nba_api endpoint with retry logic."""
    for attempt in range(retries):
        try:
            time.sleep(SLEEP_BETWEEN_CALLS)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"NBA API call failed (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 2)
    return None


def get_all_teams():
    """Return static team list with IDs and abbreviations."""
    return {t["abbreviation"]: t for t in static_teams.get_teams()}


def get_today_games():
    """
    Get today's scheduled games from the live scoreboard.
    Returns list of game dicts with team IDs, names, status.
    """
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


def get_team_game_logs(team_id: int, last_n: int = 15):
    """
    Get last N game logs for a team.
    Returns list with pts, opp_pts, pace, off_rating, def_rating, etc.
    """
    result = safe_call(
        teamgamelogs.TeamGameLogs,
        team_id_nullable=team_id,
        season_nullable=CURRENT_SEASON,
        season_type_nullable=SEASON_TYPE,
        last_n_games_nullable=last_n,
    )
    if not result:
        return []

    df = result.get_data_frames()[0]
    if df.empty:
        return []

    games = []
    for _, row in df.iterrows():
        games.append({
            "game_id": row.get("GAME_ID"),
            "game_date": str(row.get("GAME_DATE", "")),
            "matchup": row.get("MATCHUP", ""),
            "is_home": "vs." in str(row.get("MATCHUP", "")),
            "win": row.get("WL", "") == "W",
            "points": int(row.get("PTS", 0) or 0),
            "opp_points": int(row.get("OPP_PTS", 0) or 0) if "OPP_PTS" in row else None,
            "fg_pct": float(row.get("FG_PCT", 0) or 0),
            "three_pct": float(row.get("FG3_PCT", 0) or 0),
            "three_attempted": float(row.get("FG3A", 0) or 0),
            "ft_attempted": float(row.get("FTA", 0) or 0),
            "rebounds": int(row.get("REB", 0) or 0),
            "assists": int(row.get("AST", 0) or 0),
            "turnovers": int(row.get("TOV", 0) or 0),
            "plus_minus": float(row.get("PLUS_MINUS", 0) or 0),
        })
    return games


def get_team_advanced_stats():
    """
    Get league-wide team advanced stats:
    OFF_RATING, DEF_RATING, NET_RATING, PACE, TS_PCT, etc.
    This is the gold standard — same as NBA.com/stats
    """
    result = safe_call(
        leaguedashteamstats.LeagueDashTeamStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_simple="PerGame",
    )
    if not result:
        return {}

    df = result.get_data_frames()[0]
    stats = {}
    for _, row in df.iterrows():
        team_id = row.get("TEAM_ID")
        stats[team_id] = {
            "team_name": row.get("TEAM_NAME"),
            "off_rating": float(row.get("OFF_RATING", 110) or 110),
            "def_rating": float(row.get("DEF_RATING", 110) or 110),
            "net_rating": float(row.get("NET_RATING", 0) or 0),
            "pace": float(row.get("PACE", 100) or 100),
            "ts_pct": float(row.get("TS_PCT", 0.55) or 0.55),
            "ast_pct": float(row.get("AST_PCT", 0) or 0),
            "reb_pct": float(row.get("REB_PCT", 0) or 0),
            "tov_pct": float(row.get("TM_TOV_PCT", 0) or 0),
            "efg_pct": float(row.get("EFG_PCT", 0) or 0),
            "wins": int(row.get("W", 0) or 0),
            "losses": int(row.get("L", 0) or 0),
        }
    return stats


def get_team_recent_stats(team_id: int, last_n: int = 10):
    """
    Get rolling window stats for a team over last N games.
    Returns averaged stats across the window.
    """
    result = safe_call(
        leaguedashteamstats.LeagueDashTeamStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_simple="PerGame",
        last_n_games=last_n,
        team_id_nullable=team_id,
    )
    if not result:
        return {}

    df = result.get_data_frames()[0]
    if df.empty:
        return {}

    row = df.iloc[0]
    return {
        "pts": float(row.get("PTS", 0) or 0),
        "opp_pts": float(row.get("OPP_PTS", 0) or 0) if "OPP_PTS" in row else None,
        "fg_pct": float(row.get("FG_PCT", 0) or 0),
        "three_pct": float(row.get("FG3_PCT", 0) or 0),
        "three_attempted": float(row.get("FG3A", 0) or 0),
        "rebounds": float(row.get("REB", 0) or 0),
        "assists": float(row.get("AST", 0) or 0),
        "turnovers": float(row.get("TOV", 0) or 0),
        "wins": int(row.get("W", 0) or 0),
        "losses": int(row.get("L", 0) or 0),
    }


def get_h2h_history(team_id: int, opponent_id: int, seasons: int = 2):
    """
    Get head-to-head game history between two teams.
    Looks back across multiple seasons.
    """
    all_games = []
    season_years = [2024, 2023]  # 2024-25, 2023-24

    for year in season_years[:seasons]:
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
        df = result.get_data_frames()[0]
        for _, row in df.iterrows():
            pts = int(row.get("PTS", 0) or 0)
            plus_minus = float(row.get("PLUS_MINUS", 0) or 0)
            all_games.append({
                "season": season_str,
                "date": str(row.get("GAME_DATE", "")),
                "home_win": plus_minus > 0 and "vs." in str(row.get("MATCHUP", "")),
                "total_pts": pts + max(0, pts - plus_minus),  # approx total
                "margin": abs(plus_minus),
            })

    return all_games


def get_player_stats_for_team(team_id: int):
    """
    Get player-level stats for a team — usage rates, minutes, scoring.
    Used to identify key players and assess injury impact.
    """
    result = safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_simple="PerGame",
        team_id_nullable=team_id,
    )
    if not result:
        return []

    df = result.get_data_frames()[0]
    players = []
    for _, row in df.iterrows():
        players.append({
            "player_id": row.get("PLAYER_ID"),
            "name": row.get("PLAYER_NAME"),
            "usage_rate": float(row.get("USG_PCT", 0) or 0),
            "minutes": float(row.get("MIN", 0) or 0),
            "pie": float(row.get("PIE", 0) or 0),  # Player Impact Estimate
            "off_rating": float(row.get("OFF_RATING", 0) or 0),
            "def_rating": float(row.get("DEF_RATING", 0) or 0),
            "net_rating": float(row.get("NET_RATING", 0) or 0),
        })

    # Sort by usage rate — highest usage = most important
    players.sort(key=lambda p: p["usage_rate"], reverse=True)
    return players[:12]  # Top 12 rotation players


def get_rest_days(team_id: int) -> dict:
    """
    Calculate rest days for a team by checking their last game date.
    Returns rest_days count and whether they're on a b2b.
    """
    result = safe_call(
        teamgamelogs.TeamGameLogs,
        team_id_nullable=team_id,
        season_nullable=CURRENT_SEASON,
        season_type_nullable=SEASON_TYPE,
        last_n_games_nullable=2,
    )
    if not result:
        return {"rest_days": 2, "is_b2b": False}

    df = result.get_data_frames()[0]
    if df.empty or len(df) < 1:
        return {"rest_days": 2, "is_b2b": False}

    today = date.today()
    last_game_date_str = str(df.iloc[0].get("GAME_DATE", ""))
    try:
        last_game_date = datetime.strptime(last_game_date_str, "%Y-%m-%dT%H:%M:%S").date()
    except:
        try:
            last_game_date = datetime.strptime(last_game_date_str[:10], "%Y-%m-%d").date()
        except:
            return {"rest_days": 2, "is_b2b": False}

    rest = (today - last_game_date).days
    return {
        "rest_days": rest,
        "is_b2b": rest <= 1,
    }


def get_home_away_splits(team_id: int):
    """
    Get home vs away performance splits for the season.
    """
    splits = {}
    for location in ["Home", "Road"]:
        result = safe_call(
            leaguedashteamstats.LeagueDashTeamStats,
            season=CURRENT_SEASON,
            season_type_all_star=SEASON_TYPE,
            measure_type_detailed_defense="Base",
            per_mode_simple="PerGame",
            location_nullable=location,
            team_id_nullable=team_id,
        )
        if result:
            df = result.get_data_frames()[0]
            if not df.empty:
                row = df.iloc[0]
                splits[location.lower()] = {
                    "pts": float(row.get("PTS", 0) or 0),
                    "wins": int(row.get("W", 0) or 0),
                    "losses": int(row.get("L", 0) or 0),
                    "net_rating": float(row.get("PLUS_MINUS", 0) or 0),
                }
    return splits
