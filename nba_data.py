# nba_data.py — Fixed parameter names for current nba_api version

import logging
import time
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

from nba_api.stats.endpoints import (
    leaguegamefinder,
    teamgamelogs,
    leaguedashteamstats,
    leaguedashplayerstats,
)
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

SLEEP_BETWEEN_CALLS = 1.0
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
                time.sleep(2 ** attempt * 2)
    return None


def get_today_games():
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


def get_team_advanced_stats():
    """Get league-wide advanced stats. Uses per_mode_detailed (fixed)."""
    result = safe_call(
        leaguedashteamstats.LeagueDashTeamStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
    )
    if not result:
        return {}
    try:
        df = result.get_data_frames()[0]
        stats = {}
        for _, row in df.iterrows():
            team_id = int(row.get("TEAM_ID", 0))
            stats[team_id] = {
                "team_name": row.get("TEAM_NAME"),
                "off_rating": float(row.get("OFF_RATING", 110) or 110),
                "def_rating": float(row.get("DEF_RATING", 110) or 110),
                "net_rating": float(row.get("NET_RATING", 0) or 0),
                "pace": float(row.get("PACE", 100) or 100),
                "ts_pct": float(row.get("TS_PCT", 0.55) or 0.55),
                "wins": int(row.get("W", 0) or 0),
                "losses": int(row.get("L", 0) or 0),
            }
        logger.info(f"Got advanced stats for {len(stats)} teams")
        return stats
    except Exception as e:
        logger.warning(f"get_team_advanced_stats parse failed: {e}")
        return {}


def get_team_recent_stats(team_id: int, last_n: int = 10):
    result = safe_call(
        leaguedashteamstats.LeagueDashTeamStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
        last_n_games=last_n,
        team_id_nullable=team_id,
    )
    if not result:
        return {}
    try:
        df = result.get_data_frames()[0]
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "pts": float(row.get("PTS", 0) or 0),
            "fg_pct": float(row.get("FG_PCT", 0) or 0),
            "three_pct": float(row.get("FG3_PCT", 0) or 0),
            "rebounds": float(row.get("REB", 0) or 0),
            "assists": float(row.get("AST", 0) or 0),
            "turnovers": float(row.get("TOV", 0) or 0),
            "wins": int(row.get("W", 0) or 0),
            "losses": int(row.get("L", 0) or 0),
        }
    except Exception as e:
        logger.warning(f"get_team_recent_stats failed: {e}")
        return {}


def get_team_game_logs(team_id: int, last_n: int = 15):
    result = safe_call(
        teamgamelogs.TeamGameLogs,
        team_id_nullable=team_id,
        season_nullable=CURRENT_SEASON,
        season_type_nullable=SEASON_TYPE,
        last_n_games_nullable=last_n,
    )
    if not result:
        return []
    try:
        df = result.get_data_frames()[0]
        games = []
        for _, row in df.iterrows():
            games.append({
                "game_id": row.get("GAME_ID"),
                "game_date": str(row.get("GAME_DATE", "")),
                "is_home": "vs." in str(row.get("MATCHUP", "")),
                "win": row.get("WL", "") == "W",
                "points": int(row.get("PTS", 0) or 0),
                "fg_pct": float(row.get("FG_PCT", 0) or 0),
                "rebounds": int(row.get("REB", 0) or 0),
                "assists": int(row.get("AST", 0) or 0),
                "turnovers": int(row.get("TOV", 0) or 0),
                "plus_minus": float(row.get("PLUS_MINUS", 0) or 0),
            })
        return games
    except Exception as e:
        logger.warning(f"get_team_game_logs failed: {e}")
        return []


def get_h2h_history(team_id: int, opponent_id: int):
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
                plus_minus = float(row.get("PLUS_MINUS", 0) or 0)
                all_games.append({
                    "home_win": plus_minus > 0 and "vs." in str(row.get("MATCHUP", "")),
                    "total_pts": pts * 2 - plus_minus,
                    "margin": abs(plus_minus),
                })
        except Exception:
            continue
    return all_games


def get_rest_days(team_id: int) -> dict:
    result = safe_call(
        teamgamelogs.TeamGameLogs,
        team_id_nullable=team_id,
        season_nullable=CURRENT_SEASON,
        season_type_nullable=SEASON_TYPE,
        last_n_games_nullable=2,
    )
    if not result:
        return {"rest_days": 2, "is_b2b": False}
    try:
        df = result.get_data_frames()[0]
        if df.empty:
            return {"rest_days": 2, "is_b2b": False}
        today = date.today()
        last_date_str = str(df.iloc[0].get("GAME_DATE", ""))[:10]
        for fmt in ["%Y-%m-%d", "%b %d, %Y"]:
            try:
                last_date = datetime.strptime(last_date_str, fmt).date()
                rest = (today - last_date).days
                return {"rest_days": rest, "is_b2b": rest <= 1}
            except:
                continue
    except Exception:
        pass
    return {"rest_days": 2, "is_b2b": False}


def get_home_away_splits(team_id: int):
    splits = {}
    for location in ["Home", "Road"]:
        result = safe_call(
            leaguedashteamstats.LeagueDashTeamStats,
            season=CURRENT_SEASON,
            season_type_all_star=SEASON_TYPE,
            measure_type_detailed_defense="Base",
            per_mode_detailed="PerGame",
            location_nullable=location,
            team_id_nullable=team_id,
        )
        if result:
            try:
                df = result.get_data_frames()[0]
                if not df.empty:
                    row = df.iloc[0]
                    splits[location.lower()] = {
                        "pts": float(row.get("PTS", 0) or 0),
                        "wins": int(row.get("W", 0) or 0),
                        "losses": int(row.get("L", 0) or 0),
                        "net_rating": float(row.get("PLUS_MINUS", 0) or 0),
                    }
            except Exception:
                continue
    return splits


def get_player_stats_for_team(team_id: int):
    result = safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
        team_id_nullable=team_id,
    )
    if not result:
        return []
    try:
        df = result.get_data_frames()[0]
        players = []
        for _, row in df.iterrows():
            players.append({
                "name": row.get("PLAYER_NAME"),
                "usage_rate": float(row.get("USG_PCT", 0) or 0),
                "minutes": float(row.get("MIN", 0) or 0),
                "pie": float(row.get("PIE", 0) or 0),
                "net_rating": float(row.get("NET_RATING", 0) or 0),
            })
        players.sort(key=lambda p: p["usage_rate"], reverse=True)
        return players[:12]
    except Exception as e:
        logger.warning(f"get_player_stats_for_team failed: {e}")
        return []
