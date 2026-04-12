# player_logs.py — Fixed season to 2025-26

import logging
import time
from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats
from datetime import datetime

logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"   # ← FIXED: was 2024-25
SEASON_TYPE    = "Regular Season"
SLEEP          = 0.5
TIMEOUT        = 10


def safe_call(fn, *args, retries=1, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("API call failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(1)
    return None


def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    results = {}
    total = len(player_ids)
    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)
        logs = _fetch_player_logs(player_id, last_n)
        if logs:
            results[player_id] = logs
    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _fetch_player_logs(player_id: int, last_n: int) -> list:
    result = safe_call(
        playergamelogs.PlayerGameLogs,
        player_id_nullable=player_id,
        season_nullable=CURRENT_SEASON,
        season_type_nullable=SEASON_TYPE,
        last_n_games_nullable=last_n,
    )
    if not result:
        return []
    try:
        df = result.get_data_frames()[0]
        logs = []
        for _, row in df.iterrows():
            raw_date = str(row.get("GAME_DATE", "") or "")
            try:
                parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            except Exception:
                parsed_date = datetime.min

            logs.append({
                "game_id":     row.get("GAME_ID"),
                "game_date":   raw_date,
                "parsed_date": parsed_date,
                "matchup":     row.get("MATCHUP", ""),
                "is_home":     "vs." in str(row.get("MATCHUP", "")),
                "win":         row.get("WL", "") == "W",
                "mins":        float(row.get("MIN",  0) or 0),
                "pts":         int(row.get("PTS",  0) or 0),
                "reb":         int(row.get("REB",  0) or 0),
                "ast":         int(row.get("AST",  0) or 0),
                "3pm":         int(row.get("FG3M", 0) or 0),
                "stl":         int(row.get("STL",  0) or 0),
                "blk":         int(row.get("BLK",  0) or 0),
                "tov":         int(row.get("TOV",  0) or 0),
                "plus_minus":  float(row.get("PLUS_MINUS", 0) or 0),
            })

        # Sort most-recent first
        logs.sort(key=lambda g: g["parsed_date"], reverse=True)

        for log in logs:
            del log["parsed_date"]

        return logs

    except Exception as e:
        logger.warning("Failed to parse logs for player %d: %s", player_id, e)
        return []


def get_all_player_base_stats() -> dict:
    result = safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
    )
    if not result:
        return {}
    try:
        df = result.get_data_frames()[0]
        players = {}
        for _, row in df.iterrows():
            pid = int(row.get("PLAYER_ID", 0))
            players[pid] = {
                "player_id":   pid,
                "name":        row.get("PLAYER_NAME", ""),
                "team_id":     int(row.get("TEAM_ID", 0)),
                "team_abbrev": row.get("TEAM_ABBREVIATION", ""),
                "position":    row.get("START_POSITION", "G") or "G",
                "mins":        float(row.get("MIN",  0) or 0),
                "pts":         float(row.get("PTS",  0) or 0),
                "reb":         float(row.get("REB",  0) or 0),
                "ast":         float(row.get("AST",  0) or 0),
                "3pm":         float(row.get("FG3M", 0) or 0),
                "stl":         float(row.get("STL",  0) or 0),
                "blk":         float(row.get("BLK",  0) or 0),
                "tov":         float(row.get("TOV",  0) or 0),
                "gp":          int(row.get("GP", 0) or 0),
            }
        logger.info("Base player stats: %d players", len(players))
        return players
    except Exception as e:
        logger.warning("get_all_player_base_stats failed: %s", e)
        return {}
