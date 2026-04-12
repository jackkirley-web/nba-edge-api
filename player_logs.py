# player_logs.py — Correct header patching + direct HTTP fallback

import logging
import time
import requests
from datetime import datetime

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
    try:
        from nba_api.stats.library.http import NBAStatsHTTP
        NBAStatsHTTP.headers = NBA_HEADERS.copy()
    except Exception as e:
        logger.warning("Could not patch nba_api headers: %s", e)

_patch_nba_api_headers()

from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats


def safe_call(fn, *args, retries=2, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("API call failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(3)
    return None


def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    results = {}
    total = len(player_ids)
    consecutive_failures = 0

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)

        logs = _fetch_player_logs(player_id, last_n)

        if logs:
            results[player_id] = logs
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 5:
                logger.warning("5 consecutive failures — backing off 3s")
                time.sleep(3)
                consecutive_failures = 0

    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _fetch_player_logs(player_id: int, last_n: int) -> list:
    # Try nba_api first (with patched headers)
    logs = _fetch_via_nba_api(player_id, last_n)
    if logs:
        return logs
    # Fall back to direct HTTP request
    return _fetch_via_direct_request(player_id, last_n)


def _fetch_via_nba_api(player_id: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        endpoint = playergamelogs.PlayerGameLogs(
            player_id_nullable=player_id,
            season_nullable=CURRENT_SEASON,
            season_type_nullable=SEASON_TYPE,
            last_n_games_nullable=last_n,
            timeout=TIMEOUT,
        )
        df = endpoint.get_data_frames()[0]
        if df.empty:
            return []
        return _parse_logs_df(df)
    except Exception as e:
        err = str(e)
        if "Expecting value" in err or "line 1 column 1" in err:
            return []  # Empty response — try direct request
        logger.warning("nba_api logs failed for player %d: %s", player_id, e)
        return []


def _fetch_via_direct_request(player_id: int, last_n: int) -> list:
    """Direct HTTP to stats.nba.com with full browser headers."""
    try:
        time.sleep(SLEEP)
        url    = "https://stats.nba.com/stats/playergamelogs"
        params = {
            "PlayerIDNullable":   player_id,
            "Season":             CURRENT_SEASON,
            "SeasonTypeNullable": SEASON_TYPE,
            "LastNGamesNullable": last_n,
        }
        r = requests.get(url, params=params, headers=NBA_HEADERS, timeout=TIMEOUT)
        if r.status_code != 200 or not r.text.strip():
            return []

        data        = r.json()
        result_sets = data.get("resultSets", [])
        if not result_sets:
            return []

        headers_list = result_sets[0].get("headers", [])
        rows         = result_sets[0].get("rowSet", [])
        if not rows:
            return []

        col_idx = {h: i for i, h in enumerate(headers_list)}

        def get(col, default=0):
            idx = col_idx.get(col)
            if idx is None:
                return default
            val = rows[0][idx] if rows else default  # placeholder
            return val

        logs = []
        for row in rows:
            def g(col, default=0):
                idx = col_idx.get(col)
                if idx is None:
                    return default
                val = row[idx]
                return val if val is not None else default

            raw_date = str(g("GAME_DATE", ""))
            try:
                parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            except Exception:
                parsed_date = datetime.min

            logs.append({
                "game_id":     g("GAME_ID"),
                "game_date":   raw_date,
                "parsed_date": parsed_date,
                "matchup":     g("MATCHUP", ""),
                "is_home":     "vs." in str(g("MATCHUP", "")),
                "win":         g("WL", "") == "W",
                "mins":        float(g("MIN", 0)),
                "pts":         int(g("PTS",  0)),
                "reb":         int(g("REB",  0)),
                "ast":         int(g("AST",  0)),
                "3pm":         int(g("FG3M", 0)),
                "stl":         int(g("STL",  0)),
                "blk":         int(g("BLK",  0)),
                "tov":         int(g("TOV",  0)),
                "plus_minus":  float(g("PLUS_MINUS", 0)),
            })

        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs

    except Exception as e:
        logger.warning("Direct request failed for player %d: %s", player_id, e)
        return []


def _parse_logs_df(df) -> list:
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

    logs.sort(key=lambda g: g["parsed_date"], reverse=True)
    for log in logs:
        del log["parsed_date"]
    return logs


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
        df      = result.get_data_frames()[0]
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
