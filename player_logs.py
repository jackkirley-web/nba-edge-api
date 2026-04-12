# player_logs.py — NBA.com primary (working during US business hours)
# Correct season: 2025-26
# Browser headers applied via NBAStatsHTTP patch

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


def _patch():
    try:
        from nba_api.stats.library.http import NBAStatsHTTP
        NBAStatsHTTP.headers = NBA_HEADERS.copy()
    except Exception:
        pass

_patch()

from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats


def _safe_call(fn, *args, retries=2, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("NBA.com call failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(2)
    return None


def get_all_player_base_stats() -> dict:
    """Season averages via nba_api, falls back to direct HTTP."""
    # Try nba_api (patched headers)
    result = _safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
    )
    if result:
        try:
            df = result.get_data_frames()[0]
            if not df.empty:
                players = _parse_player_df(df)
                if players:
                    logger.info("Base player stats: %d players (nba_api)", len(players))
                    return players
        except Exception as e:
            logger.warning("nba_api parse failed: %s", e)

    # Direct HTTP fallback
    logger.info("nba_api failed — trying direct HTTP")
    return _direct_player_stats()


def _direct_player_stats() -> dict:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            "https://stats.nba.com/stats/leaguedashplayerstats",
            params={
                "Season": CURRENT_SEASON, "SeasonType": SEASON_TYPE,
                "MeasureType": "Base", "PerMode": "PerGame",
                "LeagueID": "00", "LastNGames": 0, "Month": 0,
                "OpponentTeamID": 0, "PaceAdjust": "N", "Period": 0,
                "PlusMinus": "N", "Rank": "N", "TeamID": 0,
            },
            headers=NBA_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200 or not r.text.strip():
            return {}
        rs   = r.json().get("resultSets", [])
        if not rs:
            return {}
        hdrs = rs[0].get("headers", [])
        rows = rs[0].get("rowSet", [])
        col  = {h: i for i, h in enumerate(hdrs)}

        def g(row, f, d=0):
            idx = col.get(f)
            return row[idx] if idx is not None and row[idx] is not None else d

        players = {}
        for row in rows:
            pid = int(g(row, "PLAYER_ID", 0))
            if not pid:
                continue
            players[pid] = {
                "player_id":   pid,
                "name":        g(row, "PLAYER_NAME", ""),
                "team_id":     int(g(row, "TEAM_ID", 0)),
                "team_abbrev": g(row, "TEAM_ABBREVIATION", ""),
                "position":    g(row, "START_POSITION", "G") or "G",
                "mins":        round(float(g(row, "MIN",  0)), 1),
                "pts":         float(g(row, "PTS",  0)),
                "reb":         float(g(row, "REB",  0)),
                "ast":         float(g(row, "AST",  0)),
                "3pm":         float(g(row, "FG3M", 0)),
                "stl":         float(g(row, "STL",  0)),
                "blk":         float(g(row, "BLK",  0)),
                "tov":         float(g(row, "TOV",  0)),
                "gp":          int(g(row, "GP", 0)),
                "source":      "nba.com-direct",
            }
        logger.info("Base player stats: %d players (direct HTTP)", len(players))
        return players
    except Exception as e:
        logger.warning("Direct player stats failed: %s", e)
        return {}


def _parse_player_df(df) -> dict:
    players = {}
    for _, row in df.iterrows():
        pid = int(row.get("PLAYER_ID", 0))
        if not pid:
            continue
        players[pid] = {
            "player_id":   pid,
            "name":        row.get("PLAYER_NAME", ""),
            "team_id":     int(row.get("TEAM_ID", 0)),
            "team_abbrev": row.get("TEAM_ABBREVIATION", ""),
            "position":    row.get("START_POSITION", "G") or "G",
            "mins":        round(float(row.get("MIN",  0) or 0), 1),
            "pts":         float(row.get("PTS",  0) or 0),
            "reb":         float(row.get("REB",  0) or 0),
            "ast":         float(row.get("AST",  0) or 0),
            "3pm":         float(row.get("FG3M", 0) or 0),
            "stl":         float(row.get("STL",  0) or 0),
            "blk":         float(row.get("BLK",  0) or 0),
            "tov":         float(row.get("TOV",  0) or 0),
            "gp":          int(row.get("GP", 0) or 0),
            "source":      "nba_api",
        }
    return players


def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    results          = {}
    total            = len(player_ids)
    consecutive_fail = 0

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)

        # Skip negative IDs (legacy BDL/ESPN ids from old code)
        if isinstance(player_id, int) and player_id < 0:
            continue
        if isinstance(player_id, str):
            continue

        logs = _fetch_nbacom_logs(player_id, last_n)

        if logs:
            results[player_id] = logs
            consecutive_fail   = max(0, consecutive_fail - 1)
        else:
            consecutive_fail += 1
            # Try direct HTTP as fallback
            logs = _fetch_direct_logs(player_id, last_n)
            if logs:
                results[player_id] = logs
                consecutive_fail   = max(0, consecutive_fail - 1)
            elif consecutive_fail >= 10:
                logger.warning("10 consecutive failures — NBA.com may be down")
                # Back off but keep trying
                time.sleep(5)
                consecutive_fail = 0

    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _fetch_nbacom_logs(player_id: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        ep = playergamelogs.PlayerGameLogs(
            player_id_nullable=player_id,
            season_nullable=CURRENT_SEASON,
            season_type_nullable=SEASON_TYPE,
            last_n_games_nullable=last_n,
            timeout=TIMEOUT,
        )
        df = ep.get_data_frames()[0]
        if df.empty:
            return []
        return _parse_logs_df(df)
    except Exception as e:
        err = str(e)
        if "Expecting value" not in err and "line 1 column 1" not in err:
            logger.warning("nba_api logs failed %d: %s", player_id, e)
        return []


def _fetch_direct_logs(player_id: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            "https://stats.nba.com/stats/playergamelogs",
            params={
                "PlayerIDNullable":   player_id,
                "Season":             CURRENT_SEASON,
                "SeasonTypeNullable": SEASON_TYPE,
                "LastNGamesNullable": last_n,
            },
            headers=NBA_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200 or not r.text.strip():
            return []
        rs = r.json().get("resultSets", [])
        if not rs or not rs[0].get("rowSet"):
            return []
        hdrs = rs[0]["headers"]
        rows = rs[0]["rowSet"]
        col  = {h: i for i, h in enumerate(hdrs)}

        def g(row, f, d=0):
            idx = col.get(f)
            return row[idx] if idx is not None and row[idx] is not None else d

        logs = []
        for row in rows:
            raw = str(g(row, "GAME_DATE", ""))
            try:
                pd = datetime.strptime(raw[:10], "%Y-%m-%d")
            except Exception:
                pd = datetime.min
            logs.append({
                "game_date":   raw,
                "parsed_date": pd,
                "matchup":     g(row, "MATCHUP", ""),
                "is_home":     "vs." in str(g(row, "MATCHUP", "")),
                "mins":        float(g(row, "MIN", 0)),
                "pts":         int(g(row, "PTS", 0)),
                "reb":         int(g(row, "REB", 0)),
                "ast":         int(g(row, "AST", 0)),
                "3pm":         int(g(row, "FG3M", 0)),
                "stl":         int(g(row, "STL", 0)),
                "blk":         int(g(row, "BLK", 0)),
                "tov":         int(g(row, "TOV", 0)),
                "plus_minus":  float(g(row, "PLUS_MINUS", 0)),
                "source":      "nba.com-direct",
            })
        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs
    except Exception as e:
        logger.warning("Direct logs failed %d: %s", player_id, e)
        return []


def _parse_logs_df(df) -> list:
    logs = []
    for _, row in df.iterrows():
        raw = str(row.get("GAME_DATE", "") or "")
        try:
            pd = datetime.strptime(raw[:10], "%Y-%m-%d")
        except Exception:
            pd = datetime.min
        logs.append({
            "game_date":   raw,
            "parsed_date": pd,
            "matchup":     row.get("MATCHUP", ""),
            "is_home":     "vs." in str(row.get("MATCHUP", "")),
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
    logs.sort(key=lambda x: x["parsed_date"], reverse=True)
    for log in logs:
        del log["parsed_date"]
    return logs
