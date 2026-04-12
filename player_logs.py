# player_logs.py — NBA.com primary, direct HTTP fallback, correct season

import logging
import time
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"
SEASON_TYPE    = "Regular Season"
SLEEP          = 0.5
TIMEOUT        = 12

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
                time.sleep(3)
    return None


def get_all_player_base_stats() -> dict:
    """Get season averages. Tries nba_api then direct HTTP request."""
    # Try via nba_api (headers patched)
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
            logger.warning("nba_api player stats parse failed: %s", e)

    # Try direct HTTP with full browser headers
    logger.info("Trying direct HTTP for player base stats...")
    return _direct_player_base_stats()


def _direct_player_base_stats() -> dict:
    """Direct HTTP request to stats.nba.com with browser headers."""
    try:
        time.sleep(SLEEP)
        url = "https://stats.nba.com/stats/leaguedashplayerstats"
        params = {
            "Season":                   CURRENT_SEASON,
            "SeasonType":               SEASON_TYPE,
            "MeasureType":              "Base",
            "PerMode":                  "PerGame",
            "LeagueID":                 "00",
            "College":                  "",
            "Conference":               "",
            "Country":                  "",
            "DateFrom":                 "",
            "DateTo":                   "",
            "Division":                 "",
            "DraftPick":                "",
            "DraftYear":                "",
            "GameScope":                "",
            "GameSegment":              "",
            "Height":                   "",
            "LastNGames":               0,
            "Location":                 "",
            "Month":                    0,
            "OpponentTeamID":           0,
            "Outcome":                  "",
            "PORound":                  0,
            "PaceAdjust":               "N",
            "Period":                   0,
            "PlayerExperience":         "",
            "PlayerPosition":           "",
            "PlusMinus":                "N",
            "Rank":                     "N",
            "SeasonSegment":            "",
            "ShotClockRange":           "",
            "StarterBench":             "",
            "TeamID":                   0,
            "TwoWay":                   0,
            "VsConference":             "",
            "VsDivision":               "",
            "Weight":                   "",
        }
        r = requests.get(url, params=params, headers=NBA_HEADERS, timeout=TIMEOUT)
        if r.status_code != 200 or not r.text.strip():
            logger.warning("Direct player stats: bad response %d", r.status_code)
            return {}

        data        = r.json()
        result_sets = data.get("resultSets", [])
        if not result_sets:
            return {}

        headers_list = result_sets[0].get("headers", [])
        rows         = result_sets[0].get("rowSet", [])
        col          = {h: i for i, h in enumerate(headers_list)}

        def g(row, field, default=0):
            idx = col.get(field)
            if idx is None: return default
            val = row[idx]
            return val if val is not None else default

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
                "mins":        float(g(row, "MIN", 0)),
                "pts":         float(g(row, "PTS", 0)),
                "reb":         float(g(row, "REB", 0)),
                "ast":         float(g(row, "AST", 0)),
                "3pm":         float(g(row, "FG3M", 0)),
                "stl":         float(g(row, "STL", 0)),
                "blk":         float(g(row, "BLK", 0)),
                "tov":         float(g(row, "TOV", 0)),
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
            "mins":        float(row.get("MIN",  0) or 0),
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
    results = {}
    total   = len(player_ids)
    nba_fail_streak = 0

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)

        if player_id < 0:
            # Negative IDs are from BDL — skip for now
            continue

        logs = None

        # Only try NBA.com if it hasn't been failing repeatedly
        if nba_fail_streak < 8:
            logs = _fetch_nbacom_logs(player_id, last_n)

        if logs:
            results[player_id] = logs
            nba_fail_streak = max(0, nba_fail_streak - 1)
        else:
            nba_fail_streak += 1
            # Try direct HTTP fallback
            if nba_fail_streak < 15:
                logs = _fetch_direct_logs(player_id, last_n)
                if logs:
                    results[player_id] = logs
                    nba_fail_streak = max(0, nba_fail_streak - 1)

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
        if "Expecting value" in err or "line 1 column 1" in err:
            return []
        logger.warning("nba_api logs failed for %d: %s", player_id, e)
        return []


def _fetch_direct_logs(player_id: int, last_n: int) -> list:
    """Direct HTTP fallback for game logs."""
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
            headers=NBA_HEADERS,
            timeout=TIMEOUT,
        )
        if r.status_code != 200 or not r.text.strip():
            return []

        data        = r.json()
        result_sets = data.get("resultSets", [])
        if not result_sets or not result_sets[0].get("rowSet"):
            return []

        hdrs = result_sets[0].get("headers", [])
        rows = result_sets[0].get("rowSet", [])
        col  = {h: i for i, h in enumerate(hdrs)}

        def g(row, field, default=0):
            idx = col.get(field)
            if idx is None: return default
            val = row[idx]
            return val if val is not None else default

        logs = []
        for row in rows:
            raw_date = str(g(row, "GAME_DATE", ""))
            try:
                pd = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            except Exception:
                pd = datetime.min
            logs.append({
                "game_date":   raw_date,
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
            })

        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs

    except Exception as e:
        logger.warning("Direct logs failed for %d: %s", player_id, e)
        return []


def _parse_logs_df(df) -> list:
    logs = []
    for _, row in df.iterrows():
        raw_date = str(row.get("GAME_DATE", "") or "")
        try:
            pd = datetime.strptime(raw_date[:10], "%Y-%m-%d")
        except Exception:
            pd = datetime.min
        logs.append({
            "game_date":   raw_date,
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
    logs.sort(key=lambda g: g["parsed_date"], reverse=True)
    for log in logs:
        del log["parsed_date"]
    return logs
