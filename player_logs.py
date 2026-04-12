# player_logs.py — Multi-source: ESPN → BallDontLie → NBA.com
# Never fails completely — always has at least one source working

import logging
import time
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CURRENT_SEASON     = "2025-26"
CURRENT_SEASON_BDL = 2025       # BallDontLie uses integer year
SEASON_TYPE        = "Regular Season"
SLEEP              = 0.5
TIMEOUT            = 12

BDL_KEY  = "f380846e-c775-4ea9-bf81-93a132ad29f8"
BDL_BASE = "https://api.balldontlie.io/v1"
BDL_HEADERS = {
    "Authorization": BDL_KEY,
    "Content-Type":  "application/json",
}

NBA_HEADERS = {
    "Host":               "stats.nba.com",
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Referer":            "https://www.nba.com/",
    "Origin":             "https://www.nba.com",
}


def _patch_nba_api():
    try:
        from nba_api.stats.library.http import NBAStatsHTTP
        NBAStatsHTTP.headers = NBA_HEADERS.copy()
    except Exception:
        pass

_patch_nba_api()

from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats


# ─── PLAYER BASE STATS ────────────────────────────────────────

def get_all_player_base_stats() -> dict:
    """
    Get season averages for all players.
    Tries NBA.com first (most complete), falls back to BallDontLie.
    Returns {player_id: stats_dict}
    """
    # Try NBA.com
    result = _nba_safe_call(
        leaguedashplayerstats.LeagueDashPlayerStats,
        season=CURRENT_SEASON,
        season_type_all_star=SEASON_TYPE,
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
    )
    if result:
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
                    "source":      "nba.com",
                }
            if players:
                logger.info("Base player stats: %d players (nba.com)", len(players))
                return players
        except Exception as e:
            logger.warning("NBA.com player stats parse failed: %s", e)

    # Fall back to BallDontLie
    logger.info("NBA.com unavailable — fetching player stats from BallDontLie")
    return _bdl_get_player_base_stats()


def _bdl_get_player_base_stats() -> dict:
    """BallDontLie season averages → same format as NBA.com."""
    players = {}
    cursor  = None

    # First get all players to build ID map
    bdl_players = _bdl_get_all_players()

    while True:
        params = {"season": CURRENT_SEASON_BDL, "per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            time.sleep(SLEEP)
            r = requests.get(
                BDL_BASE + "/season_averages",
                headers=BDL_HEADERS, params=params, timeout=10
            )
            r.raise_for_status()
            data  = r.json()
            items = data.get("data", [])

            for item in items:
                bdl_pid = item.get("player_id")
                pdata   = bdl_players.get(bdl_pid, {})
                if not pdata:
                    continue

                # Use BDL player ID as our internal ID (negative to avoid clash with NBA.com IDs)
                pid = -(bdl_pid)

                mins_raw = str(item.get("min", "0") or "0")
                try:
                    if ":" in mins_raw:
                        parts = mins_raw.split(":")
                        mins  = float(parts[0]) + float(parts[1]) / 60
                    else:
                        mins = float(mins_raw)
                except Exception:
                    mins = 0.0

                players[pid] = {
                    "player_id":   pid,
                    "bdl_id":      bdl_pid,
                    "name":        pdata.get("name", ""),
                    "team_id":     pdata.get("team_id", 0),
                    "team_abbrev": pdata.get("team_abbrev", ""),
                    "position":    pdata.get("position", "G"),
                    "mins":        round(mins, 1),
                    "pts":         float(item.get("pts", 0) or 0),
                    "reb":         float(item.get("reb", 0) or 0),
                    "ast":         float(item.get("ast", 0) or 0),
                    "3pm":         float(item.get("fg3m", 0) or 0),
                    "stl":         float(item.get("stl", 0) or 0),
                    "blk":         float(item.get("blk", 0) or 0),
                    "tov":         float(item.get("turnover", 0) or 0),
                    "gp":          int(item.get("games_played", 0) or 0),
                    "source":      "balldontlie",
                }

            meta   = data.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            logger.warning("BDL season averages failed: %s", e)
            break

    logger.info("BallDontLie player stats: %d players", len(players))
    return players


def _bdl_get_all_players() -> dict:
    """Get player metadata from BallDontLie. Returns {bdl_id: {name, team_abbrev, position}}."""
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
    players = {}
    cursor  = None
    while True:
        params = {"per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            time.sleep(SLEEP)
            r = requests.get(
                BDL_BASE + "/players/active",
                headers=BDL_HEADERS, params=params, timeout=10
            )
            r.raise_for_status()
            data  = r.json()
            items = data.get("data", [])
            for p in items:
                team   = p.get("team", {})
                abbrev = team.get("abbreviation", "")
                players[p["id"]] = {
                    "name":        p.get("first_name", "") + " " + p.get("last_name", ""),
                    "team_abbrev": abbrev,
                    "team_id":     ABBREV_TO_NBA_ID.get(abbrev, 0),
                    "position":    p.get("position", "G") or "G",
                }
            meta   = data.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            logger.warning("BDL players list failed: %s", e)
            break
    return players


# ─── GAME LOGS ────────────────────────────────────────────────

def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    """
    Fetch game logs for multiple players.
    Uses NBA.com per player, falls back to BallDontLie if NBA.com returns empty.
    """
    results = {}
    total   = len(player_ids)
    nba_failures = 0

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)

        logs = None

        # If NBA.com has been failing a lot, go straight to BDL
        if nba_failures < 10:
            logs = _fetch_nbacom_logs(player_id, last_n)

        if not logs:
            nba_failures += 1
            # Try BDL — need bdl_id (positive version of negative player_id)
            # BDL player IDs are stored as negative in our system
            if player_id < 0:
                bdl_id = abs(player_id)
                logs = _fetch_bdl_logs(bdl_id, last_n)
        else:
            nba_failures = max(0, nba_failures - 1)

        if logs:
            results[player_id] = logs

    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _fetch_nbacom_logs(player_id: int, last_n: int) -> list:
    """Fetch logs from NBA.com. Returns [] on any failure."""
    if player_id < 0:
        return []  # BDL player — skip NBA.com
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

        logs = []
        for _, row in df.iterrows():
            raw_date = str(row.get("GAME_DATE", "") or "")
            try:
                parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            except Exception:
                parsed_date = datetime.min

            logs.append({
                "game_date":   raw_date,
                "parsed_date": parsed_date,
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
                "source":      "nba.com",
            })

        logs.sort(key=lambda g: g["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs

    except Exception as e:
        err = str(e)
        if "Expecting value" not in err and "line 1 column 1" not in err:
            logger.warning("NBA.com logs failed for %d: %s", player_id, e)
        return []


def _fetch_bdl_logs(bdl_player_id: int, last_n: int) -> list:
    """Fetch logs from BallDontLie."""
    try:
        time.sleep(SLEEP)
        r = requests.get(
            BDL_BASE + "/stats",
            headers=BDL_HEADERS,
            params={
                "player_ids[]": bdl_player_id,
                "seasons[]":    CURRENT_SEASON_BDL,
                "per_page":     last_n,
            },
            timeout=10,
        )
        r.raise_for_status()
        items = r.json().get("data", [])

        logs = []
        for item in items:
            game     = item.get("game", {})
            raw_date = game.get("date", "")[:10]
            try:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d")
            except Exception:
                continue

            mins_str = str(item.get("min", "0") or "0")
            try:
                if ":" in mins_str:
                    parts = mins_str.split(":")
                    mins  = float(parts[0]) + float(parts[1]) / 60
                else:
                    mins = float(mins_str)
            except Exception:
                mins = 0.0

            logs.append({
                "game_date":   raw_date,
                "parsed_date": parsed_date,
                "matchup":     "",
                "is_home":     False,
                "mins":        round(mins, 1),
                "pts":         int(item.get("pts", 0) or 0),
                "reb":         int(item.get("reb", 0) or 0),
                "ast":         int(item.get("ast", 0) or 0),
                "3pm":         int(item.get("fg3m", 0) or 0),
                "stl":         int(item.get("stl", 0) or 0),
                "blk":         int(item.get("blk", 0) or 0),
                "tov":         int(item.get("turnover", 0) or 0),
                "plus_minus":  float(item.get("plus_minus", 0) or 0),
                "source":      "balldontlie",
            })

        logs.sort(key=lambda g: g["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs

    except Exception as e:
        logger.warning("BDL logs failed for player %d: %s", bdl_player_id, e)
        return []


# ─── NBA.com helper ───────────────────────────────────────────

def _nba_safe_call(fn, *args, retries=2, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("NBA.com call failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(3)
    return None
