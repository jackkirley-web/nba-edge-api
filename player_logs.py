# player_logs.py — Fetches per-player game logs from NBA.com

# 

# NBA.com blocks nba_api’s default headers on cloud IPs (AWS/Render).

# We bypass this by making direct HTTP requests with full browser headers,

# rotating through multiple endpoints, and using session keep-alive.

import logging
import time
import random
import requests
from datetime import datetime
from nba_api.stats.endpoints import leaguedashplayerstats

logger = logging.getLogger(**name**)

CURRENT_SEASON = “2025-26”
SEASON_TYPE    = “Regular Season”

# ── Headers that mimic a real browser ─────────────────────────────────────────

# These are what worked before — full Chrome UA with all NBA.com-expected headers

def _get_headers():
return {
“Host”:                      “stats.nba.com”,
“Connection”:                “keep-alive”,
“Accept”:                    “application/json, text/plain, */*”,
“Accept-Language”:           “en-US,en;q=0.9”,
“Accept-Encoding”:           “gzip, deflate, br”,
“x-nba-stats-origin”:        “stats”,
“x-nba-stats-token”:         “true”,
“User-Agent”:                “Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36”,
“Referer”:                   “https://www.nba.com/”,
“Origin”:                    “https://www.nba.com”,
“Sec-Fetch-Site”:            “same-site”,
“Sec-Fetch-Mode”:            “cors”,
“Sec-Fetch-Dest”:            “empty”,
“Sec-Ch-Ua”:                 ‘“Chromium”;v=“122”, “Not(A:Brand”;v=“24”, “Google Chrome”;v=“122”’,
“Sec-Ch-Ua-Mobile”:          “?0”,
“Sec-Ch-Ua-Platform”:        ‘“Windows”’,
}

NBA_STATS_BASE = “https://stats.nba.com/stats”

# Shared session for connection reuse

_session = None

def _get_session():
global _session
if _session is None:
_session = requests.Session()
_session.headers.update(_get_headers())
return _session

def _fetch_direct(endpoint: str, params: dict, retries: int = 3) -> dict | None:
“””
Make a direct HTTP request to stats.nba.com with browser headers.
Retries with backoff on failure.
“””
session = _get_session()
url = f”{NBA_STATS_BASE}/{endpoint}”

```
for attempt in range(retries):
    try:
        # Random delay to avoid rate limiting — longer on retries
        delay = 0.6 + random.uniform(0, 0.4) + (attempt * 1.5)
        time.sleep(delay)

        resp = session.get(url, params=params, timeout=30)

        # NBA.com sometimes returns 200 with empty body when blocking
        if resp.status_code == 200 and len(resp.content) > 100:
            data = resp.json()
            return data

        if resp.status_code == 429:
            # Rate limited — wait longer
            logger.warning("Rate limited by NBA.com, waiting 10s...")
            time.sleep(10)
            continue

        if resp.status_code in (403, 503):
            logger.warning("NBA.com blocked (status %d), attempt %d", resp.status_code, attempt + 1)
            time.sleep(5 + attempt * 5)
            continue

        logger.warning("NBA.com returned %d for %s", resp.status_code, endpoint)

    except requests.exceptions.Timeout:
        logger.warning("Timeout fetching %s (attempt %d)", endpoint, attempt + 1)
        time.sleep(3 + attempt * 3)
    except requests.exceptions.ConnectionError as e:
        logger.warning("Connection error %s (attempt %d): %s", endpoint, attempt + 1, e)
        time.sleep(3)
    except Exception as e:
        logger.warning("Error fetching %s (attempt %d): %s", endpoint, attempt + 1, e)
        time.sleep(2)

return None
```

def _parse_nba_response(data: dict, row_mapper) -> list:
“””
Parse NBA.com stats API response format:
{ resultSets: [{ headers: […], rowSet: [[…], …] }] }
“””
try:
result_sets = data.get(“resultSets”, [])
if not result_sets:
return []
rs = result_sets[0]
headers = rs.get(“headers”, [])
rows    = rs.get(“rowSet”, [])
result  = []
for row in rows:
row_dict = dict(zip(headers, row))
item = row_mapper(row_dict)
if item is not None:
result.append(item)
return result
except Exception as e:
logger.warning(“Failed to parse NBA response: %s”, e)
return []

# ── Game log fetching ──────────────────────────────────────────────────────────

def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
“””
Fetch game logs for multiple players using direct HTTP with browser headers.
Falls back to nba_api library if direct requests fail.
Returns {player_id: [game_log_dicts]}
“””
results = {}
total = len(player_ids)
consecutive_failures = 0

```
for i, player_id in enumerate(player_ids):
    if i % 20 == 0:
        logger.info("Fetching logs: %d/%d players (got %d so far)", i, total, len(results))

    # If we've had 10 consecutive failures, NBA.com is probably blocking us
    if consecutive_failures >= 10:
        logger.warning(
            "10 consecutive failures — NBA.com is blocking this session. "
            "Got %d/%d players before block.", len(results), total
        )
        break

    logs = _fetch_player_logs_direct(player_id, last_n)

    if logs:
        results[player_id] = logs
        consecutive_failures = 0
    else:
        consecutive_failures += 1

logger.info("Got logs for %d/%d players", len(results), total)
return results
```

def _fetch_player_logs_direct(player_id: int, last_n: int) -> list:
“”“Fetch game logs for one player via direct HTTP request.”””

```
# Try direct HTTP first (bypasses nba_api's headers)
data = _fetch_direct("playergamelogs", {
    "PlayerID":        player_id,
    "Season":          CURRENT_SEASON,
    "SeasonType":      SEASON_TYPE,
    "LastNGames":      last_n,
    "LeagueID":        "00",
    "PerMode":         "Totals",
})

if data:
    logs = _parse_nba_response(data, _map_game_log_row)
    if logs:
        return logs

# Fallback: try nba_api library (different request path, sometimes works)
return _fetch_player_logs_nba_api(player_id, last_n)
```

def _fetch_player_logs_nba_api(player_id: int, last_n: int) -> list:
“”“Fallback: use nba_api library for game logs.”””
try:
from nba_api.stats.endpoints import playergamelogs
time.sleep(0.8)
result = playergamelogs.PlayerGameLogs(
player_id_nullable=player_id,
season_nullable=CURRENT_SEASON,
season_type_nullable=SEASON_TYPE,
last_n_games_nullable=last_n,
)
df = result.get_data_frames()[0]
logs = []
for _, row in df.iterrows():
raw_date = str(row.get(“GAME_DATE”, “”) or “”)
try:
parsed_date = datetime.strptime(raw_date[:10], “%Y-%m-%d”)
except Exception:
parsed_date = datetime.min
logs.append({
“game_date”:  raw_date,
“parsed_date”: parsed_date,
“matchup”:    row.get(“MATCHUP”, “”),
“is_home”:    “vs.” in str(row.get(“MATCHUP”, “”)),
“win”:        row.get(“WL”, “”) == “W”,
“mins”:       float(row.get(“MIN”, 0) or 0),
“pts”:        int(row.get(“PTS”, 0) or 0),
“reb”:        int(row.get(“REB”, 0) or 0),
“ast”:        int(row.get(“AST”, 0) or 0),
“3pm”:        int(row.get(“FG3M”, 0) or 0),
“stl”:        int(row.get(“STL”, 0) or 0),
“blk”:        int(row.get(“BLK”, 0) or 0),
“tov”:        int(row.get(“TOV”, 0) or 0),
“plus_minus”: float(row.get(“PLUS_MINUS”, 0) or 0),
})
logs.sort(key=lambda g: g[“parsed_date”], reverse=True)
for log in logs:
del log[“parsed_date”]
return logs
except Exception as e:
logger.warning(“nba_api fallback failed for player %d: %s”, player_id, e)
return []

def _map_game_log_row(row: dict) -> dict | None:
“”“Map a raw NBA.com API row to our game log format.”””
try:
raw_date = str(row.get(“GAME_DATE”, “”) or “”)
return {
“game_date”:  raw_date,
“matchup”:    row.get(“MATCHUP”, “”),
“is_home”:    “vs.” in str(row.get(“MATCHUP”, “”)),
“win”:        row.get(“WL”, “”) == “W”,
“mins”:       float(row.get(“MIN”, 0) or 0),
“pts”:        int(row.get(“PTS”, 0) or 0),
“reb”:        int(row.get(“REB”, 0) or 0),
“ast”:        int(row.get(“AST”, 0) or 0),
“3pm”:        int(row.get(“FG3M”, 0) or 0),
“stl”:        int(row.get(“STL”, 0) or 0),
“blk”:        int(row.get(“BLK”, 0) or 0),
“tov”:        int(row.get(“TOV”, 0) or 0),
“plus_minus”: float(row.get(“PLUS_MINUS”, 0) or 0),
}
except Exception:
return None

# ── Player base stats ──────────────────────────────────────────────────────────

def get_all_player_base_stats() -> dict:
“””
Get season averages for all players.
Tries direct HTTP first, falls back to nba_api.
Returns {player_id: {pts, reb, ast, 3pm, stl, blk, mins, position, name}}
“””
# Try direct HTTP
data = _fetch_direct(“leaguedashplayerstats”, {
“Season”:          CURRENT_SEASON,
“SeasonType”:      SEASON_TYPE,
“MeasureType”:     “Base”,
“PerMode”:         “PerGame”,
“LeagueID”:        “00”,
“GameScope”:       “”,
“PlayerExperience”: “”,
“PlayerPosition”:  “”,
“StarterBench”:    “”,
})

```
if data:
    players = {}
    rows = _parse_nba_response(data, lambda r: r)
    for row in rows:
        pid = int(row.get("PLAYER_ID", 0))
        if pid:
            players[pid] = {
                "player_id":   pid,
                "name":        row.get("PLAYER_NAME", ""),
                "team_id":     int(row.get("TEAM_ID", 0)),
                "team_abbrev": row.get("TEAM_ABBREVIATION", ""),
                "position":    row.get("START_POSITION", "G") or "G",
                "mins":        float(row.get("MIN", 0) or 0),
                "pts":         float(row.get("PTS", 0) or 0),
                "reb":         float(row.get("REB", 0) or 0),
                "ast":         float(row.get("AST", 0) or 0),
                "3pm":         float(row.get("FG3M", 0) or 0),
                "stl":         float(row.get("STL", 0) or 0),
                "blk":         float(row.get("BLK", 0) or 0),
                "tov":         float(row.get("TOV", 0) or 0),
                "gp":          int(row.get("GP", 0) or 0),
            }
    if players:
        logger.info("Base player stats (direct): %d players", len(players))
        return players

# Fallback to nba_api
return _get_base_stats_nba_api()
```

def _get_base_stats_nba_api() -> dict:
“”“Fallback: get player base stats via nba_api library.”””
try:
import time as t
t.sleep(0.6)
result = leaguedashplayerstats.LeagueDashPlayerStats(
season=CURRENT_SEASON,
season_type_all_star=SEASON_TYPE,
measure_type_detailed_defense=“Base”,
per_mode_detailed=“PerGame”,
)
df = result.get_data_frames()[0]
players = {}
for _, row in df.iterrows():
pid = int(row.get(“PLAYER_ID”, 0))
players[pid] = {
“player_id”:   pid,
“name”:        row.get(“PLAYER_NAME”, “”),
“team_id”:     int(row.get(“TEAM_ID”, 0)),
“team_abbrev”: row.get(“TEAM_ABBREVIATION”, “”),
“position”:    row.get(“START_POSITION”, “G”) or “G”,
“mins”:        float(row.get(“MIN”, 0) or 0),
“pts”:         float(row.get(“PTS”, 0) or 0),
“reb”:         float(row.get(“REB”, 0) or 0),
“ast”:         float(row.get(“AST”, 0) or 0),
“3pm”:         float(row.get(“FG3M”, 0) or 0),
“stl”:         float(row.get(“STL”, 0) or 0),
“blk”:         float(row.get(“BLK”, 0) or 0),
“tov”:         float(row.get(“TOV”, 0) or 0),
“gp”:          int(row.get(“GP”, 0) or 0),
}
logger.info(“Base player stats (nba_api): %d players”, len(players))
return players
except Exception as e:
logger.warning(“get_all_player_base_stats nba_api failed: %s”, e)
return {}
