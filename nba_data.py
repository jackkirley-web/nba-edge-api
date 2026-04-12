# nba_data.py
# Direct NBA stats request layer with browser headers, serialized requests,
# fast-fail retries, and optional proxy support via environment variables.
#
# Render env vars you can set:
# NBA_PROXY_URL=http://user:pass@host:port
# NBA_PROXY_URL_HTTPS=http://user:pass@host:port   (optional, falls back to NBA_PROXY_URL)
# NBA_DISABLE_PROXY=false
# NBA_TIMEOUT=12
# NBA_RETRIES=2

import logging
import os
import random
import threading
import time
from datetime import date

import requests

logger = logging.getLogger(__name__)

NBA_STATS_BASE = "https://stats.nba.com/stats"
NBA_SCOREBOARD_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"

REQUEST_LOCK = threading.Lock()
SESSION = requests.Session()

DEFAULT_TIMEOUT = int(os.getenv("NBA_TIMEOUT", "12"))
DEFAULT_RETRIES = int(os.getenv("NBA_RETRIES", "2"))
DISABLE_PROXY = os.getenv("NBA_DISABLE_PROXY", "false").lower() == "true"

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Host": "stats.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-AU,en;q=0.9,en-US;q=0.8",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

TEAM_ABBR_MAP = {
    "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751, "CHA": 1610612766,
    "CHI": 1610612741, "CLE": 1610612739, "DAL": 1610612742, "DEN": 1610612743,
    "DET": 1610612765, "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763, "MIA": 1610612748,
    "MIL": 1610612749, "MIN": 1610612750, "NOP": 1610612740, "NYK": 1610612752,
    "OKC": 1610612760, "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "TOR": 1610612761,
    "UTA": 1610612762, "WAS": 1610612764,
}
TEAM_ABBR_MAP["UTAH"] = TEAM_ABBR_MAP["UTA"]
TEAM_ABBR_MAP["GS"] = TEAM_ABBR_MAP["GSW"]
TEAM_ABBR_MAP["SA"] = TEAM_ABBR_MAP["SAS"]
TEAM_ABBR_MAP["NO"] = TEAM_ABBR_MAP["NOP"]
TEAM_ABBR_MAP["NY"] = TEAM_ABBR_MAP["NYK"]


def _season_string() -> str:
    today = date.today()
    if today.month >= 10:
        start_year = today.year
    else:
        start_year = today.year - 1
    end_year = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year}"


def _proxies():
    if DISABLE_PROXY:
        return None
    http_proxy = os.getenv("NBA_PROXY_URL", "").strip()
    https_proxy = os.getenv("NBA_PROXY_URL_HTTPS", "").strip() or http_proxy
    if not http_proxy and not https_proxy:
        return None
    return {
        "http": http_proxy or https_proxy,
        "https": https_proxy or http_proxy,
    }


def _resultset_to_rows(payload):
    rs = payload.get("resultSet") or payload.get("resultSets")
    if not rs:
        return []

    if isinstance(rs, list):
        first = rs[0] if rs else {}
        headers = first.get("headers", [])
        rows = first.get("rowSet", [])
    else:
        headers = rs.get("headers", [])
        rows = rs.get("rowSet", [])

    return [dict(zip(headers, row)) for row in rows]


def _request_json(url, params=None, timeout=None, retries=None, use_stats_headers=True):
    timeout = timeout or DEFAULT_TIMEOUT
    retries = retries if retries is not None else DEFAULT_RETRIES
    headers = COMMON_HEADERS.copy() if use_stats_headers else {
        "User-Agent": COMMON_HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with REQUEST_LOCK:
                time.sleep(random.uniform(0.8, 1.3))
                resp = SESSION.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    proxies=_proxies(),
                )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            logger.warning("NBA API call failed (attempt %d): %s", attempt, e)
            if attempt < retries:
                time.sleep(1.5 + attempt)
    raise last_err


def _stats(endpoint: str, params: dict):
    return _request_json(f"{NBA_STATS_BASE}/{endpoint}", params=params, use_stats_headers=True)


def get_today_games():
    try:
        payload = _request_json(NBA_SCOREBOARD_URL, use_stats_headers=False, retries=1, timeout=10)
        games = payload.get("scoreboard", {}).get("games", [])
        out = []
        for g in games:
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            out.append({
                "game_id": g.get("gameId"),
                "home_team_id": int(home.get("teamId", 0) or 0),
                "away_team_id": int(away.get("teamId", 0) or 0),
                "home_team_abbrev": home.get("teamTricode"),
                "away_team_abbrev": away.get("teamTricode"),
                "home_team_city": home.get("teamCity"),
                "away_team_city": away.get("teamCity"),
                "home_team": home.get("teamName"),
                "away_team": away.get("teamName"),
                "game_status": g.get("gameStatusText"),
                "game_et": g.get("gameEt"),
            })
        return out
    except Exception as e:
        logger.warning("Today games fetch failed: %s", e)
        return []


def get_all_team_stats_batch(measure_type="Base", location=None):
    params = {
        "Conference": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "GameScope": "",
        "GameSegment": "",
        "LastNGames": 0,
        "LeagueID": "00",
        "Location": location or "",
        "MeasureType": measure_type,
        "Month": 0,
        "OpponentTeamID": 0,
        "Outcome": "",
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "PerGame",
        "Period": 0,
        "PlayerExperience": "",
        "PlayerPosition": "",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": _season_string(),
        "SeasonSegment": "",
        "SeasonType": "Regular Season",
        "ShotClockRange": "",
        "StarterBench": "",
        "TeamID": 0,
        "TwoWay": "",
        "VsConference": "",
        "VsDivision": "",
    }
    try:
        rows = _resultset_to_rows(_stats("leaguedashteamstats", params))
        out = {}
        for r in rows:
            tid = int(r.get("TEAM_ID", 0) or 0)
            if tid:
                out[tid] = {k.lower(): v for k, v in r.items()}
        return out
    except Exception as e:
        logger.warning("Team stats returned None (%s loc=%s)", measure_type, location)
        logger.warning("Team stats error detail: %s", e)
        return {}


def get_all_team_recent_batch(last_n_games=10):
    params = {
        "Conference": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "GameScope": "",
        "GameSegment": "",
        "LastNGames": last_n_games,
        "LeagueID": "00",
        "Location": "",
        "MeasureType": "Base",
        "Month": 0,
        "OpponentTeamID": 0,
        "Outcome": "",
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "PerGame",
        "Period": 0,
        "PlayerExperience": "",
        "PlayerPosition": "",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": _season_string(),
        "SeasonSegment": "",
        "SeasonType": "Regular Season",
        "ShotClockRange": "",
        "StarterBench": "",
        "TeamID": 0,
        "TwoWay": "",
        "VsConference": "",
        "VsDivision": "",
    }
    try:
        rows = _resultset_to_rows(_stats("leaguedashteamstats", params))
        out = {}
        for r in rows:
            tid = int(r.get("TEAM_ID", 0) or 0)
            if tid:
                out[tid] = {k.lower(): v for k, v in r.items()}
        return out
    except Exception as e:
        logger.warning("Recent team stats failed for last_n=%s: %s", last_n_games, e)
        return {}


def get_all_player_stats_batch():
    params = {
        "College": "",
        "Conference": "",
        "Country": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "DraftPick": "",
        "DraftYear": "",
        "GameScope": "",
        "GameSegment": "",
        "Height": "",
        "LastNGames": 0,
        "LeagueID": "00",
        "Location": "",
        "MeasureType": "Advanced",
        "Month": 0,
        "OpponentTeamID": 0,
        "Outcome": "",
        "PORound": 0,
        "PaceAdjust": "N",
        "PerMode": "PerGame",
        "Period": 0,
        "PlayerExperience": "",
        "PlayerPosition": "",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": _season_string(),
        "SeasonSegment": "",
        "SeasonType": "Regular Season",
        "ShotClockRange": "",
        "StarterBench": "",
        "TeamID": 0,
        "VsConference": "",
        "VsDivision": "",
        "Weight": "",
    }
    try:
        rows = _resultset_to_rows(_stats("leaguedashplayerstats", params))
        grouped = {}
        for r in rows:
            tid = int(r.get("TEAM_ID", 0) or 0)
            if not tid:
                continue
            grouped.setdefault(tid, []).append({
                "player_id": r.get("PLAYER_ID"),
                "name": r.get("PLAYER_NAME"),
                "team_id": tid,
                "team_abbrev": r.get("TEAM_ABBREVIATION"),
                "minutes": float(r.get("MIN", 0) or 0),
                "usage_rate": float(r.get("USG_PCT", 0) or 0),
                "off_rating": float(r.get("OFF_RATING", 0) or 0),
                "def_rating": float(r.get("DEF_RATING", 0) or 0),
                "net_rating": float(r.get("NET_RATING", 0) or 0),
                "ast_pct": float(r.get("AST_PCT", 0) or 0),
                "reb_pct": float(r.get("REB_PCT", 0) or 0),
            })
        return grouped
    except Exception as e:
        logger.warning("All player advanced stats failed: %s", e)
        return {}
