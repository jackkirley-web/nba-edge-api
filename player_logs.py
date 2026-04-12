# player_logs.py
# Source priority: NBA.com → ESPN → BallDontLie
# All three return the same data format so the rest of the system
# doesn't need to know which source was used.

import logging
import time
import requests
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

CURRENT_SEASON     = "2025-26"
SEASON_TYPE        = "Regular Season"
SLEEP              = 0.5
TIMEOUT            = 12

# ─── API KEYS & ENDPOINTS ─────────────────────────────────────
BDL_KEY  = "41d44065-0c14-4a66-b633-f93fb1680fb2"
BDL_BASE = "https://api.balldontlie.io/v1"
BDL_HDR  = {"Authorization": BDL_KEY}

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
ESPN_CORE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"
ESPN_HDR  = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

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

# ESPN abbreviation → NBA.com team ID
ESPN_TO_NBA_ID = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GS":1610612744,"GSW":1610612744,"HOU":1610612745,
    "IND":1610612754,"LAC":1610612746,"LAL":1610612747,"MEM":1610612763,
    "MIA":1610612748,"MIL":1610612749,"MIN":1610612750,"NO":1610612740,
    "NOP":1610612740,"NY":1610612752,"NYK":1610612752,"OKC":1610612760,
    "ORL":1610612753,"PHI":1610612755,"PHX":1610612756,"POR":1610612757,
    "SA":1610612759,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,"WSH":1610612764,
}

# BallDontLie team abbrev → NBA.com team ID
BDL_TO_NBA_ID = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}


def _patch_nba_api():
    try:
        from nba_api.stats.library.http import NBAStatsHTTP
        NBAStatsHTTP.headers = NBA_HEADERS.copy()
    except Exception:
        pass

_patch_nba_api()

from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats


# ══════════════════════════════════════════════════════════════
# PLAYER BASE STATS
# ══════════════════════════════════════════════════════════════

def get_all_player_base_stats() -> dict:
    """
    Season averages for all players.
    Returns {player_id: stats_dict} with consistent format.
    Tries NBA.com → ESPN → BallDontLie.
    """
    # ── 1. NBA.com via nba_api ─────────────────────────────────
    result = _nba_safe_call(
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
                players = _parse_nba_player_df(df, source="nba_api")
                if players:
                    logger.info("Player base stats: %d players (nba_api)", len(players))
                    return players
        except Exception as e:
            logger.warning("nba_api player stats parse failed: %s", e)

    # ── 2. NBA.com direct HTTP ────────────────────────────────
    players = _nbacom_direct_player_stats()
    if players:
        logger.info("Player base stats: %d players (nba.com direct)", len(players))
        return players

    # ── 3. ESPN ───────────────────────────────────────────────
    logger.info("NBA.com unavailable — trying ESPN for player stats")
    players = _espn_player_stats()
    if players:
        logger.info("Player base stats: %d players (ESPN)", len(players))
        return players

    # ── 4. BallDontLie ───────────────────────────────────────
    logger.info("ESPN unavailable — trying BallDontLie for player stats")
    players = _bdl_player_stats()
    if players:
        logger.info("Player base stats: %d players (BallDontLie)", len(players))
        return players

    logger.error("All player stats sources failed")
    return {}


def _nbacom_direct_player_stats() -> dict:
    """Direct HTTP to stats.nba.com with browser headers."""
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
            headers=NBA_HEADERS, timeout=TIMEOUT
        )
        if r.status_code != 200 or not r.text.strip():
            return {}
        data  = r.json()
        rs    = data.get("resultSets", [])
        if not rs:
            return {}
        hdrs  = rs[0].get("headers", [])
        rows  = rs[0].get("rowSet", [])
        col   = {h: i for i, h in enumerate(hdrs)}

        players = {}
        for row in rows:
            def g(f, d=0):
                idx = col.get(f)
                return row[idx] if idx is not None and row[idx] is not None else d
            pid = int(g("PLAYER_ID", 0))
            if not pid:
                continue
            players[pid] = _player_record(
                pid, g("PLAYER_NAME",""), int(g("TEAM_ID",0)),
                g("TEAM_ABBREVIATION",""), g("START_POSITION","G") or "G",
                float(g("MIN",0)), float(g("PTS",0)), float(g("REB",0)),
                float(g("AST",0)), float(g("FG3M",0)), float(g("STL",0)),
                float(g("BLK",0)), float(g("TOV",0)), int(g("GP",0)),
                "nba.com-direct"
            )
        return players
    except Exception as e:
        logger.warning("NBA.com direct player stats failed: %s", e)
        return {}


def _espn_player_stats() -> dict:
    """
    ESPN team rosters + per-season stats.
    ESPN provides season averages via athlete stats endpoints.
    """
    players = {}
    try:
        # Get all NBA teams from ESPN
        r = requests.get(ESPN_BASE + "/teams", headers=ESPN_HDR, timeout=10)
        r.raise_for_status()
        sports = r.json().get("sports", [])
        teams  = sports[0].get("leagues", [{}])[0].get("teams", []) if sports else []
    except Exception as e:
        logger.warning("ESPN teams list failed: %s", e)
        return {}

    for team_entry in teams:
        team        = team_entry.get("team", {})
        espn_tid    = team.get("id", "")
        team_abbrev = team.get("abbreviation", "")
        nba_team_id = ESPN_TO_NBA_ID.get(team_abbrev, 0)

        try:
            r = requests.get(
                f"{ESPN_BASE}/teams/{espn_tid}/roster",
                headers=ESPN_HDR, timeout=10
            )
            r.raise_for_status()
            athletes = r.json().get("athletes", [])
        except Exception:
            continue

        for athlete in athletes:
            espn_pid = str(athlete.get("id", ""))
            name     = athlete.get("fullName", "")
            pos      = athlete.get("position", {}).get("abbreviation", "G") or "G"

            try:
                sr = requests.get(
                    f"{ESPN_BASE}/athletes/{espn_pid}/stats",
                    headers=ESPN_HDR, timeout=8
                )
                sr.raise_for_status()
                sdata = sr.json()

                # Parse splits → find "avg" category
                avgs = {}
                for cat in sdata.get("splits", {}).get("categories", []):
                    if cat.get("name") == "avg":
                        for stat in cat.get("stats", []):
                            avgs[stat.get("name","")] = float(stat.get("value",0) or 0)
                        break

                # ESPN uses a unique numeric ID — store as negative to avoid
                # clashing with NBA.com integer player IDs
                internal_id = -(int(espn_pid))

                players[internal_id] = _player_record(
                    internal_id, name, nba_team_id, team_abbrev, pos,
                    avgs.get("avgMinutes", 0),
                    avgs.get("avgPoints", 0),
                    avgs.get("avgRebounds", 0),
                    avgs.get("avgAssists", 0),
                    avgs.get("avg3PointFieldGoalsMade", 0),
                    avgs.get("avgSteals", 0),
                    avgs.get("avgBlocks", 0),
                    avgs.get("avgTurnovers", 0),
                    int(avgs.get("gamesPlayed", 0)),
                    "espn",
                )
                players[internal_id]["espn_id"] = espn_pid
                time.sleep(0.2)
            except Exception:
                continue

    return players


def _bdl_player_stats() -> dict:
    """BallDontLie season averages — reliable fallback."""
    # First build player metadata map
    bdl_meta = {}
    cursor   = None
    while True:
        params = {"per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            time.sleep(SLEEP)
            r = requests.get(f"{BDL_BASE}/players/active",
                             headers=BDL_HDR, params=params, timeout=10)
            r.raise_for_status()
            data  = r.json()
            for p in data.get("data", []):
                team   = p.get("team", {})
                abbrev = team.get("abbreviation", "")
                bdl_meta[p["id"]] = {
                    "name":     (p.get("first_name","")+" "+p.get("last_name","")).strip(),
                    "team_id":  BDL_TO_NBA_ID.get(abbrev, 0),
                    "abbrev":   abbrev,
                    "position": p.get("position","G") or "G",
                }
            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            logger.warning("BDL players/active failed: %s", e)
            break

    # Now get season averages
    players = {}
    cursor  = None
    while True:
        params = {"season": 2025, "per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            time.sleep(SLEEP)
            r = requests.get(f"{BDL_BASE}/season_averages",
                             headers=BDL_HDR, params=params, timeout=10)
            r.raise_for_status()
            data  = r.json()
            for item in data.get("data", []):
                bdl_pid = item.get("player_id")
                meta    = bdl_meta.get(bdl_pid, {})
                if not meta:
                    continue
                # Use negative BDL id to avoid clash with NBA.com ids
                internal_id = -(bdl_pid + 1_000_000)
                mins_raw    = str(item.get("min","0") or "0")
                try:
                    mins = float(mins_raw.split(":")[0]) if ":" in mins_raw else float(mins_raw)
                except Exception:
                    mins = 0.0
                players[internal_id] = _player_record(
                    internal_id, meta["name"], meta["team_id"], meta["abbrev"],
                    meta["position"], mins,
                    float(item.get("pts",0) or 0),
                    float(item.get("reb",0) or 0),
                    float(item.get("ast",0) or 0),
                    float(item.get("fg3m",0) or 0),
                    float(item.get("stl",0) or 0),
                    float(item.get("blk",0) or 0),
                    float(item.get("turnover",0) or 0),
                    int(item.get("games_played",0) or 0),
                    "balldontlie",
                )
                players[internal_id]["bdl_id"] = bdl_pid
            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            logger.warning("BDL season averages failed: %s", e)
            break

    return players


def _player_record(pid, name, team_id, team_abbrev, pos,
                   mins, pts, reb, ast, tpm, stl, blk, tov, gp, source):
    return {
        "player_id":   pid,
        "name":        name,
        "team_id":     team_id,
        "team_abbrev": team_abbrev,
        "position":    pos,
        "mins":        round(float(mins), 1),
        "pts":         round(float(pts),  1),
        "reb":         round(float(reb),  1),
        "ast":         round(float(ast),  1),
        "3pm":         round(float(tpm),  1),
        "stl":         round(float(stl),  1),
        "blk":         round(float(blk),  1),
        "tov":         round(float(tov),  1),
        "gp":          int(gp),
        "source":      source,
    }


def _parse_nba_player_df(df, source="nba_api") -> dict:
    players = {}
    for _, row in df.iterrows():
        pid = int(row.get("PLAYER_ID", 0))
        if not pid:
            continue
        players[pid] = _player_record(
            pid, row.get("PLAYER_NAME",""),
            int(row.get("TEAM_ID",0)), row.get("TEAM_ABBREVIATION",""),
            row.get("START_POSITION","G") or "G",
            float(row.get("MIN",0) or 0), float(row.get("PTS",0) or 0),
            float(row.get("REB",0) or 0), float(row.get("AST",0) or 0),
            float(row.get("FG3M",0) or 0), float(row.get("STL",0) or 0),
            float(row.get("BLK",0) or 0), float(row.get("TOV",0) or 0),
            int(row.get("GP",0) or 0), source
        )
    return players


# ══════════════════════════════════════════════════════════════
# GAME LOGS
# ══════════════════════════════════════════════════════════════

def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    """
    Fetch game logs for multiple players.
    Routes each player to the right source based on their ID prefix:
      positive IDs  → NBA.com player  → try NBA.com first, then BDL
      negative IDs (< -1M) → BDL player → try BDL first
      negative IDs (small) → ESPN player → try ESPN first, then BDL
    """
    results  = {}
    total    = len(player_ids)
    nba_fail = 0

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)

        logs = None

        if player_id > 0:
            # NBA.com player
            if nba_fail < 10:
                logs = _nbacom_logs(player_id, last_n)
                if logs:
                    nba_fail = max(0, nba_fail - 1)
                else:
                    nba_fail += 1
                    logs = _nbacom_direct_logs(player_id, last_n)
                    if not logs:
                        # Try BDL by searching player name
                        pass  # BDL requires their own ID, skip
            else:
                logs = _nbacom_direct_logs(player_id, last_n)

        elif player_id > -1_000_000:
            # ESPN player — use ESPN game log endpoint
            espn_pid = str(abs(player_id))
            logs = _espn_logs(espn_pid, last_n)
            if not logs:
                logs = _nbacom_direct_logs(abs(player_id), last_n)

        else:
            # BDL player
            bdl_pid = abs(player_id) - 1_000_000
            logs = _bdl_logs(bdl_pid, last_n)

        if logs:
            results[player_id] = logs

    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _nbacom_logs(player_id: int, last_n: int) -> list:
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
        return _parse_nbacom_df(df)
    except Exception as e:
        if "Expecting value" not in str(e) and "line 1 column 1" not in str(e):
            logger.warning("nba_api logs failed %d: %s", player_id, e)
        return []


def _nbacom_direct_logs(player_id: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            "https://stats.nba.com/stats/playergamelogs",
            params={
                "PlayerIDNullable": player_id,
                "Season": CURRENT_SEASON,
                "SeasonTypeNullable": SEASON_TYPE,
                "LastNGamesNullable": last_n,
            },
            headers=NBA_HEADERS, timeout=TIMEOUT
        )
        if r.status_code != 200 or not r.text.strip():
            return []
        rs   = r.json().get("resultSets", [])
        if not rs or not rs[0].get("rowSet"):
            return []
        hdrs = rs[0]["headers"]
        rows = rs[0]["rowSet"]
        col  = {h: i for i, h in enumerate(hdrs)}
        logs = []
        for row in rows:
            def g(f, d=0):
                idx = col.get(f)
                return row[idx] if idx is not None and row[idx] is not None else d
            raw = str(g("GAME_DATE",""))
            try:
                pd = datetime.strptime(raw[:10], "%Y-%m-%d")
            except Exception:
                pd = datetime.min
            logs.append({
                "game_date":   raw, "parsed_date": pd,
                "matchup":     g("MATCHUP",""), "is_home": "vs." in str(g("MATCHUP","")),
                "mins":        float(g("MIN",0)), "pts": int(g("PTS",0)),
                "reb":         int(g("REB",0)),  "ast": int(g("AST",0)),
                "3pm":         int(g("FG3M",0)), "stl": int(g("STL",0)),
                "blk":         int(g("BLK",0)),  "tov": int(g("TOV",0)),
                "plus_minus":  float(g("PLUS_MINUS",0)),
            })
        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs
    except Exception as e:
        logger.warning("Direct logs failed %d: %s", player_id, e)
        return []


def _espn_logs(espn_pid: str, last_n: int) -> list:
    """ESPN athlete gamelog endpoint."""
    try:
        time.sleep(SLEEP)
        r = requests.get(
            f"{ESPN_BASE}/athletes/{espn_pid}/gamelog",
            headers=ESPN_HDR, timeout=10
        )
        r.raise_for_status()
        data = r.json()

        events     = data.get("events", {})
        categories = data.get("categories", [])
        col_names  = []
        for cat in categories:
            for stat in cat.get("stats", []):
                col_names.append(stat.get("name",""))

        logs = []
        for event_id, event_data in events.items():
            if not isinstance(event_data, dict):
                continue
            stats_list = event_data.get("stats", [])
            game_date  = event_data.get("gameDate","")
            at_vs      = event_data.get("atVs","")
            opp        = event_data.get("opponent",{}).get("abbreviation","")

            stat_map = {col_names[i]: v for i, v in enumerate(stats_list) if i < len(col_names)}

            try:
                pd = datetime.strptime(game_date[:10], "%Y-%m-%d")
            except Exception:
                continue

            mins_str = str(stat_map.get("minutes","0:0"))
            try:
                parts = mins_str.split(":")
                mins  = float(parts[0]) + float(parts[1])/60 if len(parts)>1 else float(parts[0])
            except Exception:
                mins = 0.0

            logs.append({
                "game_date":   game_date[:10],
                "parsed_date": pd,
                "matchup":     ("vs. " if at_vs=="vs" else "@ ") + opp,
                "is_home":     at_vs == "vs",
                "mins":        round(mins, 1),
                "pts":         int(stat_map.get("points",0) or 0),
                "reb":         int(stat_map.get("rebounds",0) or 0),
                "ast":         int(stat_map.get("assists",0) or 0),
                "3pm":         int(stat_map.get("threePointFieldGoalsMade",0) or 0),
                "stl":         int(stat_map.get("steals",0) or 0),
                "blk":         int(stat_map.get("blocks",0) or 0),
                "tov":         int(stat_map.get("turnovers",0) or 0),
                "plus_minus":  float(stat_map.get("plusMinus",0) or 0),
            })

        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs[:last_n]

    except Exception as e:
        logger.warning("ESPN logs failed %s: %s", espn_pid, e)
        return []


def _bdl_logs(bdl_pid: int, last_n: int) -> list:
    """BallDontLie game stats."""
    try:
        time.sleep(SLEEP)
        r = requests.get(
            f"{BDL_BASE}/stats",
            headers=BDL_HDR,
            params={"player_ids[]": bdl_pid, "seasons[]": 2025, "per_page": last_n},
            timeout=10
        )
        r.raise_for_status()
        items = r.json().get("data", [])
        logs  = []
        for item in items:
            game    = item.get("game", {})
            raw     = game.get("date","")[:10]
            try:
                pd = datetime.strptime(raw, "%Y-%m-%d")
            except Exception:
                continue
            mins_str = str(item.get("min","0") or "0")
            try:
                mins = float(mins_str.split(":")[0]) + float(mins_str.split(":")[1])/60 if ":" in mins_str else float(mins_str)
            except Exception:
                mins = 0.0
            logs.append({
                "game_date":   raw,
                "parsed_date": pd,
                "matchup":     "",
                "is_home":     False,
                "mins":        round(mins, 1),
                "pts":         int(item.get("pts",0) or 0),
                "reb":         int(item.get("reb",0) or 0),
                "ast":         int(item.get("ast",0) or 0),
                "3pm":         int(item.get("fg3m",0) or 0),
                "stl":         int(item.get("stl",0) or 0),
                "blk":         int(item.get("blk",0) or 0),
                "tov":         int(item.get("turnover",0) or 0),
                "plus_minus":  float(item.get("plus_minus",0) or 0),
            })
        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs
    except Exception as e:
        logger.warning("BDL logs failed %d: %s", bdl_pid, e)
        return []


def _parse_nbacom_df(df) -> list:
    logs = []
    for _, row in df.iterrows():
        raw = str(row.get("GAME_DATE","") or "")
        try:
            pd = datetime.strptime(raw[:10], "%Y-%m-%d")
        except Exception:
            pd = datetime.min
        logs.append({
            "game_date":   raw, "parsed_date": pd,
            "matchup":     row.get("MATCHUP",""),
            "is_home":     "vs." in str(row.get("MATCHUP","")),
            "mins":        float(row.get("MIN",0) or 0),
            "pts":         int(row.get("PTS",0) or 0),
            "reb":         int(row.get("REB",0) or 0),
            "ast":         int(row.get("AST",0) or 0),
            "3pm":         int(row.get("FG3M",0) or 0),
            "stl":         int(row.get("STL",0) or 0),
            "blk":         int(row.get("BLK",0) or 0),
            "tov":         int(row.get("TOV",0) or 0),
            "plus_minus":  float(row.get("PLUS_MINUS",0) or 0),
        })
    logs.sort(key=lambda x: x["parsed_date"], reverse=True)
    for log in logs:
        del log["parsed_date"]
    return logs


def _nba_safe_call(fn, *args, retries=2, **kwargs):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            return fn(*args, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            logger.warning("NBA.com call failed (attempt %d): %s", attempt+1, e)
            if attempt < retries - 1:
                time.sleep(3)
    return None
