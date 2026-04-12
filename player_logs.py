# player_logs.py
# BallDontLie v2 is PRIMARY — reliable, not cloud-blocked
# ESPN is secondary — free, no key, very reliable
# NBA.com is tertiary — IP banned on cloud, only tried as last resort

import logging
import time
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

BDL_KEY    = "41d44065-0c14-4a66-b633-f93fb1680fb2"
BDL_BASE   = "https://api.balldontlie.io/nba/v2"
BDL_HDR    = {"Authorization": BDL_KEY}
BDL_SEASON = 2025

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
ESPN_HDR  = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

NBA_COM_BASE = "https://stats.nba.com/stats"
NBA_SEASON   = "2025-26"
NBA_HEADERS  = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

SLEEP   = 0.4
TIMEOUT = 12

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

ESPN_TO_NBA_ID = {**BDL_TO_NBA_ID,
    "GS":1610612744,"NO":1610612740,"NY":1610612752,
    "SA":1610612759,"WSH":1610612764,
}

_bdl_player_cache = {}


def _bdl_get(path, params=None, retries=2):
    for attempt in range(retries):
        try:
            time.sleep(SLEEP)
            r = requests.get(
                BDL_BASE + path,
                headers=BDL_HDR,
                params=params or {},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("BDL call failed (attempt %d) %s: %s", attempt+1, path, e)
            if attempt < retries - 1:
                time.sleep(2)
    return {}


def _load_bdl_players():
    global _bdl_player_cache
    if _bdl_player_cache:
        return _bdl_player_cache

    players = {}
    cursor  = None
    while True:
        params = {"per_page": 100}
        if cursor:
            params["cursor"] = cursor
        data = _bdl_get("/players/active", params)
        for p in data.get("data", []):
            team   = p.get("team") or {}
            abbrev = team.get("abbreviation", "")
            players[p["id"]] = {
                "bdl_id":   p["id"],
                "name":     (p.get("first_name","")+" "+p.get("last_name","")).strip(),
                "team_id":  BDL_TO_NBA_ID.get(abbrev, 0),
                "abbrev":   abbrev,
                "position": p.get("position","G") or "G",
            }
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    _bdl_player_cache = players
    logger.info("BDL player cache loaded: %d players", len(players))
    return players


def get_all_player_base_stats() -> dict:
    """
    Season averages for all players.
    Priority: BallDontLie v2 -> ESPN -> NBA.com
    """
    players = _bdl_season_averages()
    if players:
        logger.info("Player base stats: %d players (BallDontLie v2)", len(players))
        return players

    logger.warning("BDL unavailable -- trying ESPN")
    players = _espn_player_stats()
    if players:
        logger.info("Player base stats: %d players (ESPN)", len(players))
        return players

    logger.warning("ESPN unavailable -- trying NBA.com (likely blocked on cloud)")
    players = _nbacom_player_stats()
    if players:
        logger.info("Player base stats: %d players (NBA.com)", len(players))
        return players

    logger.error("All player stats sources failed")
    return {}


def _bdl_season_averages() -> dict:
    bdl_players = _load_bdl_players()
    if not bdl_players:
        return {}

    players = {}
    cursor  = None

    while True:
        params = {"season": BDL_SEASON, "per_page": 100}
        if cursor:
            params["cursor"] = cursor

        data  = _bdl_get("/seasonaverages/general", params)
        items = data.get("data", [])

        for item in items:
            player  = item.get("player") or {}
            bdl_pid = player.get("id") or item.get("player_id")
            if not bdl_pid:
                continue

            meta   = bdl_players.get(bdl_pid, {})
            name   = meta.get("name") or (
                (player.get("first_name","")+" "+player.get("last_name","")).strip()
            )
            abbrev = meta.get("abbrev", "")
            tid    = meta.get("team_id", 0)
            pos    = meta.get("position","G")

            mins_raw = str(item.get("min","0") or "0")
            try:
                if ":" in mins_raw:
                    parts = mins_raw.split(":")
                    mins  = float(parts[0]) + float(parts[1])/60
                else:
                    mins = float(mins_raw)
            except Exception:
                mins = 0.0

            players[bdl_pid] = {
                "player_id":   bdl_pid,
                "bdl_id":      bdl_pid,
                "name":        name,
                "team_id":     tid,
                "team_abbrev": abbrev,
                "position":    pos,
                "mins":        round(mins, 1),
                "pts":         float(item.get("pts",  0) or 0),
                "reb":         float(item.get("reb",  0) or 0),
                "ast":         float(item.get("ast",  0) or 0),
                "3pm":         float(item.get("fg3m", 0) or 0),
                "stl":         float(item.get("stl",  0) or 0),
                "blk":         float(item.get("blk",  0) or 0),
                "tov":         float(item.get("turnover", 0) or 0),
                "gp":          int(item.get("games_played", 0) or 0),
                "source":      "balldontlie_v2",
            }

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    return players


def _espn_player_stats() -> dict:
    players = {}
    try:
        r = requests.get(ESPN_BASE+"/teams", headers=ESPN_HDR, timeout=10)
        r.raise_for_status()
        sports = r.json().get("sports",[])
        teams  = sports[0].get("leagues",[{}])[0].get("teams",[]) if sports else []
    except Exception as e:
        logger.warning("ESPN teams failed: %s", e)
        return {}

    for te in teams:
        team    = te.get("team",{})
        eid     = team.get("id","")
        abbrev  = team.get("abbreviation","")
        team_id = ESPN_TO_NBA_ID.get(abbrev, 0)
        try:
            r = requests.get(f"{ESPN_BASE}/teams/{eid}/roster",
                             headers=ESPN_HDR, timeout=8)
            r.raise_for_status()
            for athlete in r.json().get("athletes",[]):
                espn_pid = str(athlete.get("id",""))
                name     = athlete.get("fullName","")
                pos      = athlete.get("position",{}).get("abbreviation","G") or "G"
                try:
                    sr = requests.get(f"{ESPN_BASE}/athletes/{espn_pid}/stats",
                                      headers=ESPN_HDR, timeout=6)
                    sr.raise_for_status()
                    avgs = {}
                    for cat in sr.json().get("splits",{}).get("categories",[]):
                        if cat.get("name") == "avg":
                            for s in cat.get("stats",[]):
                                avgs[s.get("name","")] = float(s.get("value",0) or 0)
                            break
                    pid = "espn_" + espn_pid
                    players[pid] = {
                        "player_id":   pid,
                        "espn_id":     espn_pid,
                        "name":        name,
                        "team_id":     team_id,
                        "team_abbrev": abbrev,
                        "position":    pos,
                        "mins":        avgs.get("avgMinutes", 0),
                        "pts":         avgs.get("avgPoints", 0),
                        "reb":         avgs.get("avgRebounds", 0),
                        "ast":         avgs.get("avgAssists", 0),
                        "3pm":         avgs.get("avg3PointFieldGoalsMade", 0),
                        "stl":         avgs.get("avgSteals", 0),
                        "blk":         avgs.get("avgBlocks", 0),
                        "tov":         avgs.get("avgTurnovers", 0),
                        "gp":          int(avgs.get("gamesPlayed", 0)),
                        "source":      "espn",
                    }
                    time.sleep(0.2)
                except Exception:
                    continue
        except Exception:
            continue
    return players


def _nbacom_player_stats() -> dict:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            NBA_COM_BASE + "/leaguedashplayerstats",
            params={
                "Season": NBA_SEASON, "SeasonType": "Regular Season",
                "MeasureType": "Base", "PerMode": "PerGame",
                "LeagueID": "00", "LastNGames": 0, "Month": 0,
                "OpponentTeamID": 0, "PaceAdjust": "N", "Period": 0,
                "PlusMinus": "N", "Rank": "N", "TeamID": 0,
            },
            headers=NBA_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200 or not r.text.strip():
            return {}
        rs = r.json().get("resultSets", [])
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
                "source":      "nba.com",
            }
        return players
    except Exception as e:
        logger.warning("NBA.com player stats failed: %s", e)
        return {}


def get_player_game_logs_batch(player_ids: list, last_n: int = 15) -> dict:
    results = {}
    total   = len(player_ids)

    for i, player_id in enumerate(player_ids):
        if i % 20 == 0:
            logger.info("Fetching logs: %d/%d", i, total)
        logs = _fetch_logs_for_player(player_id, last_n)
        if logs:
            results[player_id] = logs

    logger.info("Got logs for %d/%d players", len(results), total)
    return results


def _fetch_logs_for_player(player_id, last_n: int) -> list:
    pid_str = str(player_id)

    if pid_str.startswith("espn_"):
        return _espn_logs(pid_str.replace("espn_", ""), last_n)

    try:
        bdl_pid = int(player_id)
        if bdl_pid > 0:
            logs = _bdl_logs(bdl_pid, last_n)
            if logs:
                return logs
            return _nbacom_logs(bdl_pid, last_n)
    except (ValueError, TypeError):
        pass

    return []


def _bdl_logs(bdl_pid: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            BDL_BASE + "/stats",
            headers=BDL_HDR,
            params={
                "player_ids[]": bdl_pid,
                "seasons[]":    BDL_SEASON,
                "per_page":     last_n,
            },
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return _parse_bdl_stats(r.json().get("data", []))
    except Exception as e:
        logger.warning("BDL logs failed for %d: %s", bdl_pid, e)
        return []


def _parse_bdl_stats(items: list) -> list:
    logs = []
    for item in items:
        game = item.get("game") or {}
        raw  = str(game.get("date",""))[:10]
        try:
            pd = datetime.strptime(raw, "%Y-%m-%d")
        except Exception:
            continue

        mins_raw = str(item.get("min","0") or "0")
        try:
            if ":" in mins_raw:
                p    = mins_raw.split(":")
                mins = float(p[0]) + float(p[1])/60
            else:
                mins = float(mins_raw)
        except Exception:
            mins = 0.0

        logs.append({
            "game_date":   raw,
            "parsed_date": pd,
            "matchup":     "",
            "is_home":     False,
            "mins":        round(mins, 1),
            "pts":         int(item.get("pts",  0) or 0),
            "reb":         int(item.get("reb",  0) or 0),
            "ast":         int(item.get("ast",  0) or 0),
            "3pm":         int(item.get("fg3m", 0) or 0),
            "stl":         int(item.get("stl",  0) or 0),
            "blk":         int(item.get("blk",  0) or 0),
            "tov":         int(item.get("turnover", 0) or 0),
            "plus_minus":  float(item.get("plus_minus", 0) or 0),
            "source":      "balldontlie_v2",
        })

    logs.sort(key=lambda x: x["parsed_date"], reverse=True)
    for log in logs:
        del log["parsed_date"]
    return logs


def _espn_logs(espn_pid: str, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            f"{ESPN_BASE}/athletes/{espn_pid}/gamelog",
            headers=ESPN_HDR, timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        events    = data.get("events", {})
        cats      = data.get("categories", [])
        col_names = [s.get("name","") for cat in cats for s in cat.get("stats",[])]

        logs = []
        for _, ev in events.items():
            if not isinstance(ev, dict):
                continue
            stats_list = ev.get("stats", [])
            game_date  = ev.get("gameDate", "")
            at_vs      = ev.get("atVs", "")
            opp        = ev.get("opponent", {}).get("abbreviation", "")
            sm         = {col_names[i]: v for i, v in enumerate(stats_list)
                          if i < len(col_names)}
            try:
                pd = datetime.strptime(game_date[:10], "%Y-%m-%d")
            except Exception:
                continue
            mins_str = str(sm.get("minutes","0:0"))
            try:
                p    = mins_str.split(":")
                mins = float(p[0]) + float(p[1])/60 if len(p)>1 else float(p[0])
            except Exception:
                mins = 0.0
            logs.append({
                "game_date":   game_date[:10],
                "parsed_date": pd,
                "matchup":     ("vs. " if at_vs=="vs" else "@ ") + opp,
                "is_home":     at_vs == "vs",
                "mins":        round(mins, 1),
                "pts":         int(sm.get("points",0) or 0),
                "reb":         int(sm.get("rebounds",0) or 0),
                "ast":         int(sm.get("assists",0) or 0),
                "3pm":         int(sm.get("threePointFieldGoalsMade",0) or 0),
                "stl":         int(sm.get("steals",0) or 0),
                "blk":         int(sm.get("blocks",0) or 0),
                "tov":         int(sm.get("turnovers",0) or 0),
                "plus_minus":  float(sm.get("plusMinus",0) or 0),
                "source":      "espn",
            })

        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs[:last_n]
    except Exception as e:
        logger.warning("ESPN logs failed %s: %s", espn_pid, e)
        return []


def _nbacom_logs(player_id: int, last_n: int) -> list:
    try:
        time.sleep(SLEEP)
        r = requests.get(
            NBA_COM_BASE + "/playergamelogs",
            params={
                "PlayerIDNullable":   player_id,
                "Season":             NBA_SEASON,
                "SeasonTypeNullable": "Regular Season",
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
                "source":      "nba.com",
            })
        logs.sort(key=lambda x: x["parsed_date"], reverse=True)
        for log in logs:
            del log["parsed_date"]
        return logs
    except Exception as e:
        logger.warning("NBA.com logs failed %d: %s", player_id, e)
        return []
