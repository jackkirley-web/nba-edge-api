# player_logs.py
# Player base stats + player game logs with the same request discipline as nba_data.py.

import logging
from collections import defaultdict

from nba_data import _resultset_to_rows, _season_string, _stats

logger = logging.getLogger(__name__)


def get_all_player_base_stats():
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
        "VsConference": "",
        "VsDivision": "",
        "Weight": "",
    }

    try:
        rows = _resultset_to_rows(_stats("leaguedashplayerstats", params))
        out = {}
        for r in rows:
            pid = int(r.get("PLAYER_ID", 0) or 0)
            tid = int(r.get("TEAM_ID", 0) or 0)
            if not pid or not tid:
                continue

            out[pid] = {
                "player_id": pid,
                "team_id": tid,
                "name": r.get("PLAYER_NAME"),
                "team_abbrev": r.get("TEAM_ABBREVIATION"),
                "mins": float(r.get("MIN", 0) or 0),
                "pts": float(r.get("PTS", 0) or 0),
                "reb": float(r.get("REB", 0) or 0),
                "ast": float(r.get("AST", 0) or 0),
                "3pm": float(r.get("FG3M", 0) or 0),
                "stl": float(r.get("STL", 0) or 0),
                "blk": float(r.get("BLK", 0) or 0),
                "tov": float(r.get("TOV", 0) or 0),
                "fga": float(r.get("FGA", 0) or 0),
                "fgm": float(r.get("FGM", 0) or 0),
                "fta": float(r.get("FTA", 0) or 0),
                "ftm": float(r.get("FTM", 0) or 0),
                "position": "G",
            }
        return out
    except Exception as e:
        logger.warning("Direct player stats failed: %s", e)
        return {}


def get_player_game_logs_batch(player_ids, last_n=15):
    logs = defaultdict(list)

    for pid in player_ids:
        params = {
            "DateFrom": "",
            "DateTo": "",
            "GameSegment": "",
            "LastNGames": last_n,
            "LeagueID": "00",
            "Location": "",
            "MeasureType": "Base",
            "Month": 0,
            "OpposingTeamID": 0,
            "Outcome": "",
            "PORound": 0,
            "PerMode": "PerGame",
            "Period": 0,
            "PlayerID": pid,
            "Season": _season_string(),
            "SeasonSegment": "",
            "SeasonType": "Regular Season",
            "ShotClockRange": "",
            "TeamID": 0,
            "VsConference": "",
            "VsDivision": "",
        }

        try:
            rows = _resultset_to_rows(_stats("playergamelogs", params))
            for r in rows[:last_n]:
                logs[pid].append({
                    "game_id": r.get("GAME_ID"),
                    "game_date": r.get("GAME_DATE"),
                    "matchup": r.get("MATCHUP"),
                    "wl": r.get("WL"),
                    "mins": float(r.get("MIN", 0) or 0),
                    "pts": float(r.get("PTS", 0) or 0),
                    "reb": float(r.get("REB", 0) or 0),
                    "ast": float(r.get("AST", 0) or 0),
                    "3pm": float(r.get("FG3M", 0) or 0),
                    "stl": float(r.get("STL", 0) or 0),
                    "blk": float(r.get("BLK", 0) or 0),
                    "tov": float(r.get("TOV", 0) or 0),
                    "fga": float(r.get("FGA", 0) or 0),
                    "fgm": float(r.get("FGM", 0) or 0),
                    "fta": float(r.get("FTA", 0) or 0),
                    "ftm": float(r.get("FTM", 0) or 0),
                })
        except Exception as e:
            logger.warning("PlayerGameLogs failed for %s: %s", pid, e)
            logs[pid] = []

    return dict(logs)
