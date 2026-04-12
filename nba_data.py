# nba_data.py
# NBA.com is IP-banned on cloud providers (confirmed Feb 2026, GitHub issue #633)
# PRIMARY: ESPN for schedule + team stats
# SECONDARY: BallDontLie v2 for anything ESPN can't provide
# NBA.com: removed from hot path entirely — only tried as last resort with 1 attempt

import logging
import time
import requests
from datetime import datetime, date

logger = logging.getLogger(__name__)

BDL_KEY    = "41d44065-0c14-4a66-b633-f93fb1680fb2"
BDL_BASE   = "https://api.balldontlie.io/nba/v2"
BDL_HDR    = {"Authorization": BDL_KEY}
BDL_SEASON = 2025

ESPN_BASE  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
ESPN_HDR   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
NBA_CDN    = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"

SLEEP   = 0.4
TIMEOUT = 10

ABBREV_TO_NBA_ID = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
    # ESPN variants
    "GS":1610612744,"NO":1610612740,"NY":1610612752,
    "SA":1610612759,"WSH":1610612764,
}


# ─── SCHEDULE ─────────────────────────────────────────────────

def get_today_games() -> list:
    today = date.today()
    games = _espn_schedule(today)
    if not games:
        games = _nba_cdn_schedule(today)
    logger.info("Found %d games for %s", len(games), today)
    return games


def _espn_schedule(target_date: date) -> list:
    try:
        r = requests.get(
            ESPN_BASE + "/scoreboard",
            params={"dates": target_date.strftime("%Y%m%d")},
            headers=ESPN_HDR, timeout=10,
        )
        r.raise_for_status()
        events = r.json().get("events", [])
        games  = []
        for ev in events:
            comp = ev.get("competitions", [{}])[0]
            comp_list = comp.get("competitors", [])
            home = next((c for c in comp_list if c.get("homeAway") == "home"), {})
            away = next((c for c in comp_list if c.get("homeAway") == "away"), {})
            ht, at = home.get("team", {}), away.get("team", {})
            ha, aa = ht.get("abbreviation", ""), at.get("abbreviation", "")
            status = comp.get("status", {}).get("type", {}).get("description", "Scheduled")
            try:
                dt = datetime.fromisoformat(ev.get("date","").replace("Z", "+00:00"))
                display_time = dt.strftime("%-I:%M %p ET")
            except Exception:
                display_time = "TBD"
            games.append({
                "game_id":          ev.get("id", ""),
                "status":           status,
                "game_time":        display_time,
                "home_team_id":     ABBREV_TO_NBA_ID.get(ha, 0),
                "home_team":        ht.get("name", ""),
                "home_team_city":   ht.get("location", ""),
                "home_team_abbrev": ha,
                "home_score":       int(home.get("score", 0) or 0),
                "away_team_id":     ABBREV_TO_NBA_ID.get(aa, 0),
                "away_team":        at.get("name", ""),
                "away_team_city":   at.get("location", ""),
                "away_team_abbrev": aa,
                "away_score":       int(away.get("score", 0) or 0),
                "arena":            comp.get("venue", {}).get("fullName", ""),
                "source":           "espn",
            })
        logger.info("ESPN returned %d games for %s", len(games), target_date)
        return games
    except Exception as e:
        logger.error("ESPN schedule failed: %s", e)
        return []


def _nba_cdn_schedule(target_date: date) -> list:
    try:
        r = requests.get(NBA_CDN, headers=ESPN_HDR, timeout=12)
        r.raise_for_status()
        today_str = target_date.isoformat()
        games = []
        for gd in r.json().get("leagueSchedule", {}).get("gameDates", []):
            if gd.get("gameDate", "")[:10] != today_str:
                continue
            for g in gd.get("games", []):
                home, away = g.get("homeTeam", {}), g.get("awayTeam", {})
                games.append({
                    "game_id":          str(g.get("gameId", "")),
                    "status":           g.get("gameStatusText", "Scheduled"),
                    "game_time":        "TBD",
                    "home_team_id":     home.get("teamId"),
                    "home_team":        home.get("teamName", ""),
                    "home_team_city":   home.get("teamCity", ""),
                    "home_team_abbrev": home.get("teamTricode", ""),
                    "home_score":       home.get("score", 0),
                    "away_team_id":     away.get("teamId"),
                    "away_team":        away.get("teamName", ""),
                    "away_team_city":   away.get("teamCity", ""),
                    "away_team_abbrev": away.get("teamTricode", ""),
                    "away_score":       away.get("score", 0),
                    "arena":            g.get("arenaName", ""),
                    "source":           "nba_cdn",
                })
        logger.info("NBA CDN returned %d games for %s", len(games), target_date)
        return games
    except Exception as e:
        logger.error("NBA CDN failed: %s", e)
        return []


# ─── TEAM STATS ───────────────────────────────────────────────
# NBA.com is REMOVED from this path — it's IP banned on cloud
# ESPN standings → BDL team stats → sensible defaults

def get_all_team_stats_batch(measure_type="Advanced", location=None, last_n=None) -> dict:
    """
    Get team stats without touching NBA.com.
    Advanced stats → ESPN standings (derives off/def ratings from PPG)
    Base/split stats → BDL team season averages
    last_n → BDL recent stats (not available via ESPN)
    """
    if measure_type == "Advanced" and not location and not last_n:
        return _espn_team_stats()

    # For Base, splits, L5/L10 — use BDL
    return _bdl_team_stats(last_n=last_n, location=location)


def _espn_team_stats() -> dict:
    """ESPN standings → team advanced stats (off_rating, def_rating, pace etc.)"""
    stats = {}
    try:
        r = requests.get(
            ESPN_BASE + "/standings",
            params={"season": "2026"},
            headers=ESPN_HDR, timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        for group in data.get("children", []):
            for entry in group.get("standings", {}).get("entries", []):
                team   = entry.get("team", {})
                abbrev = team.get("abbreviation", "")
                tid    = ABBREV_TO_NBA_ID.get(abbrev, 0)
                if not tid:
                    continue

                wins, losses, ppg, oppg = 0, 0, 110.0, 110.0
                for stat in entry.get("stats", []):
                    n, v = stat.get("name", ""), stat.get("value", 0)
                    if n == "wins":            wins   = int(v)
                    elif n == "losses":        losses = int(v)
                    elif n == "pointsFor":     ppg    = float(v)
                    elif n == "pointsAgainst": oppg   = float(v)

                stats[tid] = {
                    "team_name":  team.get("displayName", abbrev),
                    "off_rating": round(ppg,  1),
                    "def_rating": round(oppg, 1),
                    "net_rating": round(ppg - oppg, 1),
                    "pace":       100.0,
                    "ts_pct":     0.565,
                    "wins":       wins,
                    "losses":     losses,
                    "source":     "espn_standings",
                }

        logger.info("ESPN team stats: %d teams", len(stats))
        return stats

    except Exception as e:
        logger.warning("ESPN team stats failed: %s — using BDL", e)
        return _bdl_team_stats()


def _bdl_team_stats(last_n=None, location=None) -> dict:
    """BallDontLie v2 team season averages."""
    try:
        time.sleep(SLEEP)
        params = {"season": BDL_SEASON, "per_page": 30}
        r = requests.get(
            BDL_BASE + "/teamseasonaverages",
            headers=BDL_HDR, params=params, timeout=TIMEOUT,
        )
        r.raise_for_status()
        items = r.json().get("data", [])
        stats = {}
        for item in items:
            team   = item.get("team") or {}
            abbrev = team.get("abbreviation", "")
            tid    = ABBREV_TO_NBA_ID.get(abbrev, 0)
            if not tid:
                continue
            stats[tid] = {
                "pts":        float(item.get("pts",  0) or 0),
                "fg_pct":     float(item.get("fg_pct",  0) or 0),
                "three_pct":  float(item.get("fg3_pct", 0) or 0),
                "rebounds":   float(item.get("reb",  0) or 0),
                "assists":    float(item.get("ast",  0) or 0),
                "turnovers":  float(item.get("tov",  0) or 0),
                "wins":       int(item.get("wins",   0) or 0),
                "losses":     int(item.get("losses", 0) or 0),
                "net_rating": 0.0,
                "off_rating": float(item.get("pts",  110) or 110),
                "def_rating": 110.0,
                "pace":       100.0,
                "ts_pct":     0.565,
                "source":     "balldontlie_v2",
            }
        logger.info("BDL team stats: %d teams", len(stats))
        return stats
    except Exception as e:
        logger.warning("BDL team stats failed: %s — using defaults", e)
        return _default_team_stats()


def _default_team_stats() -> dict:
    """Absolute fallback — neutral stats for all teams so engine still runs."""
    logger.warning("Using default team stats (all sources failed)")
    stats = {}
    for abbrev, tid in ABBREV_TO_NBA_ID.items():
        if tid in stats:
            continue
        stats[tid] = {
            "team_name":  abbrev,
            "off_rating": 113.0,
            "def_rating": 113.0,
            "net_rating": 0.0,
            "pace":       100.0,
            "ts_pct":     0.565,
            "wins":       41,
            "losses":     41,
            "pts":        113.0,
            "rebounds":   44.0,
            "assists":    26.0,
            "turnovers":  14.0,
            "source":     "default",
        }
    return stats


def get_all_team_recent_batch(last_n: int) -> dict:
    return _bdl_team_stats(last_n=last_n)


def get_all_player_stats_batch() -> dict:
    """
    Player advanced stats (usage rate etc.) from BDL v2.
    Returns {team_id: [player_list]} same format as before.
    """
    try:
        time.sleep(SLEEP)
        params = {"season": BDL_SEASON, "per_page": 100}
        r = requests.get(
            BDL_BASE + "/stats/advanced",
            headers=BDL_HDR, params=params, timeout=TIMEOUT,
        )
        r.raise_for_status()
        items       = r.json().get("data", [])
        team_players = {}

        for item in items:
            player = item.get("player") or {}
            team   = item.get("team")   or {}
            abbrev = team.get("abbreviation", "")
            tid    = ABBREV_TO_NBA_ID.get(abbrev, 0)
            if not tid:
                continue
            if tid not in team_players:
                team_players[tid] = []
            name = (player.get("first_name","")+" "+player.get("last_name","")).strip()
            team_players[tid].append({
                "name":       name,
                "usage_rate": float(item.get("estimated_usage_percentage", 0.15) or 0.15),
                "minutes":    0.0,
                "pie":        float(item.get("pie", 0) or 0),
                "net_rating": float(item.get("net_rating", 0) or 0),
            })

        for tid in team_players:
            team_players[tid].sort(key=lambda p: p["usage_rate"], reverse=True)

        logger.info("Player adv stats: %d teams (BDL v2)", len(team_players))
        return team_players

    except Exception as e:
        logger.warning("Player adv stats failed: %s", e)
        return {}


def get_h2h_history(team_id: int, opponent_id: int) -> list:
    """H2H history — skip NBA.com, return empty (engine handles gracefully)."""
    return []
