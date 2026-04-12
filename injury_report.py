# injury_report.py — Fetches NBA injury data from ESPN

import logging
import requests

logger = logging.getLogger(__name__)

ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"


def fetch_official_injury_report() -> dict:
    """
    Fetch injury report from ESPN.
    Returns {team_name: [{name, status, reason}]}
    """
    try:
        r = requests.get(
            ESPN_INJURIES_URL,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        data = r.json()

        injuries_by_team = {}
        for team_entry in data:
            team = team_entry.get("team", {})
            team_name = team.get("displayName", "")
            team_abbrev = team.get("abbreviation", "")
            injuries = []

            for injury in team_entry.get("injuries", []):
                athlete = injury.get("athlete", {})
                name = athlete.get("displayName", "")
                status = injury.get("status", "Questionable")
                # Normalise status
                status_lower = status.lower()
                if "out" in status_lower:
                    status = "Out"
                elif "day-to-day" in status_lower or "questionable" in status_lower:
                    status = "Questionable"
                elif "probable" in status_lower:
                    status = "Probable"
                else:
                    status = "Questionable"

                reason = injury.get("type", {}).get("description", "")
                injuries.append({
                    "name":   name,
                    "status": status,
                    "reason": reason,
                })

            if injuries:
                injuries_by_team[team_name] = injuries
                if team_abbrev:
                    injuries_by_team[team_abbrev] = injuries

        logger.info("Injuries fetched: %d teams with injuries", len(injuries_by_team) // 2)
        return injuries_by_team

    except Exception as e:
        logger.error("Injury report fetch failed: %s", e)
        return {}


def get_injury_impact_score(team_injuries: list, team_players: list) -> float:
    """
    Calculate how impactful team injuries are based on usage rates of injured players.
    Returns a score 0.0-1.0 where higher = more impacted.
    """
    if not team_injuries or not team_players:
        return 0.0

    # Build usage map from team roster
    usage_map = {p["name"].lower(): p.get("usage_rate", 0) for p in team_players}

    total_impact = 0.0
    for injury in team_injuries:
        if injury.get("status") == "Out":
            name = injury.get("name", "").lower()
            usage = usage_map.get(name, 0.12)  # Default to 12% if unknown
            total_impact += usage

    return min(total_impact, 1.0)
