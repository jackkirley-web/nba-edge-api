# injury_report.py — ESPN injuries (reliable) + NBA official page scrape
# Removed PDF parsing — the PDF link on NBA.com points to rulebook not injury report

import logging
import requests
import re
from datetime import date

logger = logging.getLogger(__name__)

ESPN_INJURIES = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
NBA_INJURY_PAGE = "https://official.nba.com/nba-injury-report-2025-26-season/"

STATUS_MAP = {
    "Out": "Out",
    "Doubtful": "Doubtful", 
    "Questionable": "Questionable",
    "Probable": "Probable",
    "Available": "Available",
    "Game Time Decision": "Questionable",
}


def fetch_official_injury_report() -> dict:
    """
    Fetch injury data. Tries ESPN first (most reliable),
    then attempts to scrape NBA official page as supplement.
    """
    injuries = _fetch_espn_injuries()
    
    # Try to supplement with NBA official page (HTML scrape, no PDF)
    try:
        nba_injuries = _scrape_nba_injury_page()
        # Merge — NBA official takes priority for status
        for team, players in nba_injuries.items():
            if team not in injuries:
                injuries[team] = players
            else:
                # Add any players not already in ESPN data
                existing_names = {p["name"].lower() for p in injuries[team]}
                for p in players:
                    if p["name"].lower() not in existing_names:
                        injuries[team].append(p)
    except Exception as e:
        logger.warning(f"NBA official page scrape failed (ESPN data used): {e}")

    return injuries


def _fetch_espn_injuries() -> dict:
    """ESPN public injuries API — most reliable free source."""
    try:
        resp = requests.get(ESPN_INJURIES, timeout=10,
                           headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        data = resp.json()
        injuries = {}
        for team_data in data.get("injuries", []):
            team_name = team_data.get("team", {}).get("displayName", "")
            if not team_name:
                continue
            injuries[team_name] = []
            for inj in team_data.get("injuries", []):
                status_raw = inj.get("status", "Questionable")
                injuries[team_name].append({
                    "name": inj.get("athlete", {}).get("displayName", "Unknown"),
                    "status": STATUS_MAP.get(status_raw, status_raw),
                    "reason": inj.get("details", {}).get("detail", ""),
                    "position": inj.get("athlete", {}).get("position", {}).get("abbreviation", ""),
                    "source": "ESPN",
                })
        logger.info(f"ESPN injuries: {sum(len(v) for v in injuries.values())} players across {len(injuries)} teams")
        return injuries
    except Exception as e:
        logger.error(f"ESPN injuries failed: {e}")
        return {}


def _scrape_nba_injury_page() -> dict:
    """
    Scrape the NBA official injury report HTML page.
    The page has a table with: Team | Player | Current Status | Reason
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAEdge/2.0)"}
    resp = requests.get(NBA_INJURY_PAGE, headers=headers, timeout=15)
    resp.raise_for_status()
    
    injuries = {}
    html = resp.text
    
    # Find table rows in the injury report
    # NBA injury page uses a specific table structure
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        # Clean HTML tags from cells
        cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
        cells = [c for c in cells if c]  # Remove empty
        
        if len(cells) >= 3:
            # Try to identify team/player/status pattern
            # Format varies but usually: Date | Time | Matchup | Team | Player | Status | Reason
            # or: Team | Player | Status | Reason
            for i, cell in enumerate(cells):
                if _is_nba_team(cell) and i + 2 < len(cells):
                    team = cell
                    player = cells[i+1] if i+1 < len(cells) else ""
                    status_raw = cells[i+2] if i+2 < len(cells) else ""
                    reason = cells[i+3] if i+3 < len(cells) else ""
                    
                    if player and status_raw and status_raw in STATUS_MAP:
                        if team not in injuries:
                            injuries[team] = []
                        injuries[team].append({
                            "name": player,
                            "status": STATUS_MAP[status_raw],
                            "reason": reason,
                            "source": "NBA Official",
                        })
                    break
    
    logger.info(f"NBA official page: {sum(len(v) for v in injuries.values())} entries")
    return injuries


NBA_TEAMS = {
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
    "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
    "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards",
}

def _is_nba_team(name: str) -> bool:
    return name in NBA_TEAMS


def get_injury_impact_score(injuries: list, team_players: list) -> dict:
    """Calculate usage-weighted injury impact score."""
    out_players = {p["name"].lower() for p in injuries if p["status"] == "Out"}
    ques_players = {p["name"].lower() for p in injuries if p["status"] in ("Questionable", "Doubtful")}

    total_usage_lost = 0.0
    key_player_out = False
    severity = 0

    for player in team_players:
        name_lower = player.get("name", "").lower()
        usage = player.get("usage_rate", 0)

        if name_lower in out_players:
            total_usage_lost += usage
            severity += usage * 10
            if usage >= 0.20:
                key_player_out = True
        elif name_lower in ques_players:
            total_usage_lost += usage * 0.3
            severity += usage * 3

    return {
        "total_usage_lost": round(total_usage_lost, 3),
        "key_player_out": key_player_out,
        "injury_severity": min(10, round(severity, 1)),
        "out_count": len(out_players),
        "ques_count": len(ques_players),
    }
