# injury_report.py — Official NBA Injury Report scraper
# Pulls from the official PDF: official.nba.com/nba-injury-report-2025-26-season/
# Also falls back to ESPN public API

import logging
import re
import requests
from datetime import date
from typing import Optional
import io

logger = logging.getLogger(__name__)

# Official NBA injury report PDF index page
NBA_INJURY_PDF_INDEX = "https://official.nba.com/nba-injury-report-2025-26-season/"
NBA_INJURY_PDF_BASE = "https://official.nba.com"

# ESPN fallback
ESPN_INJURIES = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

STATUS_MAP = {
    "Out": "Out",
    "Doubtful": "Doubtful",
    "Questionable": "Questionable",
    "Probable": "Probable",
    "Available": "Available",
    "GTD": "Questionable",  # Game-Time Decision
}


def fetch_official_injury_report() -> dict:
    """
    Attempt to fetch and parse the official NBA injury report PDF.
    Falls back to ESPN if PDF parsing fails.

    Returns dict: { "Team Name": [ { name, status, reason, current_status } ] }
    """
    try:
        return _fetch_pdf_report()
    except Exception as e:
        logger.warning(f"PDF injury report failed ({e}), falling back to ESPN")
        return _fetch_espn_injuries()


def _fetch_pdf_report() -> dict:
    """
    Fetch the latest injury report PDF from official.nba.com and parse it.
    The PDF lists: Team | Player | Current Status | Reason
    """
    # Step 1: Find the latest PDF link on the page
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NBAEdge/2.0)",
        "Accept": "text/html,application/xhtml+xml",
    }
    resp = requests.get(NBA_INJURY_PDF_INDEX, headers=headers, timeout=15)
    resp.raise_for_status()

    # Find PDF links in the page HTML
    pdf_links = re.findall(r'href="([^"]+\.pdf)"', resp.text)
    if not pdf_links:
        # Try finding links with different format
        pdf_links = re.findall(r'"(https?://[^"]+Injury[^"]+\.pdf)"', resp.text, re.IGNORECASE)

    if not pdf_links:
        raise ValueError("No PDF links found on NBA injury report page")

    # Get the most recent PDF (usually the last one listed or first)
    pdf_url = pdf_links[0]
    if not pdf_url.startswith("http"):
        pdf_url = NBA_INJURY_PDF_BASE + pdf_url

    logger.info(f"Fetching injury PDF: {pdf_url}")

    # Step 2: Download PDF
    pdf_resp = requests.get(pdf_url, headers=headers, timeout=20)
    pdf_resp.raise_for_status()

    # Step 3: Parse PDF with pdfplumber
    import pdfplumber
    injuries = {}

    with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 4:
                        continue
                    # Expected columns: Game Date | Game Time | Matchup | Team | Player | Status | Reason
                    # OR: Team | Player | Current Status | Reason (older format)
                    row = [str(c).strip() if c else "" for c in row]

                    # Skip header rows
                    if any(h in row[0].lower() for h in ["team", "game", "date"]):
                        continue

                    # Try to identify team + player + status columns
                    team = _clean_team_name(row[0])
                    if not team:
                        continue

                    player = row[1] if len(row) > 1 else ""
                    status_raw = row[2] if len(row) > 2 else ""
                    reason = row[3] if len(row) > 3 else ""

                    # Sometimes format is: GameDate | GameTime | Matchup | Team | Player | Status | Reason
                    if len(row) >= 7 and _looks_like_date(row[0]):
                        team = _clean_team_name(row[3])
                        player = row[4] if len(row) > 4 else ""
                        status_raw = row[5] if len(row) > 5 else ""
                        reason = row[6] if len(row) > 6 else ""

                    if not player or not status_raw:
                        continue

                    status = STATUS_MAP.get(status_raw, status_raw)

                    if team not in injuries:
                        injuries[team] = []

                    injuries[team].append({
                        "name": player,
                        "status": status,
                        "reason": reason,
                        "source": "NBA Official",
                    })

    logger.info(f"Parsed {sum(len(v) for v in injuries.values())} injury entries from PDF")
    return injuries


def _fetch_espn_injuries() -> dict:
    """ESPN public injuries API fallback."""
    try:
        resp = requests.get(ESPN_INJURIES, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        injuries = {}
        for team_data in data.get("injuries", []):
            team_name = team_data.get("team", {}).get("displayName", "")
            if not team_name:
                continue
            injuries[team_name] = []
            for inj in team_data.get("injuries", []):
                injuries[team_name].append({
                    "name": inj.get("athlete", {}).get("displayName", "Unknown"),
                    "status": inj.get("status", "Questionable"),
                    "reason": inj.get("details", {}).get("detail", ""),
                    "position": inj.get("athlete", {}).get("position", {}).get("abbreviation", ""),
                    "source": "ESPN",
                })
        logger.info(f"ESPN injuries: {sum(len(v) for v in injuries.values())} entries")
        return injuries
    except Exception as e:
        logger.error(f"ESPN injuries also failed: {e}")
        return {}


def _clean_team_name(raw: str) -> Optional[str]:
    """Attempt to clean up a team name string from the PDF."""
    if not raw or len(raw) < 3:
        return None
    # Remove common noise
    clean = raw.strip().replace("\n", " ").replace("  ", " ")
    # Skip obviously wrong values
    if any(skip in clean.lower() for skip in ["status", "player", "game", "date", "time", "reason"]):
        return None
    return clean


def _looks_like_date(val: str) -> bool:
    """Check if a string looks like a date."""
    return bool(re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", val.strip()))


def get_injury_impact_score(injuries: list, team_players: list) -> dict:
    """
    Calculate how much a team's injuries affect their scoring.
    
    Weights injury impact by the player's usage rate and role.
    Returns:
      - total_usage_lost: sum of usage rates of Out players
      - key_player_out: bool (starter-caliber player out)
      - injury_severity: 0-10 score
    """
    out_players = {p["name"].lower() for p in injuries if p["status"] == "Out"}
    ques_players = {p["name"].lower() for p in injuries if p["status"] in ("Questionable", "Doubtful", "GTD")}

    total_usage_lost = 0.0
    key_player_out = False
    severity = 0

    for player in team_players:
        name_lower = player["name"].lower()
        usage = player.get("usage_rate", 0)

        if name_lower in out_players:
            total_usage_lost += usage
            severity += usage * 10  # High-usage player out = high severity
            if usage >= 0.20:  # 20%+ usage = key player
                key_player_out = True

        elif name_lower in ques_players:
            total_usage_lost += usage * 0.3  # 30% impact if questionable
            severity += usage * 3

    return {
        "total_usage_lost": round(total_usage_lost, 3),
        "key_player_out": key_player_out,
        "injury_severity": min(10, round(severity, 1)),
        "out_count": len(out_players),
        "ques_count": len(ques_players),
    }
