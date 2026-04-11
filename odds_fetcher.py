# odds_fetcher.py — Fixed team name matching

import logging
import requests

logger = logging.getLogger(__name__)

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE = "https://api.the-odds-api.com/v4"

# Full name → tricode
FULL_TO_ABBREV = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}

# Build reverse maps for flexible matching
ABBREV_TO_FULL = {v: k for k, v in FULL_TO_ABBREV.items()}

# Last word of team name → abbrev (e.g. "Celtics" → "BOS")
NICKNAME_TO_ABBREV = {
    full.split()[-1]: abbrev for full, abbrev in FULL_TO_ABBREV.items()
}
# Special cases
NICKNAME_TO_ABBREV.update({
    "76ers": "PHI", "Blazers": "POR", "Thunder": "OKC",
    "Timberwolves": "MIN", "Pelicans": "NOP",
})


def _to_abbrev(name: str) -> str:
    """Convert any team name format to 3-letter abbrev."""
    if not name:
        return ""
    name = name.strip()
    # Direct full name match
    if name in FULL_TO_ABBREV:
        return FULL_TO_ABBREV[name]
    # Already an abbrev
    if name.upper() in ABBREV_TO_FULL:
        return name.upper()
    # Last word (nickname)
    last = name.split()[-1]
    if last in NICKNAME_TO_ABBREV:
        return NICKNAME_TO_ABBREV[last]
    # Partial match
    for full, abbrev in FULL_TO_ABBREV.items():
        if name.lower() in full.lower() or full.lower() in name.lower():
            return abbrev
    # Fallback: first 3 letters uppercased
    return name[:3].upper()


def fetch_odds_for_games(games: list) -> dict:
    """
    Fetch NBA odds and map to game IDs.
    Returns {game_id: {spread_line, spread_odds, total_line, total_odds, home_odds, away_odds}}
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/basketball_nba/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "decimal",
            },
            timeout=15,
        )
        r.raise_for_status()
        odds_data = r.json()
        logger.info(f"Odds API returned {len(odds_data)} events")
    except Exception as e:
        logger.error(f"Odds fetch failed: {e}")
        return {}

    # Build lookup: (away_abbrev, home_abbrev) → odds event
    odds_lookup = {}
    for event in odds_data:
        h = _to_abbrev(event.get("home_team", ""))
        a = _to_abbrev(event.get("away_team", ""))
        if h and a:
            odds_lookup[(a, h)] = event

    logger.info(f"Odds lookup keys: {list(odds_lookup.keys())[:5]}...")

    # Match to today's games
    result = {}
    for game in games:
        h = game.get("home_team_abbrev", "")
        a = game.get("away_team_abbrev", "")
        game_id = game.get("game_id")

        event = odds_lookup.get((a, h))

        # Try alternate abbrev forms if no match
        if not event:
            # Try matching by city+name
            home_full = f"{game.get('home_team_city','')} {game.get('home_team','')}".strip()
            away_full = f"{game.get('away_team_city','')} {game.get('away_team','')}".strip()
            h2 = _to_abbrev(home_full)
            a2 = _to_abbrev(away_full)
            event = odds_lookup.get((a2, h2))

        if not event:
            logger.warning(f"No odds match for {a} @ {h}")
            continue

        parsed = _parse_event(event)
        if parsed:
            result[game_id] = parsed
            logger.info(f"Matched odds for {a} @ {h}: spread={parsed.get('spread_line')}, total={parsed.get('total_line')}")

    logger.info(f"Matched odds for {len(result)}/{len(games)} games")
    return result


def _parse_event(event: dict) -> dict:
    """Extract best available odds from bookmakers."""
    home_odds = away_odds = spread_line = spread_odds = total_line = total_odds = None

    # Preferred books in order
    preferred = ["draftkings", "fanduel", "betmgm", "williamhill_us", "bovada"]
    bookmakers = event.get("bookmakers", [])

    # Sort by preference
    def book_priority(b):
        key = b.get("key", "")
        return preferred.index(key) if key in preferred else 99

    bookmakers = sorted(bookmakers, key=book_priority)

    for book in bookmakers:
        for mkt in book.get("markets", []):
            key = mkt.get("key")
            outcomes = mkt.get("outcomes", [])

            if key == "h2h" and home_odds is None:
                for o in outcomes:
                    if o["name"] == event["home_team"]:
                        home_odds = o["price"]
                    elif o["name"] == event["away_team"]:
                        away_odds = o["price"]

            elif key == "spreads" and spread_line is None:
                for o in outcomes:
                    if o["name"] == event["home_team"]:
                        spread_line = o.get("point")
                        spread_odds = o.get("price")

            elif key == "totals" and total_line is None:
                for o in outcomes:
                    if o.get("name") == "Over":
                        total_line = o.get("point")
                        total_odds = o.get("price")

        # Stop once we have everything from one book
        if home_odds and spread_line is not None and total_line is not None:
            break

    if not any([home_odds, spread_line, total_line]):
        return {}

    return {
        "home_odds":   home_odds,
        "away_odds":   away_odds,
        "spread_line": spread_line,
        "spread_odds": spread_odds or 1.91,
        "total_line":  total_line,
        "total_odds":  total_odds or 1.91,
    }
