# odds_fetcher.py — Matches by full team name (what Odds API actually returns)

import logging
import requests

logger = logging.getLogger(__name__)

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE = "https://api.the-odds-api.com/v4"

# Tricode → full name (for converting NBA live data to match Odds API names)
ABBREV_TO_FULL = {
    "ATL": "Atlanta Hawks",        "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",        "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",        "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",     "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",      "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",      "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers", "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",      "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans", "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder","ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",   "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers","SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",            "WAS": "Washington Wizards",
}


def fetch_odds_for_games(games: list) -> dict:
    """
    Fetch NBA odds and map to game IDs by matching full team names.
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
        if not isinstance(odds_data, list):
            logger.error(f"Odds API unexpected response: {odds_data}")
            return {}
        logger.info(f"Odds API returned {len(odds_data)} events")
    except Exception as e:
        logger.error(f"Odds fetch failed: {e}")
        return {}

    # Build lookup by frozenset of both team names (order-independent)
    # This handles home/away flip between data sources
    odds_lookup = {}
    for event in odds_data:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        if home and away:
            key = frozenset([home, away])
            odds_lookup[key] = event

    event_list = [f"{e.get('away_team')} @ {e.get('home_team')}" for e in odds_data]
logger.info(f"Odds events available: {event_list}")

    result = {}
    for game in games:
        game_id = game.get("game_id")
        home_abbrev = game.get("home_team_abbrev", "")
        away_abbrev = game.get("away_team_abbrev", "")

        # Convert tricodes to full names
        home_full = ABBREV_TO_FULL.get(home_abbrev, "")
        away_full = ABBREV_TO_FULL.get(away_abbrev, "")

        if not home_full or not away_full:
            # Try building from city + name
            home_full = f"{game.get('home_team_city','')} {game.get('home_team','')}".strip()
            away_full = f"{game.get('away_team_city','')} {game.get('away_team','')}".strip()

        key = frozenset([home_full, away_full])
        event = odds_lookup.get(key)

        if not event:
            logger.warning(f"No odds for {away_abbrev} @ {home_abbrev} ({away_full} @ {home_full})")
            continue

        parsed = _parse_event(event, home_full)
        if parsed:
            result[game_id] = parsed
            logger.info(
                f"✅ Matched {away_abbrev} @ {home_abbrev}: "
                f"spread={parsed.get('spread_line')}, total={parsed.get('total_line')}, "
                f"home_odds={parsed.get('home_odds')}"
            )

    logger.info(f"Odds matched {len(result)}/{len(games)} games")
    return result


def _parse_event(event: dict, home_full: str) -> dict:
    """Extract best odds from bookmakers. home_full used to identify home team outcomes."""
    home_odds = away_odds = spread_line = spread_odds = total_line = total_odds = None

    # Preferred bookmakers in order
    preferred = ["draftkings", "fanduel", "betmgm", "williamhill_us", "bovada", "barstool"]

    def book_rank(b):
        k = b.get("key", "")
        return preferred.index(k) if k in preferred else 99

    bookmakers = sorted(event.get("bookmakers", []), key=book_rank)

    for book in bookmakers:
        for mkt in book.get("markets", []):
            key = mkt.get("key")
            outcomes = mkt.get("outcomes", [])

            if key == "h2h" and home_odds is None:
                for o in outcomes:
                    if o.get("name") == home_full:
                        home_odds = o.get("price")
                    else:
                        away_odds = o.get("price")

            elif key == "spreads" and spread_line is None:
                for o in outcomes:
                    if o.get("name") == home_full:
                        spread_line = o.get("point")
                        spread_odds = o.get("price")

            elif key == "totals" and total_line is None:
                for o in outcomes:
                    if o.get("name") == "Over":
                        total_line = o.get("point")
                        total_odds = o.get("price")

        if home_odds and spread_line is not None and total_line is not None:
            break

    if spread_line is None and total_line is None and home_odds is None:
        return {}

    return {
        "home_odds":   home_odds,
        "away_odds":   away_odds,
        "spread_line": spread_line,
        "spread_odds": spread_odds or 1.91,
        "total_line":  total_line,
        "total_odds":  total_odds or 1.91,
    }
