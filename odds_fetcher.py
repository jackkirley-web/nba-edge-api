# odds_fetcher.py — Robust matching by frozenset of abbreviations

import logging
import requests

logger = logging.getLogger(__name__)

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE    = "https://api.the-odds-api.com/v4"

ABBREV_TO_FULL = {
    "ATL": "Atlanta Hawks",          "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",          "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",          "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",       "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",        "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",        "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",   "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",      "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",        "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",   "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",  "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",     "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",      "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",              "WAS": "Washington Wizards",
}

FULL_TO_ABBREV = {v: k for k, v in ABBREV_TO_FULL.items()}


def fetch_odds_for_games(games: list) -> dict:
    try:
        r = requests.get(
            ODDS_BASE + "/sports/basketball_nba/odds",
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
            logger.error("Odds API bad response: %s", type(odds_data))
            return {}
        logger.info("Odds API: %d events", len(odds_data))
    except Exception as e:
        logger.error("Odds API fetch failed: %s", e)
        return {}

    # Log what Odds API has
    for ev in odds_data:
        a = ev.get("away_team", "")
        h = ev.get("home_team", "")
        logger.info("Odds event: %s @ %s", a, h)

    # Build lookup: frozenset({abbrev1, abbrev2}) -> event info
    odds_lookup = {}
    for event in odds_data:
        home_full  = event.get("home_team", "")
        away_full  = event.get("away_team", "")
        home_abbrev = FULL_TO_ABBREV.get(home_full, "")
        away_abbrev = FULL_TO_ABBREV.get(away_full, "")
        if home_abbrev and away_abbrev:
            key = frozenset([home_abbrev, away_abbrev])
            odds_lookup[key] = {
                "event":             event,
                "odds_home_abbrev":  home_abbrev,
                "odds_home_full":    home_full,
                "odds_away_full":    away_full,
            }

    result = {}
    for game in games:
        game_id    = game.get("game_id")
        nba_home   = game.get("home_team_abbrev", "")
        nba_away   = game.get("away_team_abbrev", "")

        key   = frozenset([nba_home, nba_away])
        match = odds_lookup.get(key)

        if not match:
            logger.warning("No odds for %s @ %s", nba_away, nba_home)
            continue

        event            = match["event"]
        odds_home_abbrev = match["odds_home_abbrev"]
        odds_home_full   = match["odds_home_full"]

        parsed = _parse_event(event, odds_home_full, odds_home_abbrev, nba_home)
        if parsed:
            result[game_id] = parsed
            logger.info(
                "Matched %s @ %s — spread=%s total=%s",
                nba_away, nba_home,
                parsed.get("spread_line"),
                parsed.get("total_line")
            )

    logger.info("Odds matched %d/%d games", len(result), len(games))
    return result


def _parse_event(event: dict, odds_home_full: str,
                 odds_home_abbrev: str, nba_home_abbrev: str) -> dict:
    home_odds = away_odds = spread_line = spread_odds = total_line = total_odds = None

    preferred = ["draftkings", "fanduel", "betmgm", "williamhill_us", "bovada"]

    def book_rank(b):
        k = b.get("key", "")
        return preferred.index(k) if k in preferred else 99

    for book in sorted(event.get("bookmakers", []), key=book_rank):
        for mkt in book.get("markets", []):
            mkt_key  = mkt.get("key")
            outcomes = mkt.get("outcomes", [])

            if mkt_key == "h2h" and home_odds is None:
                for o in outcomes:
                    if o.get("name") == odds_home_full:
                        home_odds = o.get("price")
                    else:
                        away_odds = o.get("price")

            elif mkt_key == "spreads" and spread_line is None:
                for o in outcomes:
                    if o.get("name") == odds_home_full:
                        spread_line = o.get("point")
                        spread_odds = o.get("price")

            elif mkt_key == "totals" and total_line is None:
                for o in outcomes:
                    if o.get("name") == "Over":
                        total_line = o.get("point")
                        total_odds = o.get("price")

        if home_odds and spread_line is not None and total_line is not None:
            break

    if spread_line is None and total_line is None and home_odds is None:
        return {}

    # If Odds API home != NBA home, flip spread and swap ML odds
    if odds_home_abbrev != nba_home_abbrev and spread_line is not None:
        spread_line   = -spread_line
        home_odds, away_odds = away_odds, home_odds
        logger.info(
            "Spread flipped: Odds home=%s NBA home=%s new spread=%s",
            odds_home_abbrev, nba_home_abbrev, spread_line
        )

    return {
        "home_odds":   home_odds,
        "away_odds":   away_odds,
        "spread_line": spread_line,
        "spread_odds": spread_odds or 1.91,
        "total_line":  total_line,
        "total_odds":  total_odds or 1.91,
    }
