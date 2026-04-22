# greyhound_odds.py -- Live greyhound odds from The Odds API
# Sport key: greyhound_racing_au
# Region: au (Sportsbet, TAB, Ladbrokes, Pointsbet)
# Odds are the single strongest predictor in greyhound racing

import logging
import requests
import time

logger = logging.getLogger(__name__)

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE    = "https://api.the-odds-api.com/v4"
SPORT_KEY    = "greyhound_racing_au"
REGION       = "au"

PREFERRED_BOOKS = ["sportsbet", "tab", "ladbrokes", "pointsbet", "betr", "betfair_au"]


def fetch_greyhound_events() -> list:
    """
    Fetch all upcoming AU greyhound events from The Odds API.
    Returns list of events with their IDs, names, and start times.
    Each event = one race at one meeting.
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events",
            params={
                "apiKey":  ODDS_API_KEY,
                "regions": REGION,
            },
            timeout=20,
        )
        r.raise_for_status()
        events = r.json()
        if not isinstance(events, list):
            logger.warning("Unexpected events response: %s", type(events))
            return []
        logger.info("Greyhound events: %d races from The Odds API", len(events))
        return events
    except Exception as e:
        logger.error("Failed to fetch greyhound events: %s", e)
        return []


def fetch_race_odds(event_id: str) -> dict:
    """
    Fetch win odds for all runners in a single race.
    Returns {runner_name: best_odds} using best available price across books.
    """
    try:
        time.sleep(0.4)
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{event_id}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    REGION,
                "markets":    "h2h",
                "oddsFormat": "decimal",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return _parse_race_odds(data)
    except Exception as e:
        logger.warning("Failed to fetch odds for event %s: %s", event_id, e)
        return {}


def fetch_all_race_odds_bulk() -> dict:
    """
    Fetch win odds for ALL upcoming AU greyhound races in one call.
    More efficient than per-race calls.
    Returns {event_id: {runner_name: odds}}
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    REGION,
                "markets":    "h2h",
                "oddsFormat": "decimal",
            },
            timeout=30,
        )
        r.raise_for_status()
        events = r.json()
        if not isinstance(events, list):
            return {}

        result = {}
        for event in events:
            event_id = event.get("id")
            if event_id:
                odds = _parse_race_odds(event)
                if odds:
                    result[event_id] = {
                        "runner_odds": odds,
                        "event_name":  event.get("home_team", ""),
                        "commence":    event.get("commence_time", ""),
                    }

        logger.info("Greyhound bulk odds: %d races with markets", len(result))
        return result
    except Exception as e:
        logger.error("Bulk greyhound odds fetch failed: %s", e)
        return {}


def _parse_race_odds(data: dict) -> dict:
    """
    Parse The Odds API response to extract best win odds per runner.
    Returns {runner_name: best_decimal_odds}
    """
    runner_odds = {}

    def book_rank(b):
        key = b.get("key", "")
        return PREFERRED_BOOKS.index(key) if key in PREFERRED_BOOKS else 99

    bookmakers = sorted(data.get("bookmakers", []), key=book_rank)

    for book in bookmakers:
        for mkt in book.get("markets", []):
            if mkt.get("key") != "h2h":
                continue
            for outcome in mkt.get("outcomes", []):
                name  = outcome.get("name", "")
                price = outcome.get("price", 0)
                if name and price > 1.0:
                    # Keep best (highest) odds for each runner across books
                    if name not in runner_odds or price > runner_odds[name]:
                        runner_odds[name] = price

    return runner_odds


def odds_to_implied_prob(odds: float) -> float:
    """Convert decimal odds to implied probability (0-1)."""
    if odds <= 1.0:
        return 0.0
    return 1.0 / odds


def normalise_probs(runner_odds: dict) -> dict:
    """
    Convert raw implied probabilities to sum-to-1 normalised probabilities.
    Removes bookmaker margin (overround).
    Returns {runner_name: normalised_prob}
    """
    if not runner_odds:
        return {}
    raw = {name: odds_to_implied_prob(odds) for name, odds in runner_odds.items()}
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {name: prob / total for name, prob in raw.items()}


def match_runner_to_odds(runner_name: str, odds_dict: dict) -> float:
    """
    Match a runner name from the form guide to the odds dict.
    Handles partial matches since names may differ slightly.
    Returns best odds found, or 0 if not found.
    """
    if not runner_name or not odds_dict:
        return 0.0

    clean = runner_name.upper().strip()

    # Exact match
    for name, odds in odds_dict.items():
        if name.upper().strip() == clean:
            return odds

    # Partial match (first word or last word)
    parts = clean.split()
    for name, odds in odds_dict.items():
        name_upper = name.upper()
        if any(p in name_upper for p in parts if len(p) > 3):
            return odds

    return 0.0
