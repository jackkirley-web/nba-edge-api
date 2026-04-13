# afl_odds.py -- AFL odds fetcher using The Odds API
# Game odds: /v4/sports/aussierules_afl/odds (featured markets - h2h, spreads, totals)
# Player props: /v4/sports/aussierules_afl/events/{id}/odds (per-event props)
# Region: au (Sportsbet, Ladbrokes, TAB, Pointsbet, Betr)

import logging
import requests
import time

logger = logging.getLogger(__name__)

ODDS_API_KEY  = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE     = "https://api.the-odds-api.com/v4"
SPORT_KEY     = "aussierules_afl"
REGION        = "au"

# AFL player prop market keys on The Odds API
# These are fetched per-event using the event-odds endpoint
AFL_PROP_MARKETS = [
    "player_disposals",
    "player_goals",
    "player_marks",
    "player_tackles",
    "player_kicks",
    "player_handballs",
    "player_clearances",
    "player_hitouts",
    "player_fantasy_score",
]

# Bookmaker preference order for AU markets
PREFERRED_BOOKS = ["sportsbet", "tab", "pointsbet", "ladbrokes", "betr", "betfair_au"]


def fetch_afl_game_odds(games: list) -> dict:
    """
    Fetch h2h, spread, and total odds for all upcoming AFL games.
    Returns {game_id: {home_odds, away_odds, spread_line, spread_odds, total_line, total_odds}}
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/odds",
            params={
                "apiKey":      ODDS_API_KEY,
                "regions":     REGION,
                "markets":     "h2h,spreads,totals",
                "oddsFormat":  "decimal",
            },
            timeout=15,
        )
        r.raise_for_status()
        odds_data = r.json()
        if not isinstance(odds_data, list):
            logger.error("Unexpected AFL odds response: %s", type(odds_data))
            return {}

        logger.info("AFL Odds API: %d events returned", len(odds_data))

        # Log available events for debugging
        for ev in odds_data:
            logger.info("AFL odds available: %s vs %s", ev.get("away_team"), ev.get("home_team"))

    except Exception as e:
        logger.error("AFL odds fetch failed: %s", e)
        return {}

    result = {}
    for game in games:
        matched = _match_game_to_odds(game, odds_data)
        if matched:
            parsed = _parse_game_odds(matched)
            if parsed:
                result[game["game_id"]] = parsed
                logger.info("Matched odds: %s vs %s", game["away_team"], game["home_team"])
            else:
                logger.warning("Could not parse odds for %s vs %s", game["away_team"], game["home_team"])
        else:
            logger.warning("No odds match for %s vs %s", game["away_team"], game["home_team"])

    logger.info("AFL odds matched %d/%d games", len(result), len(games))
    return result


def fetch_afl_events() -> list:
    """
    Get list of AFL events with their IDs (needed for prop lookups).
    Returns [{ event_id, home_team, away_team, commence_time }]
    """
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events",
            params={
                "apiKey":  ODDS_API_KEY,
                "regions": REGION,
            },
            timeout=15,
        )
        r.raise_for_status()
        events = r.json()
        if not isinstance(events, list):
            return []
        return [
            {
                "event_id":      ev.get("id"),
                "home_team":     ev.get("home_team"),
                "away_team":     ev.get("away_team"),
                "commence_time": ev.get("commence_time"),
            }
            for ev in events
        ]
    except Exception as e:
        logger.warning("AFL events fetch failed: %s", e)
        return []


def fetch_afl_player_props(event_id: str, markets: list = None) -> dict:
    """
    Fetch player prop odds for a single AFL event.
    Uses the event-odds endpoint which supports non-featured markets.
    Returns {player_name: {stat: {line, over_odds, under_odds, book}}}
    """
    if not markets:
        markets = AFL_PROP_MARKETS

    # Request all prop markets in one call
    markets_str = ",".join(markets)
    try:
        time.sleep(0.5)
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{event_id}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    REGION,
                "markets":    markets_str,
                "oddsFormat": "decimal",
            },
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.warning("AFL props fetch failed for event %s: %s", event_id, e)
        return {}

    return _parse_player_props(data)


def fetch_all_game_props(games: list, events: list) -> dict:
    """
    Fetch player props for all games.
    Returns {game_id: {player_name: {stat: {line, over_odds, under_odds}}}}
    """
    # Build event_id lookup by matching team names
    event_lookup = {}
    for ev in events:
        key = frozenset([
            _normalize_team(ev["home_team"]),
            _normalize_team(ev["away_team"]),
        ])
        event_lookup[key] = ev["event_id"]

    all_props = {}
    for game in games:
        key = frozenset([game["home_team"], game["away_team"]])
        event_id = event_lookup.get(key)

        if not event_id:
            # Try with abbreviations
            logger.warning("No event ID found for %s vs %s", game["away_team"], game["home_team"])
            continue

        props = fetch_afl_player_props(event_id)
        if props:
            all_props[game["game_id"]] = props
            logger.info("Props fetched for %s vs %s: %d players",
                        game["away_team"], game["home_team"], len(props))
        else:
            logger.warning("No props returned for %s vs %s", game["away_team"], game["home_team"])

    return all_props


# -- Parsing helpers --------------------------------------------------------

def _match_game_to_odds(game: dict, odds_data: list) -> dict | None:
    """Match a game dict to an odds event using team names."""
    home = _normalize_team(game["home_team"])
    away = _normalize_team(game["away_team"])
    game_key = frozenset([home, away])

    for ev in odds_data:
        odds_home = _normalize_team(ev.get("home_team", ""))
        odds_away = _normalize_team(ev.get("away_team", ""))
        if frozenset([odds_home, odds_away]) == game_key:
            return ev
    return None


def _parse_game_odds(event: dict) -> dict:
    """Parse h2h, spread, total from a matched odds event."""
    home_team = event.get("home_team", "")
    h2h_home = h2h_away = spread_line = spread_odds = total_line = total_odds = None

    def book_rank(b):
        key = b.get("key", "")
        return PREFERRED_BOOKS.index(key) if key in PREFERRED_BOOKS else 99

    bookmakers = sorted(event.get("bookmakers", []), key=book_rank)

    for book in bookmakers:
        for mkt in book.get("markets", []):
            key = mkt.get("key")
            outcomes = mkt.get("outcomes", [])

            if key == "h2h" and h2h_home is None:
                for o in outcomes:
                    if o.get("name") == home_team:
                        h2h_home = o.get("price")
                    else:
                        h2h_away = o.get("price")

            elif key == "spreads" and spread_line is None:
                for o in outcomes:
                    if o.get("name") == home_team:
                        spread_line = o.get("point")
                        spread_odds = o.get("price")

            elif key == "totals" and total_line is None:
                for o in outcomes:
                    if o.get("name") == "Over":
                        total_line = o.get("point")
                        total_odds = o.get("price")

        if h2h_home and spread_line is not None and total_line is not None:
            break

    return {
        "home_odds":   h2h_home,
        "away_odds":   h2h_away,
        "spread_line": spread_line,
        "spread_odds": spread_odds or 1.91,
        "total_line":  total_line,
        "total_odds":  total_odds or 1.91,
    }


def _parse_player_props(data: dict) -> dict:
    """
    Parse player props from event-odds response.
    Returns {player_name: {stat_key: {line, over_odds, under_odds, book}}}
    """
    players = {}

    def book_rank(b):
        key = b.get("key", "")
        return PREFERRED_BOOKS.index(key) if key in PREFERRED_BOOKS else 99

    bookmakers = sorted(data.get("bookmakers", []), key=book_rank)

    for book in bookmakers:
        book_key = book.get("key", "")
        for mkt in book.get("markets", []):
            market_key = mkt.get("key", "")
            if not market_key.startswith("player_"):
                continue

            # Map market key to our stat name
            stat = _market_key_to_stat(market_key)
            if not stat:
                continue

            # Group outcomes by player (they come as Over/Under pairs)
            player_lines = {}
            for o in mkt.get("outcomes", []):
                player = o.get("description", "")
                if not player:
                    continue
                if player not in player_lines:
                    player_lines[player] = {"line": o.get("point"), "over_odds": None, "under_odds": None, "book": book_key}
                if o.get("name") == "Over":
                    player_lines[player]["over_odds"] = o.get("price")
                    player_lines[player]["line"] = o.get("point")
                elif o.get("name") == "Under":
                    player_lines[player]["under_odds"] = o.get("price")

            # Add to players dict (prefer best-ranked book, don't overwrite)
            for player, line_data in player_lines.items():
                if player not in players:
                    players[player] = {}
                if stat not in players[player]:
                    players[player][stat] = line_data

    return players


def _market_key_to_stat(market_key: str) -> str:
    """Map The Odds API market key to our internal stat name."""
    mapping = {
        "player_disposals":     "disposals",
        "player_goals":         "goals",
        "player_marks":         "marks",
        "player_tackles":       "tackles",
        "player_kicks":         "kicks",
        "player_handballs":     "handballs",
        "player_clearances":    "clearances",
        "player_hitouts":       "hitouts",
        "player_fantasy_score": "fantasy_pts",
        # Alternate markets
        "player_disposals_alternate": "disposals",
        "player_goals_alternate":     "goals",
        "player_marks_alternate":     "marks",
    }
    return mapping.get(market_key, "")


def _normalize_team(name: str) -> str:
    """Normalize team name for matching."""
    from afl_data import TEAM_CANONICAL
    canonical = TEAM_CANONICAL.get(name, name)
    return canonical.lower().strip()
