# odds_fetcher.py
import logging
import requests
logger = logging.getLogger(__name__)

ODDS_API_KEY = "61040feb939ef2fe29c0e8c8fa8eb152"
ODDS_BASE = "https://api.the-odds-api.com/v4"

TEAM_NAME_MAP = {
    "Atlanta Hawks":"ATL","Boston Celtics":"BOS","Brooklyn Nets":"BKN","Charlotte Hornets":"CHA",
    "Chicago Bulls":"CHI","Cleveland Cavaliers":"CLE","Dallas Mavericks":"DAL","Denver Nuggets":"DEN",
    "Detroit Pistons":"DET","Golden State Warriors":"GSW","Houston Rockets":"HOU","Indiana Pacers":"IND",
    "Los Angeles Clippers":"LAC","Los Angeles Lakers":"LAL","Memphis Grizzlies":"MEM","Miami Heat":"MIA",
    "Milwaukee Bucks":"MIL","Minnesota Timberwolves":"MIN","New Orleans Pelicans":"NOP","New York Knicks":"NYK",
    "Oklahoma City Thunder":"OKC","Orlando Magic":"ORL","Philadelphia 76ers":"PHI","Phoenix Suns":"PHX",
    "Portland Trail Blazers":"POR","Sacramento Kings":"SAC","San Antonio Spurs":"SAS","Toronto Raptors":"TOR",
    "Utah Jazz":"UTA","Washington Wizards":"WAS",
}

def fetch_odds_for_games(games: list) -> dict:
    """Fetch odds and map to game IDs."""
    try:
        r = requests.get(f"{ODDS_BASE}/sports/basketball_nba/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "h2h,spreads,totals", "oddsFormat": "decimal"},
            timeout=15)
        r.raise_for_status()
        odds_data = r.json()
    except Exception as e:
        logger.error(f"Odds fetch failed: {e}")
        return {}

    # Build lookup by team abbrev pair
    odds_lookup = {}
    for event in odds_data:
        h = _abbrev(event.get("home_team", ""))
        a = _abbrev(event.get("away_team", ""))
        odds_lookup[f"{a}@{h}"] = event

    result = {}
    for game in games:
        key = f"{game['away_team_abbrev']}@{game['home_team_abbrev']}"
        event = odds_lookup.get(key)
        if not event:
            continue
        parsed = _parse_event(event)
        if parsed:
            result[game["game_id"]] = parsed

    return result

def _parse_event(event):
    home_odds = away_odds = spread_line = spread_odds = total_line = total_odds = None
    for book in event.get("bookmakers", []):
        for mkt in book.get("markets", []):
            if mkt["key"] == "h2h":
                for o in mkt.get("outcomes", []):
                    if o["name"] == event["home_team"]: home_odds = o["price"]
                    if o["name"] == event["away_team"]: away_odds = o["price"]
            if mkt["key"] == "spreads":
                for o in mkt.get("outcomes", []):
                    if o["name"] == event["home_team"]:
                        spread_line = o.get("point"); spread_odds = o.get("price")
            if mkt["key"] == "totals":
                for o in mkt.get("outcomes", []):
                    if o["name"] == "Over":
                        total_line = o.get("point"); total_odds = o.get("price")
        if home_odds and spread_line and total_line:
            break
    return {"home_odds": home_odds, "away_odds": away_odds,
            "spread_line": spread_line, "spread_odds": spread_odds or 1.91,
            "total_line": total_line, "total_odds": total_odds or 1.91}

def _abbrev(name):
    return TEAM_NAME_MAP.get(name, name.split()[-1][:3].upper())
