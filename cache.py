# cache.py — Lazy cache (warms on first request, not startup)
# This prevents OOM crashes on Render free tier

import logging
import threading
import math
from datetime import datetime

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 1800  # 30 minutes


class NBACache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._last_refresh = None

    def get(self, force_refresh=False) -> dict:
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > CACHE_TTL_SECONDS or not self._data:
                logger.info("Refreshing cache...")
                try:
                    self._data = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error(f"Cache refresh failed: {e}")
                    if not self._data:
                        self._data = {
                            "error": str(e),
                            "games": [],
                            "picks": _empty_picks(),
                            "injuries": {},
                            "legs": [],
                            "last_updated": _now(),
                            "games_analyzed": 0,
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from nba_data import (
            get_today_games, get_team_advanced_stats, get_team_recent_stats,
            get_team_game_logs, get_rest_days, get_h2h_history,
            get_home_away_splits, get_player_stats_for_team,
        )
        from injury_report import fetch_official_injury_report, get_injury_impact_score
        from engine import score_spread_leg, score_total_leg, build_multis
        from odds_fetcher import fetch_odds_for_games

        logger.info("Starting full data refresh...")

        # Today's games
        games = get_today_games()
        logger.info(f"Found {len(games)} games today")
        if not games:
            return {"games": [], "picks": _empty_picks(), "injuries": {},
                    "legs": [], "last_updated": _now(), "games_analyzed": 0}

        # League advanced stats (single call — efficient)
        logger.info("Fetching league advanced stats...")
        adv_stats = get_team_advanced_stats()

        # Injuries
        logger.info("Fetching injuries...")
        injuries_by_team = fetch_official_injury_report()

        # Odds
        logger.info("Fetching odds...")
        odds_by_game = fetch_odds_for_games(games)

        # Per-game analysis
        all_legs = []
        enriched_games = []

        for game in games:
            home_id = game["home_team_id"]
            away_id = game["away_team_id"]
            home_name = f"{game['home_team_city']} {game['home_team']}"
            away_name = f"{game['away_team_city']} {game['away_team']}"
            logger.info(f"Analysing: {game['away_team_abbrev']} @ {game['home_team_abbrev']}")

            try:
                home_ctx = _build_team_context(
                    home_id, game["home_team_abbrev"], home_name,
                    adv_stats, injuries_by_team, is_home=True
                )
                away_ctx = _build_team_context(
                    away_id, game["away_team_abbrev"], away_name,
                    adv_stats, injuries_by_team, is_home=False
                )
                home_ctx["h2h"] = get_h2h_history(home_id, away_id)

                game_odds = odds_by_game.get(game["game_id"], {})
                enriched_game = {
                    **game, **game_odds,
                    "home_name": home_name,
                    "away_name": away_name,
                    "home_injuries": home_ctx.get("injuries", []),
                    "away_injuries": away_ctx.get("injuries", []),
                }
                enriched_games.append(enriched_game)

                # Score spread leg
                if game_odds.get("spread_line") is not None:
                    line = game_odds["spread_line"]
                    home_fav = line < 0
                    spread_result = score_spread_leg(
                        home_ctx, away_ctx, abs(line), home_fav,
                        game_odds.get("spread_odds", 1.91)
                    )
                    h = game["home_team_abbrev"]
                    a = game["away_team_abbrev"]
                    sel = f"{h} {line:+.1f}" if home_fav else f"{a} +{abs(line):.1f}"
                    all_legs.append({
                        **spread_result,
                        "game_id": game["game_id"],
                        "game": f"{a} @ {h}",
                        "selection": sel,
                        "odds": game_odds.get("spread_odds", 1.91),
                    })

                # Score total leg
                if game_odds.get("total_line") is not None:
                    total_line = game_odds["total_line"]
                    total_result = score_total_leg(
                        home_ctx, away_ctx, total_line,
                        game_odds.get("total_odds", 1.91)
                    )
                    all_legs.append({
                        **total_result,
                        "game_id": game["game_id"],
                        "game": f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}",
                        "selection": f"{total_result['selection_direction']} {total_line}",
                        "odds": game_odds.get("total_odds", 1.91),
                    })

            except Exception as e:
                logger.warning(f"Game analysis failed for {game.get('game_id')}: {e}")
                enriched_games.append(game)

        logger.info(f"Scored {len(all_legs)} legs — building multis...")
        picks = build_multis(all_legs)

        return {
            "games": enriched_games,
            "legs": all_legs,
            "picks": picks,
            "injuries": injuries_by_team,
            "last_updated": _now(),
            "games_analyzed": len(games),
        }


def _build_team_context(team_id, abbrev, full_name, adv_stats, injuries_by_team, is_home):
    from nba_data import (
        get_team_recent_stats, get_team_game_logs,
        get_rest_days, get_home_away_splits, get_player_stats_for_team,
    )
    from injury_report import get_injury_impact_score

    advanced = adv_stats.get(int(team_id), {})
    recent_l5  = get_team_recent_stats(team_id, 5)
    recent_l10 = get_team_recent_stats(team_id, 10)
    game_logs  = get_team_game_logs(team_id, 15)
    rest       = get_rest_days(team_id)
    splits     = get_home_away_splits(team_id)
    players    = get_player_stats_for_team(team_id)

    # Try multiple name formats for injury lookup
    team_injuries = (
        injuries_by_team.get(full_name) or
        injuries_by_team.get(abbrev) or
        []
    )

    injury_impact = get_injury_impact_score(team_injuries, players)

    return {
        "team_id": team_id,
        "team_abbrev": abbrev,
        "team_name": full_name,
        "is_home": is_home,
        "advanced": advanced,
        "recent_l5": recent_l5,
        "recent_l10": recent_l10,
        "game_logs": game_logs,
        "rest": rest,
        "splits": splits,
        "players": players,
        "injuries": team_injuries,
        "injury_impact": injury_impact,
    }


def _empty_picks():
    empty = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**empty, "key":"safe",  "label":"Safe Multi",     "emoji":"🔵", "accentColor":"#30D158", "subtitle":"No games today"},
        "mid":   {**empty, "key":"mid",   "label":"Mid-Risk Multi", "emoji":"🟡", "accentColor":"#FF9F0A", "subtitle":"No games today"},
        "lotto": {**empty, "key":"lotto", "label":"Lotto Multi",    "emoji":"🔴", "accentColor":"#FF453A", "subtitle":"No games today"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


cache = NBACache()
