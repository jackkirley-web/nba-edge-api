# cache.py — Data orchestrator and cache
# Coordinates all data fetching, caches results, builds picks

import logging
import time
import threading
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 1800  # 30 minutes


class NBACache:
    """
    Thread-safe cache that orchestrates all NBA data fetching.
    Refreshes automatically every 30 minutes.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._last_refresh: Optional[datetime] = None

    def get(self, force_refresh=False) -> dict:
        """Return cached data, refreshing if stale."""
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > CACHE_TTL_SECONDS or not self._data:
                logger.info("Cache stale or forced — refreshing...")
                try:
                    self._data = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error(f"Cache refresh failed: {e}")
                    if not self._data:
                        self._data = {"error": str(e), "games": [], "picks": {}, "injuries": {}}
        return self._data

    def refresh_all(self):
        """Force a full refresh (called at startup)."""
        self.get(force_refresh=True)

    def _fetch_all(self) -> dict:
        """
        Main orchestration:
        1. Get today's games
        2. Get injuries
        3. For each game: get team stats, rest, h2h, player data
        4. Score all legs
        5. Build multis
        """
        from nba_data import (
            get_today_games, get_team_advanced_stats, get_team_recent_stats,
            get_team_game_logs, get_rest_days, get_h2h_history,
            get_home_away_splits, get_player_stats_for_team,
        )
        from injury_report import fetch_official_injury_report, get_injury_impact_score
        from engine import score_spread_leg, score_total_leg, build_multis
        from odds_fetcher import fetch_odds_for_games

        logger.info("Starting full data refresh...")

        # ── Step 1: Today's games ──────────────────────────────
        games = get_today_games()
        logger.info(f"Found {len(games)} games today")
        if not games:
            return {"games": [], "picks": {}, "injuries": {}, "last_updated": _now()}

        # ── Step 2: League-wide advanced stats ────────────────
        logger.info("Fetching league advanced stats...")
        adv_stats = get_team_advanced_stats()  # {team_id: stats_dict}

        # ── Step 3: Injury report ─────────────────────────────
        logger.info("Fetching injury report...")
        injuries_by_team = fetch_official_injury_report()

        # ── Step 4: Odds ──────────────────────────────────────
        logger.info("Fetching odds...")
        odds_by_game = fetch_odds_for_games(games)

        # ── Step 5: Per-game deep analysis ────────────────────
        all_legs = []
        enriched_games = []

        for game in games:
            logger.info(f"Analysing: {game['away_team_abbrev']} @ {game['home_team_abbrev']}")
            home_id = game["home_team_id"]
            away_id = game["away_team_id"]
            home_name = f"{game['home_team_city']} {game['home_team']}"
            away_name = f"{game['away_team_city']} {game['away_team']}"

            try:
                # Team contexts
                home_ctx = _build_team_context(
                    home_id, game["home_team_abbrev"], home_name,
                    adv_stats, injuries_by_team, is_home=True
                )
                away_ctx = _build_team_context(
                    away_id, game["away_team_abbrev"], away_name,
                    adv_stats, injuries_by_team, is_home=False
                )

                # H2H (attach to home context)
                home_ctx["h2h"] = get_h2h_history(home_id, away_id)

                # Odds for this game
                game_odds = odds_by_game.get(game["game_id"], {})

                # Enrich game dict for frontend
                enriched_game = {**game, **game_odds, "home_name": home_name, "away_name": away_name,
                                 "home_injuries": home_ctx.get("injuries", []),
                                 "away_injuries": away_ctx.get("injuries", [])}
                enriched_games.append(enriched_game)

                # Score spread
                if game_odds.get("spread_line") is not None:
                    market_line = game_odds["spread_line"]
                    home_fav = market_line < 0
                    spread_result = score_spread_leg(home_ctx, away_ctx, abs(market_line), home_fav, game_odds.get("spread_odds", 1.91))
                    home_abbrev = game["home_team_abbrev"]
                    away_abbrev = game["away_team_abbrev"]
                    spread_sel = (f"{home_abbrev} {market_line:+.1f}" if home_fav
                                  else f"{away_abbrev} +{abs(market_line):.1f}")
                    all_legs.append({
                        **spread_result,
                        "game_id": game["game_id"],
                        "game": f"{away_abbrev} @ {home_abbrev}",
                        "selection": spread_sel,
                        "odds": game_odds.get("spread_odds", 1.91),
                    })

                # Score total
                if game_odds.get("total_line") is not None:
                    total_line = game_odds["total_line"]
                    total_result = score_total_leg(home_ctx, away_ctx, total_line, game_odds.get("total_odds", 1.91))
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

        # ── Step 6: Build multis ───────────────────────────────
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
    """Build complete context dict for one team."""
    from nba_data import (
        get_team_recent_stats, get_team_game_logs,
        get_rest_days, get_home_away_splits, get_player_stats_for_team,
    )
    from injury_report import get_injury_impact_score

    # Advanced stats (season-long)
    advanced = adv_stats.get(team_id, {})

    # Rolling windows
    recent_l5  = get_team_recent_stats(team_id, 5)
    recent_l10 = get_team_recent_stats(team_id, 10)
    recent_l15 = get_team_recent_stats(team_id, 15)
    game_logs  = get_team_game_logs(team_id, 15)

    # Rest
    rest = get_rest_days(team_id)

    # Home/away splits
    splits = get_home_away_splits(team_id)

    # Players (for injury impact calculation)
    players = get_player_stats_for_team(team_id)

    # Injuries for this team
    team_injuries = (
        injuries_by_team.get(full_name, []) or
        injuries_by_team.get(abbrev, []) or
        []
    )

    # Injury impact score
    injury_impact = get_injury_impact_score(team_injuries, players)

    return {
        "team_id": team_id,
        "team_abbrev": abbrev,
        "team_name": full_name,
        "is_home": is_home,
        "advanced": advanced,
        "recent_l5": recent_l5,
        "recent_l10": recent_l10,
        "recent_l15": recent_l15,
        "game_logs": game_logs,
        "rest": rest,
        "splits": splits,
        "players": players,
        "injuries": team_injuries,
        "injury_impact": injury_impact,
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


# Singleton
cache = NBACache()
