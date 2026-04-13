# cache_afl.py -- AFL data cache
# Completely separate from NBA cache -- only loads when AFL sport is selected
# MainAFLCache: upcoming round, game odds, player season averages, props
# AFLStreakCache: player game logs + streak calculation (background)

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

AFL_MAIN_TTL   = 600   # 10 min (round data doesn't change as often)
AFL_STREAK_TTL = 3600  # 60 min (logs don't change between rounds)


class MainAFLCache:
    """
    Loads the upcoming round of AFL data:
    - Fixtures from Squiggle
    - Game odds (h2h, spreads, totals) from The Odds API
    - Player prop odds from The Odds API
    - Player season averages from Footywire
    - Ladder from Squiggle
    - Squiggle model tips
    - Scored game legs + multis
    - Player props with projections
    """
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = {}
        self._last_refresh = None

    def get(self, force_refresh=False) -> dict:
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > AFL_MAIN_TTL or not self._data:
                logger.info("AFL main cache refreshing...")
                try:
                    self._data = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error("AFL main cache failed: %s", e)
                    if not self._data:
                        self._data = {
                            "round": None, "games": [], "picks": _empty_picks(),
                            "legs": [], "props": [], "ladder": [],
                            "last_updated": _now(), "error": str(e),
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from afl_data import (
            get_upcoming_round, get_ladder, get_squiggle_tips,
            get_h2h_history, get_venue_stats, get_player_season_averages,
            get_team_news, TEAM_ABBREV,
        )
        from afl_odds import fetch_afl_game_odds, fetch_afl_events, fetch_all_game_props
        from afl_engine import build_game_context, score_afl_line, score_afl_total, build_afl_multis
        from afl_props_engine import project_afl_player_props

        logger.info("=== AFL cache fetch start ===")

        # -- Round fixtures -----------------------------------------------
        round_data = get_upcoming_round()
        games = round_data.get("games", [])
        round_num = round_data.get("round")
        year = round_data.get("year")

        if not games:
            logger.warning("No AFL games found for upcoming round")
            return {
                "round": round_num, "year": year, "games": [],
                "picks": _empty_picks(), "legs": [], "props": [],
                "ladder": [], "last_updated": _now(),
                "message": "No upcoming round found",
            }

        logger.info("AFL Round %s: %d games", round_num, len(games))

        # -- Ladder + tips ------------------------------------------------
        logger.info("Fetching AFL ladder and tips...")
        ladder   = get_ladder(year)
        tips     = get_squiggle_tips(year, round_num)

        # -- Game odds ----------------------------------------------------
        logger.info("Fetching AFL game odds...")
        game_odds_map = fetch_afl_game_odds(games)

        # -- Player season averages (for props + streaks) -----------------
        logger.info("Fetching AFL player season averages from Footywire...")
        player_avgs = get_player_season_averages(year)
        logger.info("Got season averages for %d AFL players", len(player_avgs))

        # -- Team news (lineups) ------------------------------------------
        logger.info("Fetching AFL team news...")
        team_news = get_team_news(year, round_num)

        # -- Player prop odds from The Odds API ---------------------------
        logger.info("Fetching AFL player prop odds...")
        events = fetch_afl_events()
        prop_odds_by_game = fetch_all_game_props(games, events)
        logger.info("AFL prop odds fetched for %d games", len(prop_odds_by_game))

        # -- Score game legs + props -------------------------------------
        playing_teams = set()
        all_legs  = []
        all_props = []
        enriched_games = []

        for game in games:
            home = game["home_team"]
            away = game["away_team"]
            playing_teams.add(home)
            playing_teams.add(away)

            # Venue stats
            venue = get_venue_stats(game.get("venue", ""))

            # H2H history
            h2h = get_h2h_history(home, away, year)

            # Team news for each side
            home_news = team_news.get(home, {})
            away_news = team_news.get(away, {})

            # Build enriched game
            game_odds = game_odds_map.get(game["game_id"], {})
            enriched = {
                **game,
                **game_odds,
                "venue_stats":   venue,
                "home_news":     home_news,
                "away_news":     away_news,
                "ladder_home":   next((t for t in ladder if t["team"] == home), {}),
                "ladder_away":   next((t for t in ladder if t["team"] == away), {}),
                "squiggle_tip":  next((t for t in tips if
                                      (t["home_team"] == home and t["away_team"] == away) or
                                      (t["home_team"] == away and t["away_team"] == home)), {}),
            }
            enriched_games.append(enriched)

            # Score game line
            if game_odds.get("home_odds") or game_odds.get("spread_line") is not None:
                ctx = build_game_context(
                    game=game,
                    team_stats={},
                    ladder=ladder,
                    h2h_history=h2h,
                    venue_stats=venue,
                    squiggle_tips=tips,
                    game_odds=game_odds,
                )

                # Head-to-head line leg
                line_result = score_afl_line(ctx)
                lean = line_result.get("lean_team", home)
                home_odds = game_odds.get("home_odds")
                away_odds = game_odds.get("away_odds")
                leg_odds   = home_odds if lean == home else away_odds
                all_legs.append({
                    **line_result,
                    "game_id":   game["game_id"],
                    "game":      f"{away} @ {home}",
                    "selection": lean,
                    "odds":      leg_odds or 1.85,
                    "sport":     "AFL",
                })

                # Total leg
                if game_odds.get("total_line"):
                    total_result = score_afl_total(ctx)
                    if total_result:
                        tl = game_odds["total_line"]
                        direction = total_result.get("lean_direction", "Over")
                        all_legs.append({
                            **total_result,
                            "game_id":   game["game_id"],
                            "game":      f"{away} @ {home}",
                            "selection": f"{direction} {tl}",
                            "odds":      game_odds.get("total_odds", 1.91),
                            "sport":     "AFL",
                        })

            # -- Player props for this game --------------------------------
            game_prop_odds = prop_odds_by_game.get(game["game_id"], {})

            for is_home, team_name in [(True, home), (False, away)]:
                news = home_news if is_home else away_news

                # Get players from this team
                team_players = {
                    name: p for name, p in player_avgs.items()
                    if p.get("team") == team_name and p.get("games", 0) >= 3
                }
                # Sort by disposals (best players first)
                sorted_players = sorted(
                    team_players.items(),
                    key=lambda x: x[1].get("disposals", 0),
                    reverse=True
                )[:22]  # Top 22 (full team)

                for pname, pdata in sorted_players:
                    # Get real prop odds for this player if available
                    real_lines = game_prop_odds.get(pname, {})

                    try:
                        result = project_afl_player_props(
                            player=pdata,
                            game_logs=[],  # No logs yet -- streak cache handles logs
                            opponent=away if is_home else home,
                            is_home=is_home,
                            venue_stats=venue,
                            real_lines=real_lines,
                            team_news=news,
                        )
                        if result and result["scored_props"]:
                            for prop in result["scored_props"]:
                                all_props.append({
                                    **prop,
                                    "game_id":  game["game_id"],
                                    "game":     f"{away} @ {home}",
                                    "team":     TEAM_ABBREV.get(team_name, team_name[:3]),
                                    "sport":    "AFL",
                                })
                    except Exception as e:
                        logger.warning("AFL props failed for %s: %s", pname, e)

        logger.info("AFL: %d game legs, %d props", len(all_legs), len(all_props))

        # Top props for multis (only real-line props preferred)
        top_prop_legs = []
        top_props = sorted(
            [p for p in all_props if p.get("confidence", 0) >= 60 and p.get("has_real_line")],
            key=lambda x: x["confidence"], reverse=True
        )[:10]

        for p in top_props:
            sel = f"{p['player']} {p['direction']} {p['book_line']} {p['stat_label']}"
            top_prop_legs.append({
                "game_id":    p["game_id"],
                "game":       p["game"],
                "type":       f"Prop - {p['stat_label']}",
                "selection":  sel,
                "odds":       p.get("odds", 1.91),
                "confidence": p["confidence"],
                "prob":       p["prob"],
                "tags":       p.get("tags", []),
                "reasoning":  p.get("reasoning", ""),
                "factors":    [],
                "projected_margin": None,
                "projected_total":  None,
                "edge":       p.get("edge"),
                "sport":      "AFL",
            })

        picks = build_afl_multis(all_legs + top_prop_legs)
        all_props.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        return {
            "round":           round_num,
            "year":            year,
            "games":           enriched_games,
            "legs":            all_legs,
            "props":           all_props,
            "picks":           picks,
            "ladder":          ladder,
            "tips":            tips,
            "last_updated":    _now(),
            "legs_scored":     len(all_legs),
            "props_scored":    len(all_props),
            "_player_avgs":    player_avgs,
            "_playing_teams":  playing_teams,
        }


class AFLStreakCache:
    """
    Background cache for AFL streak data.
    Fetches game-by-game logs from Footywire for all players in the round.
    Never blocks the main cache.
    """
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = []
        self._last_refresh = None
        self._loading      = False

    def get(self, force_refresh=False):
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            stale = age > AFL_STREAK_TTL or not self._last_refresh
            should_refresh = (force_refresh or stale) and not self._loading

        if should_refresh:
            self._trigger_refresh()

        with self._lock:
            return {
                "streaks":     self._data,
                "loading":     self._loading,
                "last_updated": self._last_refresh.strftime("%I:%M %p") if self._last_refresh else None,
            }

    def _trigger_refresh(self):
        with self._lock:
            if self._loading:
                return
            self._loading = True
        t = threading.Thread(target=self._background_fetch, daemon=True)
        t.start()
        logger.info("AFL streak background fetch started")

    def _background_fetch(self):
        try:
            from afl_data import get_player_logs_by_name_batch
            from afl_streak_engine import calculate_afl_streaks

            # Get player data from main cache
            main_data     = afl_cache.get()
            player_avgs   = main_data.get("_player_avgs", {})
            playing_teams = main_data.get("_playing_teams", set())

            if not player_avgs:
                logger.warning("AFL streak: no player averages in main cache")
                with self._lock:
                    self._loading = False
                return

            # Get players from playing teams only
            players_to_fetch = [
                name for name, p in player_avgs.items()
                if p.get("team") in playing_teams and p.get("games", 0) >= 3
            ]
            # Limit to top players by disposals to keep fetch time reasonable
            players_to_fetch.sort(
                key=lambda n: player_avgs[n].get("disposals", 0),
                reverse=True
            )
            players_to_fetch = players_to_fetch[:120]  # Top ~6-7 per team

            logger.info("AFL streak: fetching logs for %d players...", len(players_to_fetch))
            player_logs = get_player_logs_by_name_batch(players_to_fetch, last_n=15)
            logger.info("AFL streak: got logs for %d players", len(player_logs))

            streaks = calculate_afl_streaks(
                players=player_avgs,
                player_logs=player_logs,
                playing_teams=playing_teams,
                windows=[5, 10, 15],
            )
            logger.info("AFL streaks: %d calculated", len(streaks))

            with self._lock:
                self._data         = streaks
                self._last_refresh = datetime.now()
                self._loading      = False

        except Exception as e:
            logger.error("AFL streak fetch failed: %s", e)
            with self._lock:
                self._loading = False


# -- Helpers ----------------------------------------------------------------

def _empty_picks():
    e = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**e, "key": "safe",  "label": "Safe Multi",    "accentColor": "#4CAF7D", "subtitle": "No round data"},
        "mid":   {**e, "key": "mid",   "label": "Mid-Risk Multi","accentColor": "#C9A84C", "subtitle": "No round data"},
        "lotto": {**e, "key": "lotto", "label": "Lotto Multi",   "accentColor": "#E05252", "subtitle": "No round data"},
    }

def _now():
    return datetime.now().strftime("%I:%M %p")


# -- Singletons -------------------------------------------------------------
afl_cache        = MainAFLCache()
afl_streak_cache = AFLStreakCache()
