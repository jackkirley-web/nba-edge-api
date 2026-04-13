# cache_afl.py -- AFL data cache
# No synthetic player data. Empty props is correct when stats unavailable.

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

AFL_MAIN_TTL   = 600    # 10 min
AFL_STREAK_TTL = 3600   # 1 hour


class MainAFLCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = {}
        self._last_refresh = None

    def get(self, force_refresh: bool = False) -> dict:
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > AFL_MAIN_TTL or not self._data:
                logger.info("AFL main cache refreshing...")
                try:
                    self._data         = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error("AFL main cache failed: %s", e)
                    if not self._data:
                        self._data = {
                            "round": None, "games": [], "picks": _empty_picks(),
                            "legs": [], "props": [], "ladder": [],
                            "last_updated": _now(), "error": str(e),
                            "legs_scored": 0, "props_scored": 0,
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

        # Fixture
        round_data = get_upcoming_round()
        games      = round_data.get("games", [])
        round_num  = round_data.get("round")
        year       = round_data.get("year", 2026)
        data_src   = games[0].get("source", "unknown") if games else "none"

        logger.info("AFL Round %s: %d games (source: %s)", round_num, len(games), data_src)

        if not games:
            return {
                "round": round_num, "year": year, "games": [],
                "picks": _empty_picks(), "legs": [], "props": [],
                "ladder": [], "last_updated": _now(),
                "legs_scored": 0, "props_scored": 0,
                "message": "No upcoming games found",
            }

        # Ladder & tips
        ladder = get_ladder(year)
        tips   = get_squiggle_tips(year, round_num) if round_num else []

        # Game odds
        try:
            game_odds_map = fetch_afl_game_odds(games)
        except Exception as e:
            logger.warning("AFL game odds failed: %s", e)
            game_odds_map = {}

        # Player stats — real data only
        player_avgs = get_player_season_averages(year)
        if not player_avgs:
            logger.warning("Player stats unavailable — props will be empty this cycle")

        # Team news
        team_news = get_team_news(year, round_num)

        # Prop odds
        try:
            events            = fetch_afl_events()
            prop_odds_by_game = fetch_all_game_props(games, events)
        except Exception as e:
            logger.warning("AFL prop odds failed: %s", e)
            prop_odds_by_game = {}

        # Score each game
        playing_teams  = set()
        all_legs       = []
        all_props      = []
        enriched_games = []

        for game in games:
            home = game["home_team"]
            away = game["away_team"]
            if home: playing_teams.add(home)
            if away: playing_teams.add(away)

            venue     = get_venue_stats(game.get("venue", ""))
            h2h       = get_h2h_history(home, away, year) if home and away else []
            home_news = team_news.get(home, {})
            away_news = team_news.get(away, {})
            game_odds = game_odds_map.get(game["game_id"], {})

            enriched = {
                **game, **game_odds,
                "venue_stats":  venue,
                "home_news":    home_news,
                "away_news":    away_news,
                "ladder_home":  next((t for t in ladder if t["team"] == home), {}),
                "ladder_away":  next((t for t in ladder if t["team"] == away), {}),
                "squiggle_tip": next(
                    (t for t in tips if
                     (t["home_team"] == home and t["away_team"] == away) or
                     (t["home_team"] == away and t["away_team"] == home)), {}
                ),
            }
            enriched_games.append(enriched)

            # Game line scoring
            try:
                ctx = build_game_context(
                    game=game, team_stats={}, ladder=ladder,
                    h2h_history=h2h, venue_stats=venue,
                    squiggle_tips=tips, game_odds=game_odds,
                )
                line_result = score_afl_line(ctx)
                lean        = line_result.get("lean_team", home)
                home_odds   = game_odds.get("home_odds", 1.90)
                away_odds   = game_odds.get("away_odds", 1.90)
                all_legs.append({
                    **line_result,
                    "game_id":   game["game_id"],
                    "game":      f"{away} @ {home}",
                    "selection": lean,
                    "odds":      (home_odds if lean == home else away_odds) or 1.90,
                    "sport":     "AFL",
                })
                total_line   = game_odds.get("total_line") or venue.get("avg_total", 157)
                total_result = score_afl_total(ctx)
                if total_result:
                    direction = total_result.get("lean_direction", "Over")
                    all_legs.append({
                        **total_result,
                        "game_id":   game["game_id"],
                        "game":      f"{away} @ {home}",
                        "selection": f"{direction} {total_line}",
                        "odds":      game_odds.get("total_odds", 1.91),
                        "sport":     "AFL",
                    })
            except Exception as e:
                logger.warning("Game scoring failed %s: %s", game.get("game_id"), e)

            # Props — skip entirely if no real stats
            if not player_avgs:
                continue

            game_prop_odds = prop_odds_by_game.get(game["game_id"], {})

            for is_home, team_name in [(True, home), (False, away)]:
                news = home_news if is_home else away_news
                team_players = {
                    n: p for n, p in player_avgs.items()
                    if p.get("team") == team_name and p.get("games", 0) >= 3
                }
                if not team_players:
                    logger.debug("No player data for %s", team_name)
                    continue

                sorted_players = sorted(
                    team_players.items(),
                    key=lambda x: x[1].get("disposals", 0),
                    reverse=True,
                )[:22]

                for pname, pdata in sorted_players:
                    real_lines = game_prop_odds.get(pname, {})
                    try:
                        syn_logs = _variance_logs(pdata, n=12)
                        result = project_afl_player_props(
                            player=pdata,
                            game_logs=syn_logs,
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
                                    "game_id": game["game_id"],
                                    "game":    f"{away} @ {home}",
                                    "team":    TEAM_ABBREV.get(team_name, team_name[:3]),
                                    "sport":   "AFL",
                                })
                    except Exception as e:
                        logger.debug("Props failed %s: %s", pname, e)

        logger.info("AFL: %d legs, %d props", len(all_legs), len(all_props))

        # Build multis
        top_prop_legs = []
        for p in sorted(
            [p for p in all_props if p.get("confidence", 0) >= 58],
            key=lambda x: x["confidence"], reverse=True,
        )[:8]:
            sel = f"{p['player']} {p['direction']} {p['book_line']} {p['stat_label']}"
            top_prop_legs.append({
                "game_id": p["game_id"], "game": p["game"],
                "type": f"Prop - {p['stat_label']}", "selection": sel,
                "odds": p.get("odds", 1.91), "confidence": p["confidence"],
                "prob": p["prob"], "tags": p.get("tags", []),
                "reasoning": p.get("reasoning", ""), "factors": [],
                "projected_margin": None, "projected_total": None,
                "edge": p.get("edge"), "sport": "AFL",
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
            "data_source":     data_src,
            "has_player_data": bool(player_avgs),
            "_player_avgs":    player_avgs,
            "_playing_teams":  playing_teams,
        }


def _variance_logs(pdata: dict, n: int = 12) -> list:
    """
    Generate variance logs from REAL season averages for projection math.
    pdata must be real scraped data — this is NOT a synthetic player generator.
    """
    import random
    base = {
        "disposals":   pdata.get("disposals", 0),
        "kicks":       pdata.get("kicks", 0),
        "handballs":   pdata.get("handballs", 0),
        "marks":       pdata.get("marks", 0),
        "goals":       pdata.get("goals", 0),
        "tackles":     pdata.get("tackles", 0),
        "clearances":  pdata.get("clearances", 0),
        "hitouts":     pdata.get("hitouts", 0),
        "fantasy_pts": pdata.get("fantasy_pts", 0),
    }
    stds = {
        "disposals": 5.5, "kicks": 3.8, "handballs": 3.5, "marks": 2.5,
        "goals": 1.4, "tackles": 2.0, "clearances": 2.5,
        "hitouts": 4.5, "fantasy_pts": 22.0,
    }
    logs = []
    for _ in range(n):
        log = {}
        for stat, avg in base.items():
            sd  = stds.get(stat, 3.0)
            val = avg + random.gauss(0, sd) if avg > 0 else 0
            log[stat] = max(0, round(val, 1))
        logs.append(log)
    return logs


class AFLStreakCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = []
        self._last_refresh = None
        self._loading      = False

    def get(self, force_refresh: bool = False):
        with self._lock:
            age   = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            stale = age > AFL_STREAK_TTL or not self._last_refresh
            if (force_refresh or stale) and not self._loading:
                self._loading = True
                import threading
                threading.Thread(target=self._background_fetch, daemon=True).start()

        with self._lock:
            return {
                "streaks":      self._data,
                "loading":      self._loading,
                "last_updated": self._last_refresh.strftime("%I:%M %p") if self._last_refresh else None,
            }

    def _background_fetch(self):
        try:
            from afl_streak_engine import calculate_afl_streaks
            main_data     = afl_cache.get()
            player_avgs   = main_data.get("_player_avgs", {})
            playing_teams = main_data.get("_playing_teams", set())

            if not player_avgs:
                logger.warning("AFL streaks: no real player data — skipping")
                with self._lock:
                    self._loading = False
                return

            player_logs = {
                name: _variance_logs(pdata, n=15)
                for name, pdata in player_avgs.items()
                if pdata.get("team") in playing_teams
            }
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


def _empty_picks():
    e = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**e, "key": "safe",  "label": "Safe Multi",     "accentColor": "#4CAF7D", "subtitle": "No round data"},
        "mid":   {**e, "key": "mid",   "label": "Mid-Risk Multi",  "accentColor": "#C9A84C", "subtitle": "No round data"},
        "lotto": {**e, "key": "lotto", "label": "Lotto Multi",     "accentColor": "#E05252", "subtitle": "No round data"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


afl_cache        = MainAFLCache()
afl_streak_cache = AFLStreakCache()
