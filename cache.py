# cache.py — Two independent caches
#
# MainCache  → picks, props, games, odds, injuries
#              Uses season averages only (no per-player logs)
#              Load time: ~15-20 seconds
#
# StreakCache → streak tracker data
#              Tries real NBA.com game logs first
#              Falls back to synthetic logs from season averages if blocked
#              Does NOT block MainCache

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

MAIN_TTL   = 300   # 5 min
STREAK_TTL = 1800  # 30 min


# ─── MAIN CACHE ───────────────────────────────────────────────────────────────
class MainCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._last_refresh = None

    def get(self, force_refresh=False) -> dict:
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > MAIN_TTL or not self._data:
                logger.info("Main cache refreshing...")
                try:
                    self._data = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error("Main cache failed: %s", e)
                    if not self._data:
                        self._data = {
                            "games": [], "picks": _empty_picks(),
                            "injuries": {}, "legs": [], "props": [],
                            "last_updated": _now(), "games_analyzed": 0,
                            "props_scored": 0, "legs_scored": 0,
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from nba_data import (
            get_today_games, get_all_team_stats_batch,
            get_all_team_recent_batch, get_all_player_stats_batch,
        )
        from player_logs import get_all_player_base_stats
        from injury_report import fetch_official_injury_report, get_injury_impact_score
        from engine import score_spread_leg, score_total_leg, build_multis
        from props_engine import project_player_props
        from odds_fetcher import fetch_odds_for_games

        logger.info("=== Main cache fetch start ===")

        games = get_today_games()
        logger.info("Games: %d", len(games))
        if not games:
            return {
                "games": [], "picks": _empty_picks(), "injuries": {},
                "legs": [], "props": [], "last_updated": _now(),
                "games_analyzed": 0, "props_scored": 0, "legs_scored": 0,
            }

        logger.info("League stats...")
        adv_stats       = get_all_team_stats_batch("Advanced")
        base_l10        = get_all_team_recent_batch(10)
        base_l5         = get_all_team_recent_batch(5)
        home_splits     = get_all_team_stats_batch("Base", location="Home")
        road_splits     = get_all_team_stats_batch("Base", location="Road")
        all_players_adv = get_all_player_stats_batch()

        logger.info("Player base stats...")
        player_base = get_all_player_base_stats()

        logger.info("Injuries...")
        injuries_by_team = fetch_official_injury_report()

        logger.info("Odds...")
        odds_by_game = fetch_odds_for_games(games)
        logger.info("Odds matched: %d/%d", len(odds_by_game), len(games))

        today_team_ids = set()
        for g in games:
            if g.get("home_team_id"): today_team_ids.add(int(g["home_team_id"]))
            if g.get("away_team_id"): today_team_ids.add(int(g["away_team_id"]))

        today_players = {
            pid: p for pid, p in player_base.items()
            if int(p.get("team_id", 0)) in today_team_ids
            and p.get("mins", 0) >= 10
        }
        logger.info("Rotation players: %d", len(today_players))

        all_legs  = []
        all_props = []
        enriched_games = []

        for game in games:
            home_id     = int(game.get("home_team_id") or 0)
            away_id     = int(game.get("away_team_id") or 0)
            home_abbrev = game.get("home_team_abbrev", "")
            away_abbrev = game.get("away_team_abbrev", "")
            home_name   = (game.get("home_team_city","")+" "+game.get("home_team","")).strip()
            away_name   = (game.get("away_team_city","")+" "+game.get("away_team","")).strip()

            if not home_id or not away_id:
                enriched_games.append(game)
                continue

            try:
                home_ctx = _build_context(
                    home_id, home_abbrev, home_name, True,
                    adv_stats, base_l5, base_l10, home_splits,
                    road_splits, all_players_adv, injuries_by_team
                )
                away_ctx = _build_context(
                    away_id, away_abbrev, away_name, False,
                    adv_stats, base_l5, base_l10, home_splits,
                    road_splits, all_players_adv, injuries_by_team
                )

                game_odds = odds_by_game.get(game["game_id"], {})
                enriched_games.append({
                    **game, **game_odds,
                    "home_name":     home_name,
                    "away_name":     away_name,
                    "home_injuries": home_ctx.get("injuries", []),
                    "away_injuries": away_ctx.get("injuries", []),
                })

                if game_odds.get("spread_line") is not None:
                    line = game_odds["spread_line"]
                    home_fav = line < 0
                    sr = score_spread_leg(
                        home_ctx, away_ctx, abs(line), home_fav,
                        game_odds.get("spread_odds", 1.91)
                    )
                    sel = (home_abbrev+" "+("%+.1f"%line)) if home_fav else (away_abbrev+" +"+("%.1f"%abs(line)))
                    all_legs.append({**sr, "game_id": game["game_id"],
                        "game": away_abbrev+" @ "+home_abbrev,
                        "selection": sel, "odds": game_odds.get("spread_odds", 1.91)})

                if game_odds.get("total_line") is not None:
                    tl = game_odds["total_line"]
                    tr = score_total_leg(home_ctx, away_ctx, tl, game_odds.get("total_odds", 1.91))
                    all_legs.append({**tr, "game_id": game["game_id"],
                        "game": away_abbrev+" @ "+home_abbrev,
                        "selection": tr["selection_direction"]+" "+str(tl),
                        "odds": game_odds.get("total_odds", 1.91)})

                for is_home, team_id, team_name, team_abbrev_local in [
                    (True,  home_id, home_name, home_abbrev),
                    (False, away_id, away_name, away_abbrev),
                ]:
                    team_ctx   = home_ctx if is_home else away_ctx
                    opp_ctx    = away_ctx if is_home else home_ctx
                    team_inj   = team_ctx["injuries"]
                    injury_map = {p["name"].lower(): p["status"] for p in team_inj}

                    team_pids = [
                        pid for pid, p in today_players.items()
                        if int(p.get("team_id", 0)) == team_id
                    ]
                    team_pids.sort(key=lambda pid: today_players[pid].get("mins", 0), reverse=True)

                    for pid in team_pids[:10]:
                        pdata = today_players[pid]
                        inj_status = injury_map.get(pdata["name"].lower(), "Available")
                        if inj_status == "Out":
                            continue

                        team_adv  = all_players_adv.get(team_id, [])
                        adv_match = next((p for p in team_adv if p["name"] == pdata["name"]), {})
                        player_dict = {
                            **pdata,
                            "usage_rate": adv_match.get("usage_rate", 0.15),
                            "minutes":    pdata.get("mins", 20.0),
                            "position":   pdata.get("position", "G"),
                        }

                        synthetic_logs = _season_avg_to_synthetic_logs(pdata)

                        try:
                            prop_result = project_player_props(
                                player=player_dict,
                                game_logs=synthetic_logs,
                                opp_advanced=opp_ctx["advanced"],
                                home_ctx=home_ctx, away_ctx=away_ctx,
                                player_is_home=is_home,
                                injury_status=inj_status,
                                teammate_injuries=team_inj,
                            )
                            if prop_result and prop_result["scored_props"]:
                                for prop in prop_result["scored_props"]:
                                    all_props.append({
                                        **prop,
                                        "game_id":   game["game_id"],
                                        "game":      away_abbrev+" @ "+home_abbrev,
                                        "team":      team_abbrev_local,
                                        "player_id": pid,
                                        "is_bench":  pdata.get("mins", 0) < 28,
                                    })
                        except Exception as e:
                            logger.warning("Props failed %s: %s", pdata.get("name"), e)

            except Exception as e:
                logger.warning("Game failed %s: %s", game.get("game_id"), e)
                enriched_games.append(game)

        logger.info("Game legs: %d | Props: %d", len(all_legs), len(all_props))

        top_prop_legs = []
        for p in sorted(all_props, key=lambda x: x.get("confidence", 0), reverse=True)[:12]:
            if p.get("confidence", 0) >= 62 and p.get("est_line") is not None:
                sel = p["player"]+" "+p["direction"]+" "+str(p["est_line"])+" "+p["stat_label"]
                top_prop_legs.append({
                    "game_id": p["game_id"], "game": p["game"],
                    "type": "Prop — "+p["stat_label"], "selection": sel,
                    "odds": 1.91, "confidence": p["confidence"], "prob": p["prob"],
                    "tags": p.get("tags", []), "reasoning": p.get("reasoning", ""),
                    "factors": [], "projected_margin": None, "projected_total": None,
                    "edge": p.get("edge"),
                })

        picks = build_multis(all_legs + top_prop_legs)
        all_props.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        return {
            "games":          enriched_games,
            "legs":           all_legs,
            "props":          all_props,
            "picks":          picks,
            "injuries":       injuries_by_team,
            "last_updated":   _now(),
            "games_analyzed": len(games),
            "props_scored":   len(all_props),
            "legs_scored":    len(all_legs),
            "_today_players":  today_players,
            "_today_team_ids": today_team_ids,
        }


def _season_avg_to_synthetic_logs(pdata: dict, n: int = 15) -> list:
    """
    Build synthetic game logs from season averages.
    Used for props AND as fallback for streaks when NBA.com is blocked.
    Uses realistic variance so streak hit rates reflect natural variation.
    """
    import random
    logs = []
    base = {
        "pts": pdata.get("pts", 0),
        "reb": pdata.get("reb", 0),
        "ast": pdata.get("ast", 0),
        "3pm": pdata.get("3pm", 0),
        "stl": pdata.get("stl", 0),
        "blk": pdata.get("blk", 0),
    }
    std_devs = {
        "pts": max(4.0, base["pts"] * 0.30),
        "reb": max(1.5, base["reb"] * 0.35),
        "ast": max(1.2, base["ast"] * 0.40),
        "3pm": max(0.8, base["3pm"] * 0.50),
        "stl": max(0.5, base["stl"] * 0.60),
        "blk": max(0.4, base["blk"] * 0.60),
    }
    for _ in range(n):
        log = {}
        for stat, avg in base.items():
            if avg <= 0:
                log[stat] = 0
                continue
            variance = std_devs[stat]
            val = avg + random.uniform(-variance, variance)
            log[stat] = max(0, round(val))
        log["mins"] = pdata.get("mins", 20.0)
        logs.append(log)
    return logs


# ─── STREAK CACHE ─────────────────────────────────────────────────────────────
class StreakCache:
    """
    Independent cache for streak data.
    Tries real NBA.com game logs first. Falls back to synthetic logs
    from season averages if NBA.com is blocked (common on AWS/Render).
    Runs in background — never blocks the main cache.
    """
    def __init__(self):
        self._lock           = threading.Lock()
        self._data           = []
        self._last_refresh   = None
        self._loading        = False
        self._used_real_logs = False

    def get(self, force_refresh=False):
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            stale = age > STREAK_TTL or not self._last_refresh
            should_refresh = (force_refresh or stale) and not self._loading

        if should_refresh:
            self._trigger_background_refresh()

        with self._lock:
            return {
                "streaks":        self._data,
                "loading":        self._loading,
                "last_updated":   self._last_refresh.strftime("%I:%M %p") if self._last_refresh else None,
                "used_real_logs": self._used_real_logs,
            }

    def _trigger_background_refresh(self):
        with self._lock:
            if self._loading:
                return
            self._loading = True
        t = threading.Thread(target=self._background_fetch, daemon=True)
        t.start()
        logger.info("Streak background fetch started")

    def _background_fetch(self):
        try:
            from player_logs import get_player_game_logs_batch
            from streak_engine import calculate_streaks

            main_data = cache.get()
            today_players  = main_data.get("_today_players", {})
            today_team_ids = main_data.get("_today_team_ids", set())

            if not today_players:
                logger.warning("Streak fetch: no players in main cache yet")
                with self._lock:
                    self._loading = False
                return

            # Build player list — top 10 per team
            players_to_fetch = []
            for team_id in today_team_ids:
                team_pids = [
                    (pid, p) for pid, p in today_players.items()
                    if int(p.get("team_id", 0)) == team_id
                ]
                team_pids.sort(key=lambda x: x[1].get("mins", 0), reverse=True)
                players_to_fetch.extend([pid for pid, _ in team_pids[:10]])

            # ── Try real NBA.com game logs ────────────────────────────────
            real_logs = {}
            try:
                logger.info("Streak: trying real NBA.com logs for %d players...", len(players_to_fetch))
                real_logs = get_player_game_logs_batch(players_to_fetch, last_n=15)
                logger.info("Streak: got real logs for %d players", len(real_logs))
            except Exception as e:
                logger.warning("Streak: real logs failed (%s), falling back to synthetic", e)

            # ── Build final log dict: real where available, synthetic otherwise ──
            player_logs = {}
            real_count = 0
            synthetic_count = 0
            for pid in players_to_fetch:
                if pid in real_logs and len(real_logs[pid]) >= 3:
                    player_logs[pid] = real_logs[pid]
                    real_count += 1
                else:
                    pdata = today_players.get(pid, {})
                    if pdata:
                        player_logs[pid] = _season_avg_to_synthetic_logs(pdata, n=15)
                        synthetic_count += 1

            used_real = real_count > synthetic_count
            logger.info(
                "Streak: %d real, %d synthetic (real_dominant=%s)",
                real_count, synthetic_count, used_real
            )

            streaks = calculate_streaks(
                player_base=today_players,
                player_logs=player_logs,
                today_team_ids=today_team_ids,
                windows=[5, 10, 15],
            )
            logger.info("Streak: %d streaks calculated", len(streaks))

            with self._lock:
                self._data           = streaks
                self._last_refresh   = datetime.now()
                self._loading        = False
                self._used_real_logs = used_real

        except Exception as e:
            logger.error("Streak background fetch failed entirely: %s", e)
            # Last-resort: pure synthetic from what we already have
            try:
                from streak_engine import calculate_streaks
                main_data      = cache.get()
                today_players  = main_data.get("_today_players", {})
                today_team_ids = main_data.get("_today_team_ids", set())
                if today_players:
                    player_logs = {
                        pid: _season_avg_to_synthetic_logs(pdata, n=15)
                        for pid, pdata in today_players.items()
                    }
                    streaks = calculate_streaks(
                        player_base=today_players,
                        player_logs=player_logs,
                        today_team_ids=today_team_ids,
                        windows=[5, 10, 15],
                    )
                    logger.info("Streak fallback: %d streaks from pure synthetic", len(streaks))
                    with self._lock:
                        self._data           = streaks
                        self._last_refresh   = datetime.now()
                        self._loading        = False
                        self._used_real_logs = False
                    return
            except Exception as e2:
                logger.error("Streak pure-synthetic fallback also failed: %s", e2)
            with self._lock:
                self._loading = False


# ─── SHARED HELPERS ───────────────────────────────────────────────────────────
def _build_context(team_id, abbrev, full_name, is_home,
                   adv_stats, base_l5, base_l10, home_splits,
                   road_splits, all_players_adv, injuries_by_team):
    from injury_report import get_injury_impact_score
    advanced   = adv_stats.get(team_id, {})
    recent_l5  = base_l5.get(team_id, {})
    recent_l10 = base_l10.get(team_id, {})
    players    = all_players_adv.get(team_id, [])
    splits     = {"home": home_splits.get(team_id, {}), "road": road_splits.get(team_id, {})}
    rest       = {"rest_days": 2, "is_b2b": False}
    team_inj   = injuries_by_team.get(full_name) or injuries_by_team.get(abbrev) or []
    return {
        "team_id": team_id, "team_abbrev": abbrev, "team_name": full_name,
        "is_home": is_home, "advanced": advanced,
        "recent_l5": recent_l5, "recent_l10": recent_l10,
        "game_logs": [], "rest": rest, "splits": splits,
        "players": players, "injuries": team_inj,
        "injury_impact": get_injury_impact_score(team_inj, players),
        "h2h": [],
    }


def _empty_picks():
    e = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**e, "key": "safe",  "label": "Safe Multi",    "accentColor": "#4CAF7D", "subtitle": "No games today"},
        "mid":   {**e, "key": "mid",   "label": "Mid-Risk Multi","accentColor": "#C9A84C", "subtitle": "No games today"},
        "lotto": {**e, "key": "lotto", "label": "Lotto Multi",   "accentColor": "#E05252", "subtitle": "No games today"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


# ─── SINGLETONS ───────────────────────────────────────────────────────────────
cache        = MainCache()
streak_cache = StreakCache()
