# cache.py — Resilient: always returns something, even when NBA.com is down

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

MAIN_TTL   = 300
STREAK_TTL = 1800


class MainCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = {}
        self._last_refresh = None

    def get(self, force_refresh=False) -> dict:
        with self._lock:
            age   = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            stale = force_refresh or age > MAIN_TTL or not self._data
            if stale:
                logger.info("Main cache refreshing...")
                try:
                    fresh = self._fetch_all()
                    if fresh:
                        self._data         = fresh
                        self._last_refresh = datetime.now()
                        # Save to disk
                        try:
                            from data_store import save_main_data
                            save_main_data(fresh)
                        except Exception:
                            pass
                    else:
                        # Fetch returned nothing — try disk
                        if not self._data:
                            self._data = self._load_from_disk()
                        if not self._data:
                            self._data = self._games_only_fallback()
                        self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error("Main cache failed: %s", e)
                    if not self._data:
                        self._data = self._load_from_disk()
                    if not self._data:
                        self._data = self._games_only_fallback()
                    self._last_refresh = datetime.now()
        return self._data

    def _load_from_disk(self) -> dict:
        try:
            from data_store import load_main_data, get_data_age_str
            data = load_main_data()
            if data:
                age_str = get_data_age_str()
                data["_stale"]        = True
                data["_stale_reason"] = "Showing cached data from " + age_str
                logger.info("Loaded main data from disk (%s)", age_str)
                return data
        except Exception as e:
            logger.warning("Disk load failed: %s", e)
        return {}

    def _games_only_fallback(self) -> dict:
        """
        Last resort — fetch today's games and odds even without NBA stats.
        This always works since ESPN and Odds API are independent of NBA.com.
        """
        logger.info("Using games-only fallback (NBA.com unavailable)")
        try:
            from nba_data import get_today_games
            from odds_fetcher import fetch_odds_for_games
            from engine import build_multis

            games = get_today_games()
            if not games:
                games = []

            odds_by_game = fetch_odds_for_games(games) if games else {}

            enriched = []
            for g in games:
                game_odds = odds_by_game.get(g["game_id"], {})
                enriched.append({**g, **game_odds, "home_injuries": [], "away_injuries": []})

            # Build empty picks structure
            picks = build_multis([])

            return {
                "games":          enriched,
                "legs":           [],
                "props":          [],
                "picks":          picks,
                "injuries":       {},
                "last_updated":   _now(),
                "games_analyzed": len(games),
                "props_scored":   0,
                "legs_scored":    0,
                "_stale":         True,
                "_stale_reason":  "NBA.com unavailable — showing games and odds only. Player stats will load when NBA.com recovers.",
                "_today_players":  {},
                "_today_team_ids": set(),
            }
        except Exception as e:
            logger.error("Games-only fallback failed: %s", e)
            return {
                "games": [], "picks": _empty_picks(), "injuries": {},
                "legs": [], "props": [], "last_updated": _now(),
                "games_analyzed": 0, "props_scored": 0, "legs_scored": 0,
                "_stale": True,
                "_stale_reason": "Data unavailable — NBA.com is down. Check back later.",
                "_today_players": {}, "_today_team_ids": set(),
            }

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

        logger.info("=== Main cache fetch ===")

        games = get_today_games()
        logger.info("Games: %d", len(games))
        if not games:
            return None

        logger.info("League stats...")
        adv_stats       = get_all_team_stats_batch("Advanced")
        base_l10        = get_all_team_recent_batch(10)
        base_l5         = get_all_team_recent_batch(5)
        home_splits     = get_all_team_stats_batch("Base", location="Home")
        road_splits     = get_all_team_stats_batch("Base", location="Road")
        all_players_adv = get_all_player_stats_batch()

        logger.info("Player base stats...")
        player_base = get_all_player_base_stats()

        if not player_base:
            logger.warning("Player base stats empty — falling back to games-only")
            return None   # Triggers fallback in get()

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
            if int(p.get("team_id", 0)) in today_team_ids and p.get("mins", 0) >= 10
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
                    "home_name":     home_name, "away_name": away_name,
                    "home_injuries": home_ctx.get("injuries", []),
                    "away_injuries": away_ctx.get("injuries", []),
                })

                if game_odds.get("spread_line") is not None:
                    line = game_odds["spread_line"]
                    home_fav = line < 0
                    sr  = score_spread_leg(home_ctx, away_ctx, abs(line), home_fav,
                                          game_odds.get("spread_odds", 1.91))
                    sel = (home_abbrev+" "+("%+.1f"%line)) if home_fav else (away_abbrev+" +"+("%.1f"%abs(line)))
                    all_legs.append({**sr,"game_id":game["game_id"],
                        "game":away_abbrev+" @ "+home_abbrev,"selection":sel,
                        "odds":game_odds.get("spread_odds",1.91)})

                if game_odds.get("total_line") is not None:
                    tl = game_odds["total_line"]
                    tr = score_total_leg(home_ctx, away_ctx, tl, game_odds.get("total_odds", 1.91))
                    all_legs.append({**tr,"game_id":game["game_id"],
                        "game":away_abbrev+" @ "+home_abbrev,
                        "selection":tr["selection_direction"]+" "+str(tl),
                        "odds":game_odds.get("total_odds",1.91)})

                for is_home, team_id, team_name, ta in [
                    (True, home_id, home_name, home_abbrev),
                    (False, away_id, away_name, away_abbrev),
                ]:
                    tc       = home_ctx if is_home else away_ctx
                    oc       = away_ctx if is_home else home_ctx
                    team_inj = tc["injuries"]
                    inj_map  = {p["name"].lower(): p["status"] for p in team_inj}

                    tpids = sorted(
                        [pid for pid, p in today_players.items() if int(p.get("team_id",0))==team_id],
                        key=lambda pid: today_players[pid].get("mins",0), reverse=True
                    )
                    for pid in tpids[:10]:
                        pdata = today_players[pid]
                        inj   = inj_map.get(pdata["name"].lower(), "Available")
                        if inj == "Out":
                            continue
                        tadv     = all_players_adv.get(team_id, [])
                        am       = next((p for p in tadv if p["name"]==pdata["name"]), {})
                        pd       = {**pdata, "usage_rate":am.get("usage_rate",0.15),
                                    "minutes":pdata.get("mins",20.0), "position":pdata.get("position","G")}
                        syn_logs = _synthetic_logs(pdata)
                        try:
                            pr = project_player_props(
                                player=pd, game_logs=syn_logs,
                                opp_advanced=oc["advanced"],
                                home_ctx=home_ctx, away_ctx=away_ctx,
                                player_is_home=is_home, injury_status=inj,
                                teammate_injuries=team_inj,
                            )
                            if pr and pr["scored_props"]:
                                for prop in pr["scored_props"]:
                                    all_props.append({**prop,
                                        "game_id":game["game_id"],
                                        "game":away_abbrev+" @ "+home_abbrev,
                                        "team":ta, "player_id":pid,
                                        "is_bench":pdata.get("mins",0)<28})
                        except Exception:
                            pass

            except Exception as e:
                logger.warning("Game failed %s: %s", game.get("game_id"), e)
                enriched_games.append(game)

        logger.info("Legs: %d | Props: %d", len(all_legs), len(all_props))

        top_prop_legs = []
        for p in sorted(all_props, key=lambda x: x.get("confidence",0), reverse=True)[:12]:
            if p.get("confidence",0) >= 62 and p.get("est_line") is not None:
                sel = p["player"]+" "+p["direction"]+" "+str(p["est_line"])+" "+p["stat_label"]
                top_prop_legs.append({
                    "game_id":p["game_id"],"game":p["game"],
                    "type":"Prop — "+p["stat_label"],"selection":sel,
                    "odds":1.91,"confidence":p["confidence"],"prob":p["prob"],
                    "tags":p.get("tags",[]),"reasoning":p.get("reasoning",""),
                    "factors":[],"projected_margin":None,"projected_total":None,"edge":p.get("edge"),
                })

        picks = build_multis(all_legs + top_prop_legs)
        all_props.sort(key=lambda x: x.get("confidence",0), reverse=True)

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
            "_stale":         False,
            "_stale_reason":  "",
            "_today_players":  today_players,
            "_today_team_ids": today_team_ids,
        }


def _synthetic_logs(pdata, n=10):
    import random
    logs = []
    base = {s: pdata.get(s, 0) for s in ["pts","reb","ast","3pm","stl","blk"]}
    for _ in range(n):
        log = {stat: max(0, round(avg + random.uniform(-avg*0.15, avg*0.15)))
               for stat, avg in base.items()}
        log["mins"] = pdata.get("mins", 20.0)
        logs.append(log)
    return logs


class StreakCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = []
        self._last_refresh = None
        self._loading      = False

    def get(self, force_refresh=False):
        with self._lock:
            age   = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            stale = age > STREAK_TTL or not self._last_refresh
            should = (force_refresh or stale) and not self._loading

        if should:
            # Load from disk immediately if we have nothing to show
            with self._lock:
                if not self._data:
                    try:
                        from data_store import load_streak_data
                        disk = load_streak_data()
                        if disk:
                            self._data = disk
                            logger.info("Loaded %d streaks from disk", len(disk))
                    except Exception:
                        pass
            self._trigger_bg()

        with self._lock:
            return {
                "streaks":     self._data,
                "loading":     self._loading,
                "last_updated": self._last_refresh.strftime("%I:%M %p") if self._last_refresh else None,
            }

    def _trigger_bg(self):
        with self._lock:
            if self._loading:
                return
            self._loading = True
        threading.Thread(target=self._bg_fetch, daemon=True).start()
        logger.info("Streak background fetch started")

    def _bg_fetch(self):
        try:
            from player_logs import get_player_game_logs_batch
            from streak_engine import calculate_streaks
            main_data      = cache.get()
            today_players  = main_data.get("_today_players", {})
            today_team_ids = main_data.get("_today_team_ids", set())
            if not today_players:
                logger.warning("Streak fetch: no players in main cache")
                with self._lock:
                    self._loading = False
                return
            players_to_fetch = []
            for tid in today_team_ids:
                tp = sorted(
                    [(pid, p) for pid, p in today_players.items() if int(p.get("team_id",0))==tid],
                    key=lambda x: x[1].get("mins",0), reverse=True
                )
                players_to_fetch.extend([pid for pid, _ in tp[:10]])
            logger.info("Streak: fetching logs for %d players", len(players_to_fetch))
            player_logs = get_player_game_logs_batch(players_to_fetch, last_n=15)
            logger.info("Streak: got logs for %d players", len(player_logs))
            streaks = calculate_streaks(
                player_base=today_players,
                player_logs=player_logs,
                today_team_ids=today_team_ids,
                windows=[5, 10, 15],
            )
            logger.info("Streak: %d streaks", len(streaks))
            with self._lock:
                self._data        = streaks
                self._last_refresh = datetime.now()
                self._loading     = False
            try:
                from data_store import save_streak_data
                save_streak_data(streaks)
            except Exception:
                pass
        except Exception as e:
            logger.error("Streak fetch failed: %s", e)
            with self._lock:
                self._loading = False


def _build_context(team_id, abbrev, full_name, is_home,
                   adv_stats, base_l5, base_l10, home_splits,
                   road_splits, all_players_adv, injuries_by_team):
    from injury_report import get_injury_impact_score
    players  = all_players_adv.get(team_id, [])
    team_inj = injuries_by_team.get(full_name) or injuries_by_team.get(abbrev) or []
    return {
        "team_id": team_id, "team_abbrev": abbrev, "team_name": full_name,
        "is_home": is_home,
        "advanced":   adv_stats.get(team_id, {}),
        "recent_l5":  base_l5.get(team_id, {}),
        "recent_l10": base_l10.get(team_id, {}),
        "game_logs": [], "rest": {"rest_days":2,"is_b2b":False},
        "splits": {"home":home_splits.get(team_id,{}),"road":road_splits.get(team_id,{})},
        "players": players, "injuries": team_inj,
        "injury_impact": get_injury_impact_score(team_inj, players),
        "h2h": [],
    }


def _empty_picks():
    e = {"legs":[],"odds":"N/A","hitProb":0,"risks":[],"alts":[]}
    return {
        "safe":  {**e,"key":"safe", "label":"Safe Multi",    "accentColor":"#4CAF7D","subtitle":"No games today"},
        "mid":   {**e,"key":"mid",  "label":"Mid-Risk Multi","accentColor":"#C9A84C","subtitle":"No games today"},
        "lotto": {**e,"key":"lotto","label":"Lotto Multi",   "accentColor":"#E05252","subtitle":"No games today"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


cache        = MainCache()
streak_cache = StreakCache()
