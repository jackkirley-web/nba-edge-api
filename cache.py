# cache.py — Top 10 players per team, on-demand refresh

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

# Short TTL — data refreshes whenever user requests it
# Minimum 5 min between auto-refreshes to avoid hammering APIs
CACHE_TTL_SECONDS = 300


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
                    logger.error("Cache refresh failed: %s", e)
                    if not self._data:
                        self._data = {
                            "error": str(e), "games": [],
                            "picks": _empty_picks(), "injuries": {},
                            "legs": [], "props": [],
                            "last_updated": _now(), "games_analyzed": 0,
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from nba_data import (
            get_today_games, get_all_team_stats_batch,
            get_all_team_recent_batch, get_all_player_stats_batch,
        )
        from player_logs import get_all_player_base_stats, get_player_game_logs_batch
        from injury_report import fetch_official_injury_report, get_injury_impact_score
        from engine import score_spread_leg, score_total_leg, build_multis
        from props_engine import project_player_props
        from odds_fetcher import fetch_odds_for_games

        logger.info("=== Starting full data refresh ===")

        # ── Step 1: Today's upcoming games ────────────────────
        games = get_today_games()
        logger.info("Games today: %d", len(games))

        if not games:
            return {
                "games": [], "picks": _empty_picks(), "injuries": {},
                "legs": [], "props": [], "last_updated": _now(),
                "games_analyzed": 0, "refresh_note": "No games found for today",
            }

        # ── Step 2: League stats (6 fast batch calls) ─────────
        logger.info("Fetching league stats...")
        adv_stats       = get_all_team_stats_batch("Advanced")
        base_l10        = get_all_team_recent_batch(10)
        base_l5         = get_all_team_recent_batch(5)
        home_splits     = get_all_team_stats_batch("Base", location="Home")
        road_splits     = get_all_team_stats_batch("Base", location="Road")
        all_players_adv = get_all_player_stats_batch()

        # ── Step 3: Player base stats (1 call) ────────────────
        logger.info("Fetching player base stats...")
        player_base = get_all_player_base_stats()

        # ── Step 4: Injuries ──────────────────────────────────
        logger.info("Fetching injuries...")
        injuries_by_team = fetch_official_injury_report()

        # ── Step 5: Odds ──────────────────────────────────────
        logger.info("Fetching odds...")
        odds_by_game = fetch_odds_for_games(games)
        logger.info("Odds matched for %d/%d games", len(odds_by_game), len(games))

        # ── Step 6: Build team contexts ───────────────────────
        today_team_ids = set()
        for g in games:
            home_id = g.get("home_team_id")
            away_id = g.get("away_team_id")
            if home_id: today_team_ids.add(int(home_id))
            if away_id: today_team_ids.add(int(away_id))

        # Rotation players on today's teams (min 10+ min/game)
        today_players = {
            pid: p for pid, p in player_base.items()
            if int(p.get("team_id", 0)) in today_team_ids
            and p.get("mins", 0) >= 10
        }
        logger.info("Rotation players for today: %d", len(today_players))

        # ── Step 7: Game logs for top 10 per team ────────────
        # 10 players × up to 30 teams × 15 games per player
        # ~300 API calls at 0.6s each ≈ 3 min. We batch smartly.
        players_to_fetch = []
        for team_id in today_team_ids:
            team_players = [
                (pid, p) for pid, p in today_players.items()
                if int(p.get("team_id", 0)) == team_id
            ]
            # Sort by minutes — top 10 = starters + key bench
            team_players.sort(key=lambda x: x[1].get("mins", 0), reverse=True)
            players_to_fetch.extend([pid for pid, _ in team_players[:10]])

        logger.info("Fetching game logs for %d players (top 10 per team)...", len(players_to_fetch))
        player_logs = get_player_game_logs_batch(players_to_fetch, last_n=15)
        logger.info("Got logs for %d players", len(player_logs))

        # ── Step 8: Score everything ──────────────────────────
        all_legs  = []
        all_props = []
        enriched_games = []

        for game in games:
            home_id   = int(game.get("home_team_id") or 0)
            away_id   = int(game.get("away_team_id") or 0)
            home_abbrev = game.get("home_team_abbrev", "")
            away_abbrev = game.get("away_team_abbrev", "")
            home_name = (game.get("home_team_city", "") + " " + game.get("home_team", "")).strip()
            away_name = (game.get("away_team_city", "") + " " + game.get("away_team", "")).strip()

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
                    "home_name": home_name,
                    "away_name": away_name,
                    "home_injuries": home_ctx.get("injuries", []),
                    "away_injuries": away_ctx.get("injuries", []),
                })

                # ── Score spread ──────────────────────────────
                if game_odds.get("spread_line") is not None:
                    line     = game_odds["spread_line"]
                    home_fav = line < 0
                    sr = score_spread_leg(
                        home_ctx, away_ctx, abs(line), home_fav,
                        game_odds.get("spread_odds", 1.91)
                    )
                    if home_fav:
                        sel = home_abbrev + " " + ("%+.1f" % line)
                    else:
                        sel = away_abbrev + " +" + ("%.1f" % abs(line))
                    all_legs.append({
                        **sr,
                        "game_id":   game["game_id"],
                        "game":      away_abbrev + " @ " + home_abbrev,
                        "selection": sel,
                        "odds":      game_odds.get("spread_odds", 1.91),
                    })

                # ── Score total ───────────────────────────────
                if game_odds.get("total_line") is not None:
                    tl = game_odds["total_line"]
                    tr = score_total_leg(
                        home_ctx, away_ctx, tl,
                        game_odds.get("total_odds", 1.91)
                    )
                    all_legs.append({
                        **tr,
                        "game_id":   game["game_id"],
                        "game":      away_abbrev + " @ " + home_abbrev,
                        "selection": tr["selection_direction"] + " " + str(tl),
                        "odds":      game_odds.get("total_odds", 1.91),
                    })

                # ── Score props for both teams ────────────────
                for is_home, team_id, team_name, team_abbrev_local in [
                    (True,  home_id, home_name, home_abbrev),
                    (False, away_id, away_name, away_abbrev),
                ]:
                    team_ctx     = home_ctx if is_home else away_ctx
                    opp_ctx      = away_ctx if is_home else home_ctx
                    team_inj     = team_ctx["injuries"]
                    injury_map   = {p["name"].lower(): p["status"] for p in team_inj}

                    # Top 10 players for this team
                    team_pids = [
                        pid for pid, p in today_players.items()
                        if int(p.get("team_id", 0)) == team_id
                    ]
                    team_pids.sort(
                        key=lambda pid: today_players[pid].get("mins", 0),
                        reverse=True
                    )

                    for pid in team_pids[:10]:
                        pdata    = today_players[pid]
                        logs     = player_logs.get(pid, [])
                        if not logs:
                            continue

                        inj_status = injury_map.get(pdata["name"].lower(), "Available")
                        if inj_status == "Out":
                            continue  # Skip confirmed-out players

                        # Get usage from advanced stats
                        team_adv = all_players_adv.get(team_id, [])
                        adv_match = next(
                            (p for p in team_adv if p["name"] == pdata["name"]), {}
                        )

                        player_dict = {
                            **pdata,
                            "usage_rate": adv_match.get("usage_rate", 0.15),
                            "minutes":    pdata.get("mins", 20.0),
                            "position":   pdata.get("position", "G"),
                        }

                        try:
                            prop_result = project_player_props(
                                player=player_dict,
                                game_logs=logs,
                                opp_advanced=opp_ctx["advanced"],
                                home_ctx=home_ctx,
                                away_ctx=away_ctx,
                                player_is_home=is_home,
                                injury_status=inj_status,
                                teammate_injuries=team_inj,
                            )
                            if prop_result and prop_result["scored_props"]:
                                for prop in prop_result["scored_props"]:
                                    all_props.append({
                                        **prop,
                                        "game_id":   game["game_id"],
                                        "game":      away_abbrev + " @ " + home_abbrev,
                                        "team":      team_abbrev_local,
                                        "player_id": pid,
                                        # Flag if this is a bench player
                                        "is_bench": pdata.get("mins", 0) < 28,
                                    })
                        except Exception as e:
                            logger.warning(
                                "Props failed for %s: %s",
                                pdata.get("name", "unknown"), e
                            )

            except Exception as e:
                logger.warning("Game analysis failed %s: %s", game.get("game_id"), e)
                enriched_games.append(game)

        logger.info(
            "=== Complete: %d game legs + %d props ===",
            len(all_legs), len(all_props)
        )

        # Top props for multis
        top_props = sorted(
            [p for p in all_props
             if p.get("confidence", 0) >= 62 and p.get("est_line") is not None],
            key=lambda x: x["confidence"], reverse=True
        )[:12]

        prop_legs = []
        for p in top_props:
            sel = (p["player"] + " " + p["direction"] + " " +
                   str(p["est_line"]) + " " + p["stat_label"])
            prop_legs.append({
                "game_id":    p["game_id"],
                "game":       p["game"],
                "type":       "Prop — " + p["stat_label"],
                "selection":  sel,
                "odds":       1.91,
                "confidence": p["confidence"],
                "prob":       p["prob"],
                "tags":       p.get("tags", []),
                "reasoning":  p.get("reasoning", ""),
                "factors":    [],
                "projected_margin": None,
                "projected_total":  None,
                "edge":       p.get("edge"),
            })

        picks = build_multis(all_legs + prop_legs)
        all_props.sort(key=lambda x: x["confidence"], reverse=True)

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
        }


def _build_context(team_id, abbrev, full_name, is_home,
                   adv_stats, base_l5, base_l10, home_splits,
                   road_splits, all_players_adv, injuries_by_team):
    from injury_report import get_injury_impact_score
    advanced   = adv_stats.get(team_id, {})
    recent_l5  = base_l5.get(team_id, {})
    recent_l10 = base_l10.get(team_id, {})
    players    = all_players_adv.get(team_id, [])
    splits     = {
        "home": home_splits.get(team_id, {}),
        "road": road_splits.get(team_id, {}),
    }
    rest = {"rest_days": 2, "is_b2b": False}

    # Try multiple name formats for injury lookup
    team_injuries = (
        injuries_by_team.get(full_name) or
        injuries_by_team.get(abbrev) or []
    )
    injury_impact = get_injury_impact_score(team_injuries, players)

    return {
        "team_id":      team_id,
        "team_abbrev":  abbrev,
        "team_name":    full_name,
        "is_home":      is_home,
        "advanced":     advanced,
        "recent_l5":    recent_l5,
        "recent_l10":   recent_l10,
        "game_logs":    [],
        "rest":         rest,
        "splits":       splits,
        "players":      players,
        "injuries":     team_injuries,
        "injury_impact": injury_impact,
        "h2h":          [],
    }


def _empty_picks():
    empty = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**empty, "key": "safe",  "label": "Safe Multi",
                  "emoji": "🔵", "accentColor": "#30D158", "subtitle": "No games today"},
        "mid":   {**empty, "key": "mid",   "label": "Mid-Risk Multi",
                  "emoji": "🟡", "accentColor": "#FF9F0A", "subtitle": "No games today"},
        "lotto": {**empty, "key": "lotto", "label": "Lotto Multi",
                  "emoji": "🔴", "accentColor": "#FF453A", "subtitle": "No games today"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


cache = NBACache()
