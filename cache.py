# cache.py — Full pipeline including player props

import logging
import threading
import math
from datetime import datetime

logger = logging.getLogger(__name__)
CACHE_TTL_SECONDS = 1800


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
                            "error": str(e), "games": [], "picks": _empty_picks(),
                            "injuries": {}, "legs": [], "props": [],
                            "last_updated": _now(), "games_analyzed": 0,
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from nba_data import (
            get_today_games, get_all_team_stats_batch, get_all_team_recent_batch,
            get_all_player_stats_batch,
        )
        from player_logs import get_all_player_base_stats, get_player_game_logs_batch
        from injury_report import fetch_official_injury_report, get_injury_impact_score
        from engine import score_spread_leg, score_total_leg, build_multis
        from props_engine import project_player_props
        from odds_fetcher import fetch_odds_for_games

        logger.info("Starting full data refresh with props...")

        # ── Games ──────────────────────────────────────────────
        games = get_today_games()
        logger.info(f"Found {len(games)} games today")
        if not games:
            return {"games": [], "picks": _empty_picks(), "injuries": {},
                    "legs": [], "props": [], "last_updated": _now(), "games_analyzed": 0}

        # ── League-wide batch calls ────────────────────────────
        logger.info("Fetching league stats...")
        adv_stats   = get_all_team_stats_batch("Advanced")
        base_l10    = get_all_team_recent_batch(10)
        base_l5     = get_all_team_recent_batch(5)
        home_splits = get_all_team_stats_batch("Base", location="Home")
        road_splits = get_all_team_stats_batch("Base", location="Road")
        all_players_adv = get_all_player_stats_batch()  # {team_id: [players]}

        # ── Player base stats ─────────────────────────────────
        logger.info("Fetching player base stats...")
        player_base = get_all_player_base_stats()  # {player_id: stats}

        # ── Injuries ──────────────────────────────────────────
        logger.info("Fetching injuries...")
        injuries_by_team = fetch_official_injury_report()

        # ── Odds ──────────────────────────────────────────────
        logger.info("Fetching odds...")
        odds_by_game = fetch_odds_for_games(games)

        # ── Get today's team rosters ───────────────────────────
        # Map team_id → list of player_ids playing today
        today_team_ids = set()
        for g in games:
            today_team_ids.add(int(g["home_team_id"]))
            today_team_ids.add(int(g["away_team_id"]))

        # Filter players to only those on teams playing today
        today_players = {
            pid: pdata for pid, pdata in player_base.items()
            if int(pdata.get("team_id", 0)) in today_team_ids
            and pdata.get("mins", 0) >= 10  # Only real rotation players
        }
        logger.info(f"Found {len(today_players)} rotation players for today's games")

        # ── Fetch player game logs (batch, top players only) ───
        # Sort by minutes to get starters/key bench first
        # Limit to top 8 players per team to stay within time budget
        players_to_fetch = []
        for team_id in today_team_ids:
            team_players = [
                (pid, p) for pid, p in today_players.items()
                if int(p.get("team_id", 0)) == team_id
            ]
            team_players.sort(key=lambda x: x[1].get("mins", 0), reverse=True)
            players_to_fetch.extend([pid for pid, _ in team_players[:8]])

        logger.info(f"Fetching game logs for {len(players_to_fetch)} players...")
        player_logs = get_player_game_logs_batch(players_to_fetch, last_n=15)
        logger.info(f"Got logs for {len(player_logs)} players")

        # ── Score all games ────────────────────────────────────
        all_legs  = []
        all_props = []
        enriched_games = []

        for game in games:
            home_id   = int(game["home_team_id"])
            away_id   = int(game["away_team_id"])
            home_name = f"{game['home_team_city']} {game['home_team']}"
            away_name = f"{game['away_team_city']} {game['away_team']}"

            home_ctx = _build_context(
                home_id, game["home_team_abbrev"], home_name, True,
                adv_stats, base_l5, base_l10, home_splits, road_splits,
                all_players_adv, injuries_by_team
            )
            away_ctx = _build_context(
                away_id, game["away_team_abbrev"], away_name, False,
                adv_stats, base_l5, base_l10, home_splits, road_splits,
                all_players_adv, injuries_by_team
            )

            game_odds = odds_by_game.get(game["game_id"], {})
            enriched_game = {
                **game, **game_odds,
                "home_name": home_name, "away_name": away_name,
                "home_injuries": home_ctx.get("injuries", []),
                "away_injuries": away_ctx.get("injuries", []),
            }
            enriched_games.append(enriched_game)

            # Score spread + total
            if game_odds.get("spread_line") is not None:
                line = game_odds["spread_line"]
                home_fav = line < 0
                sr = score_spread_leg(home_ctx, away_ctx, abs(line), home_fav, game_odds.get("spread_odds", 1.91))
                h, a = game["home_team_abbrev"], game["away_team_abbrev"]
                all_legs.append({
                    **sr, "game_id": game["game_id"], "game": f"{a} @ {h}",
                    "selection": f"{h} {line:+.1f}" if home_fav else f"{a} +{abs(line):.1f}",
                    "odds": game_odds.get("spread_odds", 1.91),
                })

            if game_odds.get("total_line") is not None:
                tl = game_odds["total_line"]
                tr = score_total_leg(home_ctx, away_ctx, tl, game_odds.get("total_odds", 1.91))
                all_legs.append({
                    **tr, "game_id": game["game_id"],
                    "game": f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}",
                    "selection": f"{tr['selection_direction']} {tl}",
                    "odds": game_odds.get("total_odds", 1.91),
                })

            # ── Score props for both teams ─────────────────────
            for is_home, team_id, team_name, team_abbrev in [
                (True,  home_id, home_name, game["home_team_abbrev"]),
                (False, away_id, away_name, game["away_team_abbrev"]),
            ]:
                team_injuries = home_ctx["injuries"] if is_home else away_ctx["injuries"]
                injury_map = {p["name"].lower(): p["status"] for p in team_injuries}

                # Get top 8 players for this team
                team_pids = [
                    pid for pid, p in today_players.items()
                    if int(p.get("team_id", 0)) == team_id
                ]
                team_pids.sort(
                    key=lambda pid: today_players[pid].get("mins", 0),
                    reverse=True
                )

                for pid in team_pids[:8]:
                    pdata = today_players[pid]
                    logs  = player_logs.get(pid, [])
                    if not logs:
                        continue

                    inj_status = injury_map.get(pdata["name"].lower(), "Available")

                    # Build player dict with usage from advanced stats
                    team_adv_players = all_players_adv.get(team_id, [])
                    adv_match = next(
                        (p for p in team_adv_players if p["name"] == pdata["name"]), {}
                    )
                    player_dict = {
                        **pdata,
                        "usage_rate": adv_match.get("usage_rate", 0.18),
                        "minutes":    pdata.get("mins", 28.0),
                        "position":   pdata.get("position", "G"),
                    }

                    result = project_player_props(
                        player=player_dict,
                        game_logs=logs,
                        opp_advanced=away_ctx["advanced"] if is_home else home_ctx["advanced"],
                        home_ctx=home_ctx,
                        away_ctx=away_ctx,
                        player_is_home=is_home,
                        injury_status=inj_status,
                        teammate_injuries=team_injuries,
                    )

                    if result and result["scored_props"]:
                        for prop in result["scored_props"]:
                            all_props.append({
                                **prop,
                                "game_id":      game["game_id"],
                                "game":         f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}",
                                "team":         team_abbrev,
                                "player_id":    pid,
                            })

        # ── Build multis (now includes props) ─────────────────
        logger.info(f"Scored {len(all_legs)} game legs + {len(all_props)} prop legs")

        # Top props to include in multis (confidence >= 62)
        top_props_for_multis = [
            {
                "game_id":    p["game_id"],
                "game":       p["game"],
                "type":       f"Prop — {p['stat_label']}",
                "selection":  f"{p['player']} {p['direction']} {p['est_line']} {p['stat_label']}" if p['est_line'] else f"{p['player']} {p['direction']} DD",
                "odds":       1.91,  # Standard prop odds estimate
                "confidence": p["confidence"],
                "prob":       p["prob"],
                "tags":       p["tags"],
                "reasoning":  p["reasoning"],
                "factors":    [],
                "projected_margin": None,
                "projected_total":  None,
                "edge":       p.get("edge"),
            }
            for p in sorted(all_props, key=lambda x: x["confidence"], reverse=True)
            if p["confidence"] >= 62 and p.get("est_line") is not None
        ][:15]  # Limit to top 15 props for multi consideration

        picks = build_multis(all_legs + top_props_for_multis)

        # Sort all props by confidence for the props tab
        all_props.sort(key=lambda x: x["confidence"], reverse=True)

        return {
            "games":          enriched_games,
            "legs":           all_legs,
            "props":          all_props,
            "picks":          picks,
            "injuries":       injuries_by_team,
            "last_updated":   _now(),
            "games_analyzed": len(games),
        }


def _build_context(team_id, abbrev, full_name, is_home,
                   adv_stats, base_l5, base_l10, home_splits, road_splits,
                   all_players_adv, injuries_by_team):
    from injury_report import get_injury_impact_score
    advanced  = adv_stats.get(team_id, {})
    recent_l5  = base_l5.get(team_id, {})
    recent_l10 = base_l10.get(team_id, {})
    players   = all_players_adv.get(team_id, [])
    splits    = {"home": home_splits.get(team_id, {}), "road": road_splits.get(team_id, {})}
    rest      = {"rest_days": 2, "is_b2b": False}
    team_injuries = injuries_by_team.get(full_name) or injuries_by_team.get(abbrev) or []
    injury_impact = get_injury_impact_score(team_injuries, players)
    return {
        "team_id": team_id, "team_abbrev": abbrev, "team_name": full_name,
        "is_home": is_home, "advanced": advanced, "recent_l5": recent_l5,
        "recent_l10": recent_l10, "game_logs": [], "rest": rest,
        "splits": splits, "players": players,
        "injuries": team_injuries, "injury_impact": injury_impact, "h2h": [],
    }


def _empty_picks():
    empty = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**empty, "key": "safe",  "label": "Safe Multi",     "emoji": "🔵", "accentColor": "#30D158", "subtitle": "No games today"},
        "mid":   {**empty, "key": "mid",   "label": "Mid-Risk Multi", "emoji": "🟡", "accentColor": "#FF9F0A", "subtitle": "No games today"},
        "lotto": {**empty, "key": "lotto", "label": "Lotto Multi",    "emoji": "🔴", "accentColor": "#FF453A", "subtitle": "No games today"},
    }


def _now():
    return datetime.now().strftime("%I:%M %p")


cache = NBACache()
