# afl_streak_engine.py -- AFL streak calculator
# Same concept as NBA streak engine but for AFL stats
# Calculates how often players hit thresholds over L5/L10/L15 windows

import logging
import math

logger = logging.getLogger(__name__)

AFL_STREAK_STATS = ["disposals", "kicks", "marks", "goals", "tackles",
                    "clearances", "hitouts", "fantasy_pts", "handballs"]

STAT_LABELS = {
    "disposals":   "Disposals",
    "kicks":       "Kicks",
    "handballs":   "Handballs",
    "marks":       "Marks",
    "goals":       "Goals",
    "tackles":     "Tackles",
    "clearances":  "Clearances",
    "hitouts":     "Hitouts",
    "fantasy_pts": "Fantasy Points",
}

# Minimum season average before generating a threshold
MIN_AVG = {
    "disposals":   8.0,
    "kicks":       5.0,
    "handballs":   3.0,
    "marks":       2.0,
    "goals":       0.4,
    "tackles":     2.0,
    "clearances":  1.5,
    "hitouts":     3.0,
    "fantasy_pts": 50.0,
}

# Minimum threshold values (don't track trivial thresholds)
MIN_THRESHOLD = {
    "disposals":   8,
    "kicks":       4,
    "handballs":   3,
    "marks":       2,
    "goals":       1,
    "tackles":     2,
    "clearances":  2,
    "hitouts":     5,
    "fantasy_pts": 50,
}


def calculate_afl_streaks(
    players: dict,
    player_logs: dict,
    playing_teams: set,
    windows: list = None,
) -> list:
    """
    Calculate streak data for all players in today's round.

    players: {player_name: {team, position, games, disposals, ...season avgs}}
    player_logs: {player_name: [game_log_dicts, most recent first]}
    playing_teams: set of team names playing this round
    windows: [5, 10, 15] (default)

    Returns list of streak dicts sorted by best hit rate.
    """
    if windows is None:
        windows = [5, 10, 15]

    streaks = []

    for player_name, pdata in players.items():
        team = pdata.get("team", "")
        if team not in playing_teams:
            continue

        logs = player_logs.get(player_name, [])
        if len(logs) < 3:
            continue

        position = pdata.get("position", "MID")
        games    = pdata.get("games", 0)

        if games < 3:
            continue

        for stat in AFL_STREAK_STATS:
            season_avg = pdata.get(stat, 0) or 0
            if season_avg < MIN_AVG.get(stat, 1):
                continue

            thresholds = _generate_thresholds(stat, season_avg)

            for threshold in thresholds:
                window_results = {}
                for w in windows:
                    recent = logs[:w]
                    if len(recent) < w:
                        continue
                    hits = sum(1 for g in recent if (g.get(stat) or 0) >= threshold)
                    window_results[w] = {
                        "hits":     hits,
                        "games":    w,
                        "hit_rate": round(hits / w, 3),
                        "pct":      round((hits / w) * 100),
                    }

                if not window_results:
                    continue

                best = max(window_results.values(), key=lambda x: x["hit_rate"])
                if best["hit_rate"] < 0.50:
                    continue

                # Trend
                l5_rate  = window_results.get(5,  {}).get("hit_rate", 0)
                l10_rate = window_results.get(10, {}).get("hit_rate", 0)
                trend = (
                    "up"   if l5_rate > l10_rate + 0.10 else
                    "down" if l5_rate < l10_rate - 0.10 else
                    "stable"
                )

                recent_vals = [(g.get(stat) or 0) for g in logs[:10]]
                avg_recent  = round(sum(recent_vals) / len(recent_vals), 1) if recent_vals else 0

                streaks.append({
                    "player":        player_name,
                    "team":          team,
                    "position":      position,
                    "stat":          stat,
                    "stat_label":    STAT_LABELS.get(stat, stat),
                    "threshold":     threshold,
                    "label":         f"{threshold}+ {STAT_LABELS.get(stat, stat)}",
                    "season_avg":    round(season_avg, 1),
                    "recent_avg":    avg_recent,
                    "trend":         trend,
                    "windows":       window_results,
                    "best_window":   max(window_results, key=lambda w: window_results[w]["hit_rate"]),
                    "best_hit_rate": best["hit_rate"],
                    "best_hits":     best["hits"],
                    "best_games":    best["games"],
                    "last_5_vals":   [(g.get(stat) or 0) for g in logs[:5]],
                    "is_perfect":    best["hit_rate"] >= 1.0,
                })

    streaks.sort(key=lambda s: (-s["best_hit_rate"], -s["threshold"], s["player"]))
    logger.info("AFL streaks: %d entries calculated", len(streaks))
    return streaks


def _generate_thresholds(stat: str, season_avg: float) -> list:
    """Generate 1-2 meaningful thresholds based on season average."""
    min_t = MIN_THRESHOLD.get(stat, 1)
    thresholds = []

    if stat == "disposals":
        primary   = max(min_t, _round_down(season_avg * 0.82, 5))
        secondary = max(min_t, _round_down(season_avg * 0.67, 5))
        thresholds = [primary, secondary]
    elif stat in ("kicks", "handballs"):
        primary   = max(min_t, math.floor(season_avg * 0.80))
        secondary = max(min_t, math.floor(season_avg * 0.65))
        thresholds = [primary, secondary]
    elif stat == "marks":
        primary = max(min_t, math.floor(season_avg * 0.80))
        thresholds = [primary]
        if season_avg >= 5:
            thresholds.append(max(min_t, math.floor(season_avg * 0.65)))
    elif stat == "goals":
        thresholds = [1]
        if season_avg >= 2.0:
            thresholds.append(2)
        if season_avg >= 3.0:
            thresholds.append(3)
    elif stat == "tackles":
        primary = max(min_t, math.floor(season_avg * 0.80))
        thresholds = [primary]
    elif stat == "clearances":
        primary = max(min_t, math.floor(season_avg * 0.80))
        thresholds = [primary]
    elif stat == "hitouts":
        primary   = max(min_t, _round_down(season_avg * 0.75, 5))
        thresholds = [primary]
        if season_avg >= 20:
            thresholds.append(_round_down(season_avg * 0.60, 5))
    elif stat == "fantasy_pts":
        primary   = max(min_t, _round_down(season_avg * 0.82, 10))
        secondary = max(min_t, _round_down(season_avg * 0.67, 10))
        thresholds = [primary, secondary]
    else:
        thresholds = [max(min_t, math.floor(season_avg * 0.80))]

    # Clean up
    seen, result = set(), []
    for t in thresholds:
        if t >= min_t and t not in seen:
            seen.add(t)
            result.append(t)
    return result[:2]


def _round_down(value: float, nearest: int) -> int:
    return max(0, int(value // nearest) * nearest)
