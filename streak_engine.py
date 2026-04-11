# streak_engine.py — Real streak calculation from NBA.com game logs
# For each player, generates thresholds based on their season average
# then calculates how often they hit each threshold over L5, L10, L15

import logging
import math

logger = logging.getLogger(__name__)

# Stats we track for streaks
STREAK_STATS = ["pts", "reb", "ast", "3pm", "stl", "blk"]

STAT_LABELS = {
    "pts": "Points",
    "reb": "Rebounds",
    "ast": "Assists",
    "3pm": "3-Pointers",
    "stl": "Steals",
    "blk": "Blocks",
}

# Minimum thresholds — only track streaks that are meaningful
MIN_THRESHOLDS = {
    "pts": 10,   # No point tracking "5+ points" for a starter
    "reb": 4,
    "ast": 3,
    "3pm": 1,
    "stl": 1,
    "blk": 1,
}

# Minimum season average to bother generating a threshold
MIN_AVG_FOR_STAT = {
    "pts": 8.0,
    "reb": 3.0,
    "ast": 2.0,
    "3pm": 0.8,
    "stl": 0.5,
    "blk": 0.5,
}


def calculate_streaks(
    player_base: dict,
    player_logs: dict,
    today_team_ids: set,
    windows: list = [5, 10, 15],
) -> list:
    """
    Calculate real streak data for all players on today's teams.

    Parameters
    ----------
    player_base : {player_id: {name, team_id, pts, reb, ast, 3pm, stl, blk, mins, ...}}
    player_logs : {player_id: [game_log_dicts]}
    today_team_ids : set of team IDs playing today
    windows : list of game windows to calculate (default [5, 10, 15])

    Returns
    -------
    List of streak dicts sorted by best hit rate, ready for the API.
    """
    streaks = []

    for player_id, pdata in player_base.items():
        team_id = int(pdata.get("team_id", 0))
        if team_id not in today_team_ids:
            continue

        logs = player_logs.get(player_id, [])
        if len(logs) < 5:
            continue  # Not enough data

        name     = pdata.get("name", "")
        team     = pdata.get("team_abbrev", "")
        mins     = pdata.get("mins", 0)
        position = pdata.get("position", "G")

        # Skip players with very few minutes
        if mins < 12:
            continue

        # Generate thresholds for each stat based on season average
        for stat in STREAK_STATS:
            season_avg = pdata.get(stat, 0) or 0
            min_avg    = MIN_AVG_FOR_STAT.get(stat, 1)

            if season_avg < min_avg:
                continue

            # Generate 1-2 thresholds per stat
            thresholds = _generate_thresholds(stat, season_avg)

            for threshold in thresholds:
                # Calculate hit rate for each window
                window_results = {}
                for w in windows:
                    recent_logs = logs[:w]  # Most recent first
                    if len(recent_logs) < w:
                        continue
                    hits = sum(1 for g in recent_logs if (g.get(stat) or 0) >= threshold)
                    window_results[w] = {
                        "hits":     hits,
                        "games":    w,
                        "hit_rate": round(hits / w, 3),
                        "pct":      round((hits / w) * 100),
                    }

                if not window_results:
                    continue

                # Only include if at least one window has a meaningful hit rate
                best = max(window_results.values(), key=lambda x: x["hit_rate"])
                if best["hit_rate"] < 0.50:
                    continue  # Skip if they don't hit 50%+ in any window

                # Calculate recent form trend (is L5 better or worse than L15?)
                l5_rate  = window_results.get(5,  {}).get("hit_rate", 0)
                l15_rate = window_results.get(15, {}).get("hit_rate", 0)
                trend = "up" if l5_rate > l15_rate + 0.10 else \
                        "down" if l5_rate < l15_rate - 0.10 else "stable"

                # Calculate actual recent values for context
                recent_vals = [(g.get(stat) or 0) for g in logs[:10]]
                avg_recent  = round(sum(recent_vals) / len(recent_vals), 1) if recent_vals else 0
                std_dev     = _std_dev(recent_vals)

                streaks.append({
                    "player":       name,
                    "player_id":    player_id,
                    "team":         team,
                    "position":     position,
                    "stat":         stat,
                    "stat_label":   STAT_LABELS[stat],
                    "threshold":    threshold,
                    "label":        _threshold_label(stat, threshold),
                    "season_avg":   round(season_avg, 1),
                    "recent_avg":   avg_recent,
                    "std_dev":      round(std_dev, 1),
                    "trend":        trend,
                    "windows":      window_results,
                    "best_window":  max(window_results, key=lambda w: window_results[w]["hit_rate"]),
                    "best_hit_rate": best["hit_rate"],
                    "best_hits":    best["hits"],
                    "best_games":   best["games"],
                    # Last 5 actual values for sparkline
                    "last_5_vals":  [(g.get(stat) or 0) for g in logs[:5]],
                    "mins":         round(mins, 1),
                })

    # Sort: perfect streaks first, then by best hit rate, then by threshold
    streaks.sort(key=lambda s: (
        -s["best_hit_rate"],
        -s["threshold"],
        s["player"],
    ))

    logger.info("Calculated %d streak entries for %d windows", len(streaks), len(windows))
    return streaks


def _generate_thresholds(stat: str, season_avg: float) -> list:
    """
    Generate 1-2 meaningful thresholds for a stat based on season average.
    E.g. if player averages 28 pts, generate thresholds of 20 and 25.
    """
    thresholds = []
    min_thresh = MIN_THRESHOLDS.get(stat, 1)

    # Primary threshold: round down to nearest 5 (for pts) or nearest whole (for others)
    if stat == "pts":
        # e.g. avg 28.4 → thresholds of 25 and 20
        primary = max(min_thresh, _round_down(season_avg * 0.85, 5))
        secondary = max(min_thresh, _round_down(season_avg * 0.70, 5))
        if primary != secondary and primary >= min_thresh:
            thresholds.append(primary)
        if secondary >= min_thresh and secondary != primary:
            thresholds.append(secondary)
    elif stat in ("reb", "ast"):
        # e.g. avg 8.2 → threshold of 7 and 5
        primary   = max(min_thresh, math.floor(season_avg * 0.85))
        secondary = max(min_thresh, math.floor(season_avg * 0.65))
        thresholds.append(primary)
        if secondary != primary and secondary >= min_thresh:
            thresholds.append(secondary)
    elif stat == "3pm":
        # e.g. avg 2.8 → threshold of 2 and 3
        if season_avg >= 3.0:
            thresholds.extend([2, 3])
        elif season_avg >= 2.0:
            thresholds.extend([1, 2])
        else:
            thresholds.append(1)
    elif stat in ("stl", "blk"):
        thresholds.append(1)
        if season_avg >= 1.8:
            thresholds.append(2)

    # Remove duplicates and ensure minimum
    seen = set()
    result = []
    for t in thresholds:
        if t >= min_thresh and t not in seen:
            seen.add(t)
            result.append(t)

    return result[:2]  # Max 2 thresholds per stat


def _threshold_label(stat: str, threshold: int) -> str:
    label = STAT_LABELS.get(stat, stat.upper())
    return f"{threshold}+ {label}"


def _round_down(value: float, nearest: int) -> int:
    return int(value // nearest) * nearest


def _std_dev(values: list) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)
