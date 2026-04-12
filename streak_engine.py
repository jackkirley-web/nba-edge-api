# streak_engine.py — Fixed: explicit ordering, validation, correct slicing

import logging
import math
from datetime import datetime

logger = logging.getLogger(__name__)

STREAK_STATS = ["pts", "reb", "ast", "3pm", "stl", "blk"]

STAT_LABELS = {
    "pts": "Points",
    "reb": "Rebounds",
    "ast": "Assists",
    "3pm": "3-Pointers",
    "stl": "Steals",
    "blk": "Blocks",
}

MIN_THRESHOLDS = {
    "pts": 10,
    "reb": 4,
    "ast": 3,
    "3pm": 1,
    "stl": 1,
    "blk": 1,
}

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
    streaks = []
    max_window = max(windows)

    for player_id, pdata in player_base.items():
        team_id = int(pdata.get("team_id", 0))
        if team_id not in today_team_ids:
            continue

        raw_logs = player_logs.get(player_id, [])
        if len(raw_logs) < 3:
            continue

        # ── CRITICAL: re-sort logs most-recent-first ──────────
        # Even though player_logs.py sorts them, we sort again here
        # as a safety net in case logs arrive from anywhere else.
        try:
            sorted_logs = sorted(
                raw_logs,
                key=lambda g: datetime.strptime(
                    str(g.get("game_date", ""))[:10], "%Y-%m-%d"
                ) if g.get("game_date") else datetime.min,
                reverse=True  # Most recent first
            )
        except Exception:
            sorted_logs = raw_logs  # Fall back if dates are malformed

        # Limit to max_window most recent games
        logs = sorted_logs[:max_window]

        name     = pdata.get("name", "")
        team     = pdata.get("team_abbrev", "")
        position = pdata.get("position", "G")
        mins     = pdata.get("mins", 0)

        if mins < 12:
            continue

        # Log ordering validation (debug — can remove later)
        if len(logs) >= 2 and logs[0].get("game_date") and logs[1].get("game_date"):
            if logs[0]["game_date"] < logs[1]["game_date"]:
                logger.warning(
                    "ORDERING BUG: %s logs not sorted most-recent-first: %s then %s",
                    name, logs[0]["game_date"], logs[1]["game_date"]
                )

        for stat in STREAK_STATS:
            season_avg = pdata.get(stat, 0) or 0
            if season_avg < MIN_AVG_FOR_STAT.get(stat, 1):
                continue

            thresholds = _generate_thresholds(stat, season_avg)

            for threshold in thresholds:
                window_results = {}

                for w in windows:
                    # Slice the w most recent games (logs[0] = most recent)
                    window_logs = logs[:w]

                    if len(window_logs) < w:
                        # Not enough games played yet — skip this window
                        continue

                    # Count how many of those w games hit the threshold
                    hits = sum(
                        1 for g in window_logs
                        if int(g.get(stat) or 0) >= threshold
                    )

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

                # Trend: compare L5 vs L15 hit rate
                l5_rate  = window_results.get(5,  {}).get("hit_rate", 0)
                l15_rate = window_results.get(15, {}).get("hit_rate", 0)
                if l5_rate > l15_rate + 0.10:
                    trend = "up"
                elif l5_rate < l15_rate - 0.10:
                    trend = "down"
                else:
                    trend = "stable"

                # Last 5 actual values for display
                last_5_vals = [int(g.get(stat) or 0) for g in logs[:5]]

                # Recent average (L10 or however many we have)
                recent_vals = [int(g.get(stat) or 0) for g in logs[:10]]
                avg_recent  = round(sum(recent_vals) / len(recent_vals), 1) if recent_vals else season_avg

                streaks.append({
                    "player":        name,
                    "player_id":     player_id,
                    "team":          team,
                    "position":      position,
                    "stat":          stat,
                    "stat_label":    STAT_LABELS[stat],
                    "threshold":     threshold,
                    "label":         _threshold_label(stat, threshold),
                    "season_avg":    round(season_avg, 1),
                    "recent_avg":    avg_recent,
                    "trend":         trend,
                    "windows":       window_results,
                    "best_window":   max(window_results, key=lambda w: window_results[w]["hit_rate"]),
                    "best_hit_rate": best["hit_rate"],
                    "last_5_vals":   last_5_vals,
                    "mins":          round(mins, 1),
                    # Include actual game dates for transparency
                    "last_5_dates":  [str(g.get("game_date", ""))[:10] for g in logs[:5]],
                })

    streaks.sort(key=lambda s: (-s["best_hit_rate"], -s["threshold"], s["player"]))
    logger.info("Calculated %d streak entries", len(streaks))
    return streaks


def _generate_thresholds(stat: str, season_avg: float) -> list:
    thresholds = []
    min_thresh = MIN_THRESHOLDS.get(stat, 1)

    if stat == "pts":
        primary   = max(min_thresh, _round_down(season_avg * 0.85, 5))
        secondary = max(min_thresh, _round_down(season_avg * 0.70, 5))
        if primary >= min_thresh:
            thresholds.append(primary)
        if secondary >= min_thresh and secondary != primary:
            thresholds.append(secondary)
    elif stat in ("reb", "ast"):
        primary   = max(min_thresh, math.floor(season_avg * 0.85))
        secondary = max(min_thresh, math.floor(season_avg * 0.65))
        thresholds.append(primary)
        if secondary != primary and secondary >= min_thresh:
            thresholds.append(secondary)
    elif stat == "3pm":
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

    seen = set()
    result = []
    for t in thresholds:
        if t >= min_thresh and t not in seen:
            seen.add(t)
            result.append(t)
    return result[:2]


def _threshold_label(stat: str, threshold: int) -> str:
    label = STAT_LABELS.get(stat, stat.upper())
    return f"{threshold}+ {label}"


def _round_down(value: float, nearest: int) -> int:
    return int(value // nearest) * nearest
