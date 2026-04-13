# afl_props_engine.py -- AFL player prop projection model
# Projects disposals, goals, marks, tackles, kicks, handballs,
# clearances, hitouts, fantasy pts against real bookmaker lines
# Uses L5/L10 rolling averages, opponent matchup, venue, position factors

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# -- Position factors -------------------------------------------------------
# How each position type typically performs vs league average
# Adjusts projections based on matchup
POSITION_FACTORS = {
    "MID": {
        "disposals": 1.15, "kicks": 1.10, "handballs": 1.20,
        "clearances": 1.25, "tackles": 1.05, "marks": 0.85,
        "goals": 0.90, "hitouts": 0.05, "fantasy_pts": 1.10,
    },
    "FWD": {
        "disposals": 0.90, "kicks": 0.95, "handballs": 0.80,
        "clearances": 0.60, "tackles": 0.90, "marks": 1.05,
        "goals": 1.50, "hitouts": 0.05, "fantasy_pts": 1.05,
    },
    "DEF": {
        "disposals": 1.00, "kicks": 1.05, "handballs": 0.90,
        "clearances": 0.80, "tackles": 1.05, "marks": 1.00,
        "goals": 0.30, "hitouts": 0.05, "fantasy_pts": 0.95,
    },
    "RUC": {
        "disposals": 0.75, "kicks": 0.70, "handballs": 0.70,
        "clearances": 1.20, "tackles": 0.90, "marks": 0.90,
        "goals": 0.60, "hitouts": 5.00, "fantasy_pts": 1.00,
    },
}

# Standard deviations for each AFL stat (game-to-game variance)
AFL_STAT_STD_DEVS = {
    "disposals":  5.5,
    "kicks":      3.8,
    "handballs":  3.5,
    "marks":      2.5,
    "goals":      1.4,
    "tackles":    2.0,
    "clearances": 2.5,
    "hitouts":    4.5,
    "fantasy_pts": 22.0,
}

# How bookmakers typically shade AFL prop lines
BOOK_LINE_SHADING = {
    "disposals":  -0.5,
    "kicks":      -0.3,
    "handballs":  -0.3,
    "marks":      -0.3,
    "goals":      -0.1,
    "tackles":    -0.2,
    "clearances": -0.2,
    "hitouts":    -0.5,
    "fantasy_pts": -2.0,
}

# Stats to project (in priority order for display)
PROJ_STATS = ["disposals", "kicks", "handballs", "marks", "goals",
              "tackles", "clearances", "hitouts", "fantasy_pts"]


def project_afl_player_props(
    player: dict,
    game_logs: list,
    opponent: str,
    is_home: bool,
    venue_stats: dict,
    real_lines: dict = None,
    team_news: dict = None,
) -> Optional[dict]:
    """
    Project all AFL prop lines for a single player.

    player: { name, team, position, games, disposals, kicks, ... (season avgs) }
    game_logs: last N game-by-game stat dicts
    opponent: opponent team name
    is_home: whether player's team is at home
    venue_stats: venue dict with avg_total, home_adv etc
    real_lines: { stat: { line, over_odds, under_odds } } from bookmaker if available
    team_news: { ins, outs, selected } for the team

    Returns dict with projections, scored_props, or None if player is out.
    """
    name     = player.get("name", "Unknown")
    position = player.get("position", "MID")
    games    = player.get("games", 0)

    if games < 3:
        return None

    # Check if player is named in team (if team news available)
    if team_news:
        selected = team_news.get("selected", [])
        outs = team_news.get("outs", [])
        if selected and name not in selected:
            return None
        if name in outs:
            return None

    if not game_logs:
        return None

    # -- Rolling averages --------------------------------------------------
    l5  = _avg_logs(game_logs[:5],  PROJ_STATS)
    l10 = _avg_logs(game_logs[:10], PROJ_STATS)
    season_avg = {stat: player.get(stat, 0) for stat in PROJ_STATS}

    # Blend: 45% L5, 35% L10, 20% season
    blended = {}
    for stat in PROJ_STATS:
        l5_v  = l5.get(stat, season_avg.get(stat, 0))
        l10_v = l10.get(stat, season_avg.get(stat, 0))
        s_v   = season_avg.get(stat, 0)
        blended[stat] = 0.45 * l5_v + 0.35 * l10_v + 0.20 * s_v

    # -- Position factor ---------------------------------------------------
    pos_upper = position.upper()
    if "MID" in pos_upper or "WING" in pos_upper:
        pos_key = "MID"
    elif "FWD" in pos_upper or "FORWARD" in pos_upper or "FF" in pos_upper:
        pos_key = "FWD"
    elif "DEF" in pos_upper or "BACK" in pos_upper or "FB" in pos_upper:
        pos_key = "DEF"
    elif "RUC" in pos_upper or "RUCKMAN" in pos_upper:
        pos_key = "RUC"
    else:
        pos_key = "MID"  # Default
    pos_f = POSITION_FACTORS.get(pos_key, POSITION_FACTORS["MID"])

    # -- Venue factor ------------------------------------------------------
    # High-scoring venues produce more disposals, marks etc
    venue_total = venue_stats.get("avg_total", 157)
    venue_factor = venue_total / 157.0  # 1.0 = league average venue

    # -- Home/away factor --------------------------------------------------
    home_factor = 1.02 if is_home else 0.98

    # -- Build projections -------------------------------------------------
    projections = {}
    for stat in PROJ_STATS:
        base = blended[stat]
        proj = base

        # Apply position factor
        proj *= pos_f.get(stat, 1.0)

        # Apply venue factor (counting stats scale with venue scoring)
        if stat in ("disposals", "kicks", "handballs", "marks", "clearances", "fantasy_pts"):
            proj = proj * 0.8 + proj * venue_factor * 0.2

        # Apply home/away
        if stat != "hitouts":  # Hitouts don't change with home/away
            proj *= home_factor

        projections[stat] = round(max(0, proj), 1)

    # -- Compare to real bookmaker lines or estimate lines -----------------
    scored_props = []
    for stat in PROJ_STATS:
        proj = projections[stat]
        if proj <= 0:
            continue

        # Use real bookmaker line if available, else estimate
        if real_lines and stat in real_lines and real_lines[stat].get("line"):
            line_data = real_lines[stat]
            book_line  = float(line_data["line"])
            over_odds  = line_data.get("over_odds", 1.91)
            under_odds = line_data.get("under_odds", 1.91)
            has_real_line = True
        else:
            # Estimate the line from projection + book shading
            shade    = BOOK_LINE_SHADING.get(stat, -0.3)
            raw_line = proj + shade
            # Round to nearest 0.5
            book_line  = round(raw_line * 2) / 2
            over_odds  = 1.91
            under_odds = 1.91
            has_real_line = False

        if book_line <= 0:
            continue

        edge = proj - book_line
        direction = "Over" if edge > 0 else "Under"

        if abs(edge) < 0.2:
            continue

        std_dev = AFL_STAT_STD_DEVS.get(stat, 4.0)
        norm_edge = edge / std_dev

        confidence = _score_afl_prop(
            norm_edge=norm_edge,
            stat=stat,
            games_played=games,
            has_real_line=has_real_line,
            l5_logs=len(game_logs[:5]),
            position=pos_key,
            proj=proj,
            line=book_line,
        )

        if confidence < 50:
            continue

        # Calculate implied probability from odds vs our projection
        display_odds = over_odds if direction == "Over" else under_odds

        scored_props.append({
            "player":       name,
            "position":     position,
            "stat":         stat,
            "stat_label":   _stat_label(stat),
            "direction":    direction,
            "projection":   proj,
            "book_line":    book_line,
            "edge":         round(edge, 1),
            "confidence":   confidence,
            "prob":         round(_conf_to_prob(confidence) * 100),
            "odds":         display_odds,
            "has_real_line": has_real_line,
            "l5_avg":       round(l5.get(stat, 0), 1),
            "l10_avg":      round(l10.get(stat, 0), 1),
            "season_avg":   round(season_avg.get(stat, 0), 1),
            "tags":         _build_tags(stat, has_real_line, is_home, venue_stats, pos_key),
            "reasoning":    _build_reasoning(
                name, stat, direction, proj, book_line, edge,
                l5.get(stat, 0), l10.get(stat, 0),
                season_avg.get(stat, 0), is_home, venue_stats, has_real_line
            ),
        })

    # Sort by confidence
    scored_props.sort(key=lambda x: x["confidence"], reverse=True)

    return {
        "player":       name,
        "team":         player.get("team", ""),
        "position":     position,
        "games":        games,
        "projections":  projections,
        "scored_props": scored_props,
    }


# -- Helpers ----------------------------------------------------------------

def _avg_logs(logs: list, stats: list) -> dict:
    if not logs:
        return {}
    result = {}
    for stat in stats:
        vals = [g.get(stat, 0) or 0 for g in logs]
        result[stat] = sum(vals) / len(vals) if vals else 0
    return result


def _score_afl_prop(norm_edge, stat, games_played, has_real_line,
                     l5_logs, position, proj, line) -> int:
    score = 50

    # Edge contribution (max +22)
    score += min(22, norm_edge * 14)

    # Real vs estimated line
    if has_real_line:
        score += 8   # Real lines are much more meaningful
    else:
        score -= 3   # Estimated lines get a small penalty

    # Sample size (more games = more reliable)
    if games_played >= 15:
        score += 5
    elif games_played >= 10:
        score += 3
    elif games_played < 5:
        score -= 8

    if l5_logs >= 5:
        score += 3
    elif l5_logs < 3:
        score -= 8

    # Stat volatility penalty
    if stat == "goals":
        score -= 10   # Goals are very high variance in AFL
    elif stat in ("hitouts",):
        score -= 5    # Hitouts can vary a lot
    elif stat == "clearances":
        score -= 3

    # High volume stats (disposals) are more reliable
    if stat in ("disposals", "fantasy_pts", "kicks"):
        score += 4

    # Projection vs line sanity check
    if line > 0 and abs(proj - line) / line > 0.3:
        score -= 5  # Very large discrepancy reduces confidence

    return min(88, max(35, round(score)))


def _conf_to_prob(score: int) -> float:
    if score >= 80: return 0.68
    if score >= 72: return 0.62
    if score >= 65: return 0.57
    if score >= 58: return 0.53
    return 0.50


def _stat_label(stat: str) -> str:
    return {
        "disposals":   "Disposals",
        "kicks":       "Kicks",
        "handballs":   "Handballs",
        "marks":       "Marks",
        "goals":       "Goals",
        "tackles":     "Tackles",
        "clearances":  "Clearances",
        "hitouts":     "Hitouts",
        "fantasy_pts": "Fantasy Points",
    }.get(stat, stat.replace("_", " ").title())


def _build_tags(stat, has_real_line, is_home, venue_stats, position) -> list:
    tags = []
    if has_real_line:
        tags.append("Real Odds")
    else:
        tags.append("Model Line")
    if stat == "goals":
        tags.append("High Variance")
    if venue_stats.get("avg_total", 157) > 163:
        tags.append("High Scoring Venue")
    if not is_home:
        tags.append("Away")
    return tags


def _build_reasoning(name, stat, direction, proj, line, edge,
                      l5, l10, season, is_home, venue_stats, has_real_line) -> str:
    label = _stat_label(stat)
    line_type = "bookmaker line" if has_real_line else "estimated line"
    parts = [
        f"Model projects {name} to record {proj:.1f} {label} "
        f"({direction} the {line_type} of {line}). Edge: {edge:+.1f}."
    ]
    parts.append(
        f"Recent form: L5 avg {l5:.1f}, L10 avg {l10:.1f}, "
        f"season avg {season:.1f}."
    )
    if venue_stats.get("avg_total", 157) > 163:
        parts.append(
            f"High-scoring venue ({venue_stats.get('name', '')}) "
            f"favours elevated stat lines."
        )
    if not is_home:
        parts.append("Playing away -- minor negative adjustment applied.")
    if stat == "goals":
        parts.append("Note: Goals are high variance in AFL -- use as part of a multi with caution.")
    return " ".join(parts)
