# props_engine.py — Player prop projection model
# Uses NBA.com data to project every stat line and find edges
# No paid prop odds needed — we build our own lines from analytics

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ─── POSITION DEFENSIVE RATINGS ───────────────────────────────────────────────
POSITION_FACTORS = {
    "PG":  {"pts": 1.0, "reb": 0.85, "ast": 1.1,  "3pm": 1.05, "stl": 1.05, "blk": 0.7},
    "SG":  {"pts": 1.0, "reb": 0.85, "ast": 0.9,  "3pm": 1.05, "stl": 1.0,  "blk": 0.7},
    "SF":  {"pts": 1.0, "reb": 0.95, "ast": 0.85, "3pm": 0.95, "stl": 0.95, "blk": 0.85},
    "PF":  {"pts": 1.0, "reb": 1.1,  "ast": 0.8,  "3pm": 0.85, "stl": 0.85, "blk": 1.1},
    "C":   {"pts": 1.0, "reb": 1.2,  "ast": 0.7,  "3pm": 0.6,  "stl": 0.7,  "blk": 1.3},
    "G":   {"pts": 1.0, "reb": 0.85, "ast": 1.0,  "3pm": 1.05, "stl": 1.0,  "blk": 0.7},
    "F":   {"pts": 1.0, "reb": 1.0,  "ast": 0.85, "3pm": 0.9,  "stl": 0.9,  "blk": 1.0},
    "G-F": {"pts": 1.0, "reb": 0.9,  "ast": 0.95, "3pm": 1.0,  "stl": 1.0,  "blk": 0.85},
    "F-C": {"pts": 1.0, "reb": 1.15, "ast": 0.75, "3pm": 0.7,  "stl": 0.8,  "blk": 1.2},
}

BOOK_LINE_SHADING = {
    "pts":  -0.5,
    "reb":  -0.3,
    "ast":  -0.3,
    "3pm":  -0.2,
    "stl":  -0.1,
    "blk":  -0.1,
    "pra":  -0.8,
    "pr":   -0.5,
    "pa":   -0.5,
}

STAT_STD_DEVS = {
    "pts":  5.5,
    "reb":  2.8,
    "ast":  2.2,
    "3pm":  1.4,
    "stl":  0.9,
    "blk":  0.8,
    "pra":  7.5,
    "pr":   6.5,
    "pa":   6.0,
}


def project_player_props(
    player: dict,
    game_logs: list,
    opp_advanced: dict,
    home_ctx: dict,
    away_ctx: dict,
    player_is_home: bool,
    injury_status: str = "Available",
    teammate_injuries: list = None,
) -> Optional[dict]:
    """
    Project all prop lines for a single player.
    Returns dict with projections for each stat type, or None if player is out.
    """
    if injury_status == "Out":
        return None
    if not game_logs or len(game_logs) < 3:
        return None

    name = player.get("name", "Unknown")
    position = player.get("position", "G")
    usage = player.get("usage_rate", 0.20)
    season_mins = player.get("minutes", 28.0)

    if season_mins < 8:
        return None

    projected_mins = _project_minutes(
        season_mins, player_is_home,
        injury_status, teammate_injuries or [],
        home_ctx if player_is_home else away_ctx
    )

    mins_factor = projected_mins / max(season_mins, 1)

    l5  = _avg_logs(game_logs[-5:])
    l10 = _avg_logs(game_logs[-10:])
    l15 = _avg_logs(game_logs)

    blended = {}
    for stat in ["pts", "reb", "ast", "3pm", "stl", "blk"]:
        blended[stat] = (
            0.40 * l5.get(stat, 0) +
            0.35 * l10.get(stat, 0) +
            0.25 * l15.get(stat, 0)
        )

    team_ctx = home_ctx if player_is_home else away_ctx
    opp_ctx  = away_ctx if player_is_home else home_ctx
    team_pace = team_ctx.get("advanced", {}).get("pace", 100)
    opp_pace  = opp_ctx.get("advanced",  {}).get("pace", 100)
    avg_pace  = (team_pace + opp_pace) / 2
    pace_factor = avg_pace / 100.0

    pos_key = position.split("-")[0] if "-" in position else position
    pos_factors = POSITION_FACTORS.get(pos_key, POSITION_FACTORS["G"])

    opp_def_rating = opp_advanced.get("def_rating", 110)
    def_factor = opp_def_rating / 110.0

    team_rest = team_ctx.get("rest", {})
    fatigue_factor = 0.93 if team_rest.get("is_b2b") else 1.0

    usage_bump = _calc_usage_bump(player, teammate_injuries or [], team_ctx)

    projections = {}
    for stat in ["pts", "reb", "ast", "3pm", "stl", "blk"]:
        base = blended[stat]
        proj = base * mins_factor

        if stat in ["pts", "reb", "ast", "3pm"]:
            proj *= pace_factor

        proj *= pos_factors.get(stat, 1.0)

        if stat == "pts":
            proj *= def_factor
        elif stat in ["reb", "ast"]:
            proj *= (def_factor * 0.5 + 0.5)

        proj *= fatigue_factor

        if stat in ["pts", "ast", "3pm"]:
            proj *= (1 + usage_bump)

        if injury_status == "Questionable":
            proj *= 0.90
        elif injury_status == "Probable":
            proj *= 0.97

        projections[stat] = round(proj, 1)

    projections["pra"] = round(projections["pts"] + projections["reb"] + projections["ast"], 1)
    projections["pr"]  = round(projections["pts"] + projections["reb"], 1)
    projections["pa"]  = round(projections["pts"] + projections["ast"], 1)
    projections["ra"]  = round(projections["reb"] + projections["ast"], 1)

    projections["dd_prob"] = _calc_dd_probability(projections, game_logs)
    projections["td_prob"] = _calc_td_probability(projections, game_logs)

    est_lines = {}
    for stat, shade in BOOK_LINE_SHADING.items():
        if stat in projections:
            raw = projections[stat] + shade
            est_lines[stat] = round(raw * 2) / 2

    edges = {}
    for stat in est_lines:
        if stat in projections and est_lines[stat] > 0:
            edge = projections[stat] - est_lines[stat]
            edges[stat] = round(edge, 1)

    scored_props = []
    for stat in ["pts", "reb", "ast", "3pm", "stl", "blk", "pra", "pr", "pa"]:
        if stat not in projections or stat not in est_lines:
            continue

        edge = edges.get(stat, 0)
        est_line = est_lines[stat]
        projection = projections[stat]
        std_dev = STAT_STD_DEVS.get(stat, 4.0)

        if est_line <= 0:
            continue

        norm_edge = edge / std_dev

        confidence = _score_prop(
            norm_edge, usage, projected_mins, season_mins,
            injury_status, def_factor, pace_factor,
            len(game_logs), stat
        )

        if confidence < 50:
            continue

        direction = "Over" if edge > 0 else "Under"

        if abs(edge) < 0.3:
            continue

        scored_props.append({
            "player":       name,
            "position":     position,
            "stat":         stat,
            "stat_label":   _stat_label(stat),
            "direction":    direction,
            "projection":   projection,
            "est_line":     est_line,
            "edge":         edge,
            "confidence":   confidence,
            "prob":         round(_conf_to_prob(confidence) * 100),
            "projected_mins": round(projected_mins, 1),
            "season_mins":  round(season_mins, 1),
            "l5_avg":       round(l5.get(stat, 0), 1),
            "l10_avg":      round(l10.get(stat, 0), 1),
            "usage_rate":   round(usage * 100, 1),
            "fatigue":      team_rest.get("is_b2b", False),
            "injury_status": injury_status,
            "tags":         _build_prop_tags(
                                injury_status, team_rest, usage, projected_mins,
                                season_mins, def_factor, stat
                            ),
            "reasoning":    _build_prop_reasoning(
                                name, stat, direction, projection, est_line, edge,
                                l5.get(stat, 0), l10.get(stat, 0),
                                projected_mins, opp_def_rating, def_factor,
                                team_rest, injury_status, usage_bump
                            ),
        })

    if projections["dd_prob"] >= 0.25:
        scored_props.append({
            "player":       name,
            "position":     position,
            "stat":         "dd",
            "stat_label":   "Double Double",
            "direction":    "Yes",
            "projection":   round(projections["dd_prob"] * 100, 1),
            "est_line":     None,
            "edge":         None,
            "confidence":   min(80, round(projections["dd_prob"] * 100)),
            "prob":         round(projections["dd_prob"] * 100),
            "projected_mins": round(projected_mins, 1),
            "season_mins":  round(season_mins, 1),
            "l5_avg":       None,
            "l10_avg":      None,
            "usage_rate":   round(usage * 100, 1),
            "fatigue":      team_rest.get("is_b2b", False),
            "injury_status": injury_status,
            "tags":         [],
            "reasoning":    f"{name} has a {projections['dd_prob']*100:.0f}% estimated chance of a double double based on recent form.",
        })

    if projections["td_prob"] >= 0.08:
        scored_props.append({
            "player":       name,
            "stat":         "td",
            "stat_label":   "Triple Double",
            "direction":    "Yes",
            "projection":   round(projections["td_prob"] * 100, 1),
            "est_line":     None,
            "edge":         None,
            "confidence":   min(75, round(projections["td_prob"] * 150)),
            "prob":         round(projections["td_prob"] * 100),
            "projected_mins": round(projected_mins, 1),
            "season_mins":  round(season_mins, 1),
            "l5_avg":       None,
            "l10_avg":      None,
            "usage_rate":   round(usage * 100, 1),
            "fatigue":      team_rest.get("is_b2b", False),
            "injury_status": injury_status,
            "tags":         ["High Variance"],
            "reasoning":    f"{name} has a {projections['td_prob']*100:.0f}% estimated TD chance. High variance but high reward.",
        })

    return {
        "player":          name,
        "position":        position,
        "usage_rate":      round(usage * 100, 1),
        "projected_mins":  round(projected_mins, 1),
        "season_mins":     round(season_mins, 1),
        "injury_status":   injury_status,
        "projections":     projections,
        "est_lines":       est_lines,
        "scored_props":    sorted(scored_props, key=lambda x: x["confidence"], reverse=True),
    }


# ─── HELPER FUNCTIONS ─────────────────────────────────────────────────────────

def _avg_logs(logs: list) -> dict:
    if not logs:
        return {}
    stats = ["pts", "reb", "ast", "3pm", "stl", "blk"]
    result = {}
    for stat in stats:
        vals = [g.get(stat, 0) or 0 for g in logs]
        result[stat] = sum(vals) / len(vals) if vals else 0
    return result


def _project_minutes(season_mins, is_home, injury_status, teammate_injuries, team_ctx):
    mins = season_mins
    if is_home:
        mins *= 1.01
    if team_ctx.get("rest", {}).get("is_b2b"):
        mins *= 0.94
    if injury_status == "Questionable":
        mins *= 0.85
    elif injury_status == "Probable":
        mins *= 0.97
    key_teammates_out = sum(
        1 for p in teammate_injuries
        if p.get("status") == "Out" and p.get("usage_rate", 0) >= 0.18
    )
    mins *= (1 + key_teammates_out * 0.04)
    return min(mins, 40.0)


def _calc_usage_bump(player, teammate_injuries, team_ctx):
    bump = 0.0
    for injured in teammate_injuries:
        if injured.get("status") == "Out":
            injured_usage = injured.get("usage_rate", 0)
            bump += injured_usage * 0.25
    return min(bump, 0.12)


def _calc_dd_probability(projections, game_logs):
    pts = projections.get("pts", 0)
    reb = projections.get("reb", 0)
    ast = projections.get("ast", 0)
    blk = projections.get("blk", 0)

    historical_dd = sum(
        1 for g in game_logs
        if sum(1 for stat in ["pts", "reb", "ast", "blk", "stl"]
               if (g.get(stat) or 0) >= 10) >= 2
    )
    hist_rate = historical_dd / len(game_logs) if game_logs else 0

    cats_near_10 = sum(1 for v in [pts, reb, ast, blk] if v >= 7)
    cats_at_10   = sum(1 for v in [pts, reb, ast, blk] if v >= 10)

    if cats_at_10 >= 2:
        model_prob = 0.70
    elif cats_at_10 == 1 and cats_near_10 >= 2:
        model_prob = 0.45
    elif cats_at_10 == 1:
        model_prob = 0.25
    elif cats_near_10 >= 2:
        model_prob = 0.20
    else:
        model_prob = 0.05

    return round(0.6 * model_prob + 0.4 * hist_rate, 3)


def _calc_td_probability(projections, game_logs):
    pts = projections.get("pts", 0)
    reb = projections.get("reb", 0)
    ast = projections.get("ast", 0)

    historical_td = sum(
        1 for g in game_logs
        if sum(1 for stat in ["pts", "reb", "ast"]
               if (g.get(stat) or 0) >= 10) >= 3
    )
    hist_rate = historical_td / len(game_logs) if game_logs else 0

    cats_at_10   = sum(1 for v in [pts, reb, ast] if v >= 10)
    cats_near_10 = sum(1 for v in [pts, reb, ast] if v >= 7)

    if cats_at_10 == 3:
        model_prob = 0.60
    elif cats_at_10 == 2 and cats_near_10 == 3:
        model_prob = 0.30
    elif cats_at_10 >= 1 and cats_near_10 == 3:
        model_prob = 0.12
    else:
        model_prob = 0.03

    return round(0.5 * model_prob + 0.5 * hist_rate, 3)


def _score_prop(norm_edge, usage, proj_mins, season_mins, injury_status, def_factor, pace_factor, sample_size, stat):
    score = 50
    score += min(25, norm_edge * 15)

    if usage >= 0.28:
        score += 10
    elif usage >= 0.22:
        score += 6
    elif usage >= 0.16:
        score += 2
    else:
        score -= 5

    mins_pct = proj_mins / max(season_mins, 1)
    if mins_pct >= 0.95:
        score += 5
    elif mins_pct < 0.85:
        score -= 8

    if injury_status == "Questionable":
        score -= 12
    elif injury_status == "Probable":
        score -= 4

    if def_factor > 1.08:
        score += 6
    elif def_factor < 0.95:
        score -= 5

    if stat in ["pts", "reb", "ast", "pra"] and pace_factor > 1.03:
        score += 4

    if sample_size < 5:
        score -= 10
    elif sample_size >= 12:
        score += 3

    if stat in ["stl", "blk", "td"]:
        score -= 8

    return min(88, max(35, round(score)))


def _conf_to_prob(score):
    if score >= 80: return 0.68
    if score >= 72: return 0.62
    if score >= 65: return 0.57
    if score >= 58: return 0.53
    return 0.50


def _stat_label(stat):
    return {
        "pts": "Points", "reb": "Rebounds", "ast": "Assists",
        "3pm": "3-Pointers Made", "stl": "Steals", "blk": "Blocks",
        "pra": "Pts+Reb+Ast", "pr": "Pts+Reb", "pa": "Pts+Ast",
        "ra":  "Reb+Ast", "dd": "Double Double", "td": "Triple Double",
    }.get(stat, stat.upper())


def _build_prop_tags(injury_status, rest, usage, proj_mins, season_mins, def_factor, stat):
    tags = []
    if injury_status == "Questionable":
        tags.append("Injury Risk")
    if rest.get("is_b2b"):
        tags.append("B2B")
    mins_pct = proj_mins / max(season_mins, 1)
    if mins_pct < 0.90:
        tags.append("Minutes Risk")
    if def_factor > 1.08:
        tags.append("Weak Defense")
    if def_factor < 0.93:
        tags.append("Tough Defense")
    if stat in ["stl", "blk", "td"]:
        tags.append("High Variance")
    return tags


def _build_prop_reasoning(name, stat, direction, projection, est_line, edge,
                           l5_avg, l10_avg, proj_mins, opp_def_rating, def_factor,
                           rest, injury_status, usage_bump):
    label = _stat_label(stat)
    parts = []

    parts.append(
        f"Model projects {name} to record {projection} {label} "
        f"(estimated line: {est_line}). "
        f"Edge: {edge:+.1f} — lean {direction}."
    )

    parts.append(f"Recent form: {l5_avg:.1f} L5 avg, {l10_avg:.1f} L10 avg.")

    if def_factor > 1.08:
        parts.append(
            f"Opponent ranks in bottom third defensively "
            f"(def rating {opp_def_rating:.1f}) — favourable matchup."
        )
    elif def_factor < 0.93:
        parts.append(
            f"Opponent has a strong defense (def rating {opp_def_rating:.1f}) — "
            f"tougher matchup than usual."
        )

    parts.append(f"Projected minutes: {proj_mins:.0f} (season avg: {proj_mins:.0f}).")

    if rest.get("is_b2b"):
        parts.append("⚠️ Team on back-to-back — minutes/effort may be managed.")

    if injury_status == "Questionable":
        parts.append("⚠️ Listed as Questionable — confirm availability before betting.")

    if usage_bump > 0.03:
        parts.append(
            f"✅ Teammate(s) out — usage rate expected to increase "
            f"by ~{usage_bump*100:.0f}%."
        )

    return " ".join(parts)
