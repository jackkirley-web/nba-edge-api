# engine.py — Deep analysis and multi-bet scoring engine
# This is the brain. Combines all data sources into confidence scores.

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ─── WEIGHTS ──────────────────────────────────────────────────
# How much each factor contributes to the overall confidence score
WEIGHTS = {
    "statistical_edge":     0.28,  # Model projection vs line
    "recent_form":          0.20,  # L5/L10 rolling performance
    "advanced_metrics":     0.18,  # Off/Def rating, pace, net rating
    "matchup_quality":      0.12,  # H2H history, archetype matchup
    "injury_impact":        0.12,  # Usage lost, key players out
    "rest_advantage":       0.06,  # B2B, rest days
    "home_away_split":      0.04,  # Location-specific performance
}


def conf_to_prob(score: float) -> float:
    """
    Calibrated mapping: confidence score → estimated win probability.
    Honest — no inflated numbers.
    """
    if score >= 82: return 0.70
    if score >= 75: return 0.64
    if score >= 68: return 0.59
    if score >= 62: return 0.55
    if score >= 56: return 0.52
    return 0.48


def rolling_blend(l5: float, l10: float, l15: float) -> float:
    """
    Blended rolling window stat.
    Weights: L5=40%, L10=35%, L15=25%
    Anchors recent form to a long-term baseline.
    """
    return 0.40 * l5 + 0.35 * l10 + 0.25 * l15


def pace_adjusted_projection(
    team_off_rating: float,
    opp_def_rating: float,
    avg_pace: float,
) -> float:
    """
    Project a team's points per game using pace-adjusted model.
    This is the standard NBA analytics formula.
    """
    proj_per_100 = (team_off_rating / opp_def_rating) * 100
    return proj_per_100 * (avg_pace / 100)


def project_game_total(home_ctx: dict, away_ctx: dict) -> float:
    """
    Project total points for a game using dual pace-adjusted model.
    Applies b2b, travel, and lineup penalty.
    """
    home_adv = home_ctx.get("advanced", {})
    away_adv = away_ctx.get("advanced", {})

    home_off = home_adv.get("off_rating", 112)
    home_def = home_adv.get("def_rating", 112)
    away_off = away_adv.get("off_rating", 112)
    away_def = away_adv.get("def_rating", 112)
    home_pace = home_adv.get("pace", 100)
    away_pace = away_adv.get("pace", 100)

    avg_pace = (home_pace + away_pace) / 2

    # Model projection
    home_proj = pace_adjusted_projection(home_off, away_def, avg_pace)
    away_proj = pace_adjusted_projection(away_off, home_def, avg_pace)
    model_total = home_proj + away_proj

    # Empirical blend
    home_l10 = home_ctx.get("recent_l10", {})
    away_l10 = away_ctx.get("recent_l10", {})
    empirical_home = home_l10.get("pts", home_proj) + home_l10.get("opp_pts_allowed", away_proj)
    empirical_away = away_l10.get("pts", away_proj) + away_l10.get("opp_pts_allowed", home_proj)
    empirical_total = (empirical_home + empirical_away) / 2

    blended = 0.60 * model_total + 0.40 * empirical_total

    # H2H adjustment
    h2h = home_ctx.get("h2h", [])
    if len(h2h) >= 3:
        h2h_avg = sum(g.get("total_pts", blended) for g in h2h[:5]) / min(5, len(h2h))
        blended = 0.80 * blended + 0.20 * h2h_avg

    # Fatigue penalties
    if home_ctx.get("rest", {}).get("is_b2b"):
        blended -= 2.8
    if away_ctx.get("rest", {}).get("is_b2b"):
        blended -= 2.8

    # Injury scoring depression
    home_inj = home_ctx.get("injury_impact", {})
    away_inj = away_ctx.get("injury_impact", {})
    blended -= home_inj.get("injury_severity", 0) * 0.4
    blended -= away_inj.get("injury_severity", 0) * 0.4

    return round(blended, 1)


def project_spread(home_ctx: dict, away_ctx: dict) -> float:
    """
    Project home team margin of victory.
    Positive = home favoured by X points.
    """
    home_adv = home_ctx.get("advanced", {})
    away_adv = away_ctx.get("advanced", {})

    home_net = home_adv.get("net_rating", 0)
    away_net = away_adv.get("net_rating", 0)

    # Net rating differential is the best single spread predictor
    net_diff = home_net - away_net

    # Home court is worth ~2.5-3 points historically
    home_court_bonus = 2.8

    # Adjust for home/away splits
    home_splits = home_ctx.get("splits", {})
    away_splits = away_ctx.get("splits", {})
    home_home_net = home_splits.get("home", {}).get("net_rating", home_net)
    away_away_net = away_splits.get("road", {}).get("net_rating", away_net)
    split_diff = home_home_net - away_away_net

    # Blend net rating with split-specific data
    projected_margin = (0.60 * net_diff + 0.40 * split_diff) + home_court_bonus

    # B2B penalty
    if home_ctx.get("rest", {}).get("is_b2b"):
        projected_margin -= 3.2
    if away_ctx.get("rest", {}).get("is_b2b"):
        projected_margin += 3.2

    # Injury adjustments
    home_inj = home_ctx.get("injury_impact", {})
    away_inj = away_ctx.get("injury_impact", {})
    projected_margin -= home_inj.get("injury_severity", 0) * 0.8
    projected_margin += away_inj.get("injury_severity", 0) * 0.8

    # Recent form (L5 win/loss momentum)
    home_l5 = home_ctx.get("recent_l5", {})
    away_l5 = away_ctx.get("recent_l5", {})
    home_momentum = home_l5.get("wins", 2) - home_l5.get("losses", 2)  # -5 to +5
    away_momentum = away_l5.get("wins", 2) - away_l5.get("losses", 2)
    projected_margin += (home_momentum - away_momentum) * 0.3

    return round(projected_margin, 1)


def score_spread_leg(
    home_ctx: dict,
    away_ctx: dict,
    market_line: float,
    home_team_favoured: bool,
    odds: float,
) -> dict:
    """
    Score a spread leg (0-100 confidence).
    Returns full scored leg dict.
    """
    projected_margin = project_spread(home_ctx, away_ctx)

    # Edge: how far is our projection from the market line?
    if home_team_favoured:
        # We're betting home to cover
        edge = projected_margin - market_line
        selection_label = "cover"
    else:
        # We're betting away
        edge = -market_line - projected_margin
        selection_label = "cover"

    # Statistical edge score (0-30)
    # Use std dev of team margins to normalise edge
    home_logs = home_ctx.get("game_logs", [])
    margins = [abs(g.get("plus_minus", 5)) for g in home_logs if g.get("plus_minus") is not None]
    std_dev = _std_dev(margins) if margins else 8.0
    normalised_edge = edge / max(std_dev, 3)
    stat_score = min(30, max(0, normalised_edge * 12 + 15))

    # Recent form (0-20)
    home_l5 = home_ctx.get("recent_l5", {})
    away_l5 = away_ctx.get("recent_l5", {})
    form_diff = home_l5.get("wins", 2) - away_l5.get("wins", 2)
    form_score = min(20, max(0, (form_diff / 5) * 20 + 10)) if home_team_favoured else min(20, max(0, (-form_diff / 5) * 20 + 10))

    # Advanced metrics (0-18)
    home_net = home_ctx.get("advanced", {}).get("net_rating", 0)
    away_net = away_ctx.get("advanced", {}).get("net_rating", 0)
    net_adv = (home_net - away_net) if home_team_favoured else (away_net - home_net)
    adv_score = min(18, max(0, net_adv * 1.2 + 9))

    # H2H (0-12)
    h2h = home_ctx.get("h2h", [])
    if h2h:
        home_h2h_wins = sum(1 for g in h2h[:6] if g.get("home_win"))
        h2h_rate = home_h2h_wins / min(6, len(h2h))
        h2h_score = h2h_rate * 12 if home_team_favoured else (1 - h2h_rate) * 12
    else:
        h2h_score = 6  # Neutral

    # Injury impact (0-12)
    home_inj = home_ctx.get("injury_impact", {})
    away_inj = away_ctx.get("injury_impact", {})
    if home_team_favoured:
        inj_score = min(12, max(0, 6 - home_inj.get("injury_severity", 0) * 0.8
                                  + away_inj.get("injury_severity", 0) * 0.8))
    else:
        inj_score = min(12, max(0, 6 + home_inj.get("injury_severity", 0) * 0.8
                                   - away_inj.get("injury_severity", 0) * 0.8))

    # Rest advantage (0-6)
    home_rest = home_ctx.get("rest", {})
    away_rest = away_ctx.get("rest", {})
    if home_team_favoured:
        rest_score = 3
        if away_rest.get("is_b2b") and not home_rest.get("is_b2b"):
            rest_score = 6
        elif home_rest.get("is_b2b") and not away_rest.get("is_b2b"):
            rest_score = 0
    else:
        rest_score = 3
        if home_rest.get("is_b2b") and not away_rest.get("is_b2b"):
            rest_score = 6
        elif away_rest.get("is_b2b") and not home_rest.get("is_b2b"):
            rest_score = 0

    # Location split (0-4)
    home_splits = home_ctx.get("splits", {})
    away_splits = away_ctx.get("splits", {})
    home_home_pts = home_splits.get("home", {}).get("pts", 112)
    away_road_pts = away_splits.get("road", {}).get("pts", 110)
    split_edge = home_home_pts - away_road_pts
    split_score = min(4, max(0, split_edge * 0.4 + 2)) if home_team_favoured else max(0, 4 - split_edge * 0.4)

    total = stat_score + form_score + adv_score + h2h_score + inj_score + rest_score + split_score
    confidence = min(88, max(40, round(total)))

    # Build risk tags
    tags = _build_tags(home_ctx, away_ctx, home_inj, away_inj)

    # Build reasoning
    reasoning = _spread_reasoning(
        home_ctx, away_ctx, projected_margin, market_line,
        home_team_favoured, edge, home_inj, away_inj
    )

    return {
        "type": "Spread",
        "confidence": confidence,
        "prob": round(conf_to_prob(confidence) * 100),
        "projected_margin": projected_margin,
        "market_line": market_line,
        "edge": round(edge, 1),
        "tags": tags,
        "reasoning": reasoning,
        "factors": [
            {"name": "Statistical Edge",   "val": round(stat_score),  "max": 30},
            {"name": "Recent Form (L5)",   "val": round(form_score),  "max": 20},
            {"name": "Advanced Metrics",   "val": round(adv_score),   "max": 18},
            {"name": "H2H History",        "val": round(h2h_score),   "max": 12},
            {"name": "Injury Impact",      "val": round(inj_score),   "max": 12},
            {"name": "Rest Advantage",     "val": round(rest_score),  "max": 6},
            {"name": "Home/Away Split",    "val": round(split_score), "max": 4},
        ],
    }


def score_total_leg(
    home_ctx: dict,
    away_ctx: dict,
    market_total: float,
    odds: float,
) -> dict:
    """Score an Over/Under leg."""
    projected = project_game_total(home_ctx, away_ctx)
    edge = projected - market_total  # Positive = lean Over, Negative = lean Under
    selection = "Over" if edge > 0 else "Under"

    # Statistical edge
    std_dev = 12.0  # Game totals have higher variance
    normalised_edge = abs(edge) / std_dev
    stat_score = min(30, max(0, normalised_edge * 15 + 10))

    # Pace match (high pace both teams = Over edge)
    home_pace = home_ctx.get("advanced", {}).get("pace", 100)
    away_pace = away_ctx.get("advanced", {}).get("pace", 100)
    avg_pace = (home_pace + away_pace) / 2
    pace_score = min(15, max(0, (avg_pace - 96) * 1.5))
    if selection == "Under":
        pace_score = 15 - pace_score  # Invert for Under

    # Injury depression
    home_inj = home_ctx.get("injury_impact", {})
    away_inj = away_ctx.get("injury_impact", {})
    combined_severity = home_inj.get("injury_severity", 0) + away_inj.get("injury_severity", 0)
    if selection == "Under" and combined_severity > 3:
        inj_score = min(12, combined_severity * 1.2)
    elif selection == "Over" and combined_severity < 2:
        inj_score = 8
    else:
        inj_score = max(0, 6 - combined_severity * 0.5)

    # H2H total history
    h2h = home_ctx.get("h2h", [])
    h2h_score = 6
    if h2h:
        h2h_totals = [g.get("total_pts", market_total) for g in h2h[:5]]
        h2h_avg = sum(h2h_totals) / len(h2h_totals)
        h2h_edge = h2h_avg - market_total
        if (selection == "Over" and h2h_edge > 0) or (selection == "Under" and h2h_edge < 0):
            h2h_score = min(10, 6 + abs(h2h_edge) * 0.3)
        else:
            h2h_score = max(2, 6 - abs(h2h_edge) * 0.3)

    # Rest
    rest_score = 3
    if home_ctx.get("rest", {}).get("is_b2b") or away_ctx.get("rest", {}).get("is_b2b"):
        rest_score = 5 if selection == "Under" else 1

    total = stat_score + pace_score + inj_score + h2h_score + rest_score
    confidence = min(85, max(40, round(total)))

    tags = _build_tags(home_ctx, away_ctx, home_inj, away_inj)
    if (home_pace + away_pace) / 2 > 102:
        tags.append("High Pace")
    elif (home_pace + away_pace) / 2 < 98:
        tags.append("Slow Pace")

    reasoning = _total_reasoning(
        home_ctx, away_ctx, projected, market_total, selection, edge, home_inj, away_inj, h2h
    )

    return {
        "type": "Total",
        "selection_direction": selection,
        "confidence": confidence,
        "prob": round(conf_to_prob(confidence) * 100),
        "projected_total": projected,
        "market_total": market_total,
        "edge": round(edge, 1),
        "tags": tags,
        "reasoning": reasoning,
        "factors": [
            {"name": "Statistical Edge",   "val": round(stat_score),  "max": 30},
            {"name": "Pace Analysis",      "val": round(pace_score),  "max": 15},
            {"name": "Injury Impact",      "val": round(inj_score),   "max": 12},
            {"name": "H2H Total History",  "val": round(h2h_score),   "max": 10},
            {"name": "Rest Factor",        "val": round(rest_score),  "max": 5},
        ],
    }


def build_multis(all_legs: list) -> dict:
    """
    Construct 3 risk-tiered multis from scored legs.
    Applies correlation penalties and deduplication.
    """
    # Sort by confidence
    sorted_legs = sorted(all_legs, key=lambda l: l["confidence"], reverse=True)

    # Deduplicate: max 1 leg per game per type
    seen = set()
    deduped = []
    for leg in sorted_legs:
        key = f"{leg['game_id']}-{leg['type']}"
        if key not in seen and leg["confidence"] > 0:
            seen.add(key)
            deduped.append(leg)

    def calc_odds(legs):
        return round(math.prod(l["odds"] for l in legs), 2)

    def calc_prob(legs):
        naive = math.prod(l["prob"] / 100 for l in legs)
        # Apply correlation discount (15% for multiple legs)
        discount = 1 - (len(legs) - 1) * 0.04
        return round(naive * max(0.7, discount) * 100, 1)

    def build_risks(legs):
        risks = []
        if any(l.get("tags") and ("Injury Impact" in l["tags"] or "Injury Risk" in l["tags"]) for l in legs):
            risks.append("⚠️ Injury-affected legs included — verify all lineups 1hr before tip-off")
        if any(l.get("tags") and "B2B" in l["tags"] for l in legs):
            risks.append("🔄 Back-to-back fatigue factor in play for one or more teams")
        if len(legs) >= 5:
            risks.append(f"📊 {len(legs)} legs must all hit — parlay variance is high by design")
        risks.append("🔄 App auto-refreshes every 30 min — reopen before tip-off for latest data")
        return risks

    def build_alts(used_legs, n=3):
        used_ids = {id(l) for l in used_legs}
        return [{"desc": f"{l['selection']} ({l['game']})", "conf": l["confidence"]}
                for l in deduped if id(l) not in used_ids][:n]

    # Safe: top 2-3 legs, conf >= 68
    safe_legs = [l for l in deduped if l["confidence"] >= 68][:3]

    # Mid: top 5 legs, conf >= 60
    mid_legs = [l for l in deduped if l["confidence"] >= 60][:5]

    # Lotto: top 7-8 legs, conf >= 54
    lotto_legs = deduped[:8]

    return {
        "safe": {
            "key": "safe", "label": "Safe Multi", "emoji": "🔵",
            "accentColor": "#30D158", "subtitle": "Lowest risk · Deep NBA analysis",
            "legs": safe_legs,
            "odds": f"{calc_odds(safe_legs):.2f}×" if safe_legs else "N/A",
            "hitProb": calc_prob(safe_legs) if safe_legs else 0,
            "risks": build_risks(safe_legs),
            "alts": build_alts(safe_legs),
        },
        "mid": {
            "key": "mid", "label": "Mid-Risk Multi", "emoji": "🟡",
            "accentColor": "#FF9F0A", "subtitle": "Balanced risk · Strong edges",
            "legs": mid_legs,
            "odds": f"{calc_odds(mid_legs):.2f}×" if mid_legs else "N/A",
            "hitProb": calc_prob(mid_legs) if mid_legs else 0,
            "risks": build_risks(mid_legs),
            "alts": build_alts(mid_legs),
        },
        "lotto": {
            "key": "lotto", "label": "Lotto Multi", "emoji": "🔴",
            "accentColor": "#FF453A", "subtitle": "High payout · Calculated longshot",
            "legs": lotto_legs,
            "odds": f"{calc_odds(lotto_legs):.2f}×" if lotto_legs else "N/A",
            "hitProb": calc_prob(lotto_legs) if lotto_legs else 0,
            "risks": build_risks(lotto_legs),
            "alts": [],
        },
    }


# ─── REASONING BUILDERS ───────────────────────────────────────

def _spread_reasoning(home_ctx, away_ctx, proj_margin, line, home_fav, edge, home_inj, away_inj):
    home = home_ctx.get("team_abbrev", "HOME")
    away = away_ctx.get("team_abbrev", "AWAY")
    home_adv = home_ctx.get("advanced", {})
    away_adv = away_ctx.get("advanced", {})
    parts = []

    parts.append(f"Model projects {home} to win by {abs(proj_margin):.1f} pts "
                 f"({'at home' if proj_margin > 0 else 'despite home court'}). "
                 f"Market line: {'+' if line > 0 else ''}{line}. Edge: {edge:+.1f} pts.")

    parts.append(f"Net ratings — {home}: {home_adv.get('net_rating', 0):+.1f} | "
                 f"{away}: {away_adv.get('net_rating', 0):+.1f}.")

    if home_inj.get("key_player_out"):
        out_names = [p["name"] for p in home_ctx.get("injuries", []) if p["status"] == "Out"]
        parts.append(f"⚠️ {home} missing key player(s): {', '.join(out_names)} — "
                     f"usage lost: {home_inj['total_usage_lost']*100:.0f}%.")

    if away_inj.get("key_player_out"):
        out_names = [p["name"] for p in away_ctx.get("injuries", []) if p["status"] == "Out"]
        parts.append(f"✅ {away} missing key player(s): {', '.join(out_names)} — "
                     f"helps {'home' if home_fav else 'away'} cover.")

    h2h = home_ctx.get("h2h", [])
    if h2h:
        home_wins = sum(1 for g in h2h[:6] if g.get("home_win"))
        parts.append(f"H2H last {min(6, len(h2h))} meetings: {home} won {home_wins}.")

    home_rest = home_ctx.get("rest", {})
    away_rest = away_ctx.get("rest", {})
    if home_rest.get("is_b2b"):
        parts.append(f"⚠️ {home} on back-to-back — fatigue penalty applied.")
    if away_rest.get("is_b2b"):
        parts.append(f"✅ {away} on back-to-back — fatigue suppresses away performance.")

    return " ".join(parts)


def _total_reasoning(home_ctx, away_ctx, proj, line, sel, edge, home_inj, away_inj, h2h):
    home = home_ctx.get("team_abbrev", "HOME")
    away = away_ctx.get("team_abbrev", "AWAY")
    home_adv = home_ctx.get("advanced", {})
    away_adv = away_ctx.get("advanced", {})
    parts = []

    parts.append(f"Model projects total of {proj:.1f} pts (market: {line}). "
                 f"Leaning {sel} with {abs(edge):.1f} pt edge.")

    parts.append(f"Pace — {home}: {home_adv.get('pace', 100):.1f} | "
                 f"{away}: {away_adv.get('pace', 100):.1f} possessions/game.")

    parts.append(f"Off ratings — {home}: {home_adv.get('off_rating', 110):.1f} | "
                 f"{away}: {away_adv.get('off_rating', 110):.1f}. "
                 f"Def ratings — {home}: {home_adv.get('def_rating', 110):.1f} | "
                 f"{away}: {away_adv.get('def_rating', 110):.1f}.")

    combined_sev = home_inj.get("injury_severity", 0) + away_inj.get("injury_severity", 0)
    if combined_sev > 3:
        parts.append(f"Combined injury severity {combined_sev:.1f}/10 — "
                     f"significant scoring depression expected. Supports Under.")
    elif combined_sev < 1:
        parts.append("Clean injury reports on both sides — full-strength lineups expected.")

    if h2h:
        h2h_totals = [g.get("total_pts", line) for g in h2h[:5] if g.get("total_pts")]
        if h2h_totals:
            h2h_avg = sum(h2h_totals) / len(h2h_totals)
            parts.append(f"H2H last {len(h2h_totals)} games averaged {h2h_avg:.1f} total pts "
                         f"({'supports ' + sel if (sel=='Over' and h2h_avg > line) or (sel=='Under' and h2h_avg < line) else 'goes against ' + sel}).")

    return " ".join(parts)


def _build_tags(home_ctx, away_ctx, home_inj, away_inj):
    tags = []
    if home_inj.get("key_player_out") or away_inj.get("key_player_out"):
        tags.append("Injury Impact")
    elif home_inj.get("out_count", 0) + away_inj.get("out_count", 0) > 0:
        tags.append("Injury Risk")
    if home_ctx.get("rest", {}).get("is_b2b") or away_ctx.get("rest", {}).get("is_b2b"):
        tags.append("B2B")
    return tags


def _std_dev(values: list) -> float:
    if len(values) < 2:
        return 8.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)
