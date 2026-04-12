# engine.py — Scores spread and total legs, builds multis

import logging
import math

logger = logging.getLogger(__name__)


# ── SPREAD LEG SCORER ──────────────────────────────────────────────
def score_spread_leg(home_ctx, away_ctx, line, home_is_fav, odds=1.91):
    """
    Score a spread leg.
    home_is_fav: True if home team is favoured (negative spread).
    line: absolute value of the spread.
    Returns dict with confidence, prob, reasoning, factors, etc.
    """
    fav_ctx  = home_ctx if home_is_fav else away_ctx
    dog_ctx  = away_ctx if home_is_fav else home_ctx

    score = 50  # Baseline

    # ── Net rating advantage ─────────────────────────────────────
    fav_net = fav_ctx.get("advanced", {}).get("net_rating", 0)
    dog_net = dog_ctx.get("advanced", {}).get("net_rating", 0)
    net_diff = fav_net - dog_net

    if net_diff > 8:
        score += 15
    elif net_diff > 4:
        score += 9
    elif net_diff > 2:
        score += 5
    elif net_diff > 0:
        score += 2
    elif net_diff < -4:
        score -= 10
    elif net_diff < -2:
        score -= 5

    # ── Recent form (L5 net) ────────────────────────────────────
    fav_l5  = fav_ctx.get("recent_l5", {}).get("net_rating", 0)
    dog_l5  = dog_ctx.get("recent_l5", {}).get("net_rating", 0)
    form_diff = fav_l5 - dog_l5

    if form_diff > 6:
        score += 8
    elif form_diff > 3:
        score += 4
    elif form_diff < -6:
        score -= 8
    elif form_diff < -3:
        score -= 4

    # ── Home court advantage ────────────────────────────────────
    if home_is_fav:
        score += 5  # Home favourite gets a boost
    else:
        score -= 3  # Away favourite — slight discount

    # ── Injury impact ───────────────────────────────────────────
    fav_inj = fav_ctx.get("injury_impact", 0)
    dog_inj = dog_ctx.get("injury_impact", 0)

    if fav_inj > 0.25:
        score -= 12
        fav_injured = True
    elif fav_inj > 0.15:
        score -= 6
        fav_injured = True
    else:
        fav_injured = False

    if dog_inj > 0.25:
        score += 8
    elif dog_inj > 0.15:
        score += 4

    # ── Spread size reasonableness ──────────────────────────────
    # Large spreads are harder to cover
    if line > 12:
        score -= 8
    elif line > 9:
        score -= 4
    elif line <= 3:
        score += 3

    score = max(30, min(85, round(score)))

    # Build factors for display
    factors = [
        {"name": "Net Rating Edge",    "val": min(25, max(0, round(net_diff * 2 + 12))), "max": 25},
        {"name": "Recent Form (L5)",   "val": min(20, max(0, round(form_diff + 10))),    "max": 20},
        {"name": "Home/Away",          "val": 15 if home_is_fav else 7,                  "max": 15},
        {"name": "Injury Situation",   "val": 0 if fav_injured else 10,                  "max": 10},
        {"name": "Spread Value",       "val": max(0, 10 - round(line)),                  "max": 10},
    ]

    # Reasoning
    fav_name = fav_ctx.get("team_abbrev", "FAV")
    dog_name = dog_ctx.get("team_abbrev", "DOG")
    parts = [
        f"{'Home' if home_is_fav else 'Away'} {fav_name} -{line:.1f} vs {dog_name}.",
        f"Net rating edge: {net_diff:+.1f} pts/100.",
        f"Recent L5 form diff: {form_diff:+.1f}.",
    ]
    if fav_injured:
        parts.append(f"⚠️ {fav_name} has significant injury concerns.")
    if dog_inj > 0.15:
        parts.append(f"✅ {dog_name} missing key players — boosts {fav_name} cover chance.")

    tags = []
    if fav_inj > 0.15:
        tags.append("Injury Risk")
    if dog_inj > 0.15:
        tags.append("Injury Impact")
    if line > 9:
        tags.append("High Variance")

    prob = _score_to_prob(score)

    return {
        "type":             "Spread",
        "confidence":       score,
        "prob":             round(prob * 100),
        "reasoning":        " ".join(parts),
        "factors":          factors,
        "tags":             tags,
        "projected_margin": round(net_diff * 0.4, 1),
        "projected_total":  None,
        "edge":             round(net_diff * 0.4 - line, 1),
        "selection_direction": fav_name,
    }


# ── TOTAL LEG SCORER ───────────────────────────────────────────────
def score_total_leg(home_ctx, away_ctx, line, odds=1.91):
    """
    Score an Over/Under total leg.
    Returns dict with confidence, direction (Over/Under), etc.
    """
    home_adv = home_ctx.get("advanced", {})
    away_adv = away_ctx.get("advanced", {})
    home_l5  = home_ctx.get("recent_l5", {})
    away_l5  = away_ctx.get("recent_l5", {})

    # Combined pace
    home_pace = home_adv.get("pace", 100)
    away_pace = away_adv.get("pace", 100)
    avg_pace = (home_pace + away_pace) / 2

    # Offensive ratings
    home_off = home_adv.get("off_rating", 110)
    away_off = away_adv.get("off_rating", 110)
    home_def = home_adv.get("def_rating", 110)
    away_def = away_adv.get("def_rating", 110)

    # Project total
    projected = (home_off + away_off - home_def - away_def) / 2
    projected = projected * (avg_pace / 100) + line  # Rough projection

    score = 50
    lean_over = projected > line

    diff = abs(projected - line)
    if diff > 8:
        score += 20
    elif diff > 5:
        score += 13
    elif diff > 3:
        score += 7
    elif diff > 1:
        score += 3

    # Pace factor
    if avg_pace > 103:
        if lean_over:
            score += 8
        else:
            score -= 5
    elif avg_pace < 97:
        if not lean_over:
            score += 8
        else:
            score -= 5

    # Recent scoring form
    home_pts_l5 = home_l5.get("pts", 110)
    away_pts_l5 = away_l5.get("pts", 110)
    combined_pts = home_pts_l5 + away_pts_l5
    if combined_pts > line + 8 and lean_over:
        score += 7
    elif combined_pts < line - 8 and not lean_over:
        score += 7

    # Injury factor — injuries push toward under
    home_inj = home_ctx.get("injury_impact", 0)
    away_inj = away_ctx.get("injury_impact", 0)
    total_inj = home_inj + away_inj

    if total_inj > 0.3:
        if not lean_over:
            score += 8
        else:
            score -= 6

    score = max(30, min(83, round(score)))

    direction = "Over" if lean_over else "Under"

    tags = []
    if avg_pace > 103:
        tags.append("High Pace")
    elif avg_pace < 97:
        tags.append("Slow Pace")
    if total_inj > 0.2:
        tags.append("Injury Impact")

    factors = [
        {"name": "Pace",             "val": min(25, round((avg_pace - 95) * 2)), "max": 25},
        {"name": "Off Rating",       "val": min(20, round((home_off + away_off - 210) * 2)), "max": 20},
        {"name": "Recent Scoring",   "val": min(20, round(combined_pts - 200)), "max": 20},
        {"name": "Injury Impact",    "val": 10 if total_inj < 0.1 else 0, "max": 10},
    ]

    prob = _score_to_prob(score)

    return {
        "type":               "Total",
        "confidence":         score,
        "prob":               round(prob * 100),
        "reasoning":          (
            f"{direction} {line}. Projected combined scoring near {round(projected)}. "
            f"Avg pace: {avg_pace:.1f}. "
            f"{'High-pace matchup boosts scoring.' if avg_pace > 102 else 'Low-pace game expected.'}"
        ),
        "factors":            factors,
        "tags":               tags,
        "projected_margin":   None,
        "projected_total":    round(projected, 1),
        "edge":               round(projected - line, 1) if lean_over else round(line - projected, 1),
        "selection_direction": direction,
    }


# ── MULTI BUILDER ──────────────────────────────────────────────────
def build_multis(legs: list) -> dict:
    """
    Build Safe/Mid/Lotto multis from a list of scored legs.
    Each leg must have: confidence, prob, odds, game_id, selection, type, game.
    """
    if not legs:
        return _empty_picks()

    # Sort by confidence descending
    sorted_legs = sorted(legs, key=lambda x: x.get("confidence", 0), reverse=True)

    # Deduplicate by game_id — one leg per game
    seen_games = set()
    unique_legs = []
    for leg in sorted_legs:
        gid = leg.get("game_id")
        if gid not in seen_games:
            seen_games.add(gid)
            unique_legs.append(leg)

    def build_multi(selected_legs, key, label, emoji, accent):
        if not selected_legs:
            return {
                "key": key, "label": label, "emoji": emoji, "accentColor": accent,
                "subtitle": "Not enough qualifying legs today",
                "legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": [],
            }

        # Calculate combined odds
        combined = 1.0
        for leg in selected_legs:
            combined *= leg.get("odds", 1.91)
        combined = round(combined, 2)

        # Hit probability (product of individual probs)
        hit_prob = 1.0
        for leg in selected_legs:
            hit_prob *= (leg.get("prob", 55) / 100)
        hit_prob = round(hit_prob * 100)

        # Risks
        risks = []
        for leg in selected_legs:
            if "Injury Risk" in leg.get("tags", []):
                risks.append(f"{leg.get('selection', '')} — player injury concern")
            if "B2B" in leg.get("tags", []):
                risks.append(f"{leg.get('game', '')} — back-to-back fatigue")
        if not risks:
            risks = ["No major risks identified"]

        subtitle = f"{len(selected_legs)} legs · {combined}× odds"

        return {
            "key": key, "label": label, "emoji": emoji, "accentColor": accent,
            "subtitle": subtitle,
            "legs": selected_legs,
            "odds": f"{combined}×",
            "hitProb": hit_prob,
            "risks": risks[:3],
            "alts": [],
        }

    # Safe: top 2 highest confidence legs, conf >= 60
    safe_legs = [l for l in unique_legs if l.get("confidence", 0) >= 60][:2]

    # Mid: top 3-4 legs, conf >= 55
    mid_legs = [l for l in unique_legs if l.get("confidence", 0) >= 55][:4]

    # Lotto: top 5-6 legs, conf >= 50, includes lower conf
    lotto_legs = unique_legs[:6]

    return {
        "safe":  build_multi(safe_legs,  "safe",  "Safe Multi",     "🔵", "#4CAF7D"),
        "mid":   build_multi(mid_legs,   "mid",   "Mid-Risk Multi", "🟡", "#C9A84C"),
        "lotto": build_multi(lotto_legs, "lotto", "Lotto Multi",    "🔴", "#E05252"),
    }


def _score_to_prob(score: int) -> float:
    if score >= 80: return 0.70
    if score >= 72: return 0.64
    if score >= 65: return 0.59
    if score >= 58: return 0.55
    if score >= 50: return 0.52
    return 0.48


def _empty_picks():
    empty = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**empty, "key": "safe",  "label": "Safe Multi",     "emoji": "🔵", "accentColor": "#4CAF7D", "subtitle": "No games today"},
        "mid":   {**empty, "key": "mid",   "label": "Mid-Risk Multi", "emoji": "🟡", "accentColor": "#C9A84C", "subtitle": "No games today"},
        "lotto": {**empty, "key": "lotto", "label": "Lotto Multi",    "emoji": "🔴", "accentColor": "#E05252", "subtitle": "No games today"},
    }
