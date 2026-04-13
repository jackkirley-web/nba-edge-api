# afl_engine.py -- AFL game analysis engine
# Scores game lines (head-to-head, spreads, totals)
# Uses team form, venue factors, h2h history, ladder position
# Builds Safe/Mid/Lotto multis same structure as NBA

import logging
import math

logger = logging.getLogger(__name__)


# -- Form/rating factors ----------------------------------------------------

# How much each factor contributes to the model
WEIGHTS = {
    "ladder_pct":   0.25,   # Ladder percentage (season form)
    "recent_l5":    0.30,   # Last 5 games form
    "h2h_record":   0.15,   # Head-to-head history
    "venue":        0.15,   # Venue/home advantage
    "squiggle_tip": 0.15,   # Squiggle aggregate model consensus
}


def build_game_context(game: dict, team_stats: dict, ladder: list,
                       h2h_history: list, venue_stats: dict,
                       squiggle_tips: list, game_odds: dict) -> dict:
    """
    Build full context dict for a game needed by the scoring functions.
    """
    home = game["home_team"]
    away = game["away_team"]

    # Ladder stats
    home_ladder = next((t for t in ladder if t["team"] == home), {})
    away_ladder = next((t for t in ladder if t["team"] == away), {})

    # Team stats
    home_stats = team_stats.get(home, {})
    away_stats = team_stats.get(away, {})

    # Squiggle tip for this game
    tip = next((t for t in squiggle_tips
                if (t["home_team"] == home and t["away_team"] == away) or
                   (t["home_team"] == away and t["away_team"] == home)), {})

    return {
        "game":        game,
        "home_team":   home,
        "away_team":   away,
        "home_ladder": home_ladder,
        "away_ladder": away_ladder,
        "home_stats":  home_stats,
        "away_stats":  away_stats,
        "h2h":         h2h_history,
        "venue":       venue_stats,
        "tip":         tip,
        "odds":        game_odds,
    }


def score_afl_line(ctx: dict) -> dict:
    """
    Score a game-line (head-to-head) bet.
    Returns confidence, reasoning, projected margin.
    """
    home = ctx["home_team"]
    away = ctx["away_team"]
    home_ladder = ctx["home_ladder"]
    away_ladder = ctx["away_ladder"]
    venue = ctx["venue"]
    tip = ctx["tip"]
    odds = ctx["odds"]

    score = 50  # Baseline

    # -- Ladder percentage differential ------------------------------------
    home_pct = home_ladder.get("pct", 100)
    away_pct  = away_ladder.get("pct", 100)
    pct_diff  = home_pct - away_pct

    if pct_diff > 50:
        score += 18
    elif pct_diff > 25:
        score += 12
    elif pct_diff > 10:
        score += 7
    elif pct_diff > 0:
        score += 3
    elif pct_diff < -50:
        score -= 18
    elif pct_diff < -25:
        score -= 12
    elif pct_diff < -10:
        score -= 7
    else:
        score -= 3

    # -- Venue / home ground advantage ------------------------------------
    home_adv = venue.get("home_adv", 1.04)
    venue_score = round((home_adv - 1.0) * 100)  # e.g. 1.09 -> 9 points
    score += min(12, max(-4, venue_score))

    # -- H2H record -------------------------------------------------------
    if ctx["h2h"]:
        recent_h2h = ctx["h2h"][:10]
        home_wins = sum(1 for g in recent_h2h if g.get("winner") == home)
        h2h_rate = home_wins / len(recent_h2h)
        if h2h_rate >= 0.7:
            score += 8
        elif h2h_rate >= 0.6:
            score += 4
        elif h2h_rate <= 0.3:
            score -= 8
        elif h2h_rate <= 0.4:
            score -= 4

    # -- Squiggle aggregate model consensus --------------------------------
    if tip:
        tip_team = tip.get("tip", "")
        home_conf = tip.get("home_conf", 50)
        if tip_team == home:
            bonus = min(12, round((home_conf - 50) * 0.25))
            score += max(0, bonus)
        elif tip_team == away:
            penalty = min(12, round((50 - home_conf) * 0.25))
            score -= max(0, penalty)

    # -- Ladder position --------------------------------------------------
    home_pos = home_ladder.get("position", 9)
    away_pos  = away_ladder.get("position", 9)
    pos_diff  = away_pos - home_pos  # positive = home team higher
    if pos_diff >= 6:
        score += 8
    elif pos_diff >= 3:
        score += 4
    elif pos_diff <= -6:
        score -= 8
    elif pos_diff <= -3:
        score -= 4

    score = max(30, min(85, round(score)))

    # Projected margin using Squiggle tip or ladder pct
    if tip and tip.get("margin"):
        tip_margin = tip["margin"]
        if tip.get("tip") == away:
            tip_margin = -tip_margin
        projected_margin = round(tip_margin, 1)
    else:
        projected_margin = round(pct_diff * 0.3, 1)

    # Determine lean (home or away)
    lean_home = score >= 50
    lean_team = home if lean_home else away

    # Tags
    tags = []
    if venue.get("home_adv", 1.04) >= 1.08:
        tags.append("Strong Home Ground")
    if abs(home_pct - away_pct) > 40:
        tags.append("Large Form Gap")
    if abs(home_pos - away_pos) >= 6:
        tags.append("Ladder Gap")

    prob = _score_to_prob(score)

    return {
        "type":              "Line",
        "lean_team":         lean_team,
        "confidence":        score,
        "prob":              round(prob * 100),
        "projected_margin":  projected_margin,
        "projected_total":   None,
        "tags":              tags,
        "reasoning":         _build_line_reasoning(home, away, home_ladder, away_ladder,
                                                    venue, tip, projected_margin, lean_team),
        "factors": [
            {"name": "Form (Ladder %)",   "val": min(25, max(0, round(pct_diff * 0.25 + 12))), "max": 25},
            {"name": "Venue Advantage",   "val": min(15, max(0, venue_score + 5)),              "max": 15},
            {"name": "H2H Record",        "val": 10 if h2h_rate >= 0.6 else 5 if h2h_rate >= 0.5 else 0, "max": 10},
            {"name": "Model Consensus",   "val": min(15, max(0, round(abs(home_conf - 50) * 0.3))), "max": 15},
            {"name": "Ladder Position",   "val": min(10, max(0, abs(pos_diff) + 5)),             "max": 10},
        ] if tip else [],
    }


def score_afl_total(ctx: dict) -> dict:
    """
    Score an AFL total (game points Over/Under).
    """
    venue = ctx["venue"]
    home_stats = ctx["home_stats"]
    away_stats  = ctx["away_stats"]
    total_line  = ctx["odds"].get("total_line", 155)
    home = ctx["home_team"]
    away = ctx["away_team"]

    if not total_line:
        return {}

    # Project total from team scoring averages + venue factor
    home_avg_for     = home_stats.get("avg_score", 88)
    home_avg_against = home_stats.get("avg_conceded", 78)
    away_avg_for     = away_stats.get("avg_score", 85)
    away_avg_against = away_stats.get("avg_conceded", 80)

    # Projected game total
    projected = (
        (home_avg_for + away_avg_against) / 2 +
        (away_avg_for + home_avg_against) / 2
    )

    # Apply venue factor
    venue_avg = venue.get("avg_total", 157)
    projected = projected * 0.7 + venue_avg * 0.3

    diff = projected - total_line
    lean_over = diff > 0

    score = 50
    if abs(diff) > 12:
        score += 22
    elif abs(diff) > 8:
        score += 15
    elif abs(diff) > 5:
        score += 9
    elif abs(diff) > 2:
        score += 4

    score = max(30, min(83, round(score)))

    direction = "Over" if lean_over else "Under"
    prob = _score_to_prob(score)

    tags = []
    if venue.get("avg_total", 157) > 163:
        tags.append("High Scoring Venue")
    elif venue.get("avg_total", 157) < 150:
        tags.append("Low Scoring Venue")

    return {
        "type":             "Total",
        "lean_direction":   direction,
        "confidence":       score,
        "prob":             round(prob * 100),
        "projected_total":  round(projected, 1),
        "projected_margin": None,
        "tags":             tags,
        "reasoning": (
            f"{direction} {total_line}. Model projects combined score of "
            f"{projected:.0f} points. {home} avg for: {home_avg_for:.0f}, "
            f"{away} avg for: {away_avg_for:.0f}. "
            f"Venue ({venue.get('name', '')}) averages {venue.get('avg_total', 157):.0f} pts."
        ),
    }


def build_afl_multis(legs: list) -> dict:
    """
    Build Safe/Mid/Lotto multis from scored AFL legs.
    Same structure as NBA multis for frontend compatibility.
    """
    if not legs:
        return _empty_picks()

    sorted_legs = sorted(legs, key=lambda x: x.get("confidence", 0), reverse=True)

    # Deduplicate by game_id
    seen = set()
    unique = []
    for leg in sorted_legs:
        gid = leg.get("game_id")
        if gid not in seen:
            seen.add(gid)
            unique.append(leg)

    def build_multi(selected, key, label, emoji, accent):
        if not selected:
            return {
                "key": key, "label": label, "emoji": emoji, "accentColor": accent,
                "subtitle": "Not enough qualifying legs",
                "legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": [],
            }
        combined = 1.0
        for leg in selected:
            combined *= leg.get("odds", 1.85)
        combined = round(combined, 2)

        hit_prob = 1.0
        for leg in selected:
            hit_prob *= (leg.get("prob", 55) / 100)
        hit_prob = round(hit_prob * 100)

        risks = []
        for leg in selected:
            for tag in leg.get("tags", []):
                if "Weather" in tag or "Travel" in tag:
                    risks.append(f"{leg.get('selection', '')} -- {tag}")
        if not risks:
            risks = ["No major risk flags identified"]

        return {
            "key": key, "label": label, "emoji": emoji, "accentColor": accent,
            "subtitle": f"{len(selected)} legs - {combined}x odds",
            "legs": selected,
            "odds": f"{combined}x",
            "hitProb": hit_prob,
            "risks": risks[:3],
            "alts": [],
        }

    safe_legs  = [l for l in unique if l.get("confidence", 0) >= 62][:2]
    mid_legs   = [l for l in unique if l.get("confidence", 0) >= 56][:4]
    lotto_legs = unique[:6]

    return {
        "safe":  build_multi(safe_legs,  "safe",  "Safe Multi",     "🔵", "#4CAF7D"),
        "mid":   build_multi(mid_legs,   "mid",   "Mid-Risk Multi", "🟡", "#C9A84C"),
        "lotto": build_multi(lotto_legs, "lotto", "Lotto Multi",    "🔴", "#E05252"),
    }


def _build_line_reasoning(home, away, home_ladder, away_ladder, venue, tip,
                           projected_margin, lean_team) -> str:
    parts = []
    parts.append(
        f"Lean: {lean_team}. Projected margin: {abs(projected_margin):.0f} pts "
        f"to {lean_team}."
    )
    home_pos = home_ladder.get("position", "?")
    away_pos = away_ladder.get("position", "?")
    home_pct = home_ladder.get("pct", 0)
    away_pct = away_ladder.get("pct", 0)
    parts.append(
        f"{home} ({home_pos}th, {home_pct:.0f}%) vs {away} ({away_pos}th, {away_pct:.0f}%)."
    )
    if venue.get("home_adv", 1.04) >= 1.07:
        parts.append(f"Strong home ground advantage at {venue.get('name', 'venue')}.")
    if tip and tip.get("tip"):
        parts.append(
            f"Squiggle aggregate model tips {tip['tip']} "
            f"by {tip.get('margin', 0):.0f} pts."
        )
    return " ".join(parts)


def _score_to_prob(score: int) -> float:
    if score >= 80: return 0.70
    if score >= 72: return 0.64
    if score >= 65: return 0.59
    if score >= 58: return 0.55
    if score >= 50: return 0.52
    return 0.48


def _empty_picks():
    e = {"legs": [], "odds": "N/A", "hitProb": 0, "risks": [], "alts": []}
    return {
        "safe":  {**e, "key": "safe",  "label": "Safe Multi",    "accentColor": "#4CAF7D", "subtitle": "No games this round"},
        "mid":   {**e, "key": "mid",   "label": "Mid-Risk Multi","accentColor": "#C9A84C", "subtitle": "No games this round"},
        "lotto": {**e, "key": "lotto", "label": "Lotto Multi",   "accentColor": "#E05252", "subtitle": "No games this round"},
    }
