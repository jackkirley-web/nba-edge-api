# greyhound_model.py -- Greyhound race scoring model
#
# Scores each runner in a race and ranks top 4.
# Factors (with weights):
#   35% Market odds (implied probability, normalised)
#   20% Recent form (L5 average finishing position)
#   15% Box draw advantage (track/distance specific)
#   10% Track record (win% at this track)
#   8%  Distance record (win% at this distance)
#   7%  Track condition (wet form bonus)
#   5%  Career win%
#
# The odds factor dominates because markets efficiently price in trainer
# info, sectional times, trial data and barrier manners that we can't
# easily scrape. Combining with form and box gives a meaningful edge.

import logging
import math

logger = logging.getLogger(__name__)


# -- Model weights ----------------------------------------------------------

WEIGHTS = {
    "odds":          0.35,
    "recent_form":   0.20,
    "box_draw":      0.15,
    "track_record":  0.10,
    "dist_record":   0.08,
    "condition":     0.07,
    "career":        0.05,
}

# -- Form scoring -----------------------------------------------------------
# Lower finishing position = better. Position 1 = win.
# We convert to a score where 1st = 100, 8th = 0.

def form_score_from_positions(positions: list) -> float:
    """
    Convert list of finishing positions to a score 0-100.
    More recent races weighted more heavily.
    DNF/8+ treated as 8 (last).
    """
    if not positions:
        return 50.0  # Unknown form = neutral

    weights = [0.35, 0.25, 0.20, 0.12, 0.08]  # Most recent first
    total_w = 0.0
    score   = 0.0

    for i, pos in enumerate(positions[:5]):
        w = weights[i] if i < len(weights) else 0.05
        # Convert position to score: 1st=100, 2nd=75, 3rd=55, 4th=35, 5th+=15, 8=0
        pos_score = max(0, 100 - (pos - 1) * 14)
        score   += pos_score * w
        total_w += w

    return score / total_w if total_w > 0 else 50.0


def recent_placings_bonus(positions: list) -> float:
    """Extra bonus for consecutive top-2 finishes (in-form dogs)."""
    if not positions:
        return 0.0
    streak = 0
    for pos in positions:
        if pos <= 2:
            streak += 1
        else:
            break
    return min(15.0, streak * 5.0)


# -- Box draw scoring -------------------------------------------------------

def box_score(box_win_pct: float) -> float:
    """
    Convert box win% to a 0-100 score.
    Average box gets ~50. Box 1 gets ~80-90. Box 8 gets ~20-30.
    """
    # Typical range is 8% (box 8 at some tracks) to 22% (box 1 sprint)
    # Normalise to 0-100 within that range
    return min(100, max(0, (box_win_pct - 8.0) / (22.0 - 8.0) * 100))


# -- Track/Distance record scoring ------------------------------------------

def record_score(wins: int, starts: int) -> float:
    """
    Score a track or distance record. Returns 0-100.
    No starts = neutral (50). High win rate = high score.
    """
    if starts == 0:
        return 50.0
    win_rate = wins / starts
    # 50% win rate at a track is exceptional; 0% is bad
    return min(100, win_rate * 200)


# -- Condition scoring -------------------------------------------------------

def condition_score(runner: dict, condition_factor: float) -> float:
    """
    Score based on track condition and runner's wet form.
    If wet track (factor > 1.0), boost runners with good wet form.
    If no wet form data, neutral.
    """
    if condition_factor <= 1.0:
        return 50.0  # Good track - condition neutral

    # Check if runner has wet track wins in form string
    form_str = (runner.get("form_str") or "").upper()
    has_wet_form = "W" in form_str  # 'W' often indicates wet win in some formats

    if has_wet_form:
        return 75.0
    return 45.0  # Slight penalty for unknown wet form on heavy track


# -- Career score -----------------------------------------------------------

def career_score(wins: int, starts: int) -> float:
    if starts == 0:
        return 50.0
    win_rate = wins / starts
    # Bonus for high career win rate, penalty for never won
    if wins == 0:
        return 20.0
    return min(100, win_rate * 180 + 10)


# -- Main scoring function --------------------------------------------------

def score_race(race: dict, odds_dict: dict, normalised_probs: dict) -> list:
    """
    Score all runners in a race and return ranked top 4.

    race: { runners: [{box, name, last_5, track_wins, track_starts,
                       dist_wins, dist_starts, career_wins, career_starts,
                       scratched, form_str}],
            distance, grade, condition, track }
    odds_dict: {runner_name: decimal_odds}
    normalised_probs: {runner_name: probability 0-1}

    Returns list of runner dicts sorted by score, with rank, score, confidence.
    """
    from greyhound_data import get_box_win_pct, get_condition_factor

    distance  = race.get("distance") or 520
    track     = race.get("track") or ""
    condition = race.get("condition") or "Good"
    grade     = race.get("grade") or ""

    condition_factor = get_condition_factor(condition)

    scored_runners = []

    for runner in race.get("runners", []):
        if runner.get("scratched"):
            continue

        name = runner.get("name", "")
        box  = runner.get("box", 5)

        # ?? 1. Odds score ????????????????????????????????????????????????
        # Use normalised market probability as the base odds signal
        from greyhound_odds import match_runner_to_odds
        runner_odds = match_runner_to_odds(name, odds_dict)

        if normalised_probs:
            from greyhound_odds import odds_to_implied_prob
            # Try to get prob from normalised dict
            norm_prob = 0.0
            for odds_name, prob in normalised_probs.items():
                if _name_match(name, odds_name):
                    norm_prob = prob
                    break
            odds_s = min(100, norm_prob * 100 * 8)  # Scale to 0-100 range
        elif runner_odds > 1.0:
            # Fallback: use raw implied probability
            impl_prob = 1.0 / runner_odds
            odds_s = min(100, impl_prob * 100 * 6)
        else:
            odds_s = 50.0  # No odds available

        # ?? 2. Recent form score ?????????????????????????????????????????
        last_5    = runner.get("last_5", [])
        form_s    = form_score_from_positions(last_5)
        streak_b  = recent_placings_bonus(last_5)
        form_s    = min(100, form_s + streak_b)

        # ?? 3. Box draw score ????????????????????????????????????????????
        box_pct   = get_box_win_pct(track, distance, box)
        box_s     = box_score(box_pct)

        # ?? 4. Track record ??????????????????????????????????????????????
        track_s   = record_score(
            runner.get("track_wins", 0),
            runner.get("track_starts", 0)
        )

        # ?? 5. Distance record ???????????????????????????????????????????
        dist_s    = record_score(
            runner.get("dist_wins", 0),
            runner.get("dist_starts", 0)
        )

        # ?? 6. Condition score ???????????????????????????????????????????
        cond_s    = condition_score(runner, condition_factor)

        # ?? 7. Career score ??????????????????????????????????????????????
        career_s  = career_score(
            runner.get("career_wins", 0),
            runner.get("career_starts", 0)
        )

        # ?? Weighted composite score ?????????????????????????????????????
        composite = (
            WEIGHTS["odds"]         * odds_s   +
            WEIGHTS["recent_form"]  * form_s   +
            WEIGHTS["box_draw"]     * box_s    +
            WEIGHTS["track_record"] * track_s  +
            WEIGHTS["dist_record"]  * dist_s   +
            WEIGHTS["condition"]    * cond_s   +
            WEIGHTS["career"]       * career_s
        )

        # Confidence = how far above the average this runner scores
        # In a field of 8, average score is ~50
        confidence = min(99, max(1, round(composite)))

        scored_runners.append({
            **runner,
            "score":        round(composite, 2),
            "confidence":   confidence,
            "odds":         runner_odds if runner_odds > 1.0 else None,
            "implied_prob": round((1 / runner_odds * 100) if runner_odds > 1.0 else 0, 1),
            "score_breakdown": {
                "odds_score":   round(odds_s, 1),
                "form_score":   round(form_s, 1),
                "box_score":    round(box_s, 1),
                "track_score":  round(track_s, 1),
                "dist_score":   round(dist_s, 1),
                "cond_score":   round(cond_s, 1),
                "career_score": round(career_s, 1),
            },
            "form_str":     runner.get("form_str", ""),
            "last_5":       last_5,
        })

    # Sort by composite score descending
    scored_runners.sort(key=lambda r: r["score"], reverse=True)

    # Assign ranks
    for i, r in enumerate(scored_runners):
        r["rank"] = i + 1

    # Build race result
    top4 = scored_runners[:4]

    # Generate reasoning for top pick
    if top4:
        top4[0]["reasoning"] = _build_reasoning(top4[0], race, odds_dict)

    return scored_runners


def _build_reasoning(runner: dict, race: dict, odds_dict: dict) -> str:
    """Generate a brief reasoning string for the top pick."""
    name      = runner.get("name", "Unknown")
    box       = runner.get("box", "?")
    last_5    = runner.get("last_5", [])
    odds      = runner.get("odds")
    bd        = runner.get("score_breakdown", {})
    track     = race.get("track", "")
    condition = race.get("condition", "Good")

    parts = []

    # Odds
    if odds:
        parts.append(f"Market price ${odds:.2f}")

    # Form
    if last_5:
        form_display = "-".join(str(p) for p in last_5)
        recent_wins  = sum(1 for p in last_5[:3] if p == 1)
        if recent_wins >= 2:
            parts.append(f"excellent recent form ({form_display})")
        elif last_5 and last_5[0] <= 2:
            parts.append(f"placed last start ({form_display})")
        else:
            parts.append(f"form {form_display}")

    # Box
    if bd.get("box_score", 50) >= 70:
        parts.append(f"box {box} advantage at this track")
    elif bd.get("box_score", 50) <= 30:
        parts.append(f"wide draw from box {box}")
    else:
        parts.append(f"box {box}")

    # Track record
    tw = runner.get("track_wins", 0)
    ts = runner.get("track_starts", 0)
    if ts >= 3 and tw / ts >= 0.3:
        parts.append(f"strong track record ({tw}/{ts})")

    # Condition
    if condition.lower() in ("wet", "heavy", "soft"):
        parts.append(f"{condition.lower()} track")

    return ". ".join(p.capitalize() for p in parts) + "." if parts else ""


def _name_match(name1: str, name2: str) -> bool:
    """Fuzzy name matching between form guide and odds."""
    n1 = name1.upper().strip()
    n2 = name2.upper().strip()
    if n1 == n2:
        return True
    # Check if one contains the other
    if n1 in n2 or n2 in n1:
        return True
    # Check first word match (greyhound names sometimes have prefixes)
    parts1 = n1.split()
    parts2 = n2.split()
    if parts1 and parts2 and parts1[0] == parts2[0]:
        return True
    return False


def score_all_meetings(meetings: list, all_odds: dict) -> list:
    """
    Score all races across all meetings.
    Returns structured list ready for the API.
    """
    from greyhound_odds import normalise_probs

    results = []

    for meeting in meetings:
        track     = meeting.get("track", "")
        state     = meeting.get("state", "")
        condition = meeting.get("track_condition", "Good")

        meeting_result = {
            "track":      track,
            "state":      state,
            "condition":  condition,
            "date":       meeting.get("date", ""),
            "races":      [],
        }

        for race in meeting.get("races", []):
            # Match this race to odds events
            # The Odds API names races like "R1 Sandown Park"
            race_odds  = {}
            norm_probs = {}

            # Try to find matching event in bulk odds
            for event_id, event_data in all_odds.items():
                event_name = event_data.get("event_name", "")
                if _race_matches_event(race, track, event_name):
                    race_odds  = event_data.get("runner_odds", {})
                    norm_probs = normalise_probs(race_odds)
                    break

            # Score the race
            scored = score_race(race, race_odds, norm_probs)

            top4 = scored[:4]
            all_runners = scored

            meeting_result["races"].append({
                "race_num":     race.get("race_num"),
                "race_time":    race.get("race_time"),
                "distance":     race.get("distance"),
                "grade":        race.get("grade"),
                "condition":    condition,
                "track":        track,
                "has_odds":     bool(race_odds),
                "top_4":        top4,
                "all_runners":  all_runners,
                "runner_count": len(all_runners),
            })

        if meeting_result["races"]:
            results.append(meeting_result)

    return results


def _race_matches_event(race: dict, track: str, event_name: str) -> bool:
    """Check if a race matches an odds API event name."""
    event_upper = event_name.upper()
    track_upper = track.upper()

    # Check track name appears in event
    track_words = track_upper.split()
    if not any(w in event_upper for w in track_words if len(w) > 3):
        return False

    # Check race number if present in event name
    race_num = race.get("race_num")
    if race_num:
        if f"R{race_num}" in event_upper or f"RACE {race_num}" in event_upper:
            return True

    return True  # Track matched, assume race match
