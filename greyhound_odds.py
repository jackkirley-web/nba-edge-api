# greyhound_odds.py -- Greyhound odds extraction
# TAB API already returns fixed odds per runner in the meeting/race data.
# This module extracts and normalises those odds for the model.
# No separate API call needed - odds come bundled with race data.

import logging

logger = logging.getLogger(__name__)


def extract_odds_from_runners(runners: list) -> dict:
    """
    Extract win odds from runners already fetched from TAB API.
    Returns {runner_name: decimal_odds}
    """
    odds = {}
    for r in runners:
        name = r.get("name", "")
        runner_odds = r.get("odds")
        if name and runner_odds and runner_odds > 1.0:
            odds[name] = runner_odds
    return odds


def normalise_probs(runner_odds: dict) -> dict:
    """
    Convert raw implied probabilities to normalised probabilities (sum to 1).
    Removes bookmaker margin (overround).
    Returns {runner_name: normalised_prob}
    """
    if not runner_odds:
        return {}
    raw = {}
    for name, odds in runner_odds.items():
        if odds and odds > 1.0:
            raw[name] = 1.0 / odds
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {name: prob / total for name, prob in raw.items()}


def odds_to_implied_prob(odds: float) -> float:
    if not odds or odds <= 1.0:
        return 0.0
    return 1.0 / odds


def match_runner_odds(runner_name: str, odds_dict: dict) -> float:
    """Match a runner name to odds dict with fuzzy matching."""
    if not runner_name or not odds_dict:
        return 0.0
    clean = runner_name.upper().strip()
    for name, odds in odds_dict.items():
        if name.upper().strip() == clean:
            return odds
    parts = clean.split()
    for name, odds in odds_dict.items():
        name_upper = name.upper()
        if any(p in name_upper for p in parts if len(p) > 3):
            return odds
    return 0.0
