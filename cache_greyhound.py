# cache_greyhound.py -- Greyhound racing cache
# Data source: TAB API (meetings, runners, form, odds all in one call)
# Refreshes every 10 minutes

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

GREY_TTL = 600  # 10 min


class GreyhoundCache:
    def __init__(self):
        self._lock         = threading.Lock()
        self._data         = {}
        self._last_refresh = None

    def get(self, force_refresh=False) -> dict:
        with self._lock:
            age = (datetime.now() - self._last_refresh).seconds if self._last_refresh else 9999
            if force_refresh or age > GREY_TTL or not self._data:
                logger.info("Greyhound cache refreshing...")
                try:
                    self._data = self._fetch_all()
                    self._last_refresh = datetime.now()
                except Exception as e:
                    logger.error("Greyhound cache failed: %s", e)
                    if not self._data:
                        self._data = {
                            "meetings": [], "total_races": 0,
                            "last_updated": _now(), "error": str(e),
                        }
        return self._data

    def _fetch_all(self) -> dict:
        from greyhound_data import get_today_meetings
        from greyhound_odds import extract_odds_from_runners, normalise_probs
        from greyhound_model import score_race

        logger.info("=== Greyhound cache fetch start ===")

        meetings_raw = get_today_meetings()
        logger.info("Greyhound: %d raw meetings fetched", len(meetings_raw))

        scored_meetings = []
        total_races = 0
        total_runners = 0

        for meeting in meetings_raw:
            track     = meeting.get("track", "")
            state     = meeting.get("state", "")
            condition = meeting.get("condition", "Good")
            scored_races = []

            for race in meeting.get("races", []):
                runners = race.get("runners", [])
                if not runners:
                    continue

                # Extract odds from TAB runner data (already included)
                odds_dict  = extract_odds_from_runners(runners)
                norm_probs = normalise_probs(odds_dict)
                has_odds   = bool(odds_dict)

                # Score all runners
                try:
                    scored = score_race(race, odds_dict, norm_probs)
                except Exception as e:
                    logger.warning("Score failed for %s R%s: %s", track, race.get("race_num"), e)
                    scored = []

                if not scored:
                    continue

                total_races   += 1
                total_runners += len(scored)

                scored_races.append({
                    "race_num":     race.get("race_num"),
                    "race_time":    race.get("race_time"),
                    "distance":     race.get("distance"),
                    "grade":        race.get("grade"),
                    "condition":    condition,
                    "track":        track,
                    "has_odds":     has_odds,
                    "top_4":        scored[:4],
                    "all_runners":  scored,
                    "runner_count": len(scored),
                })

            if scored_races:
                scored_meetings.append({
                    "track":     track,
                    "state":     state,
                    "condition": condition,
                    "date":      meeting.get("date", ""),
                    "races":     scored_races,
                })

        logger.info(
            "Greyhound: %d meetings, %d races, %d runners scored",
            len(scored_meetings), total_races, total_runners
        )

        return {
            "meetings":      scored_meetings,
            "total_races":   total_races,
            "total_runners": total_runners,
            "has_odds":      any(
                r.get("has_odds")
                for m in scored_meetings
                for r in m.get("races", [])
            ),
            "last_updated":  _now(),
        }


def _now():
    return datetime.now().strftime("%I:%M %p")


greyhound_cache = GreyhoundCache()
