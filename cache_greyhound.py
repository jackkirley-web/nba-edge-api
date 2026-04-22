# cache_greyhound.py -- Greyhound racing cache
# Fetches all AU greyhound meetings, scores each race, ranks top 4
# Refreshes every 10 minutes (odds update frequently pre-race)

import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

GREY_TTL = 600  # 10 min - odds update frequently


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
        from greyhound_odds import fetch_all_race_odds_bulk
        from greyhound_model import score_all_meetings

        logger.info("=== Greyhound cache fetch start ===")

        # Fetch all AU greyhound meetings today
        logger.info("Fetching today's AU greyhound meetings...")
        meetings = get_today_meetings()
        logger.info("Found %d meetings", len(meetings))

        # Fetch all live odds in one bulk call
        logger.info("Fetching greyhound odds from The Odds API...")
        all_odds = fetch_all_race_odds_bulk()
        logger.info("Got odds for %d races", len(all_odds))

        # Score every race
        logger.info("Scoring all races...")
        scored_meetings = score_all_meetings(meetings, all_odds)

        total_races = sum(len(m["races"]) for m in scored_meetings)
        total_runners = sum(
            sum(r["runner_count"] for r in m["races"])
            for m in scored_meetings
        )

        logger.info(
            "Greyhound: %d meetings, %d races, %d runners scored",
            len(scored_meetings), total_races, total_runners
        )

        return {
            "meetings":      scored_meetings,
            "total_races":   total_races,
            "total_runners": total_runners,
            "has_odds":      bool(all_odds),
            "last_updated":  _now(),
        }


def _now():
    return datetime.now().strftime("%I:%M %p")


greyhound_cache = GreyhoundCache()
