# data_store.py — persistent disk storage + simple background scheduler

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path("/tmp/nba_edge_cache")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAIN_FILE = DATA_DIR / "main_cache.json"
STREAK_FILE = DATA_DIR / "streak_cache.json"
META_FILE = DATA_DIR / "meta.json"


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _read_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception as e:
        logger.warning("Failed reading %s: %s", path.name, e)
    return default


def _write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload))


def _read_meta() -> dict:
    return _read_json(META_FILE, {})


def _update_meta(key: str, value) -> None:
    meta = _read_meta()
    meta[key] = value
    _write_json(META_FILE, meta)


def save_main_data(data: dict):
    """Save full main cache, including internal helper fields."""
    try:
        saveable = dict(data)

        if isinstance(saveable.get("_today_team_ids"), set):
            saveable["_today_team_ids"] = list(saveable["_today_team_ids"])

        saveable["_saved_at"] = _now_iso()
        _write_json(MAIN_FILE, saveable)
        _update_meta("main_saved_at", saveable["_saved_at"])
        logger.info("Main data saved to disk")
    except Exception as e:
        logger.warning("Failed to save main data: %s", e)


def load_main_data() -> dict:
    """Load main cache from disk."""
    try:
        data = _read_json(MAIN_FILE, {})
        if data:
            if isinstance(data.get("_today_team_ids"), list):
                data["_today_team_ids"] = set(data["_today_team_ids"])
            logger.info("Loaded main data from disk")
            return data
    except Exception as e:
        logger.warning("Failed to load main data: %s", e)
    return {}


def save_streak_data(data: list):
    try:
        payload = {"streaks": data, "_saved_at": _now_iso()}
        _write_json(STREAK_FILE, payload)
        _update_meta("streak_saved_at", payload["_saved_at"])
        logger.info("Streak data saved to disk")
    except Exception as e:
        logger.warning("Failed to save streak data: %s", e)


def load_streak_data() -> list:
    try:
        payload = _read_json(STREAK_FILE, {})
        streaks = payload.get("streaks", [])
        if streaks:
            logger.info("Loaded streak data from disk")
        return streaks
    except Exception as e:
        logger.warning("Failed to load streak data: %s", e)
    return []


def get_data_age_str() -> str:
    meta = _read_meta()
    saved_at = meta.get("main_saved_at")
    if not saved_at:
        return "unknown time"

    try:
        saved_dt = datetime.fromisoformat(saved_at)
        delta = datetime.utcnow() - saved_dt

        if delta < timedelta(minutes=1):
            return "less than a minute ago"
        if delta < timedelta(hours=1):
            mins = int(delta.total_seconds() // 60)
            return f"{mins} minute{'s' if mins != 1 else ''} ago"
        if delta < timedelta(days=1):
            hrs = int(delta.total_seconds() // 3600)
            return f"{hrs} hour{'s' if hrs != 1 else ''} ago"
        days = delta.days
        return f"{days} day{'s' if days != 1 else ''} ago"
    except Exception:
        return "unknown time"


class DailyPrefetchScheduler:
    """
    Lightweight background scheduler.
    - Runs one warm-up fetch shortly after startup
    - Then refreshes every 30 minutes
    """

    def __init__(self):
        self._thread = None
        self._started = False
        self._lock = threading.Lock()

    def start(self, fetch_fn):
        with self._lock:
            if self._started:
                return
            self._started = True

        def loop():
            logger.info("Prefetch scheduler started")
            time.sleep(20)

            while True:
                try:
                    logger.info("Prefetch scheduler triggering cache refresh")
                    fetch_fn(force_refresh=True)
                except Exception as e:
                    logger.warning("Scheduled refresh failed: %s", e)

                time.sleep(1800)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()


scheduler = DailyPrefetchScheduler()
