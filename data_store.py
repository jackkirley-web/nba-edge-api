# data_store.py — Persistent disk storage + daily pre-fetch scheduler
# Saves successful fetches to disk so data survives server restarts
# and NBA.com downtime. Runs daily pre-fetch at 8am AEST (10pm UTC).

import json
import logging
import os
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# Storage location on Render's ephemeral disk
# For Render free tier, /tmp persists within a deployment but not across restarts
# Good enough — data refreshes daily anyway
STORE_DIR  = Path("/tmp/nbaedge")
STORE_DIR.mkdir(parents=True, exist_ok=True)

MAIN_FILE   = STORE_DIR / "main_cache.json"
STREAK_FILE = STORE_DIR / "streak_cache.json"
META_FILE   = STORE_DIR / "meta.json"

# 8am AEST = UTC+10 = 22:00 UTC previous day
PREFETCH_HOUR_UTC = 22   # 10pm UTC = 8am AEST


def save_main_data(data: dict):
    """Save main cache data to disk."""
    try:
        # Don't save internal fields
        saveable = {k: v for k, v in data.items() if not k.startswith("_")}
        saveable["_saved_at"] = _now_iso()
        with open(MAIN_FILE, "w") as f:
            json.dump(saveable, f)
        _update_meta("main_saved_at", _now_iso())
        logger.info("Main data saved to disk")
    except Exception as e:
        logger.warning("Failed to save main data: %s", e)


def load_main_data() -> dict:
    """Load main cache from disk. Returns {} if not found."""
    try:
        if MAIN_FILE.exists():
            with open(MAIN_FILE) as f:
                data = json.load(f)
            saved_at = data.get("_saved_at", "unknown")
            logger.info("Loaded main data from disk (saved %s)", saved_at)
            return data
    except Exception as e:
        logger.warning("Failed to load main data: %s", e)
    return {}


def save_streak_data(streaks: list):
    """Save streak data to disk."""
    try:
        payload = {"streaks": streaks, "_saved_at": _now_iso()}
        with open(STREAK_FILE, "w") as f:
            json.dump(payload, f)
        _update_meta("streak_saved_at", _now_iso())
        logger.info("Streak data saved to disk (%d entries)", len(streaks))
    except Exception as e:
        logger.warning("Failed to save streak data: %s", e)


def load_streak_data() -> list:
    """Load streak data from disk. Returns [] if not found."""
    try:
        if STREAK_FILE.exists():
            with open(STREAK_FILE) as f:
                data = json.load(f)
            saved_at = data.get("_saved_at", "unknown")
            logger.info("Loaded streak data from disk (saved %s)", saved_at)
            return data.get("streaks", [])
    except Exception as e:
        logger.warning("Failed to load streak data: %s", e)
    return []


def get_disk_meta() -> dict:
    """Get metadata about when data was last saved."""
    try:
        if META_FILE.exists():
            with open(META_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def is_data_stale(max_hours: int = 25) -> bool:
    """Check if saved data is older than max_hours."""
    meta = get_disk_meta()
    saved_at_str = meta.get("main_saved_at")
    if not saved_at_str:
        return True
    try:
        saved_at = datetime.fromisoformat(saved_at_str)
        age_hours = (datetime.now(timezone.utc) - saved_at).total_seconds() / 3600
        return age_hours > max_hours
    except Exception:
        return True


def get_data_age_str() -> str:
    """Return human-readable age of saved data e.g. '3 hours ago'."""
    meta = get_disk_meta()
    saved_at_str = meta.get("main_saved_at")
    if not saved_at_str:
        return "unknown"
    try:
        saved_at = datetime.fromisoformat(saved_at_str)
        age = datetime.now(timezone.utc) - saved_at
        hours   = int(age.total_seconds() // 3600)
        minutes = int((age.total_seconds() % 3600) // 60)
        if hours > 0:
            return f"{hours}h {minutes}m ago"
        return f"{minutes}m ago"
    except Exception:
        return "unknown"


def _update_meta(key: str, value: str):
    meta = get_disk_meta()
    meta[key] = value
    try:
        with open(META_FILE, "w") as f:
            json.dump(meta, f)
    except Exception:
        pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── DAILY SCHEDULER ──────────────────────────────────────────

class DailyPrefetchScheduler:
    """
    Runs a daily data pre-fetch at 8am AEST (10pm UTC).
    NBA.com is most reliable during US business hours (morning/afternoon ET),
    which corresponds to evening/night AEST. 8am AEST catches NBA.com at
    its most stable — after overnight maintenance windows.
    """
    def __init__(self):
        self._thread = None
        self._stop   = threading.Event()

    def start(self, fetch_fn):
        """Start the background scheduler. fetch_fn() is called at the scheduled time."""
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, args=(fetch_fn,), daemon=True
        )
        self._thread.start()
        logger.info("Daily prefetch scheduler started (runs at %d:00 UTC = 8am AEST)", PREFETCH_HOUR_UTC)

    def stop(self):
        self._stop.set()

    def _run(self, fetch_fn):
        while not self._stop.is_set():
            now_utc = datetime.now(timezone.utc)
            # Calculate seconds until next 10pm UTC
            target = now_utc.replace(hour=PREFETCH_HOUR_UTC, minute=0, second=0, microsecond=0)
            if now_utc >= target:
                target += timedelta(days=1)

            wait_seconds = (target - now_utc).total_seconds()
            logger.info("Daily prefetch scheduled in %.1f hours", wait_seconds / 3600)

            # Wait until scheduled time (check every minute so we can stop cleanly)
            while wait_seconds > 0 and not self._stop.is_set():
                sleep_time = min(60, wait_seconds)
                self._stop.wait(sleep_time)
                wait_seconds -= sleep_time

            if self._stop.is_set():
                break

            logger.info("=== Running scheduled daily prefetch ===")
            try:
                fetch_fn(force_refresh=True)
                logger.info("Daily prefetch completed successfully")
            except Exception as e:
                logger.error("Daily prefetch failed: %s", e)


scheduler = DailyPrefetchScheduler()
