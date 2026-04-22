# betfair_auth.py -- Betfair API authentication with full debug logging
import os, logging, threading, time, requests

logger = logging.getLogger(__name__)
BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/rest/v1.0"

USERNAME = os.environ.get("BETFAIR_USERNAME", "")
PASSWORD = os.environ.get("BETFAIR_PASSWORD", "")
APP_KEY  = os.environ.get("BETFAIR_APP_KEY", "ojBMSIw3ozctDeKE")

_token      = None
_token_lock = threading.Lock()
_last_login = 0
SESSION_TTL = 3600

def get_token() -> str:
    global _token, _last_login
    with _token_lock:
        now = time.time()
        if _token and (now - _last_login) < SESSION_TTL:
            return _token
        tok = _login()
        if tok:
            _token = tok
            _last_login = now
            logger.info("Betfair: login OK")
        else:
            logger.error("Betfair: all login attempts failed")
        return _token

def _login() -> str:
    if not USERNAME or not PASSWORD:
        logger.error("Env vars BETFAIR_USERNAME / BETFAIR_PASSWORD not set")
        return None

    logger.info("Betfair login for: %s", USERNAME[:3] + "***")

    # The correct interactive login endpoint for username/password (no cert needed)
    url = "https://identitysso.betfair.com/api/login"
    try:
        r = requests.post(
            url,
            data={"username": USERNAME, "password": PASSWORD},
            headers={
                "X-Application": APP_KEY,
                "Accept":        "application/json",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            timeout=20,
        )
        logger.info("Login response: status=%d len=%d body=%r",
                    r.status_code, len(r.content), r.text[:300])

        if r.status_code != 200:
            logger.error("Login HTTP error: %d", r.status_code)
            return None

        if not r.text or not r.text.strip():
            logger.error("Login returned empty body -- account may not be verified on betfair.com.au")
            return None

        d = r.json()
        status = d.get("status", "")
        error  = d.get("error", "")
        token  = d.get("token", "")

        logger.info("Login JSON: status=%s error=%s token_len=%d", status, error, len(token))

        if status == "SUCCESS" and token:
            return token

        logger.error("Login failed: status=%s error=%s", status, error)
        return None

    except Exception as e:
        logger.error("Login exception: %s", e)
        return None

def bf_post(endpoint: str, payload: dict) -> object:
    token = get_token()
    if not token:
        return None
    headers = {
        "X-Application":    APP_KEY,
        "X-Authentication": token,
        "Accept":           "application/json",
        "Content-Type":     "application/json",
    }
    try:
        r = requests.post(
            f"{BETFAIR_API_URL}/{endpoint}/",
            json=payload, headers=headers, timeout=25,
        )
        if r.status_code == 200:
            return r.json()
        if r.status_code == 401:
            global _token, _last_login
            with _token_lock:
                _token = None
                _last_login = 0
            logger.warning("Betfair 401 -- session expired")
        else:
            logger.warning("Betfair %s -> %d: %s", endpoint, r.status_code, r.text[:300])
    except Exception as e:
        logger.error("Betfair %s: %s", endpoint, e)
    return None
