# betfair_auth.py -- Betfair API authentication
# Tries multiple login endpoints for AU accounts
import os, logging, threading, time, requests

logger = logging.getLogger(__name__)

# Betfair has two login endpoints - AU accounts may need the non-interactive one
LOGIN_URLS = [
    "https://identitysso-cert.betfair.com/api/login",
    "https://identitysso.betfair.com/api/login",
]
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
            logger.error("Betfair: login FAILED - check BETFAIR_USERNAME and BETFAIR_PASSWORD env vars")
        return _token

def _login() -> str:
    if not USERNAME or not PASSWORD:
        logger.error("BETFAIR_USERNAME or BETFAIR_PASSWORD env var not set")
        return None

    logger.info("Betfair login attempt for user: %s", USERNAME[:5] + "***")

    for url in LOGIN_URLS:
        try:
            r = requests.post(
                url,
                data={"username": USERNAME, "password": PASSWORD},
                headers={
                    "X-Application":  APP_KEY,
                    "Accept":         "application/json",
                    "Content-Type":   "application/x-www-form-urlencoded",
                },
                timeout=20,
            )
            logger.info("Betfair login %s -> status %d, body: %s", url, r.status_code, r.text[:200])

            if not r.text or not r.text.strip():
                logger.warning("Empty response from %s", url)
                continue

            d = r.json()
            if d.get("status") == "SUCCESS":
                logger.info("Betfair login succeeded via %s", url)
                return d.get("token")
            logger.warning("Betfair login status=%s error=%s from %s",
                           d.get("status"), d.get("error"), url)
        except Exception as e:
            logger.warning("Betfair login exception at %s: %s", url, e)

    # Try the APING (API-NG) login endpoint as final fallback
    try:
        r = requests.post(
            "https://identitysso.betfair.com/api/login",
            params={"username": USERNAME, "password": PASSWORD},
            headers={
                "X-Application": APP_KEY,
                "Accept": "application/json",
            },
            timeout=20,
        )
        logger.info("Betfair APING login -> status %d, body: %s", r.status_code, r.text[:200])
        if r.text and r.text.strip():
            d = r.json()
            if d.get("status") == "SUCCESS":
                return d.get("token")
    except Exception as e:
        logger.warning("Betfair APING login exception: %s", e)

    return None

def bf_post(endpoint: str, payload: dict) -> object:
    """Authenticated POST to Betfair REST API."""
    token = get_token()
    if not token:
        logger.error("bf_post: no token available for %s", endpoint)
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
            # Session expired - force re-login next time
            global _token, _last_login
            with _token_lock:
                _token = None
                _last_login = 0
            logger.warning("Betfair session expired, will re-login next call")
        else:
            logger.warning("Betfair %s -> %d: %s", endpoint, r.status_code, r.text[:300])
    except Exception as e:
        logger.error("Betfair %s error: %s", endpoint, e)
    return None
