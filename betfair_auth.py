# betfair_auth.py -- Betfair API session management
# Credentials read from Render environment variables
import os, logging, threading, time, requests

logger = logging.getLogger(__name__)
BETFAIR_LOGIN_URL = "https://identitysso.betfair.com/api/login"
BETFAIR_API_URL   = "https://api.betfair.com/exchange/betting/rest/v1.0"
USERNAME = os.environ.get("BETFAIR_USERNAME","")
PASSWORD = os.environ.get("BETFAIR_PASSWORD","")
APP_KEY  = os.environ.get("BETFAIR_APP_KEY","ojBMSIw3ozctDeKE")

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
            logger.error("Betfair: login FAILED")
        return _token

def _login() -> str:
    if not USERNAME or not PASSWORD:
        logger.error("BETFAIR_USERNAME / BETFAIR_PASSWORD env vars not set")
        return None
    try:
        r = requests.post(
            BETFAIR_LOGIN_URL,
            data={"username": USERNAME, "password": PASSWORD},
            headers={"X-Application": APP_KEY, "Accept": "application/json",
                     "Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        d = r.json()
        if d.get("status") == "SUCCESS":
            return d.get("token")
        logger.error("Betfair login error: %s", d.get("error","unknown"))
    except Exception as e:
        logger.error("Betfair login exception: %s", e)
    return None

def bf_post(endpoint: str, payload: dict) -> object:
    token = get_token()
    if not token: return None
    headers = {"X-Application": APP_KEY, "X-Authentication": token,
                "Accept": "application/json", "Content-Type": "application/json"}
    try:
        r = requests.post(f"{BETFAIR_API_URL}/{endpoint}/",
                          json=payload, headers=headers, timeout=25)
        if r.status_code == 200: return r.json()
        if r.status_code == 401:
            global _token, _last_login
            with _token_lock: _token = None; _last_login = 0
        logger.warning("Betfair %s -> %d: %s", endpoint, r.status_code, r.text[:200])
    except Exception as e:
        logger.error("Betfair %s error: %s", endpoint, e)
    return None
