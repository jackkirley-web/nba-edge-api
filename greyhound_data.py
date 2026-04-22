# greyhound_data.py -- Australian greyhound racing data
# Source: Ladbrokes Affiliates API (api-affiliates.ladbrokes.com.au)
# Documented at https://nedscode.github.io/affiliate-feeds/
# Free, no auth - just identifying headers required
# Returns meetings, races, runners, form, odds in JSON

import logging, time, random, re, requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
LADBROKE_BASE = "https://api-affiliates.ladbrokes.com.au/affiliates/v1"
HEADERS = {
    "From": "sportedge@sportedge.app",
    "X-Partner": "SportEdge Racing Intelligence",
    "Accept": "application/json",
    "User-Agent": "SportEdge/1.0",
}

def _get(path, params=None, timeout=20):
    url = LADBROKE_BASE + path
    for attempt in range(3):
        try:
            time.sleep(0.5 + random.uniform(0, 0.3) + attempt * 1.0)
            r = requests.get(url, params=params or {}, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.json()
            logger.warning("Ladbrokes API %s -> %d (attempt %d)", path, r.status_code, attempt+1)
        except Exception as e:
            logger.warning("Ladbrokes API %s attempt %d: %s", path, attempt+1, e)
    return None

def get_today_meetings() -> list:
    aest = datetime.now(timezone(timedelta(hours=10)))
    date_str = aest.strftime("%Y-%m-%d")
    logger.info("Fetching AU greyhound meetings for %s AEST", date_str)
    data = _get("/racing/meetings", params={
        "category": "G", "country": "AUS",
        "date_from": date_str, "date_to": date_str, "limit": 200,
    })
    if not data:
        logger.error("Ladbrokes API: no response")
        return []
    raw = data.get("data", {}).get("meetings", [])
    logger.info("Ladbrokes: %d raw greyhound meetings", len(raw))
    meetings = []
    for m in raw:
        try:
            parsed = _parse_meeting(m, date_str)
            if parsed and parsed.get("races"):
                meetings.append(parsed)
        except Exception as e:
            logger.warning("Meeting parse error %s: %s", m.get("name"), e)
    logger.info("Parsed %d meetings with races", len(meetings))
    return meetings

def _parse_meeting(m: dict, date_str: str) -> dict:
    track = m.get("name", "").strip()
    state = m.get("state", _guess_state(track)).upper()
    condition = "Good"
    raw_races = m.get("races", [])
    races = []
    for stub in raw_races:
        race_id = stub.get("id")
        if not race_id:
            continue
        full = _fetch_race(race_id)
        if not full:
            continue
        if condition == "Good" and full.get("track_condition"):
            condition = _parse_condition(full["track_condition"])
        race = _parse_race(full, stub, track, condition)
        if race and race.get("runners"):
            races.append(race)
    return {"track": track, "state": state, "condition": condition, "date": date_str, "races": races} if races else {}

def _fetch_race(race_id: str) -> dict:
    data = _get(f"/racing/events/{race_id}")
    if not data:
        return {}
    d = data.get("data", {})
    return d.get("race") or d.get("event") or (d if isinstance(d, dict) else {})

def _parse_race(full: dict, stub: dict, track: str, condition: str) -> dict:
    race_num = int(stub.get("race_number") or full.get("race_number") or 0)
    if not race_num:
        return {}
    start = stub.get("start_time") or full.get("start_time") or ""
    race_time = ""
    if start:
        try:
            dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            race_time = dt.astimezone(timezone(timedelta(hours=10))).strftime("%-I:%M %p")
        except Exception:
            race_time = start[:5]
    distance = int(stub.get("distance") or full.get("distance") or 0) or None
    tc = full.get("track_condition") or stub.get("track_condition") or ""
    if tc:
        condition = _parse_condition(tc)
    race_name = full.get("name") or stub.get("name") or ""
    grade = _extract_grade(race_name)
    raw_runners = full.get("entrants") or full.get("runners") or full.get("selections") or []
    runners = [r for r in (_parse_runner(e) for e in raw_runners) if r]
    runners.sort(key=lambda x: x.get("box", 99))
    return {"race_num": race_num, "race_time": race_time, "distance": distance,
            "grade": grade, "condition": condition, "track": track, "runners": runners} if runners else {}

def _parse_runner(e: dict) -> dict:
    name = (e.get("name") or e.get("runner_name") or e.get("entrant_name") or "").strip()
    if not name:
        return {}
    box = None
    for key in ("barrier_number", "box_number", "tab_number", "number", "position"):
        v = e.get(key)
        if v is not None:
            try: box = int(v); break
            except Exception: pass
    if not box:
        return {}
    scratched = (e.get("is_scratched") or e.get("scratched") or
                 str(e.get("status","")).lower() in ("scratched","scr","withdrawn"))
    trainer_raw = e.get("trainer") or e.get("trainer_name") or {}
    trainer = (trainer_raw.get("name","") if isinstance(trainer_raw, dict) else str(trainer_raw))
    # Extract odds
    odds = None
    odds_raw = e.get("odds") or e.get("win_odds") or e.get("prices") or {}
    if isinstance(odds_raw, dict):
        for k in ("win","fixed_win","tote_win","returnWin","price","open"):
            v = odds_raw.get(k)
            if v:
                try: odds = float(v); break
                except Exception: pass
    elif isinstance(odds_raw, (int, float)):
        try: odds = float(odds_raw)
        except Exception: pass
    if odds and odds < 1.01:
        odds = None
    form_str = str(e.get("form") or e.get("last_starts") or e.get("form_guide") or "")
    last_5 = _parse_form_string(form_str)
    def _si(k): 
        try: return int(float(str(e.get(k) or 0)))
        except Exception: return 0
    return {
        "box": box, "name": name, "trainer": trainer,
        "form_str": form_str, "last_5": last_5, "odds": odds,
        "track_wins": _si("track_wins"), "track_starts": _si("track_starts"),
        "dist_wins": _si("distance_wins"), "dist_starts": _si("distance_starts"),
        "career_wins": _si("career_wins"), "career_starts": _si("career_starts"),
        "scratched": bool(scratched),
    }

def _parse_form_string(s: str) -> list:
    if not s or s == "None": return []
    parts = re.split(r"[-.\s,]", s)
    if len(parts) == 1: parts = list(s)
    out = []
    for p in parts:
        p = str(p).strip().upper()
        if p in ("F","D","N","X","S","E"): out.append(8)
        elif p.isdigit(): out.append(int(p))
    return out[:5]

def _extract_grade(name: str) -> str:
    if not name: return ""
    for pat in [r"(Grade\s*\d+)",r"(Maiden)",r"(Free\s*For\s*All|FFA)",
                r"(Restricted\s*Win)",r"(Open)",r"(Masters)",r"(Tier\s*\d+)"]:
        m = re.search(pat, name, re.I)
        if m: return m.group(1).strip()
    return ""

def _parse_condition(raw: str) -> str:
    mapping = {"Good":"Good","Good 4":"Good","Firm":"Good","Fast":"Good",
               "Soft":"Soft","Soft 5":"Soft","Heavy":"Heavy","Wet":"Wet","Rain Affected":"Wet"}
    return mapping.get(str(raw).strip().title(), "Good")

def _guess_state(track: str) -> str:
    t = track.lower()
    for name in ["meadows","sandown","ballarat","geelong","warragul","cranbourne",
                 "shepparton","horsham","bendigo","traralgon","sale","healesville"]:
        if name in t: return "VIC"
    for name in ["wentworth","richmond","bathurst","gosford","tamworth","gunnedah",
                 "nowra","grafton","lismore","newcastle","dapto","penrith"]:
        if name in t: return "NSW"
    for name in ["albion","ipswich","capalaba","townsville","rockhampton","logan",
                 "bundaberg","toowoomba","cairns","mackay"]:
        if name in t: return "QLD"
    for name in ["angle park","gawler","murray bridge","mount gambier"]:
        if name in t: return "SA"
    for name in ["cannington","mandurah","northam","albany"]:
        if name in t: return "WA"
    for name in ["launceston","devonport","hobart"]:
        if name in t: return "TAS"
    for name in ["darwin","alice springs"]:
        if name in t: return "NT"
    return "AU"

BOX_WIN_PCT = {
    "the meadows":   {525:{1:17.8,2:14.2,3:13.1,4:12.0,5:11.8,6:11.5,7:10.8,8:8.8},600:{1:16.5,2:13.5,3:12.5,4:12.0,5:12.0,6:11.5,7:11.0,8:11.0}},
    "sandown park":  {515:{1:18.2,2:14.5,3:13.0,4:12.0,5:11.5,6:11.0,7:10.5,8:9.3},595:{1:16.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:11.0,8:10.0}},
    "ballarat":      {450:{1:18.5,2:14.8,3:13.2,4:12.0,5:11.0,6:10.5,7:10.0,8:10.0},520:{1:17.0,2:14.0,3:12.5,4:12.0,5:11.5,6:11.5,7:11.0,8:10.5}},
    "geelong":       {400:{1:19.0,2:15.0,3:13.5,4:12.0,5:11.0,6:10.0,7:9.5,8:10.0},520:{1:17.5,2:14.5,3:13.0,4:12.0,5:11.5,6:11.0,7:10.5,8:10.0}},
    "warragul":      {390:{1:20.5,2:15.5,3:13.0,4:12.0,5:10.5,6:10.0,7:9.5,8:9.0},450:{1:18.0,2:14.5,3:13.0,4:12.0,5:11.0,6:10.5,7:11.0,8:10.0}},
    "cranbourne":    {311:{1:22.0,2:16.0,3:13.0,4:11.0,5:10.5,6:10.0,7:9.5,8:8.0},520:{1:17.0,2:14.0,3:12.5,4:12.0,5:12.0,6:11.5,7:11.0,8:10.0}},
    "wentworth park":{520:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.0,7:10.5,8:9.5},720:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0}},
    "albion park":   {520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5},600:{1:16.0,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:9.5}},
    "angle park":    {520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5},595:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0}},
    "cannington":    {520:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.0},642:{1:15.5,2:13.5,3:13.0,4:13.0,5:12.5,6:12.0,7:11.0,8:9.5}},
    "ipswich":       {431:{1:19.0,2:15.0,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:9.5},520:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5}},
    "launceston":    {461:{1:19.0,2:15.0,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:9.5},553:{1:16.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:10.0}},
    "richmond":      {410:{1:19.5,2:15.5,3:13.0,4:12.0,5:11.0,6:10.0,7:9.5,8:9.5},525:{1:17.0,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.5}},
    "default":       {300:{1:22.0,2:16.0,3:13.0,4:11.0,5:10.0,6:9.5,7:9.0,8:9.5},400:{1:19.0,2:14.5,3:13.0,4:12.0,5:11.0,6:10.5,7:10.0,8:10.0},500:{1:17.5,2:14.0,3:13.0,4:12.5,5:12.0,6:11.5,7:10.5,8:9.0},600:{1:15.5,2:13.5,3:13.0,4:12.5,5:12.5,6:12.0,7:11.0,8:10.0},700:{1:14.5,2:13.5,3:13.0,4:13.0,5:12.5,6:12.5,7:11.5,8:9.5}},
}

def get_box_win_pct(track: str, distance: int, box: int) -> float:
    t = track.lower().strip()
    td = BOX_WIN_PCT.get(t)
    if not td:
        for k in BOX_WIN_PCT:
            if k != "default" and (k in t or t in k):
                td = BOX_WIN_PCT[k]; break
    if not td: td = BOX_WIN_PCT["default"]
    dists = sorted(td.keys())
    closest = min(dists, key=lambda d: abs(d - (distance or 520)))
    return td.get(closest, {}).get(box, 12.5)

WET_FACTOR = {"good":1.0,"fast":1.0,"soft":1.05,"heavy":1.12,"wet":1.10}
def get_condition_factor(condition: str) -> float:
    return WET_FACTOR.get(condition.lower(), 1.0)
