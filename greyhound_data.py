# greyhound_data.py -- AU greyhound data via Betfair API
# Event type 4339 = Greyhound Racing Australia
import logging, time, re
from datetime import datetime, timezone, timedelta
from betfair_auth import bf_post

logger = logging.getLogger(__name__)
GH_TYPE = "4339"

def get_today_meetings() -> list:
    aest = datetime.now(timezone(timedelta(hours=10)))
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc.replace(hour=23, minute=59, second=59)
    logger.info("Fetching AU greyhound events via Betfair")

    # 1. List events (tracks) today
    events = bf_post("listEvents", {
        "filter": {
            "eventTypeIds": [GH_TYPE],
            "marketCountries": ["AU"],
            "marketStartTime": {
                "from": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to":   end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        },
        "maxResults": 200,
    }) or []
    logger.info("Betfair: %d greyhound events", len(events))

    # 2. Get all market catalogues for AU greyhounds today
    markets = bf_post("listMarketCatalogue", {
        "filter": {
            "eventTypeIds": [GH_TYPE],
            "marketCountries": ["AU"],
            "marketStartTime": {
                "from": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to":   end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        },
        "marketProjection": ["RUNNER_DESCRIPTION","RUNNER_METADATA",
                             "MARKET_START_TIME","MARKET_DESCRIPTION","EVENT"],
        "sort": "FIRST_TO_START",
        "maxResults": 1000,
    }) or []
    logger.info("Betfair: %d race markets", len(markets))

    if not markets:
        return []

    # 3. Get live prices for all markets (batches of 40)
    all_ids = [m["marketId"] for m in markets]
    prices  = _get_books(all_ids)

    # 4. Group markets by event (track)
    event_markets = {}
    for m in markets:
        ev   = m.get("event", {})
        eid  = ev.get("id","")
        name = ev.get("venue") or ev.get("name") or ""
        name = re.sub(r"\s*\(AU\)\s*$","",name).strip()
        if eid not in event_markets:
            event_markets[eid] = {"track": name, "state": _state(name), "races": []}
        race = _parse_market(m, prices.get(m["marketId"],{}), name)
        if race and race.get("runners"):
            event_markets[eid]["races"].append(race)

    # 5. Build final meetings list
    meetings = []
    for eid, ev in event_markets.items():
        if ev["races"]:
            ev["races"].sort(key=lambda r: r.get("race_num",0))
            ev["condition"] = ev["races"][0].get("condition","Good")
            ev["date"]      = aest.strftime("%Y-%m-%d")
            meetings.append(ev)

    logger.info("Betfair greyhounds: %d meetings, %d races",
                len(meetings), sum(len(m["races"]) for m in meetings))
    return meetings

def _get_books(market_ids: list) -> dict:
    result = {}
    for i in range(0, len(market_ids), 40):
        batch = market_ids[i:i+40]
        time.sleep(0.3)
        books = bf_post("listMarketBook", {
            "marketIds": batch,
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS"],
                "exBestOffersOverrides": {"bestPricesDepth": 1},
            },
        }) or []
        for b in books:
            mid = b.get("marketId")
            if mid: result[mid] = b
    return result

def _parse_market(m: dict, book: dict, track: str) -> dict:
    market_name = m.get("marketName","")
    start       = m.get("marketStartTime","")
    desc        = m.get("description",{}) or {}
    race_num    = _rnum(market_name)
    distance    = _dist(market_name) or _dist(desc.get("marketType",""))
    grade       = _grade(market_name)
    race_time   = _fmt_time(start)
    condition   = _cond(desc.get("conditions",""))

    # Price lookup
    price_map = {}
    for rp in (book.get("runners") or []):
        sid   = rp.get("selectionId")
        backs = (rp.get("ex",{}) or {}).get("availableToBack",[])
        price = backs[0].get("price") if backs else None
        price_map[sid] = {
            "odds":      price,
            "scratched": rp.get("status","ACTIVE") != "ACTIVE",
        }

    runners = []
    for rc in (m.get("runners") or []):
        sid    = rc.get("selectionId")
        name   = rc.get("runnerName","").strip()
        box    = _box(rc)
        meta   = rc.get("metadata",{}) or {}
        form   = meta.get("FORM","") or meta.get("LAST_STARTS","") or ""
        trainer= meta.get("TRAINER_NAME","") or ""
        pm     = price_map.get(sid,{})
        odds   = pm.get("odds")
        scr    = pm.get("scratched",False)
        if not name or not box or scr:
            continue
        runners.append({
            "box":           box,
            "name":          name,
            "trainer":       trainer,
            "form_str":      form,
            "last_5":        _form(form),
            "odds":          float(odds) if odds and float(odds)>1.01 else None,
            "scratched":     False,
            "track_wins":    _si(meta.get("TRACK_WINS")),
            "track_starts":  _si(meta.get("TRACK_STARTS")),
            "dist_wins":     _si(meta.get("DISTANCE_WINS")),
            "dist_starts":   _si(meta.get("DISTANCE_STARTS")),
            "career_wins":   _si(meta.get("WINS")),
            "career_starts": _si(meta.get("STARTS")),
        })
    runners.sort(key=lambda r: r["box"])
    return {
        "race_num":  race_num,
        "race_time": race_time,
        "market_id": m.get("marketId",""),
        "distance":  distance,
        "grade":     grade,
        "condition": condition,
        "track":     track,
        "runners":   runners,
    }

def _box(rc: dict) -> int:
    meta = rc.get("metadata",{}) or {}
    for k in ("STALL_DRAW","BOX_NUMBER","DRAW","CLOTH_NUMBER"):
        v = meta.get(k)
        if v:
            try: return int(v)
            except Exception: pass
    sp = rc.get("sortPriority")
    if sp:
        try: return int(sp)
        except Exception: pass
    m = re.match(r"^(\d+)\.", rc.get("runnerName",""))
    if m: return int(m.group(1))
    return 0

def _rnum(s):
    m=re.search(r"R(\d+)",s,re.I); return int(m.group(1)) if m else 0
def _dist(s):
    m=re.search(r"(\d{3,4})\s*m",str(s),re.I); return int(m.group(1)) if m else 0
def _grade(s):
    for p in [r"(Grade\s*\d+)",r"(Maiden)",r"(FFA|Free\s*For\s*All)",r"(Open)",r"(Restricted\s*Win)",r"(Masters)"]:
        m=re.search(p,s,re.I)
        if m: return m.group(1).strip()
    return ""
def _fmt_time(s):
    if not s: return ""
    try:
        dt=datetime.fromisoformat(s.replace("Z","+00:00"))
        return dt.astimezone(timezone(timedelta(hours=10))).strftime("%-I:%M %p")
    except Exception: return s[:5]
def _cond(s):
    m={"Good":"Good","Good 4":"Good","Firm":"Good","Fast":"Good","Soft":"Soft","Heavy":"Heavy","Wet":"Wet"}
    return m.get(str(s).strip().title(),"Good")
def _form(s):
    if not s: return []
    parts=re.split(r"[-.\s,]",str(s))
    if len(parts)==1: parts=list(str(s))
    out=[]
    for p in parts:
        p=str(p).strip().upper()
        if p in ("F","D","N","X","S","E"): out.append(8)
        elif p.isdigit(): out.append(int(p))
    return out[:5]
def _si(v):
    try: return int(float(str(v or 0)))
    except Exception: return 0
def _state(t):
    t=t.lower()
    for n in ["meadows","sandown","ballarat","geelong","warragul","cranbourne","shepparton","horsham","bendigo","traralgon","sale","healesville"]:
        if n in t: return "VIC"
    for n in ["wentworth","richmond","bathurst","gosford","tamworth","gunnedah","nowra","grafton","lismore","newcastle","dapto"]:
        if n in t: return "NSW"
    for n in ["albion","ipswich","capalaba","townsville","rockhampton","logan","bundaberg","toowoomba","cairns"]:
        if n in t: return "QLD"
    for n in ["angle park","gawler","murray bridge","mount gambier"]:
        if n in t: return "SA"
    for n in ["cannington","mandurah","northam","albany"]:
        if n in t: return "WA"
    for n in ["launceston","devonport","hobart"]:
        if n in t: return "TAS"
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
def get_box_win_pct(track,distance,box):
    t=track.lower().strip(); td=BOX_WIN_PCT.get(t)
    if not td:
        for k in BOX_WIN_PCT:
            if k!="default" and (k in t or t in k): td=BOX_WIN_PCT[k]; break
    if not td: td=BOX_WIN_PCT["default"]
    dists=sorted(td.keys()); closest=min(dists,key=lambda d:abs(d-(distance or 520)))
    return td.get(closest,{}).get(box,12.5)
WET_FACTOR={"good":1.0,"fast":1.0,"soft":1.05,"heavy":1.12,"wet":1.10}
def get_condition_factor(c): return WET_FACTOR.get(c.lower(),1.0)
