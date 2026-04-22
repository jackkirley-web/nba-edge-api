# greyhound_odds.py -- odds utilities (odds now come from Betfair via greyhound_data)
def extract_odds_from_runners(runners):
    return {r["name"]: r["odds"] for r in runners if r.get("odds") and r["odds"]>1.01}

def normalise_probs(runner_odds):
    if not runner_odds: return {}
    raw={n:1.0/o for n,o in runner_odds.items() if o and o>1.01}
    total=sum(raw.values())
    return {n:p/total for n,p in raw.items()} if total>0 else {}
