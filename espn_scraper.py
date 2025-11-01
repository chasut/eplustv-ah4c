#!/usr/bin/env python3
# DeepLinks / ESPN Watch Graph scraper (72h window)
# - Uses hardcoded public key by default (per user request), but still allows env override.
# - DO NOT log the key value.

import os, sys, json, time, logging, sqlite3
from datetime import datetime, timedelta, timezone, date
import requests

# -------- Config --------
DEFAULT_API_BASE = "https://watch.graph.api.espn.com/api"
DEFAULT_FEATURES = "pbov7"
# IMPORTANT: Use UTC timezone for API queries. Using regional timezones like America/New_York
# causes the API to return empty results for some dates due to timezone boundary issues.
DEFAULT_TZ       = "UTC"
DEFAULT_DEVICE   = "DESKTOP"  # also tried CONNECTED_TV/MOBILE before

# User explicitly asked to hardcode this key:
DEFAULT_API_KEY  = "0dbf88e8-cc6d-41da-aa83-18b5c630bc5c"

API_BASE  = os.getenv("WATCH_API_BASE", DEFAULT_API_BASE)
API_KEY   = os.getenv("ESPN_WATCH_API_KEY") or os.getenv("WATCH_API_KEY") or DEFAULT_API_KEY
FEATURES  = os.getenv("WATCH_FEATURES", DEFAULT_FEATURES)
REGION    = os.getenv("WATCH_API_REGION", "US")
TZN       = os.getenv("WATCH_API_TZ", DEFAULT_TZ)
DEVICE    = os.getenv("WATCH_API_DEVICE", DEFAULT_DEVICE)
VERIFYSSL = bool(int(os.getenv("WATCH_API_VERIFY_SSL", "1")))

OUT_DB = os.getenv("WATCH_DB_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", "espn_schedule.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

session = requests.Session()
session.proxies = {}  # Disable proxies
session.trust_env = False  # Don't use system proxy settings
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.espn.com",
    "Referer": "https://www.espn.com/",
})

GQL = {
    "query": (
        "query Airings($countryCode: String!, $deviceType: DeviceType!, $tz: String!, $day: String!, $limit: Int) {"
        "  airings(countryCode: $countryCode, deviceType: $deviceType, tz: $tz, day: $day, limit: $limit) {"
        "    id airingId simulcastAiringId name shortName type startDateTime endDateTime"
        "    sport { id name abbreviation }"
        "    league { id name abbreviation }"
        "    network { id name shortName }"
        "    packages { name }"
        "  }"
        "}"
    ),
    "operationName": "Airings",
}

def fetch_day(day_str: str):
    url = f"{API_BASE}?apiKey={API_KEY}"
    if FEATURES:
        url += f"&features={FEATURES}"
    payload = {
        **GQL,
        "variables": {
            "countryCode": REGION,
            "deviceType": DEVICE,
            "tz": TZN,
            "day": day_str,
            "limit": 2000
        }
    }
    
    # Debug: log the request details on first call
    if not hasattr(fetch_day, '_logged_once'):
        fetch_day._logged_once = True
        logging.info("Request URL: %s", url)
        logging.info("Request payload keys: %s", list(payload.keys()))
        logging.info("Variables: %s", payload['variables'])
    
    t0 = time.time()
    try:
        r = session.post(url, json=payload, timeout=20, verify=VERIFYSSL)
        dt = int((time.time() - t0) * 1000)
        status = r.status_code
        try:
            data = r.json()
        except Exception:
            data = {"_nonjson": r.text[:256]}

        logging.info("API request for %s: status=%s, duration_ms=%s, bytes=%s", day_str, status, dt, len(r.content))
        
        if status != 200:
            logging.warning("fetch %s failed (status=%s). Body prefix=%r - SKIPPING", day_str, status, (r.text[:200] if r.text else ""))
            return None  # Don't crash, just skip this day
        
        return data
    except Exception as e:
        logging.error("Exception fetching %s: %s - SKIPPING", day_str, e)
        return None

def ensure_db():
    db_dir = os.path.dirname(OUT_DB)
    if db_dir:  # Only create if there's a directory component
        os.makedirs(db_dir, exist_ok=True)
    with sqlite3.connect(OUT_DB) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            sport TEXT,
            league TEXT,
            title TEXT,
            subtitle TEXT,
            summary TEXT,
            image TEXT,
            start_utc TEXT,
            stop_utc TEXT,
            status TEXT,
            is_plus INTEGER DEFAULT 1,
            web_url TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            venue TEXT,
            competitors TEXT
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_events_start ON events(start_utc)")

def parse_and_store(days):
    ensure_db()
    total = 0
    logging.info("About to fetch days: %s", days)
    with sqlite3.connect(OUT_DB) as db:
        db.execute("BEGIN")
        for idx, d in enumerate(days):
            logging.info("Fetching day %d/%d: %s", idx+1, len(days), d)
            data = fetch_day(d)
            if data is None:  # Skip failed days
                logging.warning("Skipping day %s due to fetch failure", d)
                continue
            airings = (((data or {}).get("data") or {}).get("airings")) or []
            logging.info("Retrieved %d airings for %s", len(airings), d)
            
            # Debug: if 0 airings, show what we got
            if len(airings) == 0:
                logging.warning("Zero airings for %s - Response: %s", d, json.dumps(data)[:500])
            
            # Debug: show first few packages to understand format
            if airings and not hasattr(parse_and_store, '_logged_packages'):
                parse_and_store._logged_packages = True
                logging.info("Sample packages from first 5 airings:")
                for i, a in enumerate(airings[:5]):
                    pkgs = [p.get("name","") for p in (a.get("packages") or [])]
                    logging.info("  Airing %d '%s': packages=%s", i+1, a.get('name','')[:40], pkgs)
            
            for a in airings:
                pkgs = [p.get("name","") for p in (a.get("packages") or [])]
                # Filter for ESPN+ content - package name is "ESPN_PLUS" with underscore
                if "ESPN_PLUS" not in pkgs:
                    continue
                
                # Extract all fields from API response
                pid   = a.get("id") or a.get("airingId") or a.get("simulcastAiringId")
                title = a.get("shortName") or a.get("name") or "Untitled"
                sport = ((a.get("sport") or {}).get("name") or "").strip() or "sports"
                sport_abbr = ((a.get("sport") or {}).get("abbreviation") or "").strip()
                league_obj = a.get("league") or {}
                league = (league_obj.get("abbreviation") or league_obj.get("name") or "").strip()
                network_obj = a.get("network") or {}
                subtitle = (network_obj.get("shortName") or network_obj.get("name") or "").strip()
                start = a.get("startDateTime")
                stop  = a.get("endDateTime")
                
                # Event type - available in API
                event_type = a.get("type", "").strip()
                
                if not (pid and start and stop): 
                    continue
                
                # Normalize datetime format to include Z
                if start.endswith("Z"): s_iso = start
                else: s_iso = start.replace("+00:00","Z")
                if stop.endswith("Z"):  e_iso = stop
                else: e_iso = stop.replace("+00:00","Z")
                
                # Determine status (simplified - could be enhanced)
                status = "UPCOMING"
                
                db.execute("""INSERT OR REPLACE INTO events(
                    id, sport, league, title, subtitle, summary, image, 
                    start_utc, stop_utc, status, is_plus, web_url, 
                    event_type, venue, competitors
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
                (pid, sport, league, title, subtitle, "", "", 
                 s_iso, e_iso, status, 1, "", event_type, "", ""))
                total += 1
        db.execute("COMMIT")
    return total

def main():
    # Use configured timezone (defaults to UTC for API compatibility)
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(TZN)
    except ImportError:
        # Fallback for Python < 3.9
        try:
            from backports.zoneinfo import ZoneInfo
            tz = ZoneInfo(TZN)
        except ImportError:
            logging.warning("zoneinfo not available, using UTC")
            tz = timezone.utc
    
    now = datetime.now(tz)
    logging.info("Current time (TZ=%s): %s", TZN, now)
    # Only fetch today and forward - API doesn't like historical dates
    days = [
        now.strftime("%Y-%m-%d"),
        (now + timedelta(days=1)).strftime("%Y-%m-%d"),
        (now + timedelta(days=2)).strftime("%Y-%m-%d"),
        (now + timedelta(days=3)).strftime("%Y-%m-%d"),
    ]
    logging.info("="*59)
    logging.info("ESPN Watch Graph Scraper - Starting")
    logging.info("="*59)
    logging.info("Fetching dates: %s", ", ".join(days))
    total = parse_and_store(days)
    with sqlite3.connect(OUT_DB) as db:
        live = db.execute("""
          SELECT COUNT(*) FROM events WHERE datetime('now') BETWEEN start_utc AND stop_utc
        """).fetchone()[0]
        win  = db.execute("""
          SELECT COUNT(*) FROM events WHERE start_utc BETWEEN datetime('now') AND datetime('now','+72 hours')
        """).fetchone()[0]
    print(json.dumps({"db": OUT_DB, "rows_inserted": total, "live_now": live, "window_72h": win}, indent=2))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Fatal error")
        sys.exit(2)
