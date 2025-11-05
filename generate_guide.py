#!/usr/bin/env python3
# DeepLinks - ESPN+ M3U/XMLTV generator
# - Stable channel ids: dl-<event id>
# - M3U skips ended events (XMLTV keeps all events with full schedule)
# - Each channel has 16+ hours of programming: 8hrs before + event + 8hrs after
# - "NOT YET STARTED" blocks before event, "STREAM OFFLINE" after
# - Include LIVE and next ~3 hours; keep events until 65 min after end
# - Channel display-name: <short event title>, then "ESPN+"
# - Uses julianday() on BOTH sides of time window comparisons
# - Ultra-short M3U names (<=8 chars) like "NYR-SEA" or "RIT@CLG"
# - XMLTV <desc> filled with TITLE - SPORT - LEAGUE - STATUS

from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional
import xml.etree.ElementTree as ET

# --- Tunables ---------------------------------------------------------------

DB_PATH = os.environ.get("DEEPLINKS_DB", "out/espn_schedule.db")
OUT_XML = os.environ.get("DEEPLINKS_XML", "out/espn_plus.xml")
OUT_M3U = os.environ.get("DEEPLINKS_M3U", "out/espn_plus.m3u")

PLANNING_WINDOW_HOURS = int(os.environ.get("DEEPLINKS_WINDOW_HOURS", "3"))
MAX_STANDBY_HOURS = int(os.environ.get("DEEPLINKS_MAX_STANDBY_HOURS", "6"))
STANDBY_TILE_MIN = int(os.environ.get("DEEPLINKS_STANDBY_TILE_MIN", "30"))

POST_END_GRACE_MIN = int(os.environ.get("DEEPLINKS_POST_END_GRACE_MIN", "65"))
EVENT_ENDED_DURATION_MIN = int(os.environ.get("DEEPLINKS_ENDED_TILE_MIN", "30"))

GROUP_TITLE = os.environ.get("DEEPLINKS_GROUP", "ESPN+")
PROVIDER_LABEL = os.environ.get("DEEPLINKS_PROVIDER_LABEL", "ESPN+")

# Display timezone for descriptions (defaults to system local time)
# Set to timezone name like "America/New_York", "US/Pacific", "Europe/London", etc.
# Or leave empty to use system default
DISPLAY_TZ = os.environ.get("DEEPLINKS_DISPLAY_TZ", "")

# --- Helpers ----------------------------------------------------------------

def get_display_timezone():
    """Get the timezone to use for displaying times in descriptions."""
    if DISPLAY_TZ:
        try:
            from zoneinfo import ZoneInfo
            return ZoneInfo(DISPLAY_TZ)
        except ImportError:
            try:
                from backports.zoneinfo import ZoneInfo
                return ZoneInfo(DISPLAY_TZ)
            except ImportError:
                pass
    # Use system local timezone
    return None  # datetime will use local tz

def format_time_local(dt: datetime) -> str:
    """Format datetime in local timezone like '2:00 PM EST' """
    local_tz = get_display_timezone()
    if local_tz:
        local_dt = dt.astimezone(local_tz)
    else:
        local_dt = dt.astimezone()  # Use system local
    
    # Format: "2:00 PM EST" (with timezone abbreviation)
    time_str = local_dt.strftime('%I:%M %p').lstrip('0')
    tz_str = local_dt.strftime('%Z')
    return f"{time_str} {tz_str}"

# --- Helpers ----------------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_z(s: str) -> datetime:
    # Accepts "YYYY-MM-DDTHH:MM:SSZ"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)

def to_xmltv_dt(dt_: datetime) -> str:
    # XMLTV wants: YYYYMMDDHHMMSS +0000 (with a space before offset)
    return dt_.strftime("%Y%m%d%H%M%S +0000")

def minutes_between(a: datetime, b: datetime) -> int:
    return int(round((b - a).total_seconds() / 60.0))

_title_emoji_re = re.compile(r'[\u2000-\u206F\u2100-\u27FF\uFE00-\uFE0F]+')

def shorten_title(s: str, max_len: int = 38) -> str:
    if not s:
        return "Unknown Event"
    s = _title_emoji_re.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) <= max_len:
        return s
    
    # Check for important suffixes to preserve (Mat numbers, language tags, etc)
    suffix_match = re.search(r'(\s+-\s+Mat\s+\d+|\s+\(ESP\)|\s+\([A-Z]{3}\))$', s, re.I)
    if suffix_match:
        suffix = suffix_match.group(1)
        # Shorten the main part, then add suffix back
        main_part = s[:suffix_match.start()]
        max_main = max_len - len(suffix)
        if len(main_part) > max_main:
            cut = main_part[: max_main + 1]
            cut = cut.rsplit(" ", 1)[0]
            main_part = (cut or main_part[:max_main]).rstrip()
        return main_part + suffix
    
    # No suffix, just truncate normally
    cut = s[: max_len + 1]
    cut = cut.rsplit(" ", 1)[0]
    return (cut or s[:max_len]).rstrip() + "..."

def _jd_fmt(dt_: datetime) -> str:
    # SQLite-friendly UTC datetime: "YYYY-MM-DD HH:MM:SS+00:00"
    return dt_.strftime("%Y-%m-%d %H:%M:%S+00:00")

# --- Compact M3U naming (<=8 chars) ----------------------------------------

_STOPWORDS = {
    'university','college','state','st','saint','of','the','fc','sc','cf','club','athletic',
    'women','men','ladies','girls','boys'
}

def team_code(name: str) -> str:
    """Generate a compact 2-4 letter code from a team/school name."""
    s = re.sub(r'#\s*\d+\s*', ' ', name)      # drop rankings like #20
    s = re.sub(r'\(.*?\)', ' ', s)            # drop parenthetical
    s = re.sub(r'[^A-Za-z0-9\s]', ' ', s)     # keep alnum/space
    words = [w for w in s.split() if w.lower() not in _STOPWORDS]
    if not words:
        words = s.split() or ['???']

    # Prefer existing short all-caps tokens like RIT, UAB, BYU
    for w in words:
        if 2 <= len(w) <= 4 and w.isupper():
            return w[:4].upper()

    # Otherwise acronym from first letters
    acro = ''.join(w[0] for w in words[:3]).upper()
    if 2 <= len(acro) <= 4:
        return acro
    # Fallback: first 3 letters of first token
    return words[0][:4].upper()

def compact_matchup(title: str) -> str:
    """Return <=8 chars like NYR-SEA, RIT@CLG, UGA-ALA, etc."""
    t = re.sub(r'\s+', ' ', title or '').strip()
    
    # Check for Mat numbers (wrestling/multi-stream events)
    mat_match = re.search(r'\s+-\s+Mat\s+(\d+)', t, re.I)
    if mat_match:
        mat_num = mat_match.group(1)
        # Extract base event name
        base = t[:mat_match.start()].strip()
        base_code = re.sub(r'[^A-Za-z0-9]', '', base).upper()[:4]
        return f"{base_code}M{mat_num}"[:8]  # e.g., "PRINCM9"
    
    # Check for language tags like (ESP)
    lang_match = re.search(r'\(([A-Z]{3})\)$', t)
    if lang_match:
        lang = lang_match.group(1)
        base = t[:lang_match.start()].strip()
        # Try normal matchup parsing on base
        m = re.search(r'(.+?)\s+(vs\.?|v\.?|at|@)\s+(.+)', base, flags=re.I)
        if m:
            a = team_code(m.group(1))
            b = team_code(m.group(3))
            sep = '-' if m.group(2).lower().startswith(('v','vs','v.','vs.')) else '@'
            code = f"{a}{sep}{b}"[:5]  # Leave room for lang tag
            return f"{code}{lang}"[:8]  # e.g., "SPAR-ESP"
    
    # Common separators: vs, vs., v, v., at, @
    m = re.search(r'(.+?)\s+(vs\.?|v\.?|at|@)\s+(.+)', t, flags=re.I)
    if m:
        a = team_code(m.group(1))
        b = team_code(m.group(3))
        sep = '-' if m.group(2).lower().startswith(('v','vs','v.','vs.')) else '@'
        code = f"{a}{sep}{b}"
        return code[:8]
    # If we can't parse, crush to 8 alnum
    return re.sub(r'[^A-Za-z0-9]', '', t).upper()[:8]

# --- Data model -------------------------------------------------------------

@dataclass
class Event:
    id: str
    title: str
    sport: Optional[str]
    league: Optional[str]
    start: datetime
    stop: datetime
    status: Optional[str] = None

# --- DB ---------------------------------------------------------------------

def load_events_for_window(
    db_path: str,
    window_hours: int = PLANNING_WINDOW_HOURS,
    post_end_grace_min: int = POST_END_GRACE_MIN,
) -> List[Event]:
    """
    Return events that are LIVE now or start within `window_hours`,
    and also keep recently-ended events for `post_end_grace_min`.
    (Uses julianday() on both sides to avoid string-compare issues.)
    """
    now = now_utc()
    window_end = now + timedelta(hours=window_hours)
    grace_cutoff = now - timedelta(minutes=post_end_grace_min)

    q = """
    SELECT id, title, sport, league, start_utc, stop_utc, status
    FROM events
    WHERE
      julianday(replace(start_utc,'Z','+00:00')) <= julianday(?)
      AND julianday(replace(stop_utc,'Z','+00:00')) >= julianday(?)
    ORDER BY start_utc, title, id
    """
    params = (_jd_fmt(window_end), _jd_fmt(grace_cutoff))

    rows: List[Event] = []
    with sqlite3.connect(db_path) as cx:
        for r in cx.execute(q, params):
            start = parse_iso_z(r[4])
            stop = parse_iso_z(r[5])
            rows.append(
                Event(
                    id=r[0],
                    title=r[1] or "",
                    sport=r[2],
                    league=r[3],
                    start=start,
                    stop=stop,
                    status=r[6],
                )
            )
    # Deduplicate by id (belt & suspenders)
    return list({e.id: e for e in rows}.values())

# --- XMLTV helpers ----------------------------------------------------------

def format_desc(ev: Event) -> str:
    """Build a rich description like: USC VS WASHINGTON - COLLEGE FOOTBALL - NCAAF - LIVE NOW"""
    parts = []
    if ev.title:
        parts.append(ev.title.upper())
    if ev.sport:
        parts.append(ev.sport.upper())
    if ev.league:
        parts.append(ev.league.upper())
    if ev.status:
        parts.append(ev.status.upper())
    # Join with " - " and cap length for safety
    return (" - ".join(parts))[:1000] if parts else "ESPN+ EVENT"

def add_desc(p: ET.Element, text: Optional[str]) -> None:
    if not text:
        return
    d = ET.SubElement(p, "desc")
    d.set("lang", "en")
    d.text = re.sub(r"\s+", " ", text).strip()

# --- XMLTV generation -------------------------------------------------------

def emit_channel(tv: ET.Element, ev: Event) -> str:
    """
    Create <channel id="dl-<event id>">.
    First <display-name> is the event title (shortened).
    Second <display-name> is 'ESPN+' (provider label).
    """
    chan_id = f"dl-{ev.id}"
    ch = ET.SubElement(tv, "channel", id=chan_id)
    ET.SubElement(ch, "display-name").text = shorten_title(ev.title)
    ET.SubElement(ch, "display-name").text = PROVIDER_LABEL
    return chan_id

def emit_programme(
    tv: ET.Element,
    chan_id: str,
    start: datetime,
    stop: datetime,
    title: str,
    categories: Iterable[str] = (),
    desc: Optional[str] = None,
) -> None:
    p = ET.SubElement(
        tv,
        "programme",
        {"start": to_xmltv_dt(start), "stop": to_xmltv_dt(stop), "channel": chan_id},
    )
    t = ET.SubElement(p, "title")
    t.set("lang", "en")
    t.text = title
    if desc:
        add_desc(p, desc)
    for c in categories:
        ce = ET.SubElement(p, "category")
        ce.set("lang", "en")
        ce.text = c

def generate_xmltv(events: List[Event], out_path: str) -> None:
    tv = ET.Element("tv")
    now = now_utc()

    # Emit channels (one per event)
    chan_ids: dict[str, str] = {}
    for ev in events:
        chan_ids[ev.id] = emit_channel(tv, ev)

    # Emit programmes per event channel
    for ev in events:
        chan = chan_ids[ev.id]
        event_desc = format_desc(ev)  # Original event description

        # --- BEFORE EVENT: 8 hours of "NOT YET STARTED" blocks ---
        pre_start = ev.start - timedelta(hours=8)
        cursor = pre_start
        # Align to STANDBY_TILE_MIN grid
        align_min = (cursor.minute // STANDBY_TILE_MIN) * STANDBY_TILE_MIN
        cursor = cursor.replace(minute=align_min, second=0, microsecond=0)
        
        # Description for pre-event blocks (in local timezone)
        pre_desc = f"{ev.title.upper()} - STARTS AT {format_time_local(ev.start)}"
        
        while cursor < ev.start:
            block_end = min(cursor + timedelta(minutes=STANDBY_TILE_MIN), ev.start)
            # Skip tiny slivers
            if minutes_between(cursor, block_end) >= 5:
                emit_programme(tv, chan, cursor, block_end, "NOT YET STARTED", (), pre_desc)
            cursor += timedelta(minutes=STANDBY_TILE_MIN)

        # --- THE ACTUAL EVENT ---
        cats = ["Sports", "Sports event"]
        if ev.sport:
            cats.append(ev.sport)
        if ev.league:
            cats.append(ev.league)
        emit_programme(tv, chan, ev.start, ev.stop, ev.title, cats, event_desc)

        # --- AFTER EVENT: 8 hours of "STREAM OFFLINE" blocks ---
        post_end = ev.stop + timedelta(hours=8)
        cursor = ev.stop
        
        # Description for post-event blocks (in local timezone)
        post_desc = f"{ev.title.upper()} - EVENT ENDED AT {format_time_local(ev.stop)}"
        
        while cursor < post_end:
            block_end = min(cursor + timedelta(minutes=STANDBY_TILE_MIN), post_end)
            # Skip tiny slivers
            if minutes_between(cursor, block_end) >= 5:
                emit_programme(tv, chan, cursor, block_end, "STREAM OFFLINE", (), post_desc)
            cursor += timedelta(minutes=STANDBY_TILE_MIN)

    # Pretty-print (Python 3.9+)
    try:
        ET.indent(tv)  # type: ignore[attr-defined]
    except Exception:
        pass
    ET.ElementTree(tv).write(out_path, encoding="utf-8", xml_declaration=True)

# --- M3U generation ---------------------------------------------------------

def deep_link_for(ev: Event) -> str:
    # If AH4C=true, output tuner URL format
    if os.environ.get("AH4C", "").lower() in ("1", "true", "yes"):
        return f"http://10.0.0.122:7654/play/tuner/play/tuner/{ev.id}"
    
    # Default (existing behavior)
    return f"sportscenter://x-callback-url/showWatchStream?playID={ev.id}"

def generate_m3u(events: List[Event], out_path: str) -> None:
    now = now_utc()
    lines = ["#EXTM3U"]

    for ev in events:
        # Hide ended events from M3U but keep in XMLTV
        if ev.stop < now:
            continue

        tvg_id = f"dl-{ev.id}"
        m3u_name = compact_matchup(ev.title)  # <= 8 chars

        lines.append(
            f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{m3u_name}" tvg-logo="" '
            f'group-title="{GROUP_TITLE}",{m3u_name}'
        )
        lines.append(deep_link_for(ev))

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

# --- CLI --------------------------------------------------------------------

def summarize_run(events: List[Event]) -> None:
    now_str = now_utc().strftime("%Y-%m-%d %H:%M:%S UTC")
    print("ESPN+ M3U/XMLTV Generator")
    print("============================================================")
    print(f"Database: {os.path.abspath(DB_PATH)}")
    print(f"Time: {now_str}\n")
    print(f"Fetching live and upcoming events (next {PLANNING_WINDOW_HOURS} hours)...")
    print(f"Found {len(events)} events\n")

    print("Generating M3U playlist...")
    generate_m3u(events, OUT_M3U)
    m3u_count = 0
    try:
        with open(OUT_M3U, "r", encoding="utf-8") as fh:
            m3u_count = sum(1 for ln in fh if ln.startswith("#EXTINF"))
    except FileNotFoundError:
        pass
    print(f"  Saved: {os.path.abspath(OUT_M3U)}")
    print(f"  Channels: {m3u_count}\n")

    print("Generating XMLTV guide...")
    generate_xmltv(events, OUT_XML)
    print(f"  Saved: {os.path.abspath(OUT_XML)}\n")

    # Sample titles
    print("Sample events:")
    print("------------------------------------------------------------")
    for i, ev in enumerate(events[:5], 1):
        live = "LIVE - " if ev.start <= now_utc() <= ev.stop else ""
        print(f"{i}. {live}{ev.title}")
    if len(events) > 5:
        print(f"... and {len(events)-5} more\n")

    print("============================================================")
    print("Generation complete!\n")
    print("Files created:")
    print(f"  M3U:  {os.path.abspath(OUT_M3U)}")
    print(f"  XMLTV: {os.path.abspath(OUT_XML)}")

def main() -> None:
    # Ensure output dirs exist
    os.makedirs(os.path.dirname(OUT_XML), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_M3U), exist_ok=True)

    events = load_events_for_window(
        DB_PATH,
        window_hours=PLANNING_WINDOW_HOURS,
        post_end_grace_min=POST_END_GRACE_MIN,
    )
    # Stable ordering already in SQL; keep a final sort for determinism
    events.sort(key=lambda e: (e.start, e.title, e.id))
    summarize_run(events)

if __name__ == "__main__":
    main()
