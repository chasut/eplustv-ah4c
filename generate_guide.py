#!/usr/bin/env python3
"""
ESPN+ M3U and XMLTV Generator
Generates playlists and guide for live and upcoming events.
- Uses ./out/espn_schedule.db
- Writes outputs to ./out/espn_plus.xml and ./out/espn_plus.m3u
- Normalizes XMLTV categories: "Sports" and "Sports event"
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from xml.dom import minidom
import os

# ------------------------------------------------------------------
# Configuration (repo-local paths)
# ------------------------------------------------------------------
REPO_DIR   = os.path.dirname(os.path.abspath(__file__))
OUT_DIR    = os.path.join(REPO_DIR, "out")
DB_PATH    = os.path.join(OUT_DIR, "espn_schedule.db")
M3U_OUTPUT = os.path.join(OUT_DIR, "espn_plus.m3u")
XML_OUTPUT = os.path.join(OUT_DIR, "espn_plus.xml")

STANDBY_BLOCK_DURATION_MIN = 30  # 30-minute standby blocks
MAX_STANDBY_HOURS          = 6   # up to 6 hours of standby before event
EVENT_ENDED_DURATION_MIN   = 30  # 30-minute "event ended" block

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def ensure_out_dir():
    """Create ./out/ if it doesn't exist."""
    os.makedirs(OUT_DIR, exist_ok=True)

def get_live_and_upcoming_events(db_path, hours_ahead=3):
    """
    Get events that are either:
    - Live now (started but not ended)
    - Starting within next N hours
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    window_end = now + timedelta(hours=hours_ahead)

    query = f"""
        SELECT * FROM events
        WHERE datetime(stop_utc) > datetime('now')
          AND datetime(start_utc) <= datetime('now', '+{hours_ahead} hours')
        ORDER BY start_utc, title
    """
    cursor = conn.execute(query)
    events = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return events

def parse_iso_datetime(dt_str):
    """Parse ISO8601 datetime string to timezone-aware datetime"""
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1] + '+00:00'
    return datetime.fromisoformat(dt_str)

def format_datetime_xmltv(dt):
    """Format datetime for XMLTV: YYYYMMDDHHmmss +0000"""
    return dt.strftime('%Y%m%d%H%M%S +0000')

def extract_play_id(event):
    """Extract play ID from event (the UUID)"""
    return event['id']

def build_deep_link(play_id):
    """Build the ESPN deep link URL"""
    return f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"

def generate_m3u(events):
    """Generate M3U playlist"""
    lines = ['#EXTM3U']

    for idx, event in enumerate(events, 1):
        channel_num = idx
        play_id = extract_play_id(event)
        deep_link = build_deep_link(play_id)

        title = event.get('title', 'Unknown Event')
        sport = event.get('sport', '')
        league = event.get('league', '')

        channel_name = f"ESPN+ {channel_num}: {title}"
        if league:
            channel_name += f" ({league})"

        lines.append(f'#EXTINF:-1 tvg-id="espnplus{channel_num}" tvg-name="{channel_name}" tvg-logo="" group-title="ESPN+",{channel_name}')
        lines.append(deep_link)

    return '\n'.join(lines)

def generate_standby_blocks(channel_id, start_time, now):
    """
    Generate STAND BY placeholder blocks before an event starts.
    Creates 30-minute blocks from now until event start (max 6 hours).
    """
    blocks = []

    time_until = (start_time - now).total_seconds() / 60  # minutes
    if time_until <= 0:
        return []  # Event already started or starting now

    max_standby_min = MAX_STANDBY_HOURS * 60
    standby_duration = min(time_until, max_standby_min)

    current_time = now
    blocks_needed = int(standby_duration / STANDBY_BLOCK_DURATION_MIN)

    for _ in range(blocks_needed):
        block_start = current_time
        block_stop = current_time + timedelta(minutes=STANDBY_BLOCK_DURATION_MIN)

        if block_stop > start_time:
            block_stop = start_time

        blocks.append({
            'start': format_datetime_xmltv(block_start),
            'stop': format_datetime_xmltv(block_stop),
            'title': 'STAND BY',
            'desc': f'Event starts at {start_time.strftime("%H:%M UTC")}'
        })

        current_time = block_stop
        if current_time >= start_time:
            break

    return blocks

def generate_event_ended_block(channel_id, end_time):
    """Generate EVENT ENDED block after event finishes"""
    block_start = end_time
    block_stop = end_time + timedelta(minutes=EVENT_ENDED_DURATION_MIN)

    return {
        'start': format_datetime_xmltv(block_start),
        'stop': format_datetime_xmltv(block_stop),
        'title': 'EVENT ENDED',
        'desc': 'This event has concluded'
    }

def generate_xmltv(events):
    """Generate XMLTV guide"""
    now = datetime.now(timezone.utc)

    tv = ET.Element('tv')
    tv.set('generator-info-name', 'DeepLinks ESPN+ Guide Generator')
    tv.set('generator-info-url', 'https://github.com/kineticman/DeepLinks')

    # Channels
    for idx, event in enumerate(events, 1):
        channel_id = f"espnplus{idx}"
        channel = ET.SubElement(tv, 'channel')
        channel.set('id', channel_id)

        display_name = ET.SubElement(channel, 'display-name')
        title = event.get('title', 'Unknown Event')
        league = event.get('league', '')
        display_name.text = f"ESPN+ {idx}: {title}" + (f" ({league})" if league else "")

    # Programmes
    for idx, event in enumerate(events, 1):
        channel_id = f"espnplus{idx}"

        start_time = parse_iso_datetime(event['start_utc'])
        stop_time  = parse_iso_datetime(event['stop_utc'])

        is_live = start_time <= now < stop_time
        is_upcoming = start_time > now

        # Pre-event standby placeholders
        if is_upcoming:
            for block in generate_standby_blocks(channel_id, start_time, now):
                programme = ET.SubElement(tv, 'programme')
                programme.set('start', block['start'])
                programme.set('stop', block['stop'])
                programme.set('channel', channel_id)

                title_elem = ET.SubElement(programme, 'title')
                title_elem.set('lang', 'en')
                title_elem.text = block['title']

                desc_elem = ET.SubElement(programme, 'desc')
                desc_elem.set('lang', 'en')
                desc_elem.text = block['desc']

        # The actual event
        programme = ET.SubElement(tv, 'programme')
        programme.set('start', format_datetime_xmltv(start_time))
        programme.set('stop', format_datetime_xmltv(stop_time))
        programme.set('channel', channel_id)

        if is_live:
            ET.SubElement(programme, 'live')

        title_elem = ET.SubElement(programme, 'title')
        title_elem.set('lang', 'en')
        title_elem.text = event.get('title', 'Unknown Event')

        if event.get('subtitle') or event.get('league'):
            subtitle_elem = ET.SubElement(programme, 'sub-title')
            subtitle_elem.set('lang', 'en')
            subtitle_elem.text = event.get('subtitle') or event.get('league')

        desc_elem = ET.SubElement(programme, 'desc')
        desc_elem.set('lang', 'en')
        sport = event.get('sport', '')
        league = event.get('league', '')
        parts = []
        if sport: parts.append(f"Sport: {sport}")
        if league: parts.append(f"League: {league}")
        parts.append(f"Status: {'LIVE NOW' if is_live else 'Upcoming'}")
        desc_elem.text = ' | '.join(parts)

        # Categories (normalized)
        cat1 = ET.SubElement(programme, 'category'); cat1.set('lang', 'en'); cat1.text = 'Sports'
        cat2 = ET.SubElement(programme, 'category'); cat2.set('lang', 'en'); cat2.text = 'Sports event'
        if sport:
            cat3 = ET.SubElement(programme, 'category'); cat3.set('lang', 'en'); cat3.text = sport

        # Post-event placeholder
        ended = generate_event_ended_block(channel_id, stop_time)
        programme = ET.SubElement(tv, 'programme')
        programme.set('start', ended['start'])
        programme.set('stop', ended['stop'])
        programme.set('channel', channel_id)

        t = ET.SubElement(programme, 'title'); t.set('lang', 'en'); t.text = ended['title']
        d = ET.SubElement(programme, 'desc');  d.set('lang', 'en'); d.text = ended['desc']

    xml_str = ET.tostring(tv, encoding='unicode')
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent='  ')

def main():
    ensure_out_dir()

    # Allow a positional override for DB path, but default to ./out/espn_schedule.db
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH

    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    print("ESPN+ M3U/XMLTV Generator")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    print("Fetching live and upcoming events (next 3 hours)...")
    events = get_live_and_upcoming_events(db_path, hours_ahead=3)

    if not events:
        print("No live or upcoming events found!")
        sys.exit(0)

    print(f"Found {len(events)} events")
    print()

    # Generate M3U
    print("Generating M3U playlist...")
    m3u_content = generate_m3u(events)
    with open(M3U_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(m3u_content)
    print(f"  Saved: {M3U_OUTPUT}")
    print(f"  Channels: {len(events)}")
    print()

    # Generate XMLTV
    print("Generating XMLTV guide...")
    xml_content = generate_xmltv(events)
    with open(XML_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    print(f"  Saved: {XML_OUTPUT}")
    print()

    # Show sample events
    print("Sample events:")
    print("-" * 60)
    now = datetime.now(timezone.utc)
    for i, event in enumerate(events[:5], 1):
        title = event.get('title', 'Unknown')
        start = parse_iso_datetime(event['start_utc'])
        stop  = parse_iso_datetime(event['stop_utc'])
        is_live = start <= now < stop
        status = "🔴 LIVE" if is_live else f"⏰ {start.strftime('%H:%M UTC')}"
        print(f"{i}. {status} - {title}")
    if len(events) > 5:
        print(f"... and {len(events) - 5} more")

    print()
    print("=" * 60)
    print("✓ Generation complete!")
    print()
    print("Files created:")
    print(f"  M3U:  {M3U_OUTPUT}")
    print(f"  XMLTV: {XML_OUTPUT}")

if __name__ == "__main__":
    main()
