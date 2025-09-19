# ein_enrichment.py
import os
import re
import json
import time
from typing import Any, Dict, Optional, List

# SerpAPI client (optional)
try:
    from serpapi import GoogleSearch
except Exception:
    GoogleSearch = None

# Google CSE (optional)
try:
    from googleapiclient.discovery import build as google_build
except Exception:
    google_build = None

import requests
from openai import OpenAI

from db import (
    create_connection,
    update_organizer_ein,
    log_ein_result,
    # update_organizer_full is used in parser/enrichment pipeline separately
)

# ======================
# Config / Keys
# ======================
# Preferred: set in environment. If not, you can hardcode keys here (not recommended if sharing code).
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID", "").strip()
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# Hardcoded fallback (replace these with your real keys if you prefer not to set env vars)
if not SERPAPI_KEY:
    SERPAPI_KEY = "SERPAI_KEY"  # <-- replace if desired
if not GOOGLE_API_KEY:
    GOOGLE_API_KEY = "GOOGLE_API_KEY"  # <-- replace if desired
if not GOOGLE_CSE_ID:
    GOOGLE_CSE_ID = "GOOGLE_CSE_ID"  # <-- replace if desired
if not OPENAI_KEY:
    OPENAI_KEY = "OPENAI_KEY"  # <-- replace with your paid key

client = OpenAI(api_key=OPENAI_KEY)

# ======================
# Constants / Timeouts
# ======================
REQUEST_TIMEOUT = 1
MAX_BACKOFF = 1

# ======================
# Helpers
# ======================
def backoff_sleep(i: int):
    """Exponential backoff: 2s, 4s, 8s... capped."""
    delay = min(2 * (2 ** (i - 1)), MAX_BACKOFF)
    time.sleep(delay)

def safe_json(text: str) -> Dict[str, Any]:
    """Try to extract JSON from the string (strip fences, find {...})."""
    if not text:
        return {}
    cleaned = re.sub(r"^```json|^```|```$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    if not cleaned.strip().startswith("{"):
        m = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if m:
            cleaned = m.group(0)
    try:
        return json.loads(cleaned)
    except Exception:
        try:
            alt = cleaned.replace("'", "\"")
            return json.loads(alt)
        except Exception:
            return {}

def trim_len(val: Optional[str], maxlen: int) -> Optional[str]:
    if not val:
        return val
    s = str(val)
    return s[:maxlen]

def json_if_needed(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)

# ======================
# Search providers
# ======================
def serpapi_search(query: str, num_results: int = 6) -> List[Dict[str, Any]]:
    """Search via SerpAPI (organic results). Returns list of dicts with 'title','link','snippet'."""
    if not SERPAPI_KEY or GoogleSearch is None:
        # SerpAPI not available in environment
        return []

    params = {"engine": "google", "q": query, "api_key": SERPAPI_KEY, "num": num_results}
    try:
        results = GoogleSearch(params).get_dict()
        out = []
        for item in results.get("organic_results", []):
            out.append({
                "title": item.get("title"),
                "link": item.get("link") or item.get("url"),
                "snippet": item.get("snippet", "") or ""
            })
        return out
    except Exception as e:
        print(f"❌ SerpAPI error for query '{query}': {e}")
        return []

def google_cse_search(query: str, num_results: int = 6) -> List[Dict[str, Any]]:
    """
    Use Google Custom Search JSON API.
    Tries googleapiclient if available; otherwise makes HTTP request.
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []

    # Use googleapiclient if installed
    if google_build:
        try:
            service = google_build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
            resp = service.cse().list(q=query, cx=GOOGLE_CSE_ID, num=min(num_results, 500)).execute()
            items = resp.get("items", []) or []
            out = []
            for it in items:
                out.append({
                    "title": it.get("title"),
                    "link": it.get("link"),
                    "snippet": it.get("snippet", "")
                })
            return out
        except Exception as e:
            print(f"⚠️ googleapiclient CSE client failed for '{query}': {e}")
            # fall through to HTTP approach

    # HTTP fallback
    try:
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            "key": GOOGLE_API_KEY,
            "cx": GOOGLE_CSE_ID,
            "q": query,
            "num": num_results
        }
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", []) or []
        out = []
        for it in items:
            out.append({
                "title": it.get("title"),
                "link": it.get("link"),
                "snippet": it.get("snippet", "")
            })
        return out
    except Exception as e:
        print(f"❌ Google CSE HTTP error for '{query}': {e}")
        return []

def unified_search(query: str, num_results: int = 6) -> List[Dict[str, Any]]:
    """
    Try SerpAPI first; if it returns empty, fall back to Google CSE.
    Returns list of results.
    """
    results = serpapi_search(query, num_results=num_results)
    if results:
        return results
    print("ℹ️ SerpAPI returned no results — falling back to Google CSE.")
    return google_cse_search(query, num_results=num_results)

# ======================
# EIN extraction logic
# ======================
EIN_REGEX = re.compile(r"\b\d{2}-\d{7}\b")

def extract_ein_from_snippet(snippet: str) -> Optional[str]:
    if not snippet:
        return None
    m = EIN_REGEX.search(snippet)
    if m:
        return m.group(0)
    return None

def search_site_for_ein(query: str) -> Optional[str]:
    """
    Use unified_search to locate potential pages; search snippets for EIN pattern.
    Returns the first EIN match found.
    """
    try:
        results = unified_search(query, num_results=6)
    except Exception as e:
        print(f"❌ Unified search failed for '{query}': {e}")
        return None

    for r in results:
        snippet = (r.get("snippet") or "") + " " + (r.get("title") or "")
        ein = extract_ein_from_snippet(snippet)
        if ein:
            return ein

    # As a last-ditch attempt, try fetching the top result pages and searching page text
    for r in results:
        link = r.get("link")
        if not link:
            continue
        try:
            html = requests.get(link, timeout=REQUEST_TIMEOUT).text
            if not html:
                continue
            # simple search across HTML for EIN pattern
            m = EIN_REGEX.search(html)
            if m:
                return m.group(0)
        except Exception:
            continue

    return None

def search_ein_multisource(org_name: str) -> Optional[str]:
    # Try ProPublica / IRS
    ein = search_site_for_ein(f'{org_name} site:projects.propublica.org OR site:apps.irs.gov')
    if ein:
        return ein
    # Try CauseIQ
    ein = search_site_for_ein(f'{org_name} site:causeiq.com')
    if ein:
        return ein
    # Try Charity Navigator
    ein = search_site_for_ein(f'{org_name} site:charitynavigator.org')
    if ein:
        return ein
    # Try general web search as a last resort
    ein = search_site_for_ein(f'{org_name} nonprofit EIN')
    return ein

# ======================
# GPT Enrichment (organizers + events)
# ======================
def gpt_call(prompt: str, max_retries: int = 4) -> Dict[str, Any]:
    for i in range(1, max_retries + 1):
        try:
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            return safe_json(r.choices[0].message.content or "{}")
        except Exception as e:
            msg = str(e)
            if "rate limit" in msg.lower() or "429" in msg:
                backoff_sleep(i)
                continue
            # transient network errors
            if any(tok in msg.lower() for tok in ("timeout", "timed out", "502", "503", "504")):
                backoff_sleep(i)
                continue
            print(f"❌ OpenAI call failed: {e}")
            break
    return {}

def enrich_social_and_contact(org_name: str, website: Optional[str]) -> Dict[str, Any]:
    prompt = f"""
    We have a nonprofit or event organizer profile.

    Organizer: {org_name}
    Website: {website or 'unknown'}

    Return ONLY JSON with:
    {{
      "facebook": "full URL or empty string",
      "instagram": "full URL or empty string",
      "contact_title": "e.g. Development Director, Events Manager (or empty)"
    }}
    """
    return gpt_call(prompt)

def enrich_event_fields(event: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""
    Some event fields are missing. Infer them from context if possible.
    Return ONLY JSON with all keys present.

    Event Name: {event.get("name")}
    Event Date: {event.get("date")}
    Venue: {event.get("venue_name")} in {event.get("venue_city")}, {event.get("venue_state")}
    Registration URL: {event.get("registration_url")}
    Sponsorship URL: {event.get("sponsorship_url")}

    {{
      "description": "1-3 sentence summary or empty",
      "event_type": "Gala / Fundraiser / Conference / etc. or empty",
      "dress_code": "Black-tie / Formal / Business / Casual / empty",
      "venue_parking": "Parking/valet notes or empty",
      "sponsorship_tiers": "e.g. Gold/Silver/Bronze or empty",
      "sponsorship_contact": "name/email or empty",
      "past_sponsors": "list or string of sponsor names (can be empty)"
    }}
    """
    return gpt_call(prompt)

# ======================
# DB fetch/update (same logic you had)
# ======================
def fetch_organizers_to_enrich(batch_size=8):
    conn = create_connection()
    c = conn.cursor(dictionary=True)
    c.execute("""
        SELECT id, name, website, email, facebook, instagram, contact_title, ein
        FROM organizers
        WHERE (ein IS NULL OR facebook IS NULL OR instagram IS NULL OR contact_title IS NULL)
          AND name IS NOT NULL
          AND TRIM(name) != ''
          AND name NOT IN ('None','null','NULL')
        LIMIT %s
    """, (batch_size,))
    rows = c.fetchall()
    c.close(); conn.close()
    return rows

def fetch_events_to_enrich(batch_size=8):
    conn = create_connection()
    c = conn.cursor(dictionary=True)
    c.execute("""
        SELECT id, name, date, event_type, description, dress_code,
               venue_name, venue_city, venue_state, venue_zip,
               venue_parking, registration_url, sponsorship_url,
               sponsorship_tiers, sponsorship_contact, past_sponsors
        FROM events
        WHERE description IS NULL
           OR event_type IS NULL
           OR dress_code IS NULL
           OR venue_parking IS NULL
           OR sponsorship_tiers IS NULL
           OR sponsorship_contact IS NULL
           OR past_sponsors IS NULL
        LIMIT %s
    """, (batch_size,))
    rows = c.fetchall()
    c.close(); conn.close()
    return rows

def update_organizer_full(org_id: int, fields: Dict[str, Any]):
    conn = create_connection()
    c = conn.cursor()

    to_set = []
    values = []

    if fields.get("ein"):
        to_set.append("ein = %s")
        values.append(trim_len(fields["ein"], 20))

    if fields.get("facebook") is not None:
        to_set.append("facebook = %s")
        values.append(trim_len(fields["facebook"] or None, 255))

    if fields.get("instagram") is not None:
        to_set.append("instagram = %s")
        values.append(trim_len(fields["instagram"] or None, 255))

    if fields.get("contact_title") is not None:
        to_set.append("contact_title = %s")
        values.append(trim_len(fields["contact_title"] or None, 255))

    if not to_set:
        c.close(); conn.close()
        return

    sql = f"UPDATE organizers SET {', '.join(to_set)} WHERE id = %s"
    values.append(org_id)
    c.execute(sql, tuple(values))
    conn.commit()
    c.close(); conn.close()
    print(f"✅ Updated organizer {org_id}: {', '.join(to_set)}")

def update_event_fields(event_id: int, inferred: Dict[str, Any]):
    conn = create_connection()
    c = conn.cursor()

    mapping = [
        ("description", None),  # TEXT
        ("event_type", 100),
        ("dress_code", 100),
        ("venue_parking", 100),
        ("sponsorship_tiers", None),   # TEXT
        ("sponsorship_contact", 255),
        ("past_sponsors", None),       # TEXT
    ]

    to_set = []
    values = []
    for field, maxlen in mapping:
        raw_val = inferred.get(field)
        val = json_if_needed(raw_val)
        if val is not None:
            if maxlen:
                val = trim_len(val, maxlen)
            to_set.append(f"{field} = %s")
            values.append(val)

    if not to_set:
        c.close(); conn.close()
        return

    sql = f"UPDATE events SET {', '.join(to_set)} WHERE id = %s"
    values.append(event_id)
    c.execute(sql, tuple(values))
    conn.commit()
    c.close(); conn.close()
    print(f"✅ Updated event {event_id}: {', '.join(to_set)}")

# ======================
# Processors
# ======================
def process_single_organizer(org: Dict[str, Any]):
    org_id, name, website = org["id"], org["name"], org.get("website")
    print(f"\n🔍 Enriching organizer: {name}")

    ein = None
    try:
        ein = search_ein_multisource(name)
        if ein:
            update_organizer_ein(org_id, ein)
            log_ein_result(org_id, "MultiSource", ein, "success", "EIN found")
        else:
            log_ein_result(org_id, "MultiSource", None, "failed", "No EIN found")
    except Exception as e:
        log_ein_result(org_id, "MultiSource", None, "failed", str(e))

    # Socials + contact title via GPT
    enrich = enrich_social_and_contact(name, website)
    to_update = {}
    if ein:
        to_update["ein"] = ein
    if "facebook" in enrich:
        to_update["facebook"] = enrich.get("facebook") or None
    if "instagram" in enrich:
        to_update["instagram"] = enrich.get("instagram") or None
    if "contact_title" in enrich:
        to_update["contact_title"] = enrich.get("contact_title") or None

    if to_update:
        update_organizer_full(org_id, to_update)

def process_single_event(ev: Dict[str, Any]):
    ev_id = ev["id"]
    print(f"\n🧩 Enriching event: {ev.get('name')}")
    inferred = enrich_event_fields(ev)
    if inferred:
        update_event_fields(ev_id, inferred)

# ======================
# Master Runner
# ======================
def enrich_data(limit: int = 2):
    processed = 0
    while processed < limit:
        orgs = fetch_organizers_to_enrich(batch_size=8)
        for org in orgs:
            process_single_organizer(org)
            processed += 1
            time.sleep(2)

        evs = fetch_events_to_enrich(batch_size=8)
        for ev in evs:
            process_single_event(ev)
            processed += 1
            time.sleep(2)

        if not orgs and not evs:
            print("🎉 Nothing left to enrich.")
            break

if __name__ == "__main__":
    enrich_data(limit=2)
