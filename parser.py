# parser.py
import re
import json
import time
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from openai import OpenAI

from fetcher import fetch_page
from db import insert_event, insert_organizer, log_parse_result, create_connection
from search_scraper import serpapi_search  # reuses your existing SerpAPI helper

# ======================
# OpenAI Setup (Hardcoded Key)
# ======================
# ⚠️ Replace the string below with your paid OpenAI key.
# Not recommended for shared repos — ok for local testing.
OPENAI_KEY = "OPENAI_KEY"
client = OpenAI(api_key=OPENAI_KEY)

# ======================
# Config
# ======================
DEFAULT_GPT_MODEL = "gpt-4o-mini"
MAX_GPT_RETRIES = 5
SLEEP_BETWEEN_GPT_CALLS = 1.0

DEEP_SEARCH_MAX_PAGES = 6        # how many additional pages to fetch during deep search
DEEP_SEARCH_SNIPPET_CHARS = 3500 # per-page snippet limit (keeps token count sensible)
MISSING_FIELDS_THRESHOLD = 4     # when to trigger deep search
SEARCH_RESULTS_PER_QUERY = 6     # serpapi results per query

# ======================
# Helpers
# ======================
def normalize_date(date_str):
    if not date_str:
        return None
    try:
        dt = dateparser.parse(str(date_str), fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def clean_text(s):
    if not s:
        return None
    return re.sub(r"\s+", " ", str(s)).strip()

def safe_json(text: str) -> Dict[str, Any]:
    """Extract JSON from text; return {} on failure."""
    if not text:
        return {}
    cleaned = re.sub(r"^```json|^```|```$", "", text.strip(), flags=re.IGNORECASE|re.MULTILINE).strip()
    # Try to extract first {...} block if extra commentary present
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

def backoff_sleep(i: int):
    delay = min(2 * (2 ** (i - 1)), 20)
    time.sleep(delay)

# ======================
# GPT wrapper with retries
# ======================
def gpt_call(prompt: str, model: str = DEFAULT_GPT_MODEL, max_retries: int = MAX_GPT_RETRIES, sleep_between_calls: float = SLEEP_BETWEEN_GPT_CALLS) -> Dict[str, Any]:
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            content = resp.choices[0].message.content or ""
            parsed = safe_json(content)
            if parsed:
                time.sleep(sleep_between_calls)
                return parsed
            # Not parseable JSON -> raise to go through retry logic
            raise ValueError("OpenAI returned non-JSON/invalid JSON")
        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "rate_limit_exceeded" in msg or "429" in msg or "requests per min" in msg:
                print(f"⚠️ OpenAI rate-limit or RPM hit on attempt {attempt}: {e}. Backing off...")
                backoff_sleep(attempt)
                continue
            if any(tok in msg for tok in ("timeout", "timed out", "502", "503", "504", "temporarily unavailable")):
                print(f"⚠️ OpenAI transient error on attempt {attempt}: {e}. Backing off...")
                backoff_sleep(attempt)
                continue
            print(f"❌ OpenAI call failed (attempt {attempt}): {e}")
            break
    return {}

# ======================
# Fetch Raw Links
# ======================
def fetch_unprocessed_links(limit=100):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT *
        FROM raw_links
        WHERE id NOT IN (SELECT raw_link_id FROM parse_logs)
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# ======================
# Field utilities
# ======================
REQUIRED_FIELDS = [
    "event_name", "event_date", "event_type", "description", "venue_name",
    "venue_address", "venue_city", "venue_state", "venue_zip", "venue_parking",
    "venue_website", "registration_url", "sponsorship_url", "sponsorship_tiers",
    "sponsorship_contact", "past_sponsors", "dress_code", "organizer_name",
    "organizer_ein", "organizer_website", "organizer_email", "organizer_phone",
    "organizer_contact_name", "organizer_contact_title", "organizer_contact_email",
    "organizer_facebook", "organizer_instagram"
]

def count_missing_fields(parsed: Dict[str, Any]) -> int:
    if not parsed:
        return len(REQUIRED_FIELDS)
    miss = 0
    for f in REQUIRED_FIELDS:
        v = parsed.get(f)
        if v is None or str(v).strip() == "":
            miss += 1
    return miss

# ======================
# Deep search / aggregation
# ======================
def build_deep_search_queries(url: str, parsed: Dict[str, Any]) -> List[str]:
    qrs = []
    en = parsed.get("event_name") if parsed and parsed.get("event_name") else None
    if en:
        qrs.append(f'{en} "event"')
        qrs.append(f'{en} site:.org OR site:.com')
    on = parsed.get("organizer_name") if parsed and parsed.get("organizer_name") else None
    if on:
        qrs.append(f'{on} "nonprofit" OR "charity"')
        qrs.append(f'{on} site:charitynavigator.org OR site:candid.org OR site:causeiq.com')
    try:
        host = urlparse(url).netloc
        if host:
            qrs.append(f'site:{host} "{on or en or "event"}"')
    except Exception:
        pass
    parsed_url = urlparse(url)
    path_tokens = [t for t in parsed_url.path.split("/") if t and len(t) > 2]
    if path_tokens:
        qrs.append(" ".join(path_tokens[:4]))
    year = ""
    if parsed and parsed.get("event_date"):
        try:
            year = parsed.get("event_date").split("-")[0]
        except Exception:
            year = ""
    city = parsed.get("venue_city") if parsed and parsed.get("venue_city") else ""
    if city or year:
        q = "event"
        if city:
            q += f" {city}"
        if year:
            q += f" {year}"
        qrs.append(q)
    seen = set(); final = []
    for q in qrs:
        qn = q.strip()
        if qn and qn not in seen:
            seen.add(qn); final.append(qn)
    return final[:6]

def fetch_and_aggregate_snippets(urls: List[str], max_pages: int = DEEP_SEARCH_MAX_PAGES, chars_per_page: int = DEEP_SEARCH_SNIPPET_CHARS) -> str:
    aggregated = []
    cnt = 0
    for u in urls:
        if cnt >= max_pages:
            break
        try:
            print(f"   🔗 Deep-fetching: {u}")
            html = fetch_page(u)
            if not html:
                continue
            text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            if text:
                aggregated.append(text[:chars_per_page])
                cnt += 1
            time.sleep(0.6)  # gentle pause
        except Exception as e:
            print(f"   ⚠️ Fetch error for {u}: {e}")
            continue
    return "\n\n".join(aggregated)

def deep_synthesize_fields(url: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    queries = build_deep_search_queries(url, parsed or {})
    print(f"🔎 Deep search queries: {queries}")
    candidate_urls = []
    for q in queries:
        try:
            results = serpapi_search(q, num_results=SEARCH_RESULTS_PER_QUERY)
            for r in results:
                link = r.get("link")
                if link and link not in candidate_urls:
                    candidate_urls.append(link)
        except Exception as e:
            print(f"   ⚠️ SerpAPI query failed for '{q}': {e}")
        if len(candidate_urls) >= DEEP_SEARCH_MAX_PAGES * 2:
            break
    candidate_urls = candidate_urls[:DEEP_SEARCH_MAX_PAGES * 2]
    if not candidate_urls:
        print("   ⚠️ No deep-search candidate URLs found.")
        return {}
    snippets = fetch_and_aggregate_snippets(candidate_urls, max_pages=DEEP_SEARCH_MAX_PAGES)
    missing = [f for f in REQUIRED_FIELDS if not parsed or not parsed.get(f)]
    prompt_lines = [
        "You are given multiple web page text snippets related to an event and its organizer.",
        "Using the combined snippets, fill in ONLY the requested fields below. If a field cannot be determined, return an empty string.",
        "",
        f"Source URL (original): {url}",
        "",
        "Fields requested (return valid JSON with these exact keys):",
        json.dumps({k: "" for k in REQUIRED_FIELDS}, ensure_ascii=False, indent=2),
        "",
        "Only fill fields that are currently empty in the partial extraction; do not overwrite fields already provided.",
        "",
        "Combined snippets (multiple pages):",
        snippets[:DEEP_SEARCH_SNIPPET_CHARS * 6]  # cap total size passed to GPT
    ]
    prompt = "\n".join(prompt_lines)
    print("   🧠 Asking GPT to synthesize across multiple pages...")
    synth = gpt_call(prompt, model=DEFAULT_GPT_MODEL, max_retries=MAX_GPT_RETRIES)
    if not synth:
        print("   ⚠️ Synthesis returned empty.")
        return {}
    out = {}
    for k in REQUIRED_FIELDS:
        v = synth.get(k)
        if v is not None and str(v).strip() != "":
            out[k] = v
    return out

# ======================
# GPT Parser (single-page)
# ======================
def parse_event_with_gpt(page_html, url):
    snippet = BeautifulSoup(page_html, "html.parser").get_text(" ", strip=True)[:4000]
    prompt = f"""
    Extract structured event and organizer details from this webpage content snippet.
    If a field is unknown, return an empty string "".
    Return ONLY valid JSON.

    Required fields:
    {json.dumps({k: "" for k in REQUIRED_FIELDS}, ensure_ascii=False, indent=2)}

    Source URL: {url}

    CONTENT:
    {snippet}
    """
    result = gpt_call(prompt, model=DEFAULT_GPT_MODEL, max_retries=MAX_GPT_RETRIES)
    return result

# ======================
# Main Parser Loop
# ======================
def process_links(batch_limit=1000):
    links = fetch_unprocessed_links(limit=batch_limit)
    if not links:
        print("✅ No new links to process.")
        return
    print(f"🔍 Found {len(links)} unprocessed links...")
    for link in links:
        url = link["url"]
        print(f"\n➡️ Processing: {url}")
        html = fetch_page(url)
        if not html:
            log_parse_result(link["id"], status="failed", message="Fetch failed (headless + fallback)")
            continue
        try:
            data = parse_event_with_gpt(html, url)
        except Exception as e:
            print(f"❌ Unexpected error during parse: {e}")
            log_parse_result(link["id"], status="failed", message=f"Parse error: {e}")
            continue
        missing_count = count_missing_fields(data)
        print(f"   ℹ️ Missing fields after first parse: {missing_count}")
        if missing_count >= MISSING_FIELDS_THRESHOLD:
            try:
                print("   🔁 Triggering deep search + synthesize via GPT across multiple sources...")
                synth = deep_synthesize_fields(url, data)
                for k, v in synth.items():
                    if v and (not data.get(k) or str(data.get(k)).strip() == ""):
                        data[k] = v
                missing_count = count_missing_fields(data)
                print(f"   ℹ️ Missing fields after deep synthesis: {missing_count}")
            except Exception as e:
                print(f"   ⚠️ Deep synthesis error: {e}")
        if not isinstance(data, dict) or count_missing_fields(data) == len(REQUIRED_FIELDS):
            try:
                soup = BeautifulSoup(html, "html.parser")
                title = soup.title.string.strip() if soup.title and soup.title.string else None
                fallback = {
                    "event_name": title or "",
                    "event_date": "",
                    "event_type": "",
                    "description": "",
                    "venue_name": "",
                    "venue_address": "",
                    "venue_city": "",
                    "venue_state": "",
                    "venue_zip": "",
                    "venue_parking": "",
                    "venue_website": "",
                    "registration_url": url,
                    "sponsorship_url": "",
                    "sponsorship_tiers": "",
                    "sponsorship_contact": "",
                    "past_sponsors": "",
                    "dress_code": "",
                    "organizer_name": "",
                    "organizer_ein": "",
                    "organizer_website": "",
                    "organizer_email": "",
                    "organizer_phone": "",
                    "organizer_contact_name": "",
                    "organizer_contact_title": "",
                    "organizer_contact_email": "",
                    "organizer_facebook": "",
                    "organizer_instagram": ""
                }
                data = fallback
                log_parse_result(link["id"], status="failed", message="Parse failed: invalid JSON (fallback used)")
            except Exception as e:
                log_parse_result(link["id"], status="failed", message=f"Parse failed and fallback error: {e}")
                continue
        if not data.get("registration_url"):
            data["registration_url"] = url
        data["event_date"] = normalize_date(data.get("event_date"))
        for k in [
            "event_name", "event_type", "description", "venue_name", "venue_address",
            "venue_city", "venue_state", "venue_zip", "venue_parking", "venue_website",
            "sponsorship_url", "sponsorship_tiers", "sponsorship_contact",
            "past_sponsors", "dress_code", "organizer_name", "organizer_ein",
            "organizer_website", "organizer_email", "organizer_phone",
            "organizer_contact_name", "organizer_contact_title", "organizer_contact_email",
            "organizer_facebook", "organizer_instagram"
        ]:
            if k in data:
                data[k] = clean_text(data[k])
        try:
            organizer_id = insert_organizer(data)
            if organizer_id:
                insert_event(data, organizer_id)
            final_missing = count_missing_fields(data)
            log_parse_result(link["id"], status="success" if final_missing < len(REQUIRED_FIELDS) else "failed",
                             message=f"Parsed and saved (missing_fields={final_missing})")
        except Exception as e:
            print(f"❌ Save error: {e}")
            log_parse_result(link["id"], status="failed", message=f"Save error: {e}")

if __name__ == "__main__":
    process_links()
