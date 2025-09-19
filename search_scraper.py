# search_scraper.py
"""
Search scraper (SerpAPI) — hardcoded config, expanded queries, no getenv.

Instructions:
 - Replace SERPAPI_KEY with your real SerpAPI key below.
 - Adjust SEARCH_RESULTS_PER_QUERY and SEARCH_SAVE_LIMIT as desired.
 - This script writes into the `raw_links` table in the event_catalog MySQL DB.
"""

import time
import requests
import mysql.connector
from mysql.connector import Error

# -------------------------
# HARD-CODED CONFIG (edit)
# -------------------------
SERPAPI_KEY = ""      # <- REPLACE with your SerpAPI key
SEARCH_RESULTS_PER_QUERY = 500         # how many links to request from SerpAPI per query (max per search)
SEARCH_SAVE_LIMIT = 1000              # overall cap on how many raw_links to save in one run
PAUSE_BETWEEN_QUERIES = 1.0           # seconds between queries (be gentle)
# -------------------------

# Database config (hardcoded)
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "event_catalog",
    "charset": "utf8mb4",
}

# Expanded list of queries focused on Chicago fundraising events
QUERIES = [
    "auction gala Chicago Illinois",
    "fundraising event Chicago Illinois",
    "charity gala Chicago Illinois",
    "charity fundraiser Chicago 2025",
    "nonprofit gala Chicago 2025",
    "fundraising gala Chicago 2025",
    "charity golf tournament Chicago",
    "charity run Chicago 2025",
    "benefit concert Chicago 2025",
    "charity ball Chicago 2025",
    "annual gala Chicago nonprofit",
    "foundation gala Chicago",
    "philanthropy gala Chicago",
    "black tie gala Chicago",
    "charity dinner Chicago 2025",
    "fundraiser dinner Chicago",
    "nonprofit fundraiser Chicago",
    "gala fundraiser Chicago Illinois 2025",
    "charity auction Chicago",
    "benefit gala Chicago Illinois",
    "charity banquet Chicago",
    "fundraising walk Chicago 2025",
    "foundation fundraising event Chicago",
    "charity festival Chicago 2025",
    "community fundraiser Chicago",
    "corporate philanthropy event Chicago",
    "gala tickets chicago charity",
    "nonprofit events Chicago 2025",
    "charity gala 'Chicago, IL'",
    "Chicago 'fundraising gala' site:eventbrite.com"   # site-scoped query example
]

# -------------------------
# Helper: DB connection + save
# -------------------------
def create_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


def save_raw_link(query, title, url, snippet):
    conn = create_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    try:
        sql = """
        INSERT INTO raw_links (query, title, url, snippet)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE url = url
        """
        cursor.execute(sql, (query[:255], (title or "")[:255], url, snippet))
        conn.commit()
        return cursor.rowcount != 0
    except Exception as e:
        print(f"❌ Error saving raw link: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


# -------------------------
# SerpAPI Search
# -------------------------
def serpapi_search(query, num_results=SEARCH_RESULTS_PER_QUERY):
    """
    Return list of organic results from SerpAPI for the query.
    Each item is the dict returned by SerpAPI's organic_results entries.
    """
    if not SERPAPI_KEY or SERPAPI_KEY == "YOUR_SERPAPI_KEY":
        print("❌ SERPAPI_KEY is not set or left as placeholder. Please set SERPAPI_KEY in this file.")
        return []

    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": num_results,
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        # organic_results is typical; sometimes 'local_results' or 'organic' may present alternative shapes
        results = data.get("organic_results") or data.get("local_results") or []
        # Ensure predictable structure: list of dicts with title/link/snippet
        cleaned = []
        for r in results:
            title = r.get("title") or r.get("name") or ""
            link = r.get("link") or r.get("url") or r.get("displayed_link") or ""
            snippet = r.get("snippet") or r.get("description") or ""
            if link:
                cleaned.append({"title": title, "link": link, "snippet": snippet})
        # Limit to requested number explicitly (SerpAPI respects num but be safe)
        return cleaned[:num_results]
    except Exception as e:
        print(f"⚠️ SerpAPI request error for query '{query}': {e}")
        return []


# -------------------------
# Main run
# -------------------------
def main():
    print("🔍 search_scraper starting")
    print(f"Each search will request up to {SEARCH_RESULTS_PER_QUERY} links.")
    print(f"Total save cap for run: {SEARCH_SAVE_LIMIT} links.")
    total_saved = 0

    for q in QUERIES:
        if total_saved >= SEARCH_SAVE_LIMIT:
            print("Reached overall save limit. Stopping.")
            break

        print(f"\n🔎 Query: {q}")
        results = serpapi_search(q, num_results=SEARCH_RESULTS_PER_QUERY)
        print(f"  → SerpAPI returned {len(results)} items (capped to {SEARCH_RESULTS_PER_QUERY})")

        for item in results:
            if total_saved >= SEARCH_SAVE_LIMIT:
                break
            title = item.get("title") or ""
            link = item.get("link") or ""
            snippet = item.get("snippet") or ""
            if not link:
                continue
            saved = save_raw_link(q, title, link, snippet)
            if saved:
                total_saved += 1
                print(f"  ✅ Saved ({total_saved}) {link}")
            else:
                # could be duplicate; still print
                print(f"  • Skipped (exists or failed) {link}")

        # small pause between queries to avoid hammering API / local systems
        time.sleep(PAUSE_BETWEEN_QUERIES)

    print(f"\n✅ Done. Total links saved this run: {total_saved}")
    print("Tip: To increase per-search results, change SEARCH_RESULTS_PER_QUERY at the top of this file.")
    print("Tip: To increase total links per run, change SEARCH_SAVE_LIMIT at the top of this file.")


if __name__ == "__main__":
    main()
