# chicago-event-catalog

A pipeline for discovering, parsing, and enriching nonprofit fundraising event listings in the Chicago metro area.

This system crawls search results (Google Custom Search API or SerpAPI), fetches and parses event pages using an LLM-assisted parser, enriches organizer records (EIN, socials, contact titles), and optionally exports results to Google Sheets.

---

## Features

- Search scraping (Google CSE or SerpAPI) with deduplication  
- Headless fetcher using Playwright (with requests fallback)  
- LLM-guided parsing across multiple pages  
- EIN discovery from trusted sources (Candid, CauseIQ, Charity Navigator)  
- Google Sheets export helper  
- MySQL-backed storage with deduplication and schema enforcement  

---

## Quickstart

### Clone the repo

```bash
git clone https://github.com/<your-username>/chicago-event-catalog.git
cd chicago-event-catalog
```

### Create a Python virtual environment and install dependencies

```bash
# Create virtual environment
python -m venv .venv

# Activate environment
# macOS/Linux:
source .venv/bin/activate
# Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Upgrade pip and install requirements
pip install --upgrade pip
pip install -r requirements.txt
```

### Set environment variables

Create a `.env` file in the repo root:

```env
# API Keys
OPENAI_API_KEY=sk-xxxx
SERPAPI_KEY=serpapi_xxxx
GOOGLE_API_KEY=google_xxxx
GOOGLE_CSE_ID=cse_xxxx

# Database
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=yourpassword
MYSQL_DATABASE=event_catalog

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS=credentials.json
```

> The scripts use `python-dotenv` to load these automatically.

### Initialize the database (MySQL must be running)

```bash
python db.py
```

### Run the pipeline

```bash
# Full pipeline
python main.py

# Or run step by step
python search_scraper.py
python parser.py
python ein_enrichment.py
python "python export_to_sheets.py"
```

---

## Project Layout

```
chicago-event-catalog/
├─ db.py
├─ fetcher.py
├─ search_scraper.py
├─ parser.py
├─ ein_enrichment.py
├─ python export_to_sheets.py
├─ main.py
├─ requirements.txt
├─ README.md
├─ .gitignore

```

---


## Security Notes

- **Never commit `.env`, credentials, or token files**. These are ignored via `.gitignore`.  
- All API keys and DB configs are now loaded via environment variables.  
- For production scraping, respect robots.txt and API rate limits.  

---

## Contributing

1. Fork the repo  
2. Create a branch `feature/your-feature`  
3. Open a PR with a clear description  

