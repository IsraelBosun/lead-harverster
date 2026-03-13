# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

LeadHarvest is a fully built Nigerian SMB lead generation tool. It scrapes Google Places for businesses, visits their websites with Playwright to extract contact details, scores website quality (low score = strong lead), stores everything in SQLite, and sends cold outreach emails via SMTP. All three phases are complete.

## How to Run

**API backend (always start this first):**
```bash
venv/Scripts/python.exe run_api.py
```
Do NOT use `python -m uvicorn api:app` directly on Windows — it uses SelectorEventLoop which breaks Playwright subprocess spawning. `run_api.py` sets `WindowsProactorEventLoopPolicy` before Uvicorn starts.

**Streamlit frontend (separate terminal):**
```bash
venv/Scripts/python.exe -m streamlit run streamlit_app.py
```

**CLI scraper (no API needed):**
```bash
venv/Scripts/python.exe main.py
```

**Install packages (always use venv pip, never system pip):**
```bash
venv/Scripts/pip3.exe install <package>
```

**Print encoding for terminal output:**
```bash
PYTHONIOENCODING=utf-8 venv/Scripts/python.exe script.py
```

## Architecture — How the Pieces Fit Together

### Data Flow
```
User Input (category + city)
  → scraper/places.py        # Google Places Text Search + Place Details
  → scraper/website.py       # Playwright: visit each site, discover contact pages
      → scraper/extractor.py # Regex extraction: email, phone, WhatsApp, social links
      → scraper/scorer.py    # Score 0–100 (lower = weaker site = stronger lead)
  → db/database.py           # Dedup against SQLite, save new leads
  → main.py                  # Export timestamped Excel + update master Excel
```

### Two Entry Points, Same Core
`main.py` (CLI) and `api.py` (FastAPI) both call the same scraper functions. `api.py` wraps all blocking calls in `asyncio.to_thread()` and runs the job in a `BackgroundTasks` callback. Results stored in an in-memory `jobs: dict` keyed by UUID job_id. The Streamlit frontend polls `GET /results/{job_id}` every 3 seconds.

### Database Layer (`db/database.py`)
Two SQLite tables in `output/leadharvest.db`:
- `businesses` — primary key is `email` (lowercase); `place_id` is a secondary UNIQUE constraint. A business without email is saved only if it has a place_id.
- `campaigns` — log of every email send attempt (email, business_name, sent_at, status).

Dedup happens at two points: `get_existing_website_urls()` is passed to `places.py` so the Places API skips already-known sites. `filter_new_businesses()` is called after scraping to separate new leads from known ones before saving or exporting.

### Contact Extraction Priority Chain (`scraper/website.py`)
1. Homepage HTML
2. Discovered internal contact pages (dynamic link scoring, not hardcoded paths)
3. Fallback paths: /contact, /about, /contact-us
4. If still no email and social URLs were found: visit Facebook /about, then Instagram, then Twitter (Phase 3 social fallback)

`email_source` field records where the email came from: `"website"`, `"facebook"`, `"instagram"`, `"twitter"`, or `""`.

### No-Website Businesses
Businesses with no website URL from Google Places are the strongest leads. They get `has_website=False`, `website_quality_score=0`, `"No website"` in issues, and `is_high_priority()` returns `True` unconditionally.

### Concurrency Model
`scrape_all_websites()` launches one Chromium instance shared across all sites. Each site gets an isolated browser context (fresh cookies, random user agent). A semaphore limits concurrent workers (default 5, configurable via `SCRAPE_WORKERS` in `.env`).

### Email Campaigns (`emailer/`, `db/database.py`, Streamlit tab 2)
`get_leads_for_campaign(limit)` returns leads that have an email and have never been sent to (LEFT JOIN on campaigns table). `send_email()` sends via Namecheap SMTP, then `save_campaign_send()` logs the result. Daily limit enforced by counting today's `sent` rows in campaigns.

## Key Design Decisions to Preserve

- **No emoji in `print()` calls** — Windows cp1252 terminal crashes. Use `[TAG]` style labels.
- **`run_api.py` not `uvicorn` directly** — ProactorEventLoop fix is mandatory on Windows.
- **DB path is `output/leadharvest.db`** — inside project folder, satisfies IT security rules.
- **Cloudflare email decoding** in `extractor.py` — XOR-decodes `data-cfemail` hex attributes.
- **Master Excel** (`output/exports/leadharvest_master.xlsx`) grows across runs; per-run files are separate timestamped exports. Dedup in master uses place_id first, email second.
- **`@st.cache_data(ttl=300)`** on `_fetch_categories()` — reduces API calls from Streamlit.
- **Export bytes cached in session_state** — prevents re-fetching Excel on every filter interaction.

## Environment Variables (`.env`)

```
GOOGLE_PLACES_API_KEY=...
SCRAPE_DELAY_MIN=2
SCRAPE_DELAY_MAX=4
MAX_RESULTS_PER_RUN=50
TIMEOUT_PER_SITE=15
SCRAPE_WORKERS=5
EXPORT_PATH=output/exports/
LOG_PATH=output/logs/
SMTP_HOST=mail.privateemail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASSWORD=...
SENDER_NAME=Bosun
EMAIL_DAILY_LIMIT=50
EMAIL_DELAY_SECONDS=3
```

## IT Security Rules (Bank Laptop)

- Only write files inside `C:\stream\leadharvester\`
- Only install packages into the project venv (`venv/Scripts/pip3.exe`)
- No temp directories, no system file modifications, no registry changes
- No base64-encoded or obfuscated commands
- Playwright launching Chromium is expected and allowed
