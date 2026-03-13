# LeadHarvest

A fully automated Nigerian SMB lead generation and cold outreach tool. Discovers businesses via Google Places, scrapes their websites for contact details, scores lead quality, and sends personalised cold emails — all from a clean browser-based UI.

---

## What It Does

LeadHarvest runs a three-phase pipeline:

1. **Discover** — Searches Google Places for businesses by category and city (e.g. law firms in Lagos)
2. **Scrape** — Visits each business website with a headless browser to extract emails, phone numbers, WhatsApp, and social links
3. **Outreach** — Sends templated cold emails via SMTP and tracks every send in a database

All three phases are complete and production-ready.

---

## Features

### Lead Discovery
- Google Places Text Search with automatic pagination
- Extracts business name, address, phone, website, Google rating, and category
- Businesses with no website are flagged as highest-priority leads automatically

### Contact Extraction
Smart extraction chain per business:
1. Homepage HTML
2. Dynamically discovered contact pages
3. Fallback paths (`/contact`, `/about`, `/contact-us`)
4. Social media fallback — visits Facebook, Instagram, and Twitter if no email found on the website
5. Cloudflare email decoding (XOR-decodes `data-cfemail` attributes)

### Lead Scoring
Each website is scored 0–100. **Lower score = weaker web presence = stronger lead.** Scored on SSL, mobile responsiveness, contact visibility, social presence, and more.

### Database & Deduplication
- SQLite database at `output/leadharvest.db`
- Email is the primary key; Place ID is a secondary unique constraint
- Deduplication runs at two points: before scraping (skip known URLs) and after (separate new from existing)
- No lead is ever scraped or contacted twice

### Excel Exports
- Per-run timestamped exports saved to `output/exports/`
- Persistent master file (`leadharvest_master.xlsx`) grows across all runs
- All exports include Campaign Status and Date Sent columns, updated after each send
- Styled with blue headers, frozen panes, and auto-fitted columns
- Master file can be rebuilt at any time from the database via the Streamlit UI

### Email Campaigns
- Streamlit UI for selecting and sending to unsent leads
- Personalised cold outreach template with firm name substituted automatically
- Daily send limit enforced (default 50)
- Every send logged to the campaigns table with timestamp and status
- BCC on every send so you have a copy in your inbox

---

## Tech Stack

| Layer | Technology |
|---|---|
| Scraping | Playwright (Chromium), BeautifulSoup4 |
| Search | Google Places API |
| Backend API | FastAPI |
| Frontend | Streamlit |
| Database | SQLite |
| Spreadsheets | Pandas, openpyxl |
| Email | SMTP (Namecheap Private Email) |
| HTTP | httpx |
| Runtime | Python 3.13 |

---

## Project Structure

```
leadharvester/
├── run_api.py              # Start FastAPI backend (use this, not uvicorn directly)
├── main.py                 # CLI entry point + Excel export logic
├── api.py                  # FastAPI app — async scrape jobs, REST endpoints
├── streamlit_app.py        # Browser UI — scrape, view results, send campaigns
├── scraper/
│   ├── places.py           # Google Places search and pagination
│   ├── website.py          # Playwright browser scraper + social fallback
│   ├── extractor.py        # Regex extraction: email, phone, WhatsApp, social
│   └── scorer.py           # Website quality scoring (0–100)
├── models/
│   └── business.py         # Business dataclass with is_high_priority() and to_dict()
├── emailer/
│   ├── sender.py           # SMTP send via Namecheap
│   └── templates.py        # Cold outreach email template
├── db/
│   └── database.py         # SQLite init, dedup, save, campaign tracking
├── utils/
│   ├── logger.py           # File + console logging
│   └── helpers.py          # Delays, user-agent rotation, URL normalisation
└── output/
    ├── exports/            # Excel files saved here
    └── logs/               # Log files saved here
```

---

## Setup

### 1. Install dependencies
```bash
venv/Scripts/pip3.exe install -r requirements.txt
venv/Scripts/python.exe -m playwright install chromium
```

### 2. Configure environment
Copy `.env` and fill in your values:
```
GOOGLE_PLACES_API_KEY=your_key_here

SMTP_HOST=smtp.privateemail.com
SMTP_PORT=587
SMTP_USER=you@yourdomain.com
SMTP_PASSWORD=your_password
SENDER_NAME=Your Name
EMAIL_DAILY_LIMIT=50
EMAIL_DELAY_SECONDS=30

SCRAPE_WORKERS=5
SCRAPE_DELAY_MIN=2
SCRAPE_DELAY_MAX=4
MAX_RESULTS_PER_RUN=50
TIMEOUT_PER_SITE=15
EXPORT_PATH=output/exports/
LOG_PATH=output/logs/
```

---

## Running the App

**Step 1 — Start the API backend (required for the Streamlit UI):**
```bash
venv/Scripts/python.exe run_api.py
```
> Do not use `uvicorn` directly on Windows. `run_api.py` sets `WindowsProactorEventLoopPolicy` before starting, which is required for Playwright subprocess spawning.

**Step 2 — Start the Streamlit frontend (separate terminal):**
```bash
venv/Scripts/python.exe -m streamlit run streamlit_app.py
```

**CLI mode (no API needed):**
```bash
venv/Scripts/python.exe main.py
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/categories` | List available business categories |
| POST | `/scrape` | Start a scrape job, returns job_id |
| GET | `/results/{job_id}` | Poll job status and results |
| GET | `/export/{job_id}` | Download Excel export for a completed job |

Interactive docs available at `http://127.0.0.1:8000/docs`

---

## Important Notes

- **No emoji in `print()` calls** — Windows cp1252 terminal will crash. All output uses `[TAG]` style labels.
- **Database path is `output/leadharvest.db`** — all writes stay inside the project folder.
- **All files written inside `C:\stream\leadharvester\`** — no temp directories, no system modifications.
- **Master Excel** can always be rebuilt from the database using the button in the Campaigns tab of the Streamlit UI.

---

## Email Deliverability Tips

- Ensure your sending domain has SPF, DKIM, and DMARC records configured
- Start with 20–25 sends per day on a fresh domain and ramp up gradually
- Use `EMAIL_DELAY_SECONDS=30` minimum between sends
- Best sending windows: Tuesday–Thursday, 8–10am or 1–2pm (recipient's local time)
