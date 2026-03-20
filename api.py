"""
LeadHarvest — FastAPI Layer (Phase 2)

Wraps the Phase 1 scraper in HTTP endpoints so a frontend (Streamlit or Next.js)
can trigger scrape jobs and retrieve results asynchronously.

All Phase 1 modules are synchronous (httpx sync client, sync_playwright), so
blocking calls are offloaded to threads via asyncio.to_thread().

Run the server:
    venv/Scripts/python.exe -m uvicorn api:app --reload

Interactive API docs (Swagger UI):
    http://127.0.0.1:8000/docs
"""

import asyncio
import os
import uuid
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

from db.database import filter_new_businesses, get_existing_website_urls, save_businesses
from main import export_to_excel, update_master_excel
from models.business import Business
from scraper.places import CATEGORY_MAP, search_businesses
from scraper.website import scrape_all_websites
from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)


# ── App setup ──────────────────────────────────────────────────────────────────

app = FastAPI(
    title="LeadHarvest API",
    description=(
        "Business contact scraper for Nigerian SMBs. "
        "Searches Google Places and visits business websites to extract "
        "email, phone, WhatsApp, and social links."
    ),
    version="2.0.0",
)


# ── Request / Response schemas ─────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    """Request body for POST /scrape."""
    category: str
    city: str
    country: str = "Nigeria"


# ── In-memory job store ────────────────────────────────────────────────────────
# Each entry has the shape:
# {
#   "job_id": str,
#   "status": "pending" | "running" | "completed" | "failed",
#   "category": str,
#   "city": str,
#   "progress": {"current": int, "total": int, "stage": str},
#   "results": list[dict] | None,
#   "summary": dict | None,
#   "export_path": str | None,
#   "error": str | None,
#   "created_at": str (ISO-8601),
# }
jobs: dict[str, dict] = {}


# ── Background scrape task ─────────────────────────────────────────────────────

async def _run_scrape_job(job_id: str, category: str, city: str, country: str = "Nigeria") -> None:
    """
    Full scrape pipeline for one job, runs in the background.

    Stages:
        1. searching_places  — query Google Places API
        2. scraping_websites — visit each business website one by one
        3. exporting         — write Excel file
        4. done              — job complete
    """
    job = jobs[job_id]

    try:
        # ── Stage 1: Google Places search ─────────────────────────────────────
        job["status"] = "running"
        job["progress"]["stage"] = "searching_places"
        logger.info("Job %s | searching_places | %s in %s", job_id, category, city)

        known_urls: set[str] = await asyncio.to_thread(get_existing_website_urls)
        businesses: list[Business] = await asyncio.to_thread(
            search_businesses, category, city, known_urls, country
        )

        if not businesses:
            # No results — mark completed immediately with empty data
            job["status"] = "completed"
            job["progress"]["stage"] = "done"
            job["results"] = []
            job["summary"] = {
                "total": 0,
                "with_email": 0,
                "with_whatsapp": 0,
                "with_website": 0,
                "high_priority": 0,
                "avg_quality_score": 0.0,
                "message": "No businesses found for this category and city.",
            }
            logger.info("Job %s | No results found, marked completed.", job_id)
            return

        # ── Stage 2: Scrape each website ───────────────────────────────────────
        # scrape_all_websites launches Chromium once and reuses it for all sites,
        # avoiding the 1-2s browser startup cost that would apply per business.
        # The on_progress callback updates the job dict from inside the thread.
        total = len(businesses)
        job["progress"]["total"] = total
        job["progress"]["stage"] = "scraping_websites"
        logger.info("Job %s | scraping_websites | %d businesses", job_id, total)

        def _on_progress(current: int, total_count: int) -> None:
            job["progress"]["current"] = current

        await scrape_all_websites(businesses, _on_progress)

        # ── Stage 3: Dedup against DB ──────────────────────────────────────────
        job["progress"]["stage"] = "deduplicating"
        logger.info("Job %s | deduplicating against DB", job_id)

        new_businesses, skipped_count = await asyncio.to_thread(
            filter_new_businesses, businesses
        )
        await asyncio.to_thread(save_businesses, new_businesses)

        logger.info(
            "Job %s | dedup complete | new=%d | skipped=%d",
            job_id, len(new_businesses), skipped_count,
        )

        # ── Stage 4: Export to Excel ───────────────────────────────────────────
        job["progress"]["stage"] = "exporting"
        logger.info("Job %s | exporting", job_id)

        export_path: str = await asyncio.to_thread(
            export_to_excel, new_businesses, category, city
        )
        await asyncio.to_thread(update_master_excel, new_businesses)

        # ── Stage 5: Store final results ───────────────────────────────────────
        new_total = len(new_businesses)
        with_email = sum(1 for b in new_businesses if b.email)
        with_whatsapp = sum(1 for b in new_businesses if b.whatsapp)
        with_website = sum(1 for b in new_businesses if b.website_url)
        high_priority = sum(1 for b in new_businesses if b.is_high_priority())
        avg_score = (
            sum(b.website_quality_score for b in new_businesses) / new_total
            if new_total > 0 else 0.0
        )

        job["status"] = "completed"
        job["progress"]["stage"] = "done"
        job["export_path"] = export_path
        job["results"] = [b.to_dict() for b in new_businesses]
        job["summary"] = {
            "total_scraped": total,
            "already_in_db": skipped_count,
            "new_leads": new_total,
            "with_email": with_email,
            "with_whatsapp": with_whatsapp,
            "with_website": with_website,
            "high_priority": high_priority,
            "avg_quality_score": round(avg_score, 1),
        }

        logger.info(
            "Job %s | completed | total=%d | export=%s",
            job_id, total, export_path,
        )

    except (ValueError, RuntimeError) as exc:
        # Known errors: invalid category, missing API key, quota exceeded
        logger.error("Job %s | failed | %s", job_id, exc)
        job["status"] = "failed"
        job["error"] = str(exc)

    except Exception as exc:
        # Catch-all for unexpected errors so the job always gets a final status
        logger.error("Job %s | unexpected failure | %s", job_id, exc, exc_info=True)
        job["status"] = "failed"
        job["error"] = f"Unexpected error: {exc}"


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    """API info and available endpoints."""
    return {
        "name": "LeadHarvest API",
        "version": "2.0.0",
        "description": "Business contact scraper for Nigerian SMBs",
        "endpoints": {
            "GET  /":                 "API info (this page)",
            "GET  /categories":       "List valid business categories",
            "POST /scrape":           "Start a new scrape job (returns job_id, HTTP 202)",
            "GET  /results/{job_id}": "Poll job status and results",
            "GET  /export/{job_id}":  "Download the Excel file for a completed job",
        },
    }


@app.get("/categories")
async def get_categories():
    """Returns all valid business category names."""
    return {
        "categories": list(CATEGORY_MAP.keys()),
        "count": len(CATEGORY_MAP),
    }


@app.post("/scrape", status_code=202)
async def start_scrape(payload: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Starts a new scrape job and returns a job_id immediately (HTTP 202).

    Request body:
        { "category": "Law Firms", "city": "Lagos" }

    Poll GET /results/{job_id} to check progress and retrieve results.
    """
    category = payload.category.strip()
    city = payload.city.strip()
    country = payload.country.strip() or "Nigeria"

    # Validate category — freeform input is allowed; presets get mapped to
    # optimised keywords inside search_businesses(), anything else is used as-is
    if not category:
        return JSONResponse(
            status_code=400,
            content={"error": "The 'category' field must not be empty."},
        )

    # Validate city
    if not city:
        return JSONResponse(
            status_code=400,
            content={"error": "The 'city' field must not be empty."},
        )

    # Create job entry
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "category": category,
        "city": city,
        "progress": {"current": 0, "total": 0, "stage": "pending"},
        "results": None,
        "summary": None,
        "export_path": None,
        "error": None,
        "created_at": datetime.now().isoformat(),
    }

    # Kick off the background scrape
    background_tasks.add_task(_run_scrape_job, job_id, category, city, country)

    logger.info("Job %s created | category=%s | city=%s", job_id, category, city)

    return {
        "job_id": job_id,
        "status": "pending",
        "message": (
            f"Scrape job started for '{category}' in '{city}'. "
            f"Poll GET /results/{job_id} for status and results."
        ),
    }


@app.get("/results/{job_id}")
async def get_results(job_id: str):
    """
    Returns the current status and (when complete) the results of a scrape job.

    Poll this endpoint until status is 'completed' or 'failed'.
    Results are included only when status == 'completed' to keep
    in-progress payloads small.
    """
    job = jobs.get(job_id)

    if job is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"Job '{job_id}' not found."},
        )

    # Always include the status/progress fields
    response: dict = {
        "job_id": job_id,
        "status": job["status"],
        "category": job["category"],
        "city": job["city"],
        "created_at": job["created_at"],
        "progress": job["progress"],
    }

    if job["status"] == "completed":
        response["summary"] = job["summary"]
        response["results"] = job["results"]

    if job["status"] == "failed":
        response["error"] = job["error"]

    return response


@app.get("/export/{job_id}")
async def download_export(job_id: str):
    """
    Downloads the Excel file for a completed scrape job.

    Returns HTTP 400 if the job is not yet complete.
    Returns HTTP 404 if the job does not exist.
    """
    job = jobs.get(job_id)

    if job is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"Job '{job_id}' not found."},
        )

    if job["status"] != "completed":
        return JSONResponse(
            status_code=400,
            content={
                "error": "Export is not ready yet.",
                "current_status": job["status"],
                "message": (
                    f"Job is currently '{job['status']}'. "
                    "Try again once status is 'completed'."
                ),
            },
        )

    export_path = job.get("export_path")
    if not export_path or not Path(export_path).exists():
        return JSONResponse(
            status_code=500,
            content={"error": "Export file not found on disk."},
        )

    filename = Path(export_path).name
    return FileResponse(
        path=export_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


# ── Email open tracking ────────────────────────────────────────────────────────

# 1x1 transparent GIF — returned for every tracking pixel request
_PIXEL = bytes([
    0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,0x80,0x00,0x00,
    0xFF,0xFF,0xFF,0x00,0x00,0x00,0x21,0xF9,0x04,0x00,0x00,0x00,0x00,
    0x00,0x2C,0x00,0x00,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,
    0x44,0x01,0x00,0x3B,
])


@app.get("/track/open/{lead_id}")
async def track_open(lead_id: str):
    """
    Called when a recipient opens an email containing the tracking pixel.
    Logs the open timestamp to the campaigns table, then returns a 1x1
    transparent GIF so the email client doesn't show a broken image.
    """
    from urllib.parse import unquote
    from db.database import log_email_open

    email = unquote(lead_id)
    await asyncio.to_thread(log_email_open, email)
    logger.info("Email opened by %s", email)

    return Response(content=_PIXEL, media_type="image/gif")
