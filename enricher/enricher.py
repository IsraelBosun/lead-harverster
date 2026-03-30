"""
enricher/enricher.py

Orchestrates the decision-maker enrichment pipeline for businesses
already in the DB.

Pipeline per business:
  1. Fetch unenriched businesses from DB (limit configurable).
  2. Run team_scraper  => list of (person_name, title, source_page_url)
  3. Run email_pattern => list of candidate email dicts
  4. Run smtp_verifier => smtp_status added to each candidate
  5. Keep only: verified, catch_all, and the best unknown per person
     (to avoid flooding the DB with all-rejected rows).
  6. Save contacts to DB.
  7. Mark business as enriched.

Run this directly:
    venv/Scripts/python.exe -m enricher.enricher
Or call enrich_businesses() from Streamlit.
"""

from utils.logger import get_logger
from db.database import (
    get_unenriched_businesses,
    mark_business_enriched,
    save_contacts,
)
from enricher.team_scraper import scrape_team_page, LOAD_FAILED
from enricher.email_pattern import generate_candidates_for_people
from urllib.parse import urlparse

logger = get_logger(__name__)


def _extract_domain(website_url: str) -> str:
    parsed = urlparse(website_url)
    domain = parsed.netloc or parsed.path
    return domain.lstrip("www.").lower()


def _best_candidates(verified_candidates: list[dict]) -> list[dict]:
    """
    From the full verified candidate list, keep the most useful rows:
      - All 'verified' rows (confirmed to exist).
      - All 'catch_all' rows (can't confirm but worth keeping).
      - For 'unknown'/'error': keep at most one per person as a fallback,
        only if they have no verified/catch_all address.

    This prevents cluttering the DB with dozens of rejected addresses.
    """
    # Group by person_name
    by_person: dict[str, list[dict]] = {}
    for c in verified_candidates:
        key = c["person_name"].lower()
        by_person.setdefault(key, []).append(c)

    kept = []
    for person_name, group in by_person.items():
        verified   = [c for c in group if c["smtp_status"] == "verified"]
        catch_all  = [c for c in group if c["smtp_status"] == "catch_all"]
        unverified = [c for c in group if c["smtp_status"] == "unverified"]
        unknown    = [c for c in group if c["smtp_status"] == "unknown"]

        if verified:
            kept.extend(verified)
        elif catch_all:
            kept.extend(catch_all)
        elif unverified:
            kept.extend(unverified)  # port 25 blocked — keep all candidates
        elif unknown:
            kept.append(unknown[0])
        # 'rejected' and 'error' rows are intentionally dropped

    return kept


def enrich_businesses(limit: int = 10) -> dict:
    """
    Runs the enrichment pipeline on up to `limit` unenriched businesses.

    Returns a summary dict:
      businesses_processed, people_found, candidates_generated,
      verified, catch_all, saved
    """
    businesses = get_unenriched_businesses(limit=limit, country="Nigeria")
    logger.info("[ENRICH] Starting enrichment for %d businesses", len(businesses))

    summary = {
        "businesses_processed": 0,
        "people_found":         0,
        "candidates_generated": 0,
        "saved":                0,
    }

    for biz in businesses:
        place_id     = biz["place_id"]
        biz_name     = biz["business_name"]
        website_url  = biz["website_url"]
        existing_email = biz["email"]

        logger.info("[ENRICH] Processing: %s (%s)", biz_name, website_url)

        # Step 1 — scrape team page
        result = scrape_team_page(website_url)

        # If the homepage didn't load at all, skip without marking enriched
        # so the business can be retried on the next run
        if result is LOAD_FAILED:
            logger.info("[ENRICH] Load failed for %s — will retry next run", biz_name)
            continue

        people, gemini_emails = result

        summary["people_found"] += len(people)

        if not people:
            logger.info("[ENRICH] No decision-makers found for %s — marking enriched", biz_name)
            mark_business_enriched(place_id)
            summary["businesses_processed"] += 1
            continue

        # Step 2 — generate candidate emails
        candidates = generate_candidates_for_people(people, website_url, existing_email, gemini_emails)
        summary["candidates_generated"] += len(candidates)

        if not candidates:
            mark_business_enriched(place_id)
            summary["businesses_processed"] += 1
            continue

        # Step 3 — filter out noise: titles longer than 80 chars are paragraphs not roles
        candidates = [c for c in candidates if len(c.get("title", "")) <= 80]

        to_save = candidates

        # Attach business metadata to each contact row
        domain = _extract_domain(website_url)
        for c in to_save:
            c["place_id"]      = place_id
            c["business_name"] = biz_name
            c["domain"]        = domain

        # Set status for all saved candidates
        for c in to_save:
            c["smtp_status"] = "unverified"

        # Step 5 — persist
        saved = save_contacts(to_save)
        summary["saved"] += saved

        mark_business_enriched(place_id)
        summary["businesses_processed"] += 1

        logger.info(
            "[ENRICH] %s done | people=%d | saved=%d",
            biz_name, len(people), saved,
        )

    logger.info("[ENRICH] Complete: %s", summary)
    return summary


if __name__ == "__main__":
    result = enrich_businesses(limit=250)
    print("[ENRICH] Summary:")
    for k, v in result.items():
        print(f"  {k}: {v}")
