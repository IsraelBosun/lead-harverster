"""
enricher/enricher.py

Orchestrates the decision-maker enrichment pipeline for businesses
already in the DB.

Pipeline per business:
  1. Fetch unenriched businesses from DB (limit configurable).
  2. Run team_scraper  => list of (person_name, title, source_page_url)
  3. Run email_pattern => list of candidate email dicts
  4. Save all candidates to DB with smtp_status='unknown'.
  5. Mark business as enriched.

Email verification is handled separately by verify_contacts.py,
which uses mails.so with domain caching and resume support.

Run this directly:
    venv/Scripts/python.exe -m enricher.enricher
Or call enrich_businesses() from Streamlit.
"""

from utils.logger import get_logger
from db.database import (
    get_unenriched_businesses,
    mark_business_enriched,
    mark_ssl_issue,
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


def enrich_businesses(limit: int = 10) -> dict:
    """
    Runs the enrichment pipeline on up to `limit` unenriched businesses.

    Returns a summary dict:
      businesses_processed, people_found, candidates_generated, saved
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
        place_id       = biz["place_id"]
        biz_name       = biz["business_name"]
        website_url    = biz["website_url"]
        existing_email = biz["email"]

        logger.info("[ENRICH] Processing: %s (%s)", biz_name, website_url)

        # Step 1 — scrape team page
        result = scrape_team_page(website_url)

        # If the homepage didn't load at all, permanently skip it.
        if result is LOAD_FAILED:
            logger.info("[ENRICH] Load failed for %s — permanently skipping", biz_name)
            mark_business_enriched(place_id)
            continue

        people, gemini_emails, ssl_issue = result

        if ssl_issue:
            mark_ssl_issue(place_id)
            logger.info("[ENRICH] SSL issue flagged for %s", biz_name)

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

        # Step 4 — attach business metadata and save with smtp_status='unknown'
        # Verification is handled separately by verify_contacts.py
        domain = _extract_domain(website_url)
        to_save = []
        for c in candidates:
            to_save.append({
                "place_id":        place_id,
                "business_name":   biz_name,
                "domain":          domain,
                "person_name":     c["person_name"],
                "title":           c.get("title", ""),
                "candidate_email": c["candidate_email"],
                "pattern_used":    c.get("pattern_used", ""),
                "smtp_status":     "unknown",
                "source_page_url": c.get("source_page_url", ""),
            })

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
