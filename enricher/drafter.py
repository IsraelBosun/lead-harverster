"""
enricher/drafter.py

Generates personalised cold email drafts for each contact using Gemini.

For each contact without a draft:
  1. Fetch the business homepage with httpx (fast, no browser needed).
  2. Strip HTML to plain text for context.
  3. Ask Gemini to write a personalised email based on the person's role
     and what the site says about the business.
  4. Save the draft to the DB with status 'pending'.

Run directly:
    venv/Scripts/python.exe -m enricher.drafter
"""

import httpx
from bs4 import BeautifulSoup

from db.database import get_contacts_without_drafts, save_drafts, get_draft_stats
from enricher.gemini_extractor import draft_email_with_gemini
from utils.logger import get_logger

logger = get_logger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _fetch_page_text(url: str) -> str:
    """
    Fetches a URL with httpx and returns clean plain text.
    Returns empty string on any error.
    """
    try:
        resp = httpx.get(url, headers=_HEADERS, timeout=15, follow_redirects=True)
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "meta", "head"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
    except Exception as exc:
        logger.warning("[DRAFT] Could not fetch %s: %s", url, exc)
        return ""


def generate_drafts(limit: int = 50) -> dict:
    """
    Generates email drafts for up to `limit` contacts that don't yet have one.

    Returns a summary dict: contacts_processed, drafts_saved, skipped.
    """
    contacts = get_contacts_without_drafts()
    if limit:
        contacts = contacts[:limit]

    logger.info("[DRAFT] Generating drafts for %d contacts", len(contacts))

    summary = {"contacts_processed": 0, "drafts_saved": 0, "skipped": 0}

    # Cache page text per website so we don't re-fetch for every person
    page_cache: dict[str, str] = {}
    drafts_to_save = []

    for contact in contacts:
        person_name   = contact["person_name"]
        title         = contact["title"]
        business_name = contact["business_name"]
        website_url   = contact.get("website_url") or ""
        place_id      = contact["place_id"]

        summary["contacts_processed"] += 1

        # Fetch page text (cached per site)
        if website_url not in page_cache:
            page_cache[website_url] = _fetch_page_text(website_url) if website_url else ""
        page_text = page_cache[website_url]

        draft = draft_email_with_gemini(person_name, title, business_name, page_text)

        if not draft:
            logger.warning("[DRAFT] No draft generated for %s at %s", person_name, business_name)
            summary["skipped"] += 1
            continue

        drafts_to_save.append({
            "place_id":         place_id,
            "business_name":    business_name,
            "website_url":      website_url,
            "person_name":      person_name,
            "title":            title,
            "candidate_emails": contact["candidate_emails"],
            "subject":          draft["subject"],
            "body":             draft["body"],
        })

    saved = save_drafts(drafts_to_save)
    summary["drafts_saved"] = saved

    logger.info("[DRAFT] Complete: %s", summary)
    return summary


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = generate_drafts(limit=50)
    print("[DRAFT] Summary:")
    for k, v in result.items():
        print(f"  {k}: {v}")
