"""
Google Places API integration for LeadHarvest.

Uses two endpoints:
  1. Text Search — find businesses by type and city
  2. Place Details — get additional info (e.g. website) when missing from Text Search

Docs: https://developers.google.com/maps/documentation/places/web-service
"""

import os
from typing import Optional

import httpx
from dotenv import load_dotenv

from models.business import Business
from utils.helpers import normalise_url_for_dedup
from utils.logger import get_logger
from utils.timezone_utils import get_timezone

load_dotenv()

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
MAX_RESULTS = int(os.getenv("MAX_RESULTS_PER_RUN", "50"))

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Fields to request from Place Details (each field costs API quota)
DETAIL_FIELDS = "name,formatted_address,formatted_phone_number,website,rating,business_status,place_id"

# Human-readable category → Google Places search keyword
CATEGORY_MAP = {
    "Law Firms": "law firm",
    "Photography Studios": "photography studio",
    "Event Planning Companies": "event planning",
    "Real Estate Agencies": "real estate agency",
    "Hotels and Hospitality": "hotel",
    "Restaurants and Food Businesses": "restaurant",
    "Medical and Dental Clinics": "clinic",
    "Hair and Beauty Salons": "beauty salon",
    "Logistics and Courier Companies": "logistics company",
    "Churches and Religious Organisations": "church",
    "Schools and Educational Centres": "school",
    "Fashion and Clothing Brands": "fashion store",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_api_key() -> None:
    """Raises a clear error if the API key is missing or still a placeholder."""
    if not API_KEY or API_KEY == "your_google_places_api_key_here":
        raise ValueError(
            "Google Places API key is missing. "
            "Set GOOGLE_PLACES_API_KEY in your .env file."
        )


def _get_place_details(client: httpx.Client, place_id: str) -> dict:
    """
    Calls the Place Details endpoint for a single place_id.
    Used only when critical fields (e.g. website) are missing from Text Search.

    Args:
        client: Shared httpx client.
        place_id: The Google Maps place_id string.

    Returns:
        The 'result' dict from the API response, or empty dict on failure.
    """
    params = {
        "place_id": place_id,
        "fields": DETAIL_FIELDS,
        "key": API_KEY,
    }
    try:
        response = client.get(PLACE_DETAILS_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        status = data.get("status", "UNKNOWN")
        if status != "OK":
            logger.warning("Place Details returned status %s for place_id %s", status, place_id)
            return {}

        return data.get("result", {})

    except httpx.RequestError as exc:
        logger.error("Network error fetching Place Details for %s: %s", place_id, exc)
        return {}
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP error fetching Place Details for %s: %s", place_id, exc)
        return {}


def _parse_place(raw: dict, category: str, city: str, country: str = "Nigeria") -> Business:
    """
    Converts a raw Google Places result dict into a Business object.

    Args:
        raw:      Single result dict from the API response.
        category: Human-readable category label (e.g. "Law Firms").
        city:     City the search was run for.
        country:  Country the search was run for (used for timezone tagging).

    Returns:
        Partially populated Business (no website scraping yet).
    """
    return Business(
        business_name=raw.get("name", ""),
        category=category,
        city=city,
        country=country,
        timezone=get_timezone(country),
        address=raw.get("formatted_address", raw.get("vicinity", "")),
        phone=raw.get("formatted_phone_number", raw.get("phone", "")),
        website_url=raw.get("website", ""),
        google_rating=raw.get("rating"),
        place_id=raw.get("place_id", ""),
    )


# ── Main public function ───────────────────────────────────────────────────────

def search_businesses(
    category: str,
    city: str,
    known_urls: set[str] | None = None,
    country: str = "Nigeria",
) -> list[Business]:
    """
    Searches Google Places for businesses matching the given category and city.

    Strategy:
      - Run a Text Search for "{keyword} in {city}, Nigeria"
      - Skip any business whose website URL is already in known_urls (already
        scraped with a result in the DB) — so we keep fetching until we have
        MAX_RESULTS genuinely new businesses or exhaust all Google results
      - If a business has no website URL, fetch Place Details to try to get one
      - Return a list of Business objects (contacts not yet scraped)

    Args:
        category:   A preset category key from CATEGORY_MAP (e.g. "Law Firms"),
                    or any freeform keyword (e.g. "pharmacy", "car dealer").
        city:       Nigerian city name, e.g. "Lagos"
        known_urls: Set of normalised website URLs already in the DB.
                    Businesses matching these are skipped so fresh results
                    fill the quota instead. Pass None to skip the check.

    Returns:
        List of Business objects populated with Places API data.

    Raises:
        ValueError: If the API key is not configured.
        RuntimeError: If the API returns a quota or auth error.
    """
    _check_api_key()

    known_urls = known_urls or set()

    keyword = CATEGORY_MAP.get(category, category.strip())
    query = f"{keyword} in {city}, {country}"

    print(f"\n[SEARCH] Searching Google Places for: \"{query}\"")
    if known_urls:
        print(f"  [DB] Will skip businesses already in DB ({len(known_urls)} known URLs)")
    logger.info("Starting Places search | query='%s' | known_urls=%d", query, len(known_urls))

    businesses: list[Business] = []
    skipped_known: int = 0
    next_page_token: Optional[str] = None
    page_number = 0

    with httpx.Client() as client:
        while len(businesses) < MAX_RESULTS:
            page_number += 1
            params: dict = {"query": query, "key": API_KEY}
            if next_page_token:
                params["pagetoken"] = next_page_token

            print(f"  [PAGE] Fetching page {page_number} of results...")
            logger.debug("Text Search | query=%s | pagetoken=%s", query, next_page_token)

            try:
                response = client.get(TEXT_SEARCH_URL, params=params, timeout=15)
                response.raise_for_status()
            except httpx.RequestError as exc:
                logger.error("Network error during Text Search: %s", exc)
                print(f"  [ERROR] Network error: {exc}")
                break
            except httpx.HTTPStatusError as exc:
                logger.error("HTTP error during Text Search: %s", exc)
                print(f"  [ERROR] HTTP error: {exc}")
                break

            data = response.json()
            status = data.get("status", "UNKNOWN")

            if status == "REQUEST_DENIED":
                msg = data.get("error_message", "No error message provided.")
                logger.error("API key rejected: %s", msg)
                raise RuntimeError(f"Google Places API key error: {msg}")

            if status == "OVER_QUERY_LIMIT":
                logger.error("Google Places API quota exceeded.")
                print("  [ERROR] Google Places quota exceeded. Try again later.")
                break

            if status == "ZERO_RESULTS":
                print(f"  [INFO] No results found for '{query}'.")
                logger.info("Zero results for query='%s'", query)
                break

            if status not in ("OK", "ZERO_RESULTS"):
                logger.warning("Unexpected API status '%s' for query='%s'", status, query)
                break

            results = data.get("results", [])
            print(f"  [OK] Got {len(results)} results on page {page_number}")

            for raw in results:
                if len(businesses) >= MAX_RESULTS:
                    break

                business = _parse_place(raw, category, city, country)

                # If no website from Text Search, call Place Details to get one
                if not business.website_url and business.place_id:
                    logger.debug(
                        "No website in Text Search for '%s', fetching Place Details...",
                        business.business_name,
                    )
                    details = _get_place_details(client, business.place_id)
                    if details:
                        business.website_url = details.get("website", "")
                        if not business.phone:
                            business.phone = details.get("formatted_phone_number", "")
                        if not business.address:
                            business.address = details.get("formatted_address", "")

                # Skip if this website URL is already in the DB with an email
                if business.website_url and known_urls:
                    norm = normalise_url_for_dedup(business.website_url)
                    if norm in known_urls:
                        skipped_known += 1
                        print(f"  [DB SKIP] Already in DB: {business.business_name}")
                        logger.debug("Skipping known URL: %s", business.website_url)
                        continue

                businesses.append(business)

            # Check for more pages
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

            # Google requires a short delay before the next page token is valid
            import time
            time.sleep(2)

    if skipped_known:
        print(f"  [DB] Skipped {skipped_known} businesses already in DB")

    print(f"\n[DONE] Found {len(businesses)} new businesses to scrape.\n")
    logger.info(
        "Places search complete | found=%d | skipped_known=%d | category=%s | city=%s",
        len(businesses), skipped_known, category, city,
    )

    return businesses
