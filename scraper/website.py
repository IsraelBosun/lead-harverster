"""
Website scraping module for LeadHarvest.

Uses Playwright headless Chromium to visit business websites and extract:
  - Contact information (via extractor.py)
  - Page metadata needed for quality scoring (via scorer.py)

Visits the homepage first, then discovers internal contact-relevant pages
dynamically by crawling the site's own navigation links. Results from all
pages are merged so no data is lost.
"""

import asyncio
import os
import random
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PWTimeout
from playwright.async_api import async_playwright
from playwright.async_api import Page as AsyncPage
from playwright.async_api import Browser as AsyncBrowser
from playwright.async_api import TimeoutError as AsyncPWTimeout

from scraper.extractor import extract_contacts, extract_emails, merge_contacts
from bs4 import BeautifulSoup
from scraper.scorer import score_website
from models.business import Business
from utils.helpers import get_random_user_agent, normalise_url, random_delay
from utils.logger import get_logger

logger = get_logger(__name__)

TIMEOUT_MS = int(os.getenv("TIMEOUT_PER_SITE", "10")) * 1000  # convert to ms
DELAY_MIN = float(os.getenv("SCRAPE_DELAY_MIN", "2"))
DELAY_MAX = float(os.getenv("SCRAPE_DELAY_MAX", "4"))
MAX_CONCURRENT = int(os.getenv("SCRAPE_WORKERS", "5"))

# Keywords that suggest a link likely leads to a contact-relevant page.
# Checked against both the URL path and the visible link text.
# Keep these specific enough to avoid substring false positives —
# e.g. avoid "us" (matches "business", "customer"), "in" (matches everything).
CONTACT_KEYWORDS = {
    "contact", "about", "team", "reach", "info", "staff", "people",
    "enquir", "hello", "touch", "location", "address", "hire",
    "connect", "support", "find-us", "about-us", "contact-us",
    "get-in-touch", "our-team", "who-we-are", "meet-us",
}

# Used as a last resort if the homepage has no keyword-matching internal links
# (very sparse or unusual site structure).
FALLBACK_CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us"]

# Hard cap on how many internal pages we visit per site to prevent runaway crawling
MAX_CONTACT_PAGES = 10


# ── Internal helpers ───────────────────────────────────────────────────────────

def _find_contact_links(homepage_html: str, base_url: str) -> list[str]:
    """
    Crawls the homepage's own navigation to find internal pages that are
    likely to contain contact information — no hardcoded paths needed.

    Every internal link is scored:
      +2 if a contact keyword appears in the URL path
      +1 if a contact keyword appears in the visible link text

    Returns the top-scoring links (up to MAX_CONTACT_PAGES), sorted by score.
    Falls back to FALLBACK_CONTACT_PATHS if the homepage yields no matches
    (handles very sparse or unusual site structures).

    Args:
        homepage_html: Full HTML of the site's homepage.
        base_url: Root URL of the site (used to resolve relative links and
                  filter out external links).

    Returns:
        List of full URLs to visit, ordered by relevance.
    """
    soup = BeautifulSoup(homepage_html, "html.parser")
    base_domain = urlparse(base_url).netloc
    homepage_path = urlparse(base_url).path.rstrip("/") or "/"

    scored = []
    seen_paths = {homepage_path}  # never revisit the homepage itself

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()

        # Skip non-page links
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        # Internal links only — ignore external domains
        if parsed.netloc != base_domain:
            continue

        # Skip binary/media files
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in
               (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg",
                ".zip", ".doc", ".docx", ".xls", ".xlsx")):
            continue

        # Deduplicate by normalised path (ignore query strings and trailing slashes)
        norm_path = parsed.path.rstrip("/") or "/"
        if norm_path in seen_paths:
            continue
        seen_paths.add(norm_path)

        # Score: URL path keywords carry more weight than link text keywords
        link_text = a_tag.get_text(strip=True).lower()
        score = 0
        for kw in CONTACT_KEYWORDS:
            if kw in path_lower:
                score += 2
            if kw in link_text:
                score += 1

        if score > 0:
            scored.append((score, full_url))

    if not scored:
        # Nothing relevant found in navigation — fall back to common guesses
        logger.debug("No contact-relevant links found on %s, using fallback paths.", base_url)
        return [urljoin(base_url, p) for p in FALLBACK_CONTACT_PATHS]

    # Sort by score descending and cap at MAX_CONTACT_PAGES
    scored.sort(key=lambda x: x[0], reverse=True)
    links = [url for _, url in scored[:MAX_CONTACT_PAGES]]
    logger.debug("Found %d contact-relevant pages on %s: %s", len(links), base_url, links)
    return links


def _safe_goto(page: Page, url: str) -> Optional[str]:
    """
    Navigates to a URL and returns the page HTML on success.
    Returns None if the page times out, errors, or returns a non-200 status.

    After navigation we scroll to the bottom of the page and wait briefly
    so that JavaScript-rendered footer content (where emails often live)
    has time to appear before we capture the HTML.

    Args:
        page: Playwright Page object.
        url: Full URL to navigate to.

    Returns:
        HTML string, or None on failure.
    """
    try:
        response = page.goto(url, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
        if response and response.status >= 400:
            logger.debug("Page %s returned HTTP %d, skipping.", url, response.status)
            return None
        # Scroll to the bottom so lazy-loaded footer content is triggered,
        # then pause briefly for JS to finish rendering it.
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.5)
        except Exception:
            pass  # non-critical — still return whatever content we have
        return page.content()
    except PWTimeout:
        logger.warning("Timeout loading %s (limit: %dms)", url, TIMEOUT_MS)
        return None
    except Exception as exc:
        logger.warning("Error loading %s: %s", url, exc)
        return None


def _try_contact_pages(page: Page, base_url: str, homepage_html: str) -> dict:
    """
    Dynamically discovers contact-relevant pages from the site's own navigation
    and visits each one, merging all contact data found across them.

    Pages are found by scoring the homepage's internal links against
    CONTACT_KEYWORDS — no hardcoded paths needed. Falls back to common paths
    only if the homepage yields no keyword-matching links.

    Only stops early if a complete contact set (email + phone + whatsapp) is
    found, to avoid unnecessary page loads.

    Args:
        page: Playwright Page object.
        base_url: The root URL of the business site.
        homepage_html: Already-loaded homepage HTML (used for link discovery).

    Returns:
        Merged contact dict from all pages visited.
    """
    merged = {"email": "", "phone": "", "whatsapp": "", "instagram": "", "facebook": "", "twitter": ""}

    urls_to_visit = _find_contact_links(homepage_html, base_url)

    for url in urls_to_visit:
        html = _safe_goto(page, url)
        if not html:
            continue

        contacts = extract_contacts(html, source_url=url)
        merged = merge_contacts(merged, contacts)

        # Stop early once we have email + phone — that's enough to reach them.
        # Don't require WhatsApp too or the exit almost never fires.
        if merged.get("email") and merged.get("phone"):
            logger.debug("Email and phone found at %s, stopping early.", url)
            break

    return merged


# ── Main public functions ──────────────────────────────────────────────────────

def _scrape_with_browser(business: Business, browser: Browser) -> Business:
    """
    Core scraping logic using a pre-launched Browser instance.

    Creates a new browser context (isolated session) for this site, runs
    Steps 1-5, then closes the context. Does NOT close the browser — the
    caller is responsible for that.

    Args:
        business: Business object with website_url already populated.
        browser: An already-running Playwright Browser instance to reuse.

    Returns:
        The same Business object with contacts and score filled in.
    """
    url = normalise_url(business.website_url)

    if not url:
        logger.info("No website URL for '%s', skipping scrape.", business.business_name)
        return business

    print(f"  [SCRAPE] Visiting: {url}")
    logger.info("Scraping website for '%s' | url=%s", business.business_name, url)

    context = browser.new_context(
        user_agent=get_random_user_agent(),
        viewport={"width": 1280, "height": 800},
        java_script_enabled=True,
    )
    page = context.new_page()

    try:
        # ── Step 1: Load the homepage ─────────────────────────────────────────
        start_time = time.time()
        homepage_html = _safe_goto(page, url)
        load_time = time.time() - start_time

        if not homepage_html:
            # First attempt failed — retry with a different user agent
            logger.info("First attempt failed for %s, retrying with different user agent...", url)
            context.close()
            context = browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            start_time = time.time()
            homepage_html = _safe_goto(page, url)
            load_time = time.time() - start_time

        if not homepage_html:
            logger.warning("Could not load %s after retry, skipping.", url)
            print(f"  [SKIP] Could not load {url}")
            return business

        # ── Step 2: Extract contacts from homepage ────────────────────────────
        homepage_contacts = extract_contacts(homepage_html, source_url=url)

        # ── Step 3: Discover and visit contact-relevant pages, merge results ───
        # Links are found dynamically from the homepage's own navigation so
        # no paths are hardcoded. Results are merged with homepage contacts.
        extra_contacts = _try_contact_pages(page, url, homepage_html)
        final_contacts = merge_contacts(homepage_contacts, extra_contacts)

        # ── Step 4: Score the website ─────────────────────────────────────────
        score_data = score_website(
            html=homepage_html,
            url=url,
            load_time_seconds=load_time,
        )

        # ── Step 5: Write results back to the business object ─────────────────
        business.email = final_contacts.get("email", "")
        business.whatsapp = final_contacts.get("whatsapp", "")
        business.instagram = final_contacts.get("instagram", "")
        business.facebook = final_contacts.get("facebook", "")
        business.twitter = final_contacts.get("twitter", "")

        # Only overwrite phone if Places API didn't give us one
        if not business.phone:
            business.phone = final_contacts.get("phone", "")

        business.website_quality_score = score_data["score"]
        business.website_issues = score_data["issues"]

        print(
            f"  [OK] Score: {score_data['score']}/100 | "
            f"Email: {'Yes' if business.email else 'No'} | "
            f"Phone: {'Yes' if business.phone else 'No'} | "
            f"WhatsApp: {'Yes' if business.whatsapp else 'No'}"
        )

    except Exception as exc:
        logger.error("Unexpected error scraping %s: %s", url, exc, exc_info=True)
        print(f"  [ERROR] Unexpected error for {url}: {exc}")

    finally:
        context.close()  # close the context; browser stays open for reuse

    return business


def scrape_website(business: Business) -> Business:
    """
    Visits a single business website and fills in contact and scoring data.

    Launches its own Playwright browser for this one call. Use
    scrape_all_websites() when processing a list — it shares a single
    browser instance across all sites for much better performance.

    Args:
        business: Business object with website_url already populated.

    Returns:
        The same Business object with contacts and score filled in.
    """
    url = normalise_url(business.website_url)
    if not url:
        logger.info("No website URL for '%s', skipping scrape.", business.business_name)
        return business

    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(headless=True)
        try:
            return _scrape_with_browser(business, browser)
        finally:
            browser.close()


# ── Async helpers (used by the concurrent scrape_all_websites) ────────────────

async def _safe_goto_async(page: AsyncPage, url: str) -> Optional[str]:
    """
    Async equivalent of _safe_goto.
    Scrolls to bottom and waits briefly after navigation so JS-rendered
    footer content (where emails often live) has time to appear.
    """
    try:
        response = await page.goto(url, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
        if response and response.status >= 400:
            logger.debug("Page %s returned HTTP %d, skipping.", url, response.status)
            return None
        # Scroll to bottom so lazy-loaded footer content is triggered,
        # then pause briefly for JS to finish rendering it.
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.5)
        except Exception:
            pass  # non-critical — still return whatever content we have
        return await page.content()
    except AsyncPWTimeout:
        logger.warning("Timeout loading %s (limit: %dms)", url, TIMEOUT_MS)
        return None
    except Exception as exc:
        logger.warning("Error loading %s: %s", url, exc)
        return None


async def _try_contact_pages_async(page: AsyncPage, base_url: str, homepage_html: str) -> dict:
    """
    Async equivalent of _try_contact_pages.
    Uses _find_contact_links (pure HTML parsing, no I/O) to discover pages
    dynamically, then visits each and merges all results.
    Only stops early if a complete contact set (email + phone + whatsapp) is found.
    """
    merged = {"email": "", "phone": "", "whatsapp": "", "instagram": "", "facebook": "", "twitter": ""}

    urls_to_visit = _find_contact_links(homepage_html, base_url)

    for url in urls_to_visit:
        html = await _safe_goto_async(page, url)
        if not html:
            continue

        contacts = extract_contacts(html, source_url=url)
        merged = merge_contacts(merged, contacts)

        # Only stop early if we have the full critical set
        if merged.get("email") and merged.get("phone") and merged.get("whatsapp"):
            logger.debug("Full contact set found at %s, stopping early.", url)
            break

    return merged


async def _scrape_with_browser_async(business: Business, browser: AsyncBrowser) -> Business:
    """
    Async equivalent of _scrape_with_browser.

    Uses a pre-launched async Browser. Creates and closes its own context so
    each site gets a fully isolated session (fresh cookies, user agent, history).
    Does NOT close the browser — the caller owns that.
    """
    url = normalise_url(business.website_url)

    if not url:
        logger.info("No website URL for '%s', skipping scrape.", business.business_name)
        return business

    print(f"  [SCRAPE] Visiting: {url}")
    logger.info("Scraping website for '%s' | url=%s", business.business_name, url)

    context = await browser.new_context(
        user_agent=get_random_user_agent(),
        viewport={"width": 1280, "height": 800},
        java_script_enabled=True,
    )
    page = await context.new_page()

    try:
        # ── Step 1: Load the homepage ─────────────────────────────────────────
        start_time = time.time()
        homepage_html = await _safe_goto_async(page, url)
        load_time = time.time() - start_time

        if not homepage_html:
            # Retry with a different user agent
            logger.info("First attempt failed for %s, retrying with different user agent...", url)
            await context.close()
            context = await browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            start_time = time.time()
            homepage_html = await _safe_goto_async(page, url)
            load_time = time.time() - start_time

        if not homepage_html:
            logger.warning("Could not load %s after retry, skipping.", url)
            print(f"  [SKIP] Could not load {url}")
            return business

        # ── Step 2: Extract contacts from homepage ────────────────────────────
        homepage_contacts = extract_contacts(homepage_html, source_url=url)

        # ── Step 3: Discover and visit contact-relevant pages, merge results ───
        # Links are found dynamically from the homepage's own navigation so
        # no paths are hardcoded. Results are merged with homepage contacts.
        extra_contacts = await _try_contact_pages_async(page, url, homepage_html)
        final_contacts = merge_contacts(homepage_contacts, extra_contacts)

        # ── Step 4: Score the website ─────────────────────────────────────────
        score_data = score_website(
            html=homepage_html,
            url=url,
            load_time_seconds=load_time,
        )

        # ── Step 4b: If still no email, check social media pages ─────────────
        # We use the social URLs found on the website itself to find the email.
        if not final_contacts.get("email"):
            social_email, social_source = await _scrape_social_for_email_async(
                facebook_url=final_contacts.get("facebook", ""),
                instagram_url=final_contacts.get("instagram", ""),
                twitter_url=final_contacts.get("twitter", ""),
                browser=browser,
            )
            if social_email:
                final_contacts["email"] = social_email
                final_contacts["email_source"] = social_source
            else:
                final_contacts["email_source"] = ""
        else:
            final_contacts["email_source"] = "website"

        # ── Step 5: Write results back to the business object ─────────────────
        business.email = final_contacts.get("email", "")
        business.email_source = final_contacts.get("email_source", "")
        business.whatsapp = final_contacts.get("whatsapp", "")
        business.instagram = final_contacts.get("instagram", "")
        business.facebook = final_contacts.get("facebook", "")
        business.twitter = final_contacts.get("twitter", "")

        if not business.phone:
            business.phone = final_contacts.get("phone", "")

        business.website_quality_score = score_data["score"]
        business.website_issues = score_data["issues"]

        print(
            f"  [OK] Score: {score_data['score']}/100 | "
            f"Email: {'Yes' if business.email else 'No'}"
            + (f" (via {business.email_source})" if business.email_source else "")
            + f" | Phone: {'Yes' if business.phone else 'No'} | "
            f"WhatsApp: {'Yes' if business.whatsapp else 'No'}"
        )

    except Exception as exc:
        logger.error("Unexpected error scraping %s: %s", url, exc, exc_info=True)
        print(f"  [ERROR] Unexpected error for {url}: {exc}")

    finally:
        await context.close()

    return business


async def _scrape_social_for_email_async(
    facebook_url: str,
    instagram_url: str,
    twitter_url: str,
    browser: AsyncBrowser,
) -> tuple[str, str]:
    """
    Visits social media pages to find an email address when none was found
    on the business website.

    Checks in order: Facebook About page, Instagram profile, Twitter profile.
    Stops as soon as an email is found.

    Args:
        facebook_url: Facebook page URL (or empty string).
        instagram_url: Instagram profile URL (or empty string).
        twitter_url: Twitter/X profile URL (or empty string).
        browser: Pre-launched async Browser instance to reuse.

    Returns:
        Tuple of (email, platform) e.g. ("info@firm.com", "facebook"),
        or ("", "") if nothing found.
    """
    # Build list of (url, platform_label) to try in priority order.
    # For Facebook we append /about — that page shows the Contact Info section.
    attempts = []
    if facebook_url:
        attempts.append((facebook_url.rstrip("/") + "/about", "facebook"))
    if instagram_url:
        attempts.append((instagram_url, "instagram"))
    if twitter_url:
        attempts.append((twitter_url, "twitter"))

    if not attempts:
        return "", ""

    for url, platform in attempts:
        context = await browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )
        page = await context.new_page()

        try:
            html = await _safe_goto_async(page, url)
            if not html:
                continue

            # Strip scripts/styles then run email regex on visible text
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)

            emails = extract_emails(text)
            if emails:
                logger.info("Found email via %s: %s", platform, emails[0])
                print(f"  [SOCIAL] Found email via {platform}: {emails[0]}")
                return emails[0], platform

        except Exception as exc:
            logger.warning("Error scraping social page %s (%s): %s", platform, url, exc)

        finally:
            await context.close()

    return "", ""


async def scrape_all_websites(
    businesses: list[Business],
    on_progress: Optional[callable] = None,
    max_concurrent: int = MAX_CONCURRENT,
) -> list[Business]:
    """
    Scrapes websites for all businesses with up to max_concurrent workers
    running simultaneously.

    Launches Chromium once for the entire run. Each worker gets its own
    isolated browser context (fresh cookies, user agent, session history).
    A per-worker delay is applied after each site visit to pace requests.

    Args:
        businesses: List of Business objects to scrape.
        on_progress: Optional callback called after each business completes.
                     Signature: on_progress(current: int, total: int) -> None.
        max_concurrent: Maximum simultaneous scrapers. Reads from the
                        SCRAPE_WORKERS env var, default 5.

    Returns:
        The same list with contact and scoring data filled in.
    """
    total = len(businesses)
    print(f"\n[START] Scraping {total} websites ({max_concurrent} concurrent workers)...\n")

    semaphore = asyncio.Semaphore(max_concurrent)
    completed = 0

    async with async_playwright() as pw:
        browser: AsyncBrowser = await pw.chromium.launch(headless=True)

        async def _worker(idx: int, business: Business) -> None:
            nonlocal completed

            async with semaphore:
                print(f"[{idx}/{total}] {business.business_name}")

                if business.website_url:
                    await _scrape_with_browser_async(business, browser)
                else:
                    # No website — flag as high-priority lead, score = 0
                    business.has_website = False
                    business.website_quality_score = 0
                    business.website_issues = ["No website"]
                    print(f"  [NO WEBSITE] Flagged as high-priority lead (phone: {business.phone or 'N/A'})")

                # Safe to mutate without a lock — asyncio is single-threaded;
                # no await between the increment and callback so nothing interleaves
                completed += 1
                if on_progress:
                    on_progress(completed, total)

                # Per-worker cooldown before this slot picks up the next business
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        tasks = [_worker(i + 1, b) for i, b in enumerate(businesses)]
        await asyncio.gather(*tasks, return_exceptions=True)

        await browser.close()

    print(f"\n[DONE] Concurrent scraping complete for {total} businesses.\n")
    return businesses
