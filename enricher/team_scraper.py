"""
enricher/team_scraper.py

Discovers decision-makers on a business website using Playwright + Gemini.

Strategy:
  1. Visit the homepage with Playwright.
  2. Collect every qualifying internal link (skip images, PDFs, blogs, etc.).
  3. Also try the homepage itself — some small firms list their team there.
  4. Visit each page (up to MAX_PAGES) and send its text to Gemini Flash.
     Gemini returns an empty list for non-team pages, so no manual scoring
     or keyword matching is needed — Gemini reads content like a human.
  5. Keep the page that yielded the most people.
  6. Return a list of dicts: {person_name, title, source_page_url}.
"""

import asyncio
import os
import re
import sys
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from utils.logger import get_logger

logger = get_logger(__name__)

TIMEOUT_MS = int(os.getenv("ENRICH_TIMEOUT", "30")) * 1000

# Maximum internal pages to visit per site (homepage + internal links)
MAX_PAGES = 10

# Paths/extensions that virtually never contain a team listing
_SKIP_PATH_PATTERNS = re.compile(
    r"/(blog|news|article|post|tag|category|event|media|gallery|faq|"
    r"privacy|terms|cookie|sitemap|search|login|signup|register|cart|"
    r"shop|product|payment|donate|subscribe|unsubscribe|wp-content|"
    r"wp-admin|feed|rss|cdn-cgi)/",
    re.I,
)
_SKIP_EXTENSIONS = re.compile(
    r"\.(pdf|jpg|jpeg|png|gif|svg|webp|mp4|doc|docx|zip|xml)$", re.I
)


def _get_internal_links(homepage_html: str, base_url: str) -> list[str]:
    """
    Returns unique internal links found on the homepage, filtered to remove
    obvious non-team destinations (blogs, images, downloads, etc.).

    Nav links (inside <nav>, <header>, or elements with menu/nav class names)
    are placed first so the most important site sections are visited first.
    """
    soup = BeautifulSoup(homepage_html, "html.parser")
    base_domain = urlparse(base_url).netloc
    base_path = urlparse(base_url).path.rstrip("/")

    def _is_valid(href: str) -> str | None:
        """Returns the full URL if valid, else None."""
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            return None
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc != base_domain:
            return None
        path = parsed.path.rstrip("/")
        if not path or path == base_path:
            return None
        if _SKIP_EXTENSIONS.search(path):
            return None
        if _SKIP_PATH_PATTERNS.search(path + "/"):
            return None
        return full

    # Nav containers — links here are the intentional site sections
    _NAV_CLASSES = re.compile(r"\b(nav|navbar|menu|navigation|header|main-menu|site-menu)\b", re.I)

    nav_containers = soup.find_all(
        lambda tag: tag.name in ("nav", "header") or (
            tag.get("class") and _NAV_CLASSES.search(" ".join(tag.get("class", [])))
        )
    )

    nav_links: dict[str, None] = {}
    for container in nav_containers:
        for a in container.find_all("a", href=True):
            full = _is_valid(a["href"].strip())
            if full:
                nav_links[full] = None

    # All other internal links (footer, inline content, etc.)
    other_links: dict[str, None] = {}
    for a in soup.find_all("a", href=True):
        full = _is_valid(a["href"].strip())
        if full and full not in nav_links:
            other_links[full] = None

    # Nav links first, then the rest
    return list(nav_links) + list(other_links)


def _extract_people_from_html(html: str, page_url: str) -> tuple[list[dict], list[str]]:
    """
    Strips HTML to plain text and sends it to Gemini for extraction.
    Returns (people, emails_found).
    """
    from enricher.gemini_extractor import extract_people_with_gemini
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "meta", "head"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    return extract_people_with_gemini(text, page_url)


# Sentinel returned when the homepage could not be loaded (timeout/error).
# The enricher uses this to skip marking the business as enriched so it
# can be retried on the next run.
LOAD_FAILED = object()


def scrape_team_page(website_url: str):
    """
    Entry point. Visits a business website, discovers team pages, extracts
    decision-maker name+title pairs using Gemini.

    Returns:
        - List of dicts {person_name, title, source_page_url} (may be empty).
        - LOAD_FAILED sentinel if the homepage could not be loaded at all.
    """
    # Skip junk URLs that are not real websites
    junk_prefixes = (
        "http://g.page", "https://g.page",
        "http://wa.link", "https://wa.link",
        "http://bit.ly", "https://bit.ly",
        "https://maps.", "http://maps.",
    )
    if any(website_url.startswith(p) for p in junk_prefixes):
        logger.info("[TEAM] Skipping junk URL: %s", website_url)
        return [], []

    people = []
    all_emails: list[str] = []

    # Windows requires ProactorEventLoop to spawn Chromium subprocesses.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            # Step 1: load homepage — wait for networkidle so JS-rendered
            # nav links have time to appear before we parse the HTML
            try:
                page.goto(website_url, timeout=TIMEOUT_MS, wait_until="networkidle")
                homepage_html = page.content()
            except PWTimeout:
                logger.warning("[TEAM] Homepage timeout: %s", website_url)
                browser.close()
                return LOAD_FAILED
            except Exception as exc:
                logger.warning("[TEAM] Homepage error %s: %s", website_url, exc)
                browser.close()
                return LOAD_FAILED

            # Step 2: collect all internal links
            internal_links = _get_internal_links(homepage_html, website_url)
            logger.info(
                "[TEAM] %s — %d internal pages to check",
                website_url, min(len(internal_links), MAX_PAGES - 1),
            )

            # Step 3: build the visit queue — homepage first, then internal links
            queue = [website_url] + internal_links[: MAX_PAGES - 1]

            # Step 4: visit each page, collect all people and emails across all pages
            all_found: list[dict] = []
            all_emails: list[str] = []
            visited: set[str] = set()

            for link in queue:
                if link in visited:
                    continue
                visited.add(link)
                try:
                    if link != website_url:
                        page.goto(link, timeout=TIMEOUT_MS, wait_until="load")
                    html = page.content()
                    found, emails = _extract_people_from_html(html, link)
                    if found:
                        logger.info("[TEAM] Found %d people at %s", len(found), link)
                        all_found.extend(found)
                    all_emails.extend(emails)
                except (PWTimeout, Exception) as exc:
                    logger.debug("[TEAM] Skipping %s: %s", link, exc)
                    continue

            people.extend(all_found)
            browser.close()

    except Exception as exc:
        logger.error("[TEAM] Playwright error for %s: %s", website_url, exc)

    # Deduplicate people by person_name (keep first occurrence)
    seen: set[str] = set()
    unique = []
    for p in people:
        key = p["person_name"].lower()
        if key not in seen:
            seen.add(key)
            unique.append(p)

    # Deduplicate emails
    unique_emails = list(dict.fromkeys(all_emails))

    logger.info(
        "[TEAM] %s => %d decision-makers, %d emails found",
        website_url, len(unique), len(unique_emails),
    )
    return unique, unique_emails
