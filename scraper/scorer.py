"""
Website quality scoring for LeadHarvest.

Scores each business website out of 100 based on:
  - SSL (HTTPS)               — 20 points
  - Mobile responsiveness     — 20 points
  - Page load speed (<3s)     — 20 points
  - Contact info on homepage  — 15 points
  - SEO meta tags             — 15 points
  - Call-to-action button     — 10 points

A LOW score means the business is a STRONG lead — their site needs work.
"""

import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from utils.logger import get_logger

logger = get_logger(__name__)

# ── Scoring weights ────────────────────────────────────────────────────────────

POINTS_SSL = 20
POINTS_MOBILE = 20
POINTS_SPEED = 20
POINTS_CONTACT = 15
POINTS_SEO = 15
POINTS_CTA = 10

# Keywords that suggest a call-to-action button
CTA_KEYWORDS = [
    "contact us", "get in touch", "book now", "schedule", "call us",
    "whatsapp us", "get a quote", "request a quote", "free consultation",
    "enquire", "enquire now", "send a message", "start now", "learn more",
    "get started", "buy now", "order now", "make an appointment",
]

# Nigerian phone prefixes for contact info detection
NIGERIAN_PHONE_RE = re.compile(
    r"(?:\+?234|0)(?:7|8|9)0?\d[\d\s\-\.]{6,9}"
)

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)


# ── Individual checks ──────────────────────────────────────────────────────────

def _check_ssl(url: str) -> tuple[int, str]:
    """
    Awards points if the URL uses HTTPS.

    Returns:
        (points_earned, issue_message_if_failed)
    """
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return POINTS_SSL, ""
    return 0, "No SSL (HTTP only)"


def _check_mobile(soup: BeautifulSoup) -> tuple[int, str]:
    """
    Awards points if a viewport meta tag is present (mobile responsiveness signal).

    Returns:
        (points_earned, issue_message_if_failed)
    """
    viewport = soup.find("meta", attrs={"name": re.compile(r"viewport", re.I)})
    if viewport and viewport.get("content"):
        return POINTS_MOBILE, ""
    return 0, "Not mobile responsive (no viewport meta tag)"


def _check_speed(load_time_seconds: float) -> tuple[int, str]:
    """
    Awards points if the page loaded in under 3 seconds.

    Returns:
        (points_earned, issue_message_if_failed)
    """
    if load_time_seconds <= 3.0:
        return POINTS_SPEED, ""
    return 0, f"Slow page load ({load_time_seconds:.1f}s, target <3s)"


def _check_contact_info(soup: BeautifulSoup) -> tuple[int, str]:
    """
    Awards points if the homepage contains a phone number or email address.

    Returns:
        (points_earned, issue_message_if_failed)
    """
    text = soup.get_text(separator=" ", strip=True)

    has_phone = bool(NIGERIAN_PHONE_RE.search(text))
    has_email = bool(EMAIL_RE.search(text))

    if has_phone or has_email:
        return POINTS_CONTACT, ""
    return 0, "No contact info visible on homepage"


def _check_seo(soup: BeautifulSoup) -> tuple[int, str]:
    """
    Awards points if the page has both a meaningful <title> tag and a
    meta description tag.

    Returns:
        (points_earned, issue_message_if_failed)
    """
    issues = []

    title_tag = soup.find("title")
    has_title = bool(title_tag and title_tag.get_text(strip=True))
    if not has_title:
        issues.append("missing title tag")

    meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    has_desc = bool(meta_desc and meta_desc.get("content", "").strip())
    if not has_desc:
        issues.append("missing meta description")

    if has_title and has_desc:
        return POINTS_SEO, ""

    return 0, f"Weak SEO ({', '.join(issues)})"


def _check_cta(soup: BeautifulSoup) -> tuple[int, str]:
    """
    Awards points if a call-to-action button or link is visible on the page.
    Checks <button>, <a>, and elements with common CTA text.

    Returns:
        (points_earned, issue_message_if_failed)
    """
    # Check button text
    for button in soup.find_all(["button", "a"]):
        text = button.get_text(strip=True).lower()
        if any(kw in text for kw in CTA_KEYWORDS):
            return POINTS_CTA, ""

    # Also check input[type=submit] value
    for inp in soup.find_all("input", type="submit"):
        value = (inp.get("value") or "").lower()
        if any(kw in value for kw in CTA_KEYWORDS):
            return POINTS_CTA, ""

    return 0, "No clear call-to-action button"


# ── Master scoring function ────────────────────────────────────────────────────

def score_website(html: str, url: str, load_time_seconds: float) -> dict:
    """
    Scores a business website across all quality dimensions.

    Args:
        html: Full HTML content of the page.
        url: URL the page was loaded from (used for SSL check).
        load_time_seconds: How long the page took to load.

    Returns:
        Dict with:
          - 'score': int (0–100), total quality score
          - 'issues': list[str], descriptions of what's missing/broken
          - 'breakdown': dict of individual check scores (for debugging)
    """
    soup = BeautifulSoup(html, "html.parser")
    issues = []
    breakdown = {}
    total = 0

    # Run each check
    checks = [
        ("ssl",     _check_ssl(url)),
        ("mobile",  _check_mobile(soup)),
        ("speed",   _check_speed(load_time_seconds)),
        ("contact", _check_contact_info(soup)),
        ("seo",     _check_seo(soup)),
        ("cta",     _check_cta(soup)),
    ]

    for key, (points, issue) in checks:
        breakdown[key] = points
        total += points
        if issue:
            issues.append(issue)

    logger.debug(
        "Score for %s: %d/100 | issues=%s", url, total, issues
    )

    return {
        "score": total,
        "issues": issues,
        "breakdown": breakdown,
    }
