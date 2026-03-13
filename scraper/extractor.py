"""
Contact extraction logic for LeadHarvest.

Parses HTML content from business websites to extract:
  - Email addresses
  - Nigerian phone numbers (all formats)
  - WhatsApp links (wa.me)
  - Social media profile URLs (Instagram, Facebook, Twitter/X)
"""

import html as html_lib
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from utils.logger import get_logger

logger = get_logger(__name__)

# ── Regex Patterns ─────────────────────────────────────────────────────────────

# Standard email pattern
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Nigerian phone number patterns:
#   080, 081, 070, 090, 091 — 11-digit local format
#   +234 8xx, +234 7xx, +234 9xx — international format
PHONE_PATTERN = re.compile(
    r"""
    (?:
        (?:\+?234[-.\s]?)       # optional +234 country code
        |
        (?:0)                   # leading 0 for local format
    )
    (?:7|8|9)0?\d              # network prefix (70x, 80x, 81x, 90x, etc.)
    [\d\s\-\.]{6,9}            # remaining digits with optional separators
    """,
    re.VERBOSE,
)

# WhatsApp link patterns
WHATSAPP_PATTERN = re.compile(
    r"https?://(?:api\.)?wa\.me/(\+?[\d]+)",
    re.IGNORECASE,
)

WHATSAPP_TEXT_PATTERN = re.compile(
    r"(?:whatsapp|wa)[^\d+]*(\+?234[\d\s\-\.]{9,12}|0[789]0?[\d\s\-\.]{7,10})",
    re.IGNORECASE,
)

# Social media URL patterns — match profile-level links, not just domain mentions
INSTAGRAM_PATTERN = re.compile(
    r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?",
    re.IGNORECASE,
)
FACEBOOK_PATTERN = re.compile(
    r"https?://(?:www\.)?facebook\.com/([A-Za-z0-9_.@\-]+)/?",
    re.IGNORECASE,
)
TWITTER_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]+)/?",
    re.IGNORECASE,
)

# Social accounts to exclude (generic Facebook/Instagram/Twitter pages)
SOCIAL_BLACKLIST = {
    "sharer", "share", "dialog", "plugins", "login", "home",
    "pages", "groups", "events", "marketplace", "watch",
    "intent", "compose", "search",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode_cloudflare_email(encoded: str) -> str:
    """
    Decodes a Cloudflare email-protection encoded string.

    Cloudflare automatically replaces emails on protected pages with:
        <a href="/cdn-cgi/l/email-protection" data-cfemail="[hex]">
    The hex string XORs each character with the first byte (the key).
    This is a well-known, publicly documented encoding — not a security
    measure, just an anti-scraper inconvenience.

    Args:
        encoded: Hex string from the data-cfemail attribute.

    Returns:
        Decoded email string, or empty string if decoding fails.
    """
    try:
        key = int(encoded[:2], 16)
        return "".join(
            chr(int(encoded[i:i + 2], 16) ^ key)
            for i in range(2, len(encoded), 2)
        )
    except Exception:
        return ""


def _clean_phone(raw: str) -> str:
    """
    Strips whitespace, dashes, and dots from a phone number string.
    Normalises +234 format to a clean string.
    """
    cleaned = re.sub(r"[\s\-\.]", "", raw).strip()
    return cleaned


def _is_valid_social_handle(handle: str) -> bool:
    """Returns False for generic/system social media paths that are not profile URLs."""
    return handle.lower() not in SOCIAL_BLACKLIST and len(handle) >= 2


# ── Core extraction functions ──────────────────────────────────────────────────

def extract_emails(text: str) -> list[str]:
    """
    Finds all email addresses in the given text.
    Returns a deduplicated list, excluding common placeholder emails.

    Args:
        text: Raw text or HTML string to search.

    Returns:
        List of email address strings.
    """
    matches = EMAIL_PATTERN.findall(text)
    # Deduplicate and filter out obvious placeholders/test emails
    seen = set()
    results = []
    for email in matches:
        email_lower = email.lower()
        if email_lower not in seen and "example" not in email_lower and "test@" not in email_lower:
            seen.add(email_lower)
            results.append(email)
    return results


def extract_phones(text: str) -> list[str]:
    """
    Finds all Nigerian phone numbers in the given text.
    Returns a deduplicated list of cleaned phone strings.

    Args:
        text: Raw text string to search.

    Returns:
        List of phone number strings.
    """
    matches = PHONE_PATTERN.findall(text)
    seen = set()
    results = []
    for raw in matches:
        cleaned = _clean_phone(raw)
        # Must be at least 10 digits to be a valid Nigerian number
        digits_only = re.sub(r"\D", "", cleaned)
        if len(digits_only) >= 10 and cleaned not in seen:
            seen.add(cleaned)
            results.append(cleaned)
    return results


def extract_whatsapp(html: str, text: str) -> Optional[str]:
    """
    Finds a WhatsApp contact link or number from the page.

    Checks in order:
      1. wa.me links in HTML
      2. Text mentioning WhatsApp near a phone number

    Args:
        html: Full HTML source of the page.
        text: Visible text content of the page.

    Returns:
        WhatsApp number string (normalised), or None if not found.
    """
    # Check for wa.me links first — most reliable
    wa_match = WHATSAPP_PATTERN.search(html)
    if wa_match:
        number = wa_match.group(1)
        return _clean_phone(number)

    # Fall back: find WhatsApp mentioned near a phone number in text
    wa_text_match = WHATSAPP_TEXT_PATTERN.search(text)
    if wa_text_match:
        number = wa_text_match.group(1)
        return _clean_phone(number)

    return None


def extract_social_links(html: str) -> dict[str, str]:
    """
    Finds Instagram, Facebook, and Twitter/X profile URLs from the page HTML.

    Args:
        html: Full HTML source of the page.

    Returns:
        Dict with keys 'instagram', 'facebook', 'twitter', each mapped to
        a URL string or empty string if not found.
    """
    socials = {"instagram": "", "facebook": "", "twitter": ""}

    ig_match = INSTAGRAM_PATTERN.search(html)
    if ig_match and _is_valid_social_handle(ig_match.group(1)):
        socials["instagram"] = ig_match.group(0).rstrip("/")

    fb_match = FACEBOOK_PATTERN.search(html)
    if fb_match and _is_valid_social_handle(fb_match.group(1)):
        socials["facebook"] = fb_match.group(0).rstrip("/")

    tw_match = TWITTER_PATTERN.search(html)
    if tw_match and _is_valid_social_handle(tw_match.group(1)):
        socials["twitter"] = tw_match.group(0).rstrip("/")

    return socials


# ── Master extraction function ─────────────────────────────────────────────────

def extract_contacts(html: str, source_url: str = "") -> dict:
    """
    Runs all extractors on a page's HTML and returns a consolidated dict.

    Args:
        html: Full HTML source of the page.
        source_url: URL the HTML was fetched from (used for logging).

    Returns:
        Dict with keys: email, phone, whatsapp, instagram, facebook, twitter.
        Values are strings (first match) or empty string.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── Source 1: Cloudflare-protected emails (data-cfemail attribute) ─────────
    # Cloudflare replaces emails on protected pages with an XOR-encoded hex
    # string in data-cfemail. Very common on Nigerian business sites.
    cf_emails = []
    for tag in soup.find_all(attrs={"data-cfemail": True}):
        decoded = _decode_cloudflare_email(tag["data-cfemail"])
        if decoded and EMAIL_PATTERN.match(decoded):
            cf_emails.append(decoded)

    # ── Source 2: mailto: links ────────────────────────────────────────────────
    # Explicit contact links — most reliable after Cloudflare.
    # They live in HTML attributes and are invisible to get_text().
    mailto_emails = []
    for a_tag in soup.find_all("a", href=re.compile(r"^mailto:", re.I)):
        href = a_tag.get("href", "")
        email_part = href[7:].split("?")[0].strip()
        if EMAIL_PATTERN.match(email_part):
            mailto_emails.append(email_part)

    # ── Source 3: visible page text ────────────────────────────────────────────
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    raw_text = soup.get_text(separator=" ", strip=True)
    # Decode HTML entities so &#64; becomes @ and &#46; becomes .
    # Without this, emails like info&#64;company.com are invisible to the regex.
    text = html_lib.unescape(raw_text)

    text_emails = extract_emails(text)

    # Merge all sources: Cloudflare > mailto > visible text. Deduplicate.
    seen = set()
    all_emails = []
    for email in cf_emails + mailto_emails + text_emails:
        if email.lower() not in seen:
            seen.add(email.lower())
            all_emails.append(email)

    emails = all_emails
    phones = extract_phones(text)
    whatsapp = extract_whatsapp(html, text)
    socials = extract_social_links(html)

    result = {
        "email": emails[0] if emails else "",
        "phone": phones[0] if phones else "",
        "whatsapp": whatsapp or "",
        "instagram": socials["instagram"],
        "facebook": socials["facebook"],
        "twitter": socials["twitter"],
    }

    logger.debug(
        "Extracted from %s | email=%s | phone=%s | whatsapp=%s | ig=%s | fb=%s | tw=%s",
        source_url or "unknown",
        result["email"],
        result["phone"],
        result["whatsapp"],
        bool(result["instagram"]),
        bool(result["facebook"]),
        bool(result["twitter"]),
    )

    return result


def merge_contacts(primary: dict, secondary: dict) -> dict:
    """
    Merges two contact dicts, keeping the first non-empty value for each field.
    Used when combining contacts from homepage + /contact + /about pages.

    Args:
        primary: Contact dict from the primary page (homepage).
        secondary: Contact dict from a secondary page (contact/about).

    Returns:
        Merged contact dict.
    """
    merged = {}
    for key in ("email", "phone", "whatsapp", "instagram", "facebook", "twitter"):
        merged[key] = primary.get(key) or secondary.get(key) or ""
    return merged
