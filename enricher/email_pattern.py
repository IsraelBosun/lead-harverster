"""
enricher/email_pattern.py

Infers the email naming convention used by a domain from a known personal
email address, then generates candidate addresses for each discovered person.

Pattern priority:
  1. If the business has a personal-looking email (not a role address), we
     detect the pattern from it and only generate candidates using that pattern.
  2. If only a role address (info@, contact@, etc.) is available, we fall back
     to trying all 5 common patterns for each person.

The 5 patterns:
  - first          => chidi@domain.com
  - first.last     => chidi.eze@domain.com
  - f.last         => c.eze@domain.com
  - firstlast      => chidieze@domain.com
  - last           => eze@domain.com
"""

import re
from urllib.parse import urlparse

from utils.logger import get_logger

logger = get_logger(__name__)

# Role addresses that are NOT useful for pattern inference
ROLE_PREFIXES = {
    "info", "contact", "hello", "enquiries", "enquiry", "support",
    "admin", "office", "mail", "noreply", "no-reply", "help",
    "sales", "marketing", "hr", "accounts", "billing", "legal",
    "team", "general", "reception", "feedback", "media", "press",
    "careers", "jobs", "webmaster", "postmaster",
}


def _extract_domain(website_url: str) -> str:
    """Returns just the domain from a URL, e.g. 'firm.com.ng'."""
    parsed = urlparse(website_url)
    domain = parsed.netloc or parsed.path
    return domain.lstrip("www.").lower()


def _is_role_address(email: str) -> bool:
    prefix = email.split("@")[0].lower()
    return prefix in ROLE_PREFIXES


def _detect_pattern(email: str, domain: str) -> str | None:
    """
    Given a known personal email on this domain, returns which of the 5
    patterns it matches. Returns None if it doesn't match any.

    e.g. chidi.eze@firm.com  => 'first.last'
         c.eze@firm.com      => 'f.last'
         chidieze@firm.com   => 'firstlast'
         chidi@firm.com      => 'first'
         eze@firm.com        => 'last'
    """
    if "@" not in email:
        return None
    prefix, addr_domain = email.lower().split("@", 1)
    if addr_domain != domain:
        return None

    # first.last  — contains exactly one dot, both parts >= 2 chars
    if re.match(r"^[a-z]{2,}\.[a-z]{2,}$", prefix):
        return "first.last"

    # f.last  — one letter, dot, then 2+ chars
    if re.match(r"^[a-z]\.[a-z]{2,}$", prefix):
        return "f.last"

    # For no-dot patterns we need the name parts to distinguish them
    # flast  — one letter followed by the last name (2+ chars), total short
    # lastf  — last name followed by one letter
    # firstlast — full first + full last merged
    # first — just the first name
    # We can only detect these reliably if we have the actual name, so
    # fall back to trying all patterns when ambiguous.
    if re.match(r"^[a-z]{4,20}$", prefix):
        if len(prefix) <= 8:
            return "first"
        return "firstlast"

    return None


def _name_parts(full_name: str) -> tuple[str, str]:
    """
    Splits a full name into (first, last). Handles middle names by
    taking the first word as first and the last word as last.
    """
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0].lower(), parts[0].lower()
    return parts[0].lower(), parts[-1].lower()


def _generate_candidates(first: str, last: str, domain: str, patterns: list[str]) -> list[dict]:
    """
    Generates candidate email addresses for each pattern.
    Returns list of {candidate_email, pattern_used}.
    """
    mapping = {
        "first":      f"{first}@{domain}",
        "first.last": f"{first}.{last}@{domain}",
        "f.last":     f"{first[0]}.{last}@{domain}",
        "firstlast":  f"{first}{last}@{domain}",
        "last":       f"{last}@{domain}",
        "flast":      f"{first[0]}{last}@{domain}",
        "lastf":      f"{last}{first[0]}@{domain}",
    }
    candidates = []
    for p in patterns:
        if p in mapping:
            candidates.append({
                "candidate_email": mapping[p],
                "pattern_used":    p,
            })
    return candidates


ALL_PATTERNS = ["first.last", "first", "f.last", "firstlast", "last", "flast", "lastf"]


def _best_personal_email(emails: list[str], domain: str) -> str | None:
    """
    From a list of email addresses, return the first one that looks personal
    (not a role address) and belongs to the given domain.
    """
    for email in emails:
        if "@" not in email:
            continue
        _, addr_domain = email.lower().split("@", 1)
        if addr_domain != domain:
            continue
        if not _is_role_address(email):
            return email
    return None


def generate_candidates_for_people(
    people: list[dict],
    website_url: str,
    existing_email: str,
    gemini_emails: list[str] | None = None,
) -> list[dict]:
    """
    Main entry point.

    Args:
        people:         List of {person_name, title, source_page_url} from team_scraper.
        website_url:    Business homepage URL (used to extract domain).
        existing_email: The email we already have for this business (may be role addr).
        gemini_emails:  Emails Gemini spotted on the site pages (may include personal ones).

    Pattern priority:
      1. Personal email from gemini_emails (spotted directly on the site)
      2. Personal email from existing_email (from Google Places / prior scrape)
      3. Fall back to trying all 5 patterns

    Returns:
        List of candidate dicts, each with:
          person_name, title, source_page_url,
          candidate_email, pattern_used
        One dict per (person, pattern) combination.
    """
    domain = _extract_domain(website_url)
    if not domain:
        logger.warning("[PATTERN] Could not extract domain from %s", website_url)
        return []

    # Priority 1: personal email spotted by Gemini on the site pages
    gemini_personal = _best_personal_email(gemini_emails or [], domain)
    if gemini_personal:
        detected = _detect_pattern(gemini_personal, domain)
        if detected:
            patterns_to_try = [detected]
            logger.info("[PATTERN] Gemini spotted pattern '%s' from %s", detected, gemini_personal)
        else:
            patterns_to_try = ALL_PATTERNS
            logger.info("[PATTERN] Gemini email found but pattern unclear (%s) — trying all", gemini_personal)

    # Priority 2: personal email from DB / Google Places
    elif existing_email and not _is_role_address(existing_email):
        detected = _detect_pattern(existing_email, domain)
        if detected:
            patterns_to_try = [detected]
            logger.info("[PATTERN] Detected pattern '%s' from existing email %s", detected, existing_email)
        else:
            patterns_to_try = ALL_PATTERNS
            logger.info("[PATTERN] Personal email but pattern unclear — trying all patterns")

    # Priority 3: no useful email found anywhere — try all patterns
    else:
        patterns_to_try = ALL_PATTERNS
        logger.info("[PATTERN] No personal email found for %s — trying all 5 patterns", domain)

    results = []
    for person in people:
        first, last = _name_parts(person["person_name"])
        if not first or not last:
            continue
        candidates = _generate_candidates(first, last, domain, patterns_to_try)
        for c in candidates:
            results.append({
                "person_name":    person["person_name"],
                "title":          person["title"],
                "source_page_url": person["source_page_url"],
                "candidate_email": c["candidate_email"],
                "pattern_used":   c["pattern_used"],
            })

    logger.info(
        "[PATTERN] %d candidates generated for %d people on %s",
        len(results), len(people), domain,
    )
    return results
