"""
enricher/smtp_verifier.py

Verifies email addresses using the mails.so API.
Replaces the direct SMTP RCPT TO probe which requires port 25 (blocked on most
corporate networks).

API: GET https://api.mails.so/v1/validate?email={email}
Header: x-mails-api-key: {MAILS_SO_API_KEY}

smtp_status values stored in DB:
  'verified'  — mails.so confirmed mailbox is deliverable
  'rejected'  — mails.so confirmed mailbox does not exist
  'catch_all' — domain accepts all addresses (cannot distinguish)
  'unverified'— API key not set, or API quota exceeded
  'error'     — network/timeout error calling the API
"""

import os
import time

import httpx

from utils.logger import get_logger

logger = get_logger(__name__)

API_URL     = "https://api.mails.so/v1/validate"
API_TIMEOUT = 10   # seconds per request
# Small delay between calls to avoid hammering the API
CALL_DELAY  = 0.2  # seconds


def _map_status(data: dict) -> str:
    """
    Maps the mails.so response to our internal smtp_status values.

    mails.so returns a top-level 'result' field with values such as:
      deliverable, undeliverable, catch_all, risky, unknown
    It may also return a nested 'data.result' depending on API version.
    """
    # Handle both flat and nested response shapes
    result = (
        data.get("result")
        or (data.get("data") or {}).get("result")
        or ""
    ).lower()

    if result == "deliverable":
        return "verified"
    if result in ("undeliverable", "invalid"):
        return "rejected"
    if result == "catch_all":
        return "catch_all"
    if result == "risky":
        # Risky means it might exist but is a disposable/spam-trap address.
        # Treat as catch_all — worth keeping but lower confidence.
        return "catch_all"
    return "unknown"


def _verify_one(email: str, api_key: str) -> str:
    """Calls the mails.so API for a single address. Returns smtp_status string."""
    try:
        resp = httpx.get(
            API_URL,
            params={"email": email},
            headers={"x-mails-api-key": api_key},
            timeout=API_TIMEOUT,
        )
        if resp.status_code == 402:
            logger.warning("[MAILSSO] Quota exceeded — marking remaining as unverified")
            return "quota_exceeded"
        if resp.status_code != 200:
            logger.warning("[MAILSSO] Unexpected status %d for %s", resp.status_code, email)
            return "error"

        data = resp.json()
        status = _map_status(data)
        logger.info("[MAILSSO] %s => %s (raw: %s)", email, status, data)
        return status

    except httpx.TimeoutException:
        logger.warning("[MAILSSO] Timeout for %s", email)
        return "error"
    except Exception as exc:
        logger.warning("[MAILSSO] Error for %s: %s", email, exc)
        return "error"


def verify_candidates(candidates: list[dict]) -> list[dict]:
    """
    Verifies a list of candidate email dicts using the mails.so API.
    Adds 'smtp_status' to each dict in-place.

    Catch-all optimisation: once any candidate on a domain returns catch_all,
    all remaining candidates on that domain are stamped catch_all without
    making further API calls — saving credits.

    Each dict must have 'candidate_email'.
    Returns the same list with smtp_status filled in.
    """
    api_key = os.getenv("MAILS_SO_API_KEY", "")
    if not api_key:
        logger.warning("[MAILSSO] MAILS_SO_API_KEY not set — marking all as unverified")
        for c in candidates:
            c["smtp_status"] = "unverified"
        return candidates

    quota_exceeded = False
    catch_all_domains: set[str] = set()
    no_connect_domains: set[str] = set()

    for c in candidates:
        email = c.get("candidate_email", "")
        if not email or "@" not in email:
            c["smtp_status"] = "error"
            continue

        domain = email.split("@")[1].lower()

        # Skip — domain already confirmed as catch-all
        if domain in catch_all_domains:
            c["smtp_status"] = "catch_all"
            logger.debug("[MAILSSO] %s skipped — domain is catch-all", email)
            continue

        # Skip — domain server unreachable, no point retrying
        if domain in no_connect_domains:
            c["smtp_status"] = "unknown"
            logger.debug("[MAILSSO] %s skipped — domain server unreachable", email)
            continue

        if quota_exceeded:
            c["smtp_status"] = "unverified"
            continue

        status = _verify_one(email, api_key)

        if status == "quota_exceeded":
            quota_exceeded = True
            c["smtp_status"] = "unverified"
            continue

        if status == "catch_all":
            catch_all_domains.add(domain)
            logger.info("[MAILSSO] %s is catch-all — skipping remaining on this domain", domain)

        # First unknown on a domain (no_connect or timeout) means the mail
        # server is unreachable — no point probing further candidates on it.
        if status == "unknown" and domain not in no_connect_domains:
            no_connect_domains.add(domain)
            logger.info("[MAILSSO] %s unreachable — skipping remaining on this domain", domain)

        c["smtp_status"] = status
        time.sleep(CALL_DELAY)

    return candidates


def reverify_contacts(limit: int = 500) -> dict:
    """
    Re-verifies existing contacts in the DB that have smtp_status='unverified'
    and haven't been sent to yet. Updates their status in-place.

    Returns a summary dict: total, verified, rejected, catch_all, errors.
    """
    from db.database import get_unverified_contacts, update_contact_smtp_status

    api_key = os.getenv("MAILS_SO_API_KEY", "")
    if not api_key:
        logger.warning("[MAILSSO] MAILS_SO_API_KEY not set — cannot reverify")
        return {"total": 0, "verified": 0, "rejected": 0, "catch_all": 0, "errors": 0}

    contacts = get_unverified_contacts(limit=limit)
    logger.info("[MAILSSO] Re-verifying %d unverified contacts", len(contacts))

    summary = {"total": len(contacts), "verified": 0, "rejected": 0, "catch_all": 0, "errors": 0}
    quota_exceeded = False

    for c in contacts:
        email = c.get("candidate_email", "")
        if not email or "@" not in email:
            summary["errors"] += 1
            continue

        if quota_exceeded:
            break

        status = _verify_one(email, api_key)

        if status == "quota_exceeded":
            quota_exceeded = True
            logger.warning("[MAILSSO] Quota exceeded — stopping early")
            break

        update_contact_smtp_status(email, status)

        if status == "verified":
            summary["verified"] += 1
        elif status == "rejected":
            summary["rejected"] += 1
        elif status == "catch_all":
            summary["catch_all"] += 1
        else:
            summary["errors"] += 1

        time.sleep(CALL_DELAY)

    logger.info("[MAILSSO] Reverify complete: %s", summary)
    return summary

    return candidates
