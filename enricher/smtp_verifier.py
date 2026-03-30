"""
enricher/smtp_verifier.py

Verifies whether an email address exists by performing an SMTP RCPT TO probe.
No email is ever sent — the connection is closed after the RCPT TO response.

Process:
  1. DNS MX lookup to find the mail server for the domain.
  2. Catch-all detection: probe a random impossible address first.
     If the server accepts it, ALL addresses will appear valid — we mark
     every address as 'catch_all' and stop probing.
  3. For each candidate, connect and issue RCPT TO. Read the response code:
       250 => 'verified'   (mailbox confirmed to exist)
       550/551/553 => 'rejected' (mailbox does not exist)
       anything else => 'unknown' (greylisted, rate-limited, etc.)

smtp_status values stored in DB:
  'verified'  — server confirmed mailbox exists
  'rejected'  — server confirmed mailbox does NOT exist
  'catch_all' — server accepts all addresses (cannot distinguish)
  'unknown'   — connection failed, timeout, or ambiguous response
  'error'     — DNS/network error
"""

import smtplib
import socket
import random
import string

from utils.logger import get_logger

logger = get_logger(__name__)

SMTP_TIMEOUT   = 10   # seconds per connection attempt
PROBE_FROM     = "probe@leadharvest.invalid"   # sender used in MAIL FROM
PROBE_HELO     = "leadharvest.invalid"


def _get_mx(domain: str) -> str | None:
    """Returns the highest-priority MX hostname for the domain, or None."""
    try:
        import dns.resolver
        records = dns.resolver.resolve(domain, "MX", lifetime=8)
        sorted_records = sorted(records, key=lambda r: r.preference)
        return str(sorted_records[0].exchange).rstrip(".")
    except Exception as exc:
        logger.debug("[SMTP] MX lookup failed for %s: %s", domain, exc)
        return None


def _smtp_probe(mx_host: str, domain: str, address: str) -> str:
    """
    Opens one SMTP connection and probes a single address.
    Returns 'verified', 'rejected', or 'unknown'.
    """
    try:
        with smtplib.SMTP(timeout=SMTP_TIMEOUT) as smtp:
            smtp.connect(mx_host, 25)
            smtp.ehlo(PROBE_HELO)
            smtp.mail(PROBE_FROM)
            code, _ = smtp.rcpt(address)
            smtp.quit()

            if code == 250:
                return "verified"
            elif code in (550, 551, 552, 553, 554):
                return "rejected"
            else:
                return "unknown"

    except smtplib.SMTPConnectError:
        logger.debug("[SMTP] Connect error to %s", mx_host)
        return "unknown"
    except smtplib.SMTPServerDisconnected:
        logger.debug("[SMTP] Server disconnected for %s", mx_host)
        return "unknown"
    except socket.timeout:
        logger.debug("[SMTP] Timeout connecting to %s", mx_host)
        return "unknown"
    except OSError as exc:
        logger.debug("[SMTP] OS error for %s: %s", mx_host, exc)
        return "unknown"
    except Exception as exc:
        logger.debug("[SMTP] Unexpected error for %s: %s", mx_host, exc)
        return "unknown"


def _is_catch_all(mx_host: str, domain: str) -> bool:
    """
    Probes a randomly generated impossible address on the domain.
    If the server accepts it, the domain is a catch-all.
    """
    rand_prefix = "zz" + "".join(random.choices(string.ascii_lowercase, k=10))
    test_addr = f"{rand_prefix}@{domain}"
    result = _smtp_probe(mx_host, domain, test_addr)
    return result == "verified"


def _port25_available() -> bool:
    """Quick check: can we reach port 25 at all? Returns False on corporate networks that block it."""
    try:
        s = socket.create_connection(("gmail-smtp-in.l.google.com", 25), timeout=5)
        s.close()
        return True
    except Exception:
        return False


def verify_candidates(candidates: list[dict]) -> list[dict]:
    """
    Verifies a list of candidate email dicts in-place (adds 'smtp_status').

    Groups candidates by domain to reuse MX lookups and catch-all checks.
    Each dict must have 'candidate_email'. Returns the same list with
    'smtp_status' filled in on each entry.

    Args:
        candidates: List of dicts with at least 'candidate_email'.

    Returns:
        Same list with 'smtp_status' set on each entry.
    """
    # If port 25 is blocked (common on corporate networks), skip all probing
    # and mark every candidate as 'unverified' so they still get saved to DB.
    if not _port25_available():
        logger.warning("[SMTP] Port 25 blocked — skipping verification, saving as unverified")
        for c in candidates:
            c["smtp_status"] = "unverified"
        return candidates

    # Group by domain
    by_domain: dict[str, list[dict]] = {}
    for c in candidates:
        email = c.get("candidate_email", "")
        if "@" not in email:
            c["smtp_status"] = "error"
            continue
        domain = email.split("@")[1].lower()
        by_domain.setdefault(domain, []).append(c)

    for domain, group in by_domain.items():
        logger.info("[SMTP] Probing domain %s (%d candidates)", domain, len(group))

        mx = _get_mx(domain)
        if not mx:
            logger.warning("[SMTP] No MX record for %s — marking all as error", domain)
            for c in group:
                c["smtp_status"] = "error"
            continue

        # Catch-all detection first
        if _is_catch_all(mx, domain):
            logger.info("[SMTP] %s is a catch-all domain", domain)
            for c in group:
                c["smtp_status"] = "catch_all"
            continue

        # Individual probes
        for c in group:
            address = c["candidate_email"]
            status = _smtp_probe(mx, domain, address)
            c["smtp_status"] = status
            logger.info("[SMTP] %s => %s", address, status)

    return candidates
