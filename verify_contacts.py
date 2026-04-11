"""
verify_contacts.py

Verifies all enriched decision-maker contacts via the mails.so API.

Per person:
  1. Re-generate all email pattern variants and insert missing ones into DB.
  2. Probe the domain with the first variant.
     - unknown   -> domain unreachable, skip all variants, save nothing.
     - catch_all -> stamp ALL variants catch_all, save all to verification_results.
     - verified  -> save to verification_results, stop.
     - rejected  -> save, try next variant.
     - error     -> save, try next variant.

Results saved to verification_results table (email, status, verified_at).
Export to Excel separately.

Run:
    venv/Scripts/python.exe verify_contacts.py
"""

import os
import sqlite3
import time

from dotenv import load_dotenv

load_dotenv()

# Suppress console output from all loggers — we only want clean terminal output.
# utils/logger.py attaches a StreamHandler directly to each logger, so we must
# silence it at the handler level after the loggers are initialised.
import logging

def _silence_console_loggers(*names: str) -> None:
    for name in names:
        lgr = logging.getLogger(name)
        for h in lgr.handlers:
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                h.setLevel(logging.CRITICAL)

# Pre-import silence via root logger console handler (catches loggers not yet created)
logging.getLogger().setLevel(logging.WARNING)

from db.database import (
    get_all_persons_for_verification,
    get_contacts_by_person,
    get_contact_stats,
    get_unenriched_count,
    get_verification_results_map,
    save_contacts,
    save_verification_results,
)
from enricher.email_pattern import generate_candidates_for_people, ALL_PATTERNS, _generate_candidates, _name_parts
from enricher.smtp_verifier import _verify_one

# Silence console handlers that were attached during import
_silence_console_loggers(
    "enricher.email_pattern",
    "enricher.smtp_verifier",
    "db.database",
)

CALL_DELAY = 0.2   # seconds between API calls


# ── helpers ───────────────────────────────────────────────────────────────────

def _expand_variants(person: dict) -> list[str]:
    """
    Generates all pattern variants for a person and inserts missing ones
    into the contacts table. Returns the full list of candidate emails.
    """
    person_name  = person["person_name"]
    domain       = person["domain"]
    place_id     = person["place_id"] or ""
    title        = person["title"] or ""
    source_url   = person["source_page_url"] or ""
    biz_name     = person["business_name"] or ""
    website_url  = person["website_url"] or f"https://{domain}"
    existing_email = person["business_email"] or ""

    # Generate all variants via email_pattern
    people = [{"person_name": person_name, "title": title, "source_page_url": source_url}]
    generated = generate_candidates_for_people(
        people, website_url, existing_email, gemini_emails=None
    )

    # Also ensure we always try ALL patterns regardless of what pattern inference chose
    first, last = _name_parts(person_name)
    if first and last:
        all_raw = _generate_candidates(first, last, domain, ALL_PATTERNS)
        existing_emails_set = {c["candidate_email"] for c in generated}
        for c in all_raw:
            if c["candidate_email"] not in existing_emails_set:
                generated.append({
                    "person_name":     person_name,
                    "title":           title,
                    "source_page_url": source_url,
                    "candidate_email": c["candidate_email"],
                    "pattern_used":    c["pattern_used"],
                })

    if not generated:
        return []

    # Insert missing variants into contacts table (IGNORE if already exists)
    rows_to_insert = [
        {
            "place_id":        place_id,
            "business_name":   biz_name,
            "domain":          domain,
            "person_name":     person_name,
            "title":           title,
            "candidate_email": c["candidate_email"],
            "pattern_used":    c["pattern_used"],
            "smtp_status":     "unknown",
            "source_page_url": source_url,
        }
        for c in generated
    ]
    save_contacts(rows_to_insert)

    # Return full list including any that were already in DB
    existing = get_contacts_by_person(person_name, domain)
    return [r["candidate_email"] for r in existing]


def _print_stats(s: dict) -> None:
    print()
    print("[VERIFY] ============================================")
    print(f"[VERIFY] Total contacts in DB     : {s['total_contacts']}")
    print(f"[VERIFY] Businesses enriched      : {s['businesses_enriched']}")
    print(f"[VERIFY] Unenriched businesses    : {s['unenriched_biz']}  "
          f"(est. ~{s['est_new']} more contacts if enriched)")
    print("[VERIFY] ============================================")


def _get_stats() -> dict:
    stats         = get_contact_stats()
    unenriched    = get_unenriched_count(country="Nigeria")
    enriched_biz  = stats["businesses_enriched"]
    total_contacts = stats["total_contacts"]
    avg = (total_contacts / enriched_biz) if enriched_biz else 0
    return {
        "total_contacts":    total_contacts,
        "businesses_enriched": enriched_biz,
        "unenriched_biz":    unenriched,
        "est_new":           int(unenriched * avg),
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    api_key = os.getenv("MAILS_SO_API_KEY", "")
    if not api_key:
        print("[VERIFY] ERROR: MAILS_SO_API_KEY not set in .env")
        return

    # ── stats ─────────────────────────────────────────────────────────────────
    _print_stats(_get_stats())

    # ── phase 1: expand all variants ─────────────────────────────────────────
    persons = get_all_persons_for_verification()
    if not persons:
        print("[VERIFY] No contacts found in DB.")
        return

    print(f"[VERIFY] Expanding variants for {len(persons)} persons...")
    person_variants: dict[tuple, list[str]] = {}
    for p in persons:
        key = (p["person_name"].lower(), p["domain"].lower())
        person_variants[key] = (_expand_variants(p), p)

    # Count total emails to process (one per variant per person, minus unknowns skipped)
    total_emails = sum(len(v) for v, _ in person_variants.values())
    print(f"[VERIFY] {total_emails} total variants expanded across {len(persons)} persons")
    print()

    # ── phase 2: verify ───────────────────────────────────────────────────────
    already_done     = get_verification_results_map()   # {email: status}

    # Domains already in verification_results — skip entirely, only process new ones
    verified_domains = {email.split("@")[1].lower() for email in already_done if "@" in email}
    new_persons = {k: v for k, v in person_variants.items() if k[1] not in verified_domains}

    if not new_persons:
        print("[VERIFY] No new contacts to verify — all domains already processed.")
        return {"verified": 0, "catch_all": 0, "rejected": 0, "unknown": 0,
                "error": 0, "total_saved": len(already_done)}

    print(f"[VERIFY] {len(new_persons)} new persons to verify (skipping {len(person_variants) - len(new_persons)} on already-processed domains)")
    print()

    counter          = 0
    summary          = {"verified": 0, "catch_all": 0, "rejected": 0, "unknown": 0, "error": 0}
    unknown_domains: set[str] = set()
    catch_all_domains: set[str] = set()

    # Pre-seed domain caches from already-done results so we respect prior outcomes
    for email, status in already_done.items():
        if "@" not in email:
            continue
        domain = email.split("@")[1]
        if status == "catch_all":
            catch_all_domains.add(domain)
        elif status == "unknown":
            unknown_domains.add(domain)

    total_new_emails = sum(len(v) for v, _ in new_persons.values())
    print(f"[VERIFY] Pinging {total_new_emails} variants for {len(new_persons)} new persons")
    print()

    for (person_name, domain), (variants, person_meta) in new_persons.items():
        if not variants:
            continue

        # Check if domain already known unreachable from earlier in this run
        if domain in unknown_domains:
            counter += len(variants)
            continue

        # Check if domain already known catch_all from earlier in this run
        if domain in catch_all_domains:
            for email in variants:
                counter += 1
                print(f"{counter}/{total_emails}  {email:<38}  catch_all  [domain cached]")
                save_verification_results([{"email": email, "status": "catch_all"}])
                summary["catch_all"] += 1
            continue

        # Probe domain with first variant
        probe_email = variants[0]

        # Resume: if probe email already verified, use stored status — no API call
        if probe_email in already_done:
            probe_status = already_done[probe_email]
            counter += 1
            print(f"{counter}/{total_emails}  {probe_email:<38}  {probe_status}  [skipped]")
        else:
            probe_status = _verify_one(probe_email, api_key)
            counter += 1

        if probe_status == "quota_exceeded":
            print(f"\n[VERIFY] Quota exceeded after {counter} calls — stopping.")
            break

        if probe_status == "unknown":
            # Domain unreachable — save probe as unknown so domain is excluded next run
            unknown_domains.add(domain)
            print(f"{counter}/{total_emails}  {probe_email:<38}  unknown  [skipping domain]")
            save_verification_results([{"email": probe_email, "status": "unknown"}])
            summary["unknown"] += 1
            # Don't increment counter for remaining variants — they're dropped
            time.sleep(CALL_DELAY)
            continue

        if probe_status == "catch_all":
            catch_all_domains.add(domain)
            # Save ALL variants as catch_all
            print(f"{counter}/{total_emails}  {probe_email:<38}  catch_all")
            save_verification_results([{"email": probe_email, "status": "catch_all"}])
            summary["catch_all"] += 1
            for email in variants[1:]:
                counter += 1
                print(f"{counter}/{total_emails}  {email:<38}  catch_all  [domain cached]")
                save_verification_results([{"email": email, "status": "catch_all"}])
                summary["catch_all"] += 1
            time.sleep(CALL_DELAY)
            continue

        # Probe returned verified or rejected — save probe result
        print(f"{counter}/{total_emails}  {probe_email:<38}  {probe_status}")
        save_verification_results([{"email": probe_email, "status": probe_status}])
        summary[probe_status] = summary.get(probe_status, 0) + 1

        if probe_status == "verified":
            # Found it — no need to try other variants
            time.sleep(CALL_DELAY)
            continue

        # Probe was rejected/error — try remaining variants
        found = False
        for email in variants[1:]:
            counter += 1
            if email in already_done:
                status = already_done[email]
                print(f"{counter}/{total_emails}  {email:<38}  {status}  [skipped]")
                if status in ("verified", "catch_all", "unknown"):
                    summary[status] = summary.get(status, 0) + 1
                    if status == "unknown":
                        unknown_domains.add(email.split("@")[1])
                    elif status == "catch_all":
                        catch_all_domains.add(email.split("@")[1])
                    break
                summary[status] = summary.get(status, 0) + 1
                continue
            status = _verify_one(email, api_key)

            if status == "quota_exceeded":
                print(f"\n[VERIFY] Quota exceeded after {counter} calls — stopping.")
                return

            print(f"{counter}/{total_emails}  {email:<38}  {status}")

            if status == "unknown":
                unknown_domains.add(domain)
                save_verification_results([{"email": email, "status": "unknown"}])
                summary["unknown"] += 1
                time.sleep(CALL_DELAY)
                break

            if status == "catch_all":
                catch_all_domains.add(domain)
                save_verification_results([{"email": email, "status": "catch_all"}])
                summary["catch_all"] += 1
                time.sleep(CALL_DELAY)
                found = True
                break

            save_verification_results([{"email": email, "status": status}])
            summary[status] = summary.get(status, 0) + 1

            if status == "verified":
                found = True
                time.sleep(CALL_DELAY)
                break

            time.sleep(CALL_DELAY)

    # ── summary ───────────────────────────────────────────────────────────────
    total_saved = len(get_verification_results_map())
    print()
    print("[VERIFY] ============================================")
    print(f"[VERIFY] Processed : {counter}/{total_emails}")
    print(f"[VERIFY]   verified  : {summary['verified']}")
    print(f"[VERIFY]   catch_all : {summary['catch_all']}")
    print(f"[VERIFY]   rejected  : {summary.get('rejected', 0)}")
    print(f"[VERIFY]   unknown   : {summary['unknown']}  (domain unreachable, not saved)")
    print(f"[VERIFY]   error     : {summary.get('error', 0)}")
    print(f"[VERIFY] Total in verification_results: {total_saved}")
    print("[VERIFY] ============================================")

    summary["total_saved"] = total_saved
    return summary


def run_verification() -> dict:
    """
    Runs the full verification pipeline without any terminal output.
    For use from Streamlit. Returns the same summary dict as main().
    """
    import io
    import sys
    buf = io.StringIO()
    old_stdout, sys.stdout = sys.stdout, buf
    try:
        return main()
    finally:
        sys.stdout = old_stdout


if __name__ == "__main__":
    main()
