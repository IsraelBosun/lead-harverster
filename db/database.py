"""
LeadHarvest — SQLite persistence layer.

Stores every business lead so that future scrape runs can skip contacts
we have already collected.

Rules:
  - Email is the primary key; place_id is a secondary UNIQUE key
  - A business is saved if it has an email OR a place_id (at least one)
  - Businesses with neither are skipped (extremely rare edge case)
  - filter_new_businesses() separates a scrape result into new vs already-known
  - save_businesses() inserts only the new ones

The DB file lives at output/leadharvest.db (inside the project folder).
"""

import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

from utils.helpers import normalise_url_for_dedup
from utils.logger import get_logger

if TYPE_CHECKING:
    from models.business import Business

logger = get_logger(__name__)

DB_PATH = os.getenv("DB_PATH", "output/leadharvest.db")


# ── Setup ──────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Creates the DB file and the businesses table if they don't already exist.
    Safe to call multiple times — uses CREATE TABLE IF NOT EXISTS.
    """
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS businesses (
                email                  TEXT PRIMARY KEY,
                place_id               TEXT UNIQUE,
                business_name          TEXT,
                category               TEXT,
                city                   TEXT,
                phone                  TEXT,
                whatsapp               TEXT,
                website_url            TEXT,
                instagram              TEXT,
                facebook               TEXT,
                twitter                TEXT,
                google_rating          REAL,
                website_quality_score  INTEGER,
                email_source           TEXT,
                scraped_at             TEXT,
                country                TEXT DEFAULT 'Nigeria',
                timezone               TEXT DEFAULT 'Africa/Lagos'
            )
        """)
        # Migrate existing DBs that don't have the new columns yet
        for col, sql in [
            ("country",     "TEXT DEFAULT 'Nigeria'"),
            ("timezone",    "TEXT DEFAULT 'Africa/Lagos'"),
            ("enriched_at", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE businesses ADD COLUMN {col} {sql}")
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT NOT NULL,
                business_name TEXT,
                sent_at       TEXT,
                status        TEXT,
                opened_at     TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                place_id         TEXT,
                business_name    TEXT,
                domain           TEXT,
                person_name      TEXT,
                title            TEXT,
                candidate_email  TEXT UNIQUE,
                pattern_used     TEXT,
                smtp_status      TEXT DEFAULT 'unknown',
                source_page_url  TEXT,
                enriched_at      TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS drafts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                place_id         TEXT,
                business_name    TEXT,
                website_url      TEXT,
                person_name      TEXT,
                title            TEXT,
                candidate_emails TEXT,
                subject          TEXT,
                body             TEXT,
                status           TEXT DEFAULT 'pending',
                created_at       TEXT DEFAULT (datetime('now')),
                UNIQUE (person_name, place_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS verification_results (
                email       TEXT PRIMARY KEY,
                status      TEXT,
                verified_at TEXT DEFAULT (datetime('now'))
            )
        """)
        # Migrate existing campaigns table
        try:
            conn.execute("ALTER TABLE campaigns ADD COLUMN opened_at TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        # Migrate businesses table — track consecutive load failures
        try:
            conn.execute("ALTER TABLE businesses ADD COLUMN enrich_fail_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE businesses ADD COLUMN ssl_issue INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.commit()

    logger.debug("DB initialised at %s", DB_PATH)


# ── Read ───────────────────────────────────────────────────────────────────────

def get_existing_emails() -> set[str]:
    """
    Returns all email addresses currently stored in the DB as a lowercase set.
    Used to quickly check whether an incoming result is new or already known.
    """
    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT email FROM businesses WHERE email IS NOT NULL").fetchall()

    emails = {row[0].lower() for row in rows if row[0] is not None}
    logger.debug("DB contains %d existing emails", len(emails))
    return emails


def get_existing_place_ids() -> set[str]:
    """
    Returns all Google Places place_ids currently stored in the DB.
    Used to dedup businesses that have no email address.
    """
    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT place_id FROM businesses WHERE place_id IS NOT NULL AND place_id != ''"
        ).fetchall()

    ids = {row[0] for row in rows}
    logger.debug("DB contains %d existing place_ids", len(ids))
    return ids


def get_existing_website_urls() -> set[str]:
    """
    Returns normalised website URLs of every business already in the DB
    that has an email address.

    Used by the Places API fetcher to skip businesses we have already
    successfully scraped — so we keep fetching until we have enough NEW
    businesses rather than wasting time revisiting known sites.

    Returns:
        Set of normalised URL strings (no scheme, no www, no trailing slash).
    """
    init_db()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT website_url FROM businesses WHERE website_url != ''"
        ).fetchall()

    urls = {normalise_url_for_dedup(row[0]) for row in rows if row[0]}
    logger.debug("DB contains %d known website URLs", len(urls))
    return urls


def get_all_businesses() -> list[dict]:
    """
    Returns every business in the DB as a list of dicts matching
    the to_dict() column names used in Excel exports.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM businesses ORDER BY scraped_at DESC"
        ).fetchall()
    return [
        {
            "Place ID":               row["place_id"] or "",
            "Business Name":          row["business_name"] or "",
            "Category":               row["category"] or "",
            "City":                   row["city"] or "",
            "Address":                "",
            "Phone":                  row["phone"] or "",
            "Email":                  row["email"] or "",
            "Email Source":           row["email_source"] or "",
            "WhatsApp":               row["whatsapp"] or "",
            "Has Website":            "",
            "Website URL":            row["website_url"] or "",
            "Instagram":              row["instagram"] or "",
            "Facebook":               row["facebook"] or "",
            "Twitter \\ X":           row["twitter"] or "",
            "Google Rating":          row["google_rating"],
            "Website Quality Score":  row["website_quality_score"],
            "Website Issues":         "",
            "High Priority Lead":     "",
            "Scraped At":             row["scraped_at"] or "",
        }
        for row in rows
    ]


# ── Filter ─────────────────────────────────────────────────────────────────────

def filter_new_businesses(
    businesses: list["Business"],
) -> tuple[list["Business"], int]:
    """
    Splits a list of scraped businesses into new vs already-known.

    A business is considered already-known if its email OR place_id already
    exists in the DB. Both are checked so no-email businesses are also deduped.

    Args:
        businesses: Full list of scraped Business objects.

    Returns:
        Tuple of (new_businesses, skipped_count) where:
          - new_businesses: businesses not yet in the DB
          - skipped_count:  number of businesses whose email was already known
    """
    existing_emails = get_existing_emails()
    existing_place_ids = get_existing_place_ids()
    new = []
    skipped = 0

    for b in businesses:
        if b.email and b.email.lower() in existing_emails:
            skipped += 1
            logger.debug("Skipping duplicate email: %s (%s)", b.email, b.business_name)
        elif b.place_id and b.place_id in existing_place_ids:
            skipped += 1
            logger.debug("Skipping duplicate place_id: %s (%s)", b.place_id, b.business_name)
        else:
            new.append(b)

    logger.info(
        "Dedup complete | total=%d | new=%d | skipped=%d",
        len(businesses), len(new), skipped,
    )
    return new, skipped


# ── Write ──────────────────────────────────────────────────────────────────────

def save_businesses(businesses: list["Business"]) -> int:
    """
    Saves businesses to the DB using email or place_id as unique keys.

    Skips any business that:
      - Has neither email nor place_id (no unique identifier)
      - Already exists in the DB by email or place_id (INSERT OR IGNORE handles race conditions)

    Args:
        businesses: List of Business objects to persist.

    Returns:
        Number of records actually inserted.
    """
    init_db()
    existing_emails = get_existing_emails()
    existing_place_ids = get_existing_place_ids()
    inserted = 0

    with sqlite3.connect(DB_PATH) as conn:
        for b in businesses:
            # Must have at least one unique identifier to save
            if not b.email and not b.place_id:
                continue

            email_lower = b.email.lower() if b.email else None

            # Skip if already known by email or place_id
            if email_lower and email_lower in existing_emails:
                continue
            if b.place_id and b.place_id in existing_place_ids:
                continue

            conn.execute(
                """
                INSERT OR IGNORE INTO businesses
                    (email, place_id, business_name, category, city, phone, whatsapp,
                     website_url, instagram, facebook, twitter, google_rating,
                     website_quality_score, email_source, scraped_at, country, timezone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    email_lower,
                    b.place_id or None,
                    b.business_name,
                    b.category,
                    b.city,
                    b.phone,
                    b.whatsapp,
                    b.website_url,
                    b.instagram,
                    b.facebook,
                    b.twitter,
                    b.google_rating,
                    b.website_quality_score,
                    b.email_source,
                    b.scraped_at.isoformat(),
                    b.country,
                    b.timezone,
                ),
            )

            # Track locally so duplicates within the same batch are also caught
            if email_lower:
                existing_emails.add(email_lower)
            if b.place_id:
                existing_place_ids.add(b.place_id)
            inserted += 1

        conn.commit()

    logger.info("Saved %d new businesses to DB", inserted)
    return inserted


# ── Campaign tracking ───────────────────────────────────────────────────────────

def save_campaign_send(email: str, business_name: str, status: str) -> None:
    """
    Records one email send attempt in the campaigns table.
    status should be "sent" or "failed".
    """
    init_db()
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO campaigns (email, business_name, sent_at, status) VALUES (?, ?, ?, ?)",
            (email.lower(), business_name, datetime.now().isoformat(), status),
        )
        conn.commit()


def log_email_open(email: str) -> None:
    """
    Records the first time an email was opened by updating opened_at
    on the most recent sent campaign row for that address.
    Only sets opened_at once — subsequent opens are ignored.
    """
    init_db()
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE campaigns
            SET opened_at = ?
            WHERE id = (
                SELECT id FROM campaigns
                WHERE email = ? AND status = 'sent' AND opened_at IS NULL
                ORDER BY sent_at DESC
                LIMIT 1
            )
            """,
            (datetime.now().isoformat(), email.lower()),
        )
        conn.commit()


def get_today_sent_count() -> int:
    """Returns how many emails were successfully sent today."""
    init_db()
    from datetime import date
    today = date.today().isoformat()  # e.g. "2025-03-07"
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM campaigns WHERE status = 'sent' AND sent_at LIKE ?",
            (f"{today}%",),
        ).fetchone()
    return row[0] if row else 0


def get_leads_for_campaign(limit: int = 50, region: str = "All") -> list[dict]:
    """
    Returns leads from the businesses table that:
      - have an email address
      - have NOT been successfully contacted yet (no row in campaigns with status='sent')
      - optionally filtered by region (matches country against REGION_COUNTRIES)

    Returns a list of dicts with keys: email, business_name, country, timezone
    """
    from utils.timezone_utils import REGION_COUNTRIES

    init_db()

    region_clause = ""
    params: list = []

    if region and region != "All":
        countries = REGION_COUNTRIES.get(region, [])
        if countries:
            placeholders = ",".join("?" * len(countries))
            region_clause = f"AND b.country IN ({placeholders})"
            params.extend(countries)

    params.append(limit)

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            f"""
            SELECT b.email, b.business_name, b.country, b.timezone
            FROM businesses b
            WHERE b.email IS NOT NULL
              AND b.email != ''
              AND b.email NOT IN (
                  SELECT email FROM campaigns WHERE status = 'sent'
              )
              {region_clause}
            ORDER BY b.scraped_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

    return [
        {
            "email":         r[0],
            "business_name": r[1] or "",
            "country":       r[2] or "Nigeria",
            "timezone":      r[3] or "Africa/Lagos",
        }
        for r in rows
    ]


def get_available_count(region: str = "All") -> int:
    """
    Returns the count of unsent leads (with email) optionally filtered by region.
    More efficient than fetching all leads just to count them.
    """
    from utils.timezone_utils import REGION_COUNTRIES

    init_db()

    region_clause = ""
    params: list = []

    if region and region != "All":
        countries = REGION_COUNTRIES.get(region, [])
        if countries:
            placeholders = ",".join("?" * len(countries))
            region_clause = f"AND b.country IN ({placeholders})"
            params.extend(countries)

    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM businesses b
            WHERE b.email IS NOT NULL AND b.email != ''
              AND b.email NOT IN (SELECT email FROM campaigns WHERE status = 'sent')
              {region_clause}
            """,
            params,
        ).fetchone()[0]

    return count


def get_opened_leads() -> list[dict]:
    """
    Returns all leads that have opened their email, ordered by most recent open.
    Each dict has: email, business_name, sent_at, opened_at
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT c.email, c.business_name, c.sent_at, c.opened_at
            FROM campaigns c
            WHERE c.opened_at IS NOT NULL
            ORDER BY c.opened_at DESC
            """
        ).fetchall()
    return [
        {
            "email":         r[0],
            "business_name": r[1] or "",
            "sent_at":       r[2][:16].replace("T", " ") if r[2] else "",
            "opened_at":     r[3][:16].replace("T", " ") if r[3] else "",
        }
        for r in rows
    ]


def get_campaign_status_map() -> dict[str, str]:
    """
    Returns a dict mapping each emailed address to the date it was sent.
    Only includes rows with status='sent'. Used to annotate Excel exports.
    Example: {"info@firma.com": "2026-03-13"}
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT email, sent_at FROM campaigns WHERE status = 'sent'"
        ).fetchall()
    return {row[0].lower(): row[1][:10] for row in rows if row[0] and row[1]}


def get_enrichment_status(limit: int = 100) -> list[dict]:
    """
    Returns a status table for the Streamlit enrichment UI.
    Shows all businesses with their enrichment state and contact count.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT
                b.business_name,
                b.website_url,
                b.country,
                CASE
                    WHEN b.website_url IS NULL OR b.website_url = '' THEN 'No website'
                    WHEN b.enriched_at IS NOT NULL THEN 'Done'
                    ELSE 'Pending'
                END AS status,
                b.enriched_at,
                COUNT(c.id) AS contacts_found
            FROM businesses b
            LEFT JOIN contacts c ON c.place_id = b.place_id
            WHERE b.country = 'Nigeria'
            GROUP BY b.place_id
            ORDER BY
                CASE WHEN b.enriched_at IS NOT NULL THEN 0 ELSE 1 END,
                b.enriched_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "business_name":  r[0] or "",
            "website_url":    r[1] or "",
            "country":        r[2] or "",
            "status":         r[3],
            "enriched_at":    r[4][:16].replace("T", " ") if r[4] else "",
            "contacts_found": r[5],
        }
        for r in rows
    ]


def get_unenriched_count(country: str = "Nigeria") -> int:
    """Returns the count of businesses with a website that haven't been enriched yet."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        params: list = []
        country_clause = ""
        if country and country != "All":
            country_clause = "AND country = ?"
            params.append(country)
        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM businesses
            WHERE website_url IS NOT NULL AND website_url != ''
              AND enriched_at IS NULL
              {country_clause}
            """,
            params,
        ).fetchone()[0]
    return count


def get_unenriched_businesses(limit: int = 10, country: str = "Nigeria") -> list[dict]:
    """
    Returns businesses that have a website_url but have not yet been enriched
    (enriched_at IS NULL). Prioritises the given country, falls back to all
    countries if not enough rows exist for that country.
    Returns dicts with place_id, business_name, website_url, email.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        params: list = []
        country_clause = ""
        if country and country != "All":
            country_clause = "AND country = ?"
            params.append(country)
        params.append(limit)
        rows = conn.execute(
            f"""
            SELECT place_id, business_name, website_url, email
            FROM businesses
            WHERE website_url IS NOT NULL AND website_url != ''
              AND enriched_at IS NULL
              {country_clause}
            ORDER BY scraped_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [
        {
            "place_id":      r[0] or "",
            "business_name": r[1] or "",
            "website_url":   r[2] or "",
            "email":         r[3] or "",
        }
        for r in rows
    ]


def mark_business_enriched(place_id: str) -> None:
    """Stamps enriched_at on the businesses row so we don't re-process it."""
    init_db()
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE businesses SET enriched_at = ? WHERE place_id = ?",
            (datetime.now().isoformat(), place_id),
        )
        conn.commit()


def mark_ssl_issue(place_id: str) -> None:
    """Flags the business as having an SSL certificate problem on their website."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE businesses SET ssl_issue = 1 WHERE place_id = ?",
            (place_id,),
        )
        conn.commit()


def increment_enrich_fail_count(place_id: str) -> int:
    """
    Increments the enrich_fail_count for a business and returns the new count.
    Call this whenever a site fails to load during enrichment.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE businesses SET enrich_fail_count = COALESCE(enrich_fail_count, 0) + 1 WHERE place_id = ?",
            (place_id,),
        )
        conn.commit()
        row = conn.execute(
            "SELECT enrich_fail_count FROM businesses WHERE place_id = ?",
            (place_id,),
        ).fetchone()
    return row[0] if row else 1


def save_contacts(contacts: list[dict]) -> int:
    """
    Inserts contact rows. Skips duplicates (UNIQUE on candidate_email).
    Each dict must have: place_id, business_name, domain, person_name, title,
    candidate_email, pattern_used, smtp_status, source_page_url.
    Returns number of rows inserted.
    """
    init_db()
    from datetime import datetime
    now = datetime.now().isoformat()
    inserted = 0
    with sqlite3.connect(DB_PATH) as conn:
        for c in contacts:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO contacts
                        (place_id, business_name, domain, person_name, title,
                         candidate_email, pattern_used, smtp_status, source_page_url, enriched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        c.get("place_id", ""),
                        c.get("business_name", ""),
                        c.get("domain", ""),
                        c.get("person_name", ""),
                        c.get("title", ""),
                        c.get("candidate_email", "").lower(),
                        c.get("pattern_used", ""),
                        c.get("smtp_status", "unknown"),
                        c.get("source_page_url", ""),
                        now,
                    ),
                )
                inserted += conn.execute("SELECT changes()").fetchone()[0]
            except sqlite3.IntegrityError:
                pass
        conn.commit()
    return inserted


def get_all_contacts() -> list[dict]:
    """Returns every contact row ordered by most recently enriched."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM contacts ORDER BY enriched_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_unverified_contacts(limit: int = 500) -> list[dict]:
    """Returns contacts with smtp_status = 'unverified' that haven't been sent to."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.*
            FROM contacts c
            WHERE c.smtp_status = 'unverified'
              AND c.candidate_email NOT IN (
                  SELECT email FROM campaigns WHERE status = 'sent'
              )
            ORDER BY c.enriched_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_contacts_to_verify(limit: int = 1000) -> list[dict]:
    """Returns contacts that need mails.so verification (smtp_status unknown/error/unverified)
    and have not yet been sent to."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.candidate_email, c.person_name, c.title, c.business_name,
                   c.domain, c.smtp_status
            FROM contacts c
            WHERE c.smtp_status IN ('unknown', 'error', 'unverified')
              AND c.candidate_email NOT IN (
                  SELECT email FROM campaigns WHERE status = 'sent'
              )
            ORDER BY c.enriched_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_persons_for_verification() -> list[dict]:
    """
    Returns one row per unique (person_name, domain) pair across all contacts,
    joined with the business email and website_url for pattern inference.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                c.person_name,
                c.domain,
                c.place_id,
                MIN(c.title)          AS title,
                MIN(c.source_page_url) AS source_page_url,
                MIN(c.business_name)  AS business_name,
                b.email               AS business_email,
                b.website_url         AS website_url
            FROM contacts c
            LEFT JOIN businesses b ON b.place_id = c.place_id
            GROUP BY c.person_name, c.domain
            ORDER BY c.domain, c.person_name
            """
        ).fetchall()
    return [dict(r) for r in rows]


def get_contacts_by_person(person_name: str, domain: str) -> list[dict]:
    """Returns all existing candidate emails for a given person+domain."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT candidate_email, pattern_used, smtp_status
            FROM contacts
            WHERE LOWER(person_name) = LOWER(?)
              AND LOWER(domain)      = LOWER(?)
            ORDER BY enriched_at ASC
            """,
            (person_name, domain),
        ).fetchall()
    return [dict(r) for r in rows]


def save_verification_results(results: list[dict]) -> None:
    """
    Upserts rows into verification_results (email + status).
    Re-runs overwrite previous results for the same email.
    Each dict must have: email, status.
    """
    init_db()
    from datetime import datetime
    now = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        for r in results:
            conn.execute(
                """
                INSERT INTO verification_results (email, status, verified_at)
                VALUES (?, ?, ?)
                ON CONFLICT(email) DO UPDATE SET
                    status      = excluded.status,
                    verified_at = excluded.verified_at
                """,
                (r["email"].lower(), r["status"], now),
            )
        conn.commit()


def get_verification_results_map() -> dict[str, str]:
    """Returns a dict of {email: status} for all rows in verification_results."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT email, status FROM verification_results"
        ).fetchall()
    return {row[0].lower(): row[1] for row in rows}


def update_contact_smtp_status(candidate_email: str, smtp_status: str) -> None:
    """Updates the smtp_status for a single contact row."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE contacts SET smtp_status = ? WHERE candidate_email = ?",
            (smtp_status, candidate_email.lower()),
        )
        conn.commit()


def get_contact_stats() -> dict:
    """Summary counts for the Decision Makers tab."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        verified = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE smtp_status = 'verified'"
        ).fetchone()[0]
        catch_all = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE smtp_status = 'catch_all'"
        ).fetchone()[0]
        businesses_enriched = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE enriched_at IS NOT NULL"
        ).fetchone()[0]
    return {
        "total_contacts":       total,
        "verified":             verified,
        "catch_all":            catch_all,
        "businesses_enriched":  businesses_enriched,
    }


def get_verification_stats() -> dict:
    """Summary counts from the verification_results table."""
    init_db()
    from datetime import date
    today = date.today().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        total     = conn.execute("SELECT COUNT(*) FROM verification_results").fetchone()[0]
        verified  = conn.execute("SELECT COUNT(*) FROM verification_results WHERE status='verified'").fetchone()[0]
        catch_all = conn.execute("SELECT COUNT(*) FROM verification_results WHERE status='catch_all'").fetchone()[0]
        rejected  = conn.execute("SELECT COUNT(*) FROM verification_results WHERE status='rejected'").fetchone()[0]
        not_emailed = conn.execute("""
            SELECT COUNT(*) FROM verification_results
            WHERE status IN ('verified','catch_all')
              AND email NOT IN (SELECT email FROM campaigns WHERE status='sent')
        """).fetchone()[0]
        sent_today = conn.execute(
            "SELECT COUNT(*) FROM campaigns WHERE status='sent' AND sent_at LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]
        sent_total = conn.execute(
            "SELECT COUNT(DISTINCT email) FROM campaigns WHERE status='sent'"
        ).fetchone()[0]
        pending_verification = conn.execute("""
            SELECT COUNT(*) FROM contacts
            WHERE LOWER(candidate_email) NOT IN (SELECT email FROM verification_results)
        """).fetchone()[0]
        new_persons_to_verify = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT person_name, LOWER(domain) AS ldomain
                FROM contacts
                WHERE LOWER(domain) NOT IN (
                    SELECT DISTINCT LOWER(SUBSTR(email, INSTR(email, '@')+1))
                    FROM verification_results
                    WHERE email LIKE '%@%'
                )
            )
        """).fetchone()[0]
    return {
        "total":                total,
        "verified":             verified,
        "catch_all":            catch_all,
        "rejected":             rejected,
        "not_emailed":          not_emailed,
        "sent_today":           sent_today,
        "sent_total":           sent_total,
        "pending_verification": pending_verification,
        "new_persons_to_verify": new_persons_to_verify,
    }


def get_sendable_contacts() -> list[dict]:
    """
    Returns verified + catch_all contacts from verification_results not yet emailed,
    joined with contacts for display. Used in the Outreach and Enrich tabs.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                vr.email,
                vr.status,
                vr.verified_at,
                c.person_name,
                c.title,
                c.business_name,
                c.domain,
                COALESCE(b.ssl_issue, 0) AS ssl_issue
            FROM verification_results vr
            LEFT JOIN contacts c ON LOWER(c.candidate_email) = LOWER(vr.email)
            LEFT JOIN businesses b ON b.place_id = c.place_id
            WHERE vr.status IN ('verified','catch_all')
              AND vr.email NOT IN (SELECT email FROM campaigns WHERE status='sent')
            ORDER BY vr.status, c.business_name, c.person_name
        """).fetchall()
    return [dict(r) for r in rows]


def get_verified_contacts_without_drafts(limit: int = 50) -> list[dict]:
    """
    Returns sendable contacts (verified/catch_all) that don't yet have a draft,
    joined with website_url for Gemini context. Used for draft generation.
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                c.place_id,
                c.business_name,
                b.website_url,
                c.person_name,
                c.title,
                GROUP_CONCAT(vr.email, ',') AS candidate_emails
            FROM verification_results vr
            LEFT JOIN contacts c  ON LOWER(c.candidate_email) = LOWER(vr.email)
            LEFT JOIN businesses b ON b.place_id = c.place_id
            WHERE vr.status = 'verified'
              AND (c.person_name || '|' || COALESCE(c.place_id,'')) NOT IN (
                  SELECT person_name || '|' || COALESCE(place_id,'') FROM drafts
              )
            GROUP BY c.person_name, c.place_id
            ORDER BY c.business_name, c.person_name
            LIMIT ?
        """, (limit,)).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        row["candidate_emails"] = row["candidate_emails"].split(",") if row["candidate_emails"] else []
        result.append(row)
    return result



def get_campaign_stats() -> dict:
    """
    Returns a summary dict for the Campaigns tab:
      total_with_email, already_contacted, available_to_send, sent_today
    """
    init_db()
    from datetime import date
    today = date.today().isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        total_with_email = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE email IS NOT NULL AND email != ''"
        ).fetchone()[0]

        already_contacted = conn.execute(
            "SELECT COUNT(DISTINCT email) FROM campaigns WHERE status = 'sent'"
        ).fetchone()[0]

        sent_today = conn.execute(
            "SELECT COUNT(*) FROM campaigns WHERE status = 'sent' AND sent_at LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]

        total_opens = conn.execute(
            "SELECT COUNT(DISTINCT email) FROM campaigns WHERE opened_at IS NOT NULL"
        ).fetchone()[0]

    available = max(0, total_with_email - already_contacted)
    return {
        "total_with_email":  total_with_email,
        "already_contacted": already_contacted,
        "available_to_send": available,
        "sent_today":        sent_today,
        "total_opens":       total_opens,
    }


# ── Drafts ─────────────────────────────────────────────────────────────────────

def get_contacts_without_drafts() -> list[dict]:
    """
    Returns one row per person per business that doesn't yet have a draft.
    All candidate emails for that person are aggregated into a list.
    Each row has: place_id, business_name, website_url, person_name,
    title, candidate_emails (list of str).
    """
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                c.place_id,
                c.business_name,
                b.website_url,
                c.person_name,
                c.title,
                GROUP_CONCAT(c.candidate_email, ',') AS candidate_emails
            FROM contacts c
            LEFT JOIN businesses b ON c.place_id = b.place_id
            WHERE (c.person_name || '|' || c.place_id) NOT IN (
                SELECT person_name || '|' || place_id FROM drafts
            )
            GROUP BY c.person_name, c.place_id
            ORDER BY c.business_name, c.person_name
        """).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        row["candidate_emails"] = row["candidate_emails"].split(",") if row["candidate_emails"] else []
        result.append(row)
    return result


def save_drafts(drafts: list[dict]) -> int:
    """
    Inserts draft rows. One row per person per business (UNIQUE on person_name + place_id).
    Each dict must have: place_id, business_name, website_url, person_name,
    title, candidate_emails (list), subject, body.
    Returns number of rows inserted.
    """
    import json as _json
    init_db()
    inserted = 0
    with sqlite3.connect(DB_PATH) as conn:
        for d in drafts:
            try:
                emails = d.get("candidate_emails", [])
                conn.execute(
                    """
                    INSERT OR IGNORE INTO drafts
                        (place_id, business_name, website_url, person_name,
                         title, candidate_emails, subject, body, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """,
                    (
                        d.get("place_id", ""),
                        d.get("business_name", ""),
                        d.get("website_url", ""),
                        d.get("person_name", ""),
                        d.get("title", ""),
                        _json.dumps(emails),
                        d.get("subject", ""),
                        d.get("body", ""),
                    ),
                )
                inserted += conn.execute("SELECT changes()").fetchone()[0]
            except sqlite3.IntegrityError:
                pass
        conn.commit()
    return inserted


def get_all_drafts() -> list[dict]:
    """Returns all draft rows ordered by most recently created."""
    import json as _json
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM drafts ORDER BY created_at DESC"
        ).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        try:
            row["candidate_emails"] = _json.loads(row.get("candidate_emails") or "[]")
        except Exception:
            row["candidate_emails"] = []
        result.append(row)
    return result


def update_draft(draft_id: int, subject: str, body: str) -> None:
    """Updates subject and body of a draft (for inline editing)."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE drafts SET subject = ?, body = ? WHERE id = ?",
            (subject, body, draft_id),
        )
        conn.commit()


def mark_draft_sent(draft_id: int) -> None:
    """Marks a draft as sent."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE drafts SET status = 'sent' WHERE id = ?",
            (draft_id,),
        )
        conn.commit()


def delete_draft(draft_id: int) -> None:
    """Permanently deletes a draft row."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM drafts WHERE id = ?", (draft_id,))
        conn.commit()


def get_draft_stats() -> dict:
    """Summary counts for the drafts section."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        total   = conn.execute("SELECT COUNT(*) FROM drafts").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM drafts WHERE status = 'pending'").fetchone()[0]
        sent    = conn.execute("SELECT COUNT(*) FROM drafts WHERE status = 'sent'").fetchone()[0]
    return {"total": total, "pending": pending, "sent": sent, "emails_sent": sent}
