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
        for col, default in [("country", "Nigeria"), ("timezone", "Africa/Lagos")]:
            try:
                conn.execute(
                    f"ALTER TABLE businesses ADD COLUMN {col} TEXT DEFAULT '{default}'"
                )
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT NOT NULL,
                business_name TEXT,
                sent_at       TEXT,
                status        TEXT
            )
        """)
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

    available = max(0, total_with_email - already_contacted)
    return {
        "total_with_email":  total_with_email,
        "already_contacted": already_contacted,
        "available_to_send": available,
        "sent_today":        sent_today,
    }
