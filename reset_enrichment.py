"""
reset_enrichment.py

Wipes contacts and drafts, resets enriched_at on all businesses
EXCEPT those already emailed (in campaigns with status='sent').

Run with:
    venv/Scripts/python.exe reset_enrichment.py
"""

import sqlite3
from db.database import DB_PATH, init_db

init_db()

with sqlite3.connect(DB_PATH) as conn:
    # How many contacts will be deleted
    contacts_count = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]

    # How many drafts will be deleted
    drafts_count = conn.execute("SELECT COUNT(*) FROM drafts").fetchone()[0]

    # How many businesses will be reset (excluding already-emailed ones)
    reset_count = conn.execute("""
        SELECT COUNT(*) FROM businesses
        WHERE enriched_at IS NOT NULL
          AND (email IS NULL OR email NOT IN (
              SELECT email FROM campaigns WHERE status = 'sent'
          ))
    """).fetchone()[0]

    # How many are protected (already emailed — won't be touched)
    protected_count = conn.execute("""
        SELECT COUNT(*) FROM businesses
        WHERE email IN (SELECT email FROM campaigns WHERE status = 'sent')
    """).fetchone()[0]

    print(f"About to:")
    print(f"  - Delete {contacts_count} contacts")
    print(f"  - Delete {drafts_count} drafts")
    print(f"  - Reset enriched_at on {reset_count} businesses (queue them for re-enrichment)")
    print(f"  - Protect {protected_count} businesses you already emailed (left untouched)")
    print()

    confirm = input("Type YES to proceed: ").strip()
    if confirm != "YES":
        print("Aborted.")
        exit()

    # Delete contacts
    conn.execute("DELETE FROM contacts")

    # Delete drafts
    conn.execute("DELETE FROM drafts")

    # Reset enriched_at — skip businesses already emailed
    conn.execute("""
        UPDATE businesses
        SET enriched_at = NULL
        WHERE email IS NULL OR email NOT IN (
            SELECT email FROM campaigns WHERE status = 'sent'
        )
    """)

    conn.commit()

print()
print("Done.")
print(f"  Contacts deleted:  {contacts_count}")
print(f"  Drafts deleted:    {drafts_count}")
print(f"  Businesses reset:  {reset_count}")
print(f"  Protected:         {protected_count}")
