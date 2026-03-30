"""
Generates all 7 email pattern candidates for every contact in the DB
and exports them to an Excel file.

Run: venv/Scripts/python.exe export_candidates.py
"""

import sqlite3
import pandas as pd
from enricher.email_pattern import _name_parts, _generate_candidates, ALL_PATTERNS

DB_PATH = "output/leadharvest.db"
OUT_PATH = "output/exports/email_candidates.xlsx"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
contacts = conn.execute(
    "SELECT DISTINCT person_name, title, business_name, domain FROM contacts"
).fetchall()
conn.close()

rows = []
for c in contacts:
    first, last = _name_parts(c["person_name"])
    candidates = _generate_candidates(first, last, c["domain"], ALL_PATTERNS)
    for cand in candidates:
        rows.append({
            "Person":         c["person_name"],
            "Title":          c["title"],
            "Business":       c["business_name"],
            "Domain":         c["domain"],
            "Pattern":        cand["pattern_used"],
            "Candidate Email": cand["candidate_email"],
        })

df = pd.DataFrame(rows)
df.to_excel(OUT_PATH, index=False)
print(f"Done — {len(rows)} candidates for {len(contacts)} contacts saved to {OUT_PATH}")
