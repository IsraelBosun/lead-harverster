"""
startup.py

Runs before the Streamlit app starts on Render.
Seeds the persistent disk DB from the repo copy on first deploy,
then leaves it alone on all subsequent deploys.
"""

import os
import shutil

REPO_DB   = "output/leadharvest.db"
DISK_DB   = os.getenv("DB_PATH", "/data/leadharvest.db")


def main():
    if DISK_DB == REPO_DB:
        # Running locally — nothing to do
        print("[STARTUP] Local mode, skipping seed.")
        return

    if os.path.exists(DISK_DB):
        size = os.path.getsize(DISK_DB)
        print(f"[STARTUP] DB already exists at {DISK_DB} ({size:,} bytes) — skipping seed.")
        return

    if os.path.exists(REPO_DB):
        os.makedirs(os.path.dirname(DISK_DB), exist_ok=True)
        shutil.copy2(REPO_DB, DISK_DB)
        size = os.path.getsize(DISK_DB)
        print(f"[STARTUP] Seeded DB from repo to {DISK_DB} ({size:,} bytes).")
    else:
        print(f"[STARTUP] No repo DB found at {REPO_DB} — fresh DB will be created by init_db().")


if __name__ == "__main__":
    main()
