"""
Quick test: run the team scraper against a single URL and print results.

Usage:
    venv/Scripts/python.exe test_scraper.py
"""

from dotenv import load_dotenv
load_dotenv()

from enricher.team_scraper import scrape_team_page, LOAD_FAILED

URL = "https://www.accessbankplc.com/"

print(f"[TEST] Scraping: {URL}")
result = scrape_team_page(URL)

if result is LOAD_FAILED:
    print("[TEST] Homepage failed to load.")
else:
    people, emails_found = result
    print(f"\n[TEST] Emails spotted on site: {emails_found or 'none'}\n")
    if not people:
        print("[TEST] No decision-makers found.")
    else:
        print(f"[TEST] Found {len(people)} decision-makers:\n")
        for p in people:
            print(f"  {p['person_name']} — {p['title']}")
            print(f"    Source: {p['source_page_url']}")
