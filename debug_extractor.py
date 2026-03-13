"""
Diagnostic script — tests our extractor against a single URL.
Run with: venv/Scripts/python.exe debug_extractor.py
"""
import asyncio
import os
import sys

os.environ.setdefault("TIMEOUT_PER_SITE", "15")

from dotenv import load_dotenv
load_dotenv()

from playwright.async_api import async_playwright
from scraper.extractor import extract_contacts
from scraper.website import _find_contact_links

TEST_URL = "https://countryhillattorneys.com.ng/"

async def main():
    print(f"\n[TEST] Fetching: {TEST_URL}\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        print("[STEP 1] Loading homepage...")
        await page.goto(TEST_URL, wait_until="domcontentloaded", timeout=15000)

        print("[STEP 2] Scrolling to bottom and waiting for JS to render...")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.5)

        html = await page.content()
        print(f"[STEP 3] Captured HTML — {len(html)} characters\n")

        # Check if email is literally in the raw HTML
        target = "countryhillattorneys.com.ng"
        if target in html:
            print(f"[CHECK] Domain '{target}' IS present in the raw HTML")
            # Find the surrounding context
            idx = html.find(target)
            print(f"        Context: ...{html[max(0,idx-60):idx+60]}...\n")
        else:
            print(f"[CHECK] Domain '{target}' is NOT in the raw HTML at all — JS may be loading it\n")

        # Run our extractor
        print("[STEP 4] Running extract_contacts()...")
        contacts = extract_contacts(html, source_url=TEST_URL)
        print(f"  email     : {contacts['email'] or '(none found)'}")
        print(f"  phone     : {contacts['phone'] or '(none found)'}")
        print(f"  whatsapp  : {contacts['whatsapp'] or '(none found)'}")
        print(f"  instagram : {contacts['instagram'] or '(none found)'}")
        print(f"  facebook  : {contacts['facebook'] or '(none found)'}")
        print(f"  twitter   : {contacts['twitter'] or '(none found)'}")

        # Show what contact pages the crawler would visit
        print("\n[STEP 5] Contact pages discovered from homepage navigation:")
        links = _find_contact_links(html, TEST_URL)
        if links:
            for link in links:
                print(f"  {link}")
        else:
            print("  (none found — would fall back to hardcoded paths)")

        await context.close()
        await browser.close()

    print("\n[DONE]\n")

asyncio.run(main())
