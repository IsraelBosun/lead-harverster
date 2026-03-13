"""
Quick test script for the Google Places API connection.
Run this to verify the API key works before building further.
Usage: python test_places.py
"""

import json
from scraper.places import search_businesses

if __name__ == "__main__":
    print("=" * 60)
    print("  LeadHarvest - Google Places API Connection Test")
    print("=" * 60)

    results = search_businesses(category="Law Firms", city="Lagos")

    if not results:
        print("No results returned. Check your API key and quota.")
    else:
        print(f"\n{'='*60}")
        print(f"  RAW RESULTS PREVIEW ({len(results)} businesses found)")
        print(f"{'='*60}\n")

        for i, biz in enumerate(results[:5], start=1):
            print(f"[{i}] {biz.business_name}")
            print(f"     Address  : {biz.address}")
            print(f"     Phone    : {biz.phone or 'N/A'}")
            print(f"     Website  : {biz.website_url or 'N/A'}")
            print(f"     Rating   : {biz.google_rating or 'N/A'}")
            print(f"     Place ID : {biz.place_id}")
            print()

        print(f"... and {max(0, len(results) - 5)} more.")
        print(f"\n[PASSED] API connection test PASSED - {len(results)} businesses retrieved.")
