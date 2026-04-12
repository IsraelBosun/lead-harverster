"""
Quick test for the mails.so email validation integration.

Run with:
    venv/Scripts/python.exe test_mailsso.py
"""


from dotenv import load_dotenv
load_dotenv()

from enricher.smtp_verifier import verify_candidates

test_candidates = [
    {"candidate_email": "israelbosun1@gmail.com",          "pattern_used": "first"},
    {"candidate_email": "notarealaddress99999@gmail.com", "pattern_used": "first.last"},
    {"candidate_email": "invalid-not-an-email",    "pattern_used": "first"},
]

print("Testing mails.so verification...\n")
results = verify_candidates(test_candidates)

for r in results:
    print(f"  {r['candidate_email']:<45} => {r['smtp_status']}")