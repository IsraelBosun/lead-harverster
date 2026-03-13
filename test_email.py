"""
Standalone email delivery test.
Run with: venv/Scripts/python.exe test_email.py
"""

import os
from dotenv import load_dotenv
from emailer.sender import send_email
from emailer.templates import render

load_dotenv()

TO = "israelbosun1@gmail.com"

print(f"[CONFIG] SMTP_HOST     : {os.getenv('SMTP_HOST')}")
print(f"[CONFIG] SMTP_PORT     : {os.getenv('SMTP_PORT')}")
print(f"[CONFIG] SMTP_USER     : {os.getenv('SMTP_USER')}")
print(f"[CONFIG] SMTP_PASSWORD : {'SET' if os.getenv('SMTP_PASSWORD') else 'NOT SET'}")
print(f"[CONFIG] Sending to    : {TO}")
print()

subject, body = render("Test Law Firm")

print("[EMAIL] Subject:")
print(f"  {subject}")
print()
print("[EMAIL] Body preview (first 3 lines):")
for line in body.splitlines()[:3]:
    print(f"  {line}")
print("  ...")
print()

print("[SEND] Attempting send...")
ok, reason = send_email(TO, subject, body)

if ok:
    print("[OK] Email sent successfully.")
    print("     Check inbox and spam folder at israelbosun1@gmail.com")
else:
    print(f"[FAIL] Send failed: {reason}")
    print()
    print("[HELP] Possible causes:")
    print("  - Port 587/465 blocked by your network firewall (bank/office WiFi)")
    print("  - Wrong SMTP_PASSWORD in .env")
    print("  - Try from home WiFi to confirm if it is a network issue")
