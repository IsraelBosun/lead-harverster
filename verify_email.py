"""
Quick single-email SMTP verifier.
Run: python verify_email.py
Requires: pip install dnspython
"""

import smtplib
import dns.resolver

EMAIL = "ogunkeye@allanogunkeye.com"   # <-- change this
FROM  = "test@test.com"                # sender used in probe (doesn't need to exist)

def verify(email: str) -> str:
    domain = email.split("@")[1]

    # Step 1: get MX record
    try:
        mx_records = dns.resolver.resolve(domain, "MX")
        mx_host = str(sorted(mx_records, key=lambda r: r.preference)[0].exchange).rstrip(".")
        print(f"MX: {mx_host}")
    except Exception as e:
        print(f"MX lookup failed: {e}")
        return "unknown"

    # Step 2: SMTP probe
    try:
        smtp = smtplib.SMTP(timeout=10)
        smtp.connect(mx_host, 25)
        smtp.helo("test.com")
        smtp.mail(FROM)
        code, message = smtp.rcpt(email)
        smtp.quit()
        print(f"RCPT TO response: {code} {message.decode()}")

        if code == 250:
            return "verified"
        elif code == 550:
            return "rejected"
        else:
            return "unknown"

    except Exception as e:
        print(f"SMTP error: {e}")
        return "unknown"

result = verify(EMAIL)
print(f"\nResult for {EMAIL}: {result.upper()}")
