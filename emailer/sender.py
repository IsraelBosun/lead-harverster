# """
# emailer/sender.py

# Connects to Namecheap Private Email SMTP and sends a single plain-text
# email. Called once per recipient by the Streamlit campaign tab.
# """

# import os
# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText

# from dotenv import load_dotenv

# from utils.logger import get_logger

# load_dotenv()
# logger = get_logger(__name__)

# SMTP_HOST     = os.getenv("SMTP_HOST", "mail.privateemail.com")
# SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
# SMTP_USER     = os.getenv("SMTP_USER", "")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
# SENDER_NAME   = os.getenv("SENDER_NAME", "Bosun")


# def send_email(to_address: str, subject: str, body: str) -> tuple[bool, str]:
#     """
#     Sends a single plain-text email via SMTP STARTTLS.

#     Returns:
#         (True, "")        on success
#         (False, reason)   on failure, where reason is a short error string
#     """
#     if not SMTP_USER or not SMTP_PASSWORD:
#         msg = "SMTP_USER or SMTP_PASSWORD not set in .env"
#         logger.error(msg)
#         return False, msg

#     if not to_address or "@" not in to_address:
#         return False, f"Invalid recipient address: {to_address!r}"

#     mime = MIMEMultipart()
#     mime["From"]    = f"{SENDER_NAME} <{SMTP_USER}>"
#     mime["To"]      = to_address
#     mime["Subject"] = subject
#     mime.attach(MIMEText(body, "plain", "utf-8"))

#     try:
#         server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
#         server.starttls()
#         server.login(SMTP_USER, SMTP_PASSWORD)
#         server.sendmail(SMTP_USER, to_address, mime.as_string())
#         logger.info("Email sent OK -> %s", to_address)
#         return True, ""

#     except Exception as exc:
#         reason = str(exc)
#         logger.error("Email send failed to %s: %s", to_address, exc)
#         return False, reason

#     finally:
#         try:
#             server.quit()
#         except Exception:
#             pass






"""
emailer/sender.py

Connects to Namecheap Private Email SMTP and sends a single plain-text
email. Called once per recipient by the Streamlit campaign tab.
"""

import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote

from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

SMTP_HOST         = os.getenv("SMTP_HOST", "mail.privateemail.com")
SMTP_PORT         = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER         = os.getenv("SMTP_USER", "")
SMTP_PASSWORD     = os.getenv("SMTP_PASSWORD", "")
SENDER_NAME       = os.getenv("SENDER_NAME", "Bosun")
TRACKING_BASE_URL = os.getenv("TRACKING_BASE_URL", "")


def _to_html(body: str) -> str:
    """
    Converts a plain-text email body to HTML.
    - Escapes HTML special characters
    - Makes the WhatsApp wa.me link clickable with a clean phone number display
    - Makes bluehydralabs.com clickable
    - Converts newlines to <br>
    """
    text = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # WhatsApp link — show phone number as display text
    text = text.replace(
        "https://wa.me/2349133105749",
        '<a href="https://wa.me/2349133105749" style="color:#25D366">+234 913 310 5749</a>',
    )
    # Website link
    text = re.sub(
        r'(?<!["\'/])bluehydralabs\.com',
        '<a href="https://bluehydralabs.com" style="color:#4F46E5">bluehydralabs.com</a>',
        text,
    )
    text = text.replace("\n", "<br>\n")
    return (
        "<html><body>"
        "<p style='font-family:Arial,sans-serif;font-size:14px;line-height:1.8;"
        "color:#111;max-width:600px'>"
        f"{text}"
        "</p></body></html>"
    )


def send_email(to_address: str, subject: str, body: str) -> tuple[bool, str]:
    """
    Sends a single plain-text email via SMTP STARTTLS.

    Returns:
        (True, "")        on success
        (False, reason)   on failure, where reason is a short error string
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        msg = "SMTP_USER or SMTP_PASSWORD not set in .env"
        logger.error(msg)
        return False, msg

    if not to_address or "@" not in to_address:
        return False, f"Invalid recipient address: {to_address!r}"

    # Debug: confirm env vars loaded correctly
    logger.info("SMTP config -> host=%s, port=%s, user=%s", SMTP_HOST, SMTP_PORT, SMTP_USER)

    SENDER_EMAIL = os.getenv("SENDER_EMAIL", "bosun@bluehydralabs.com")

    mime = MIMEMultipart("alternative")
    mime["From"]    = f"{SENDER_NAME} <{SENDER_EMAIL}>"
    mime["To"]      = to_address
    mime["Subject"] = subject

    # Plain text part (fallback for clients that don't render HTML)
    mime.attach(MIMEText(body, "plain", "utf-8"))

    # HTML part — always sent so links are clickable
    html_body = _to_html(body)
    if TRACKING_BASE_URL:
        encoded_email = quote(to_address, safe="")
        pixel_url = f"{TRACKING_BASE_URL}/track/open/{encoded_email}"
        html_body = html_body.replace(
            "</body></html>",
            f"<img src='{pixel_url}' width='1' height='1' style='display:none' alt=''/></body></html>",
        )
    mime.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
        server.login(SMTP_USER, SMTP_PASSWORD)
        raw = mime.as_bytes()
        server.sendmail(SENDER_EMAIL, [to_address], raw)
        logger.info("Email sent OK -> %s", to_address)
        return True, ""

    except Exception as exc:
        reason = str(exc)
        logger.error("Email send failed to %s: %s", to_address, exc)
        return False, reason

    finally:
        try:
            server.quit()
        except Exception:
            pass