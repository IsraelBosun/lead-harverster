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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.privateemail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SENDER_NAME   = os.getenv("SENDER_NAME", "Bosun")


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

    mime = MIMEMultipart()
    mime["From"]    = SMTP_USER                  # plain address, matches DKIM
    mime["To"]      = to_address
    mime["Bcc"]     = SMTP_USER
    mime["Subject"] = subject
    mime.attach(MIMEText(body, "plain", "utf-8"))

    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [to_address, SMTP_USER], mime.as_string())
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