"""
Shared utility functions for LeadHarvest.
Handles delays, user-agent rotation, and other common helpers.
"""

import os
import random
import time
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

DELAY_MIN = float(os.getenv("SCRAPE_DELAY_MIN", "2"))
DELAY_MAX = float(os.getenv("SCRAPE_DELAY_MAX", "4"))

# Pool of realistic browser user agents to rotate through
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]


def random_delay() -> None:
    """
    Sleeps for a random duration between DELAY_MIN and DELAY_MAX seconds.
    Called between each website visit to be polite and avoid detection.
    """
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    print(f"  [WAIT] Waiting {delay:.1f}s before next request...")
    time.sleep(delay)


def get_random_user_agent() -> str:
    """Returns a randomly selected user agent string."""
    return random.choice(USER_AGENTS)


def normalise_url(url: Optional[str]) -> Optional[str]:
    """
    Ensures a URL has a scheme prefix.
    Returns None if the input is empty or None.

    Args:
        url: Raw URL string, possibly missing scheme.

    Returns:
        URL with https:// prefix, or None.
    """
    if not url:
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def normalise_url_for_dedup(url: str) -> str:
    """
    Normalises a URL to a bare domain+path form for deduplication.
    Strips scheme (http/https), www prefix, and trailing slash so that
    'http://www.site.com/' and 'https://site.com' are treated as the same.

    Args:
        url: Raw URL string.

    Returns:
        Normalised string, e.g. 'site.com/about'
    """
    if not url:
        return ""
    url = url.lower().strip()
    for prefix in ("https://", "http://"):
        if url.startswith(prefix):
            url = url[len(prefix):]
            break
    if url.startswith("www."):
        url = url[4:]
    return url.rstrip("/")


def truncate(text: str, max_length: int = 80) -> str:
    """Truncates a string for clean terminal display."""
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text
