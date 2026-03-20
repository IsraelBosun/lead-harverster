"""
Timezone utilities for LeadHarvest.

Maps countries to IANA timezones, groups them into regions, and checks
whether a given timezone is currently within business hours (9am-6pm).
Uses Python 3.9+ built-in zoneinfo — no extra package needed.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

# Country → IANA timezone (one representative zone per country)
COUNTRY_TIMEZONES: dict[str, str] = {
    # Africa
    "Nigeria":      "Africa/Lagos",
    "Ghana":        "Africa/Accra",
    "Kenya":        "Africa/Nairobi",
    "South Africa": "Africa/Johannesburg",
    "Egypt":        "Africa/Cairo",
    # Americas
    "United States": "America/New_York",
    "Canada":        "America/Toronto",
    "Brazil":        "America/Sao_Paulo",
    "Mexico":        "America/Mexico_City",
    # Asia
    "China":       "Asia/Shanghai",
    "Japan":       "Asia/Tokyo",
    "India":       "Asia/Kolkata",
    "Singapore":   "Asia/Singapore",
    "South Korea": "Asia/Seoul",
    "Indonesia":   "Asia/Jakarta",
    "Philippines": "Asia/Manila",
    "Thailand":    "Asia/Bangkok",
    "Malaysia":    "Asia/Kuala_Lumpur",
    "Vietnam":     "Asia/Ho_Chi_Minh",
    "Pakistan":    "Asia/Karachi",
    "Bangladesh":  "Asia/Dhaka",
    # Europe
    "United Kingdom": "Europe/London",
    "Germany":        "Europe/Berlin",
    "France":         "Europe/Paris",
    # Oceania
    "Australia":   "Australia/Sydney",
    "New Zealand": "Pacific/Auckland",
}

# Region → list of countries belonging to it
REGION_COUNTRIES: dict[str, list[str]] = {
    "Nigeria":  ["Nigeria"],
    "Africa":   ["Nigeria", "Ghana", "Kenya", "South Africa", "Egypt"],
    "Americas": ["United States", "Canada", "Brazil", "Mexico"],
    "Asia": [
        "China", "Japan", "India", "Singapore", "South Korea",
        "Indonesia", "Philippines", "Thailand", "Malaysia",
        "Vietnam", "Pakistan", "Bangladesh",
    ],
    "Europe":  ["United Kingdom", "Germany", "France"],
    "Oceania": ["Australia", "New Zealand"],
}

# Ordered list of all scrapable countries for the UI dropdown
SCRAPE_COUNTRIES: list[str] = sorted(COUNTRY_TIMEZONES.keys())

# Region cards shown in the campaign tab (one representative timezone per region)
REGION_CARDS: list[dict] = [
    {"region": "Nigeria",  "rep_timezone": "Africa/Lagos"},
    {"region": "Americas", "rep_timezone": "America/New_York"},
    {"region": "Asia",     "rep_timezone": "Asia/Tokyo"},
    {"region": "Europe",   "rep_timezone": "Europe/London"},
]


def get_timezone(country: str) -> str:
    """Returns the IANA timezone string for a country, defaulting to Africa/Lagos."""
    return COUNTRY_TIMEZONES.get(country, "Africa/Lagos")


def get_region(country: str) -> str:
    """Returns the region label for a country, defaulting to 'Nigeria'."""
    for region, countries in REGION_COUNTRIES.items():
        if country in countries:
            return region
    return "Nigeria"


def is_work_hours(timezone_str: str, start_hour: int = 9, end_hour: int = 18) -> bool:
    """
    Returns True if the current local time in timezone_str is within
    [start_hour, end_hour) — default 9am to 6pm.
    Returns True on any error so sends are never silently suppressed.
    """
    try:
        tz = ZoneInfo(timezone_str)
        now = datetime.now(tz)
        return start_hour <= now.hour < end_hour
    except Exception:
        return True


def get_local_time_str(timezone_str: str) -> str:
    """Returns the current local time in timezone_str formatted as '10:30 AM'."""
    try:
        tz = ZoneInfo(timezone_str)
        return datetime.now(tz).strftime("%I:%M %p")
    except Exception:
        return "unknown"


def get_region_work_status() -> list[dict]:
    """
    Returns a list of status dicts for each REGION_CARD:
        region, timezone, local_time, in_work_hours
    """
    return [
        {
            "region":        card["region"],
            "timezone":      card["rep_timezone"],
            "local_time":    get_local_time_str(card["rep_timezone"]),
            "in_work_hours": is_work_hours(card["rep_timezone"]),
        }
        for card in REGION_CARDS
    ]
