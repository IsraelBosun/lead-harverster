"""
Business data model for LeadHarvest.
Represents a single scraped business with all contact and quality data.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Business:
    """
    A single business result from a scrape run.

    Fields from Google Places API:
        business_name, category, city, address, phone, website_url, google_rating

    Fields extracted from the business website:
        email, whatsapp, instagram, facebook, twitter

    Scoring fields (computed by scorer.py):
        website_quality_score, website_issues
    """

    # --- Core identity ---
    business_name: str = ""
    category: str = ""
    city: str = ""

    # --- From Google Places API ---
    address: str = ""
    phone: str = ""
    website_url: str = ""
    google_rating: Optional[float] = None
    place_id: str = ""

    # --- Extracted from business website ---
    email: str = ""
    email_source: str = ""      # "website", "facebook", "instagram", "twitter", or ""
    whatsapp: str = ""
    instagram: str = ""
    facebook: str = ""
    twitter: str = ""

    # --- Website quality scoring ---
    has_website: bool = True                # False if no website URL in Google Places
    website_quality_score: int = 0          # 0–100, lower = better lead
    website_issues: list = field(default_factory=list)  # e.g. ["No SSL", "No mobile"]

    # --- Meta ---
    scraped_at: datetime = field(default_factory=datetime.now)

    def is_high_priority(self) -> bool:
        """
        Returns True if this business is a strong lead.
        No website at all, or a quality score under 50, both qualify.
        """
        return not self.has_website or self.website_quality_score < 50

    def to_dict(self) -> dict:
        """
        Converts the Business instance to a flat dictionary suitable
        for writing to a Pandas DataFrame or Excel row.
        """
        return {
            "Place ID": self.place_id,
            "Business Name": self.business_name,
            "Category": self.category,
            "City": self.city,
            "Address": self.address,
            "Phone": self.phone,
            "Email": self.email,
            "Email Source": self.email_source,
            "WhatsApp": self.whatsapp,
            "Has Website": "Yes" if self.has_website else "No",
            "Website URL": self.website_url,
            "Instagram": self.instagram,
            "Facebook": self.facebook,
            "Twitter / X": self.twitter,
            "Google Rating": self.google_rating,
            "Website Quality Score": self.website_quality_score,
            "Website Issues": ", ".join(self.website_issues) if self.website_issues else "",
            "High Priority Lead": "Yes" if self.is_high_priority() else "No",
            "Scraped At": self.scraped_at.strftime("%Y-%m-%d %H:%M:%S"),
        }
