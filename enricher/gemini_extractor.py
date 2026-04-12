"""
enricher/gemini_extractor.py

Uses Gemini Flash to extract decision-maker names and titles from raw
page text. Replaces the rules-based heuristics in team_scraper.py.

The page text is sent as-is — Gemini reads it like a human and pulls
out only real people with decision-maker roles. No regex, no blacklists.
"""

import json
import os
from google import genai
from google.genai import types
from utils.logger import get_logger

logger = get_logger(__name__)

MODEL = "gemini-2.5-flash"

DRAFT_PROMPT = """You are writing a cold outreach email from Bosun, CEO of Blue Hydra Labs.

You are writing to {person_name}, {title} at {business_name}.

Based on the website content below, write a short professional cold email that does ALL of these:
1. Opens with "Hi [first name],"
2. References something SPECIFIC you noticed on their website — a service they offer, a case they handle, a client they mention, something real. This is the most important line — it must feel personal, not generic.
3. Includes this exact sentence: "I run Blue Hydra Labs, where we build custom software and AI tools for teams like yours."
4. Mentions ONE specific thing we could build or improve for them based on what you see on their site (e.g. online booking, client portal, AI document automation, CRM, mobile app, website modernisation etc.)
5. Ends with this exact CTA line: "If this is relevant to you, simply reply or reach me directly on WhatsApp: https://wa.me/2349133105749"
6. Closes with exactly:
Yours sincerely,
Bosun
CEO, Blue Hydra Labs
bluehydralabs.com

Tone: professional but warm, not salesy. Keep the body concise — 4 to 5 sentences before the sign-off.
Structure the body with a blank line between every paragraph so it reads easily on mobile. No wall of text.

Return your response in this exact format and nothing else:
SUBJECT: <subject line here>
BODY:
<full email body here>

Website content:
"""

PROMPT = """You are extracting decision-maker contacts and email addresses from a business website page.

From the text below, do TWO things:

1. Extract ONLY real people who hold a decision-making or leadership role at the company.
   Target roles include (but are not limited to):
   - C-suite: CEO, CTO, CFO, COO, CMO, CIO, CPO, CHRO
   - Founders: Founder, Co-Founder, Owner, Co-Owner, Proprietor
   - Directors & Management: Managing Director, Director, Board Member, Chairman, Chairwoman, Executive Director, General Manager, Country Manager, Regional Manager
   - Partners: Managing Partner, Senior Partner, Partner, Principal
   - VP level: Vice President, VP, SVP, EVP, Assistant Vice President
   - Heads: Head of [anything], Lead, Team Lead
   - Professionals: Solicitor, Barrister, Attorney, Counsel, Architect, Engineer (senior/lead/principal), Consultant (senior/principal/managing), Accountant (senior/principal/managing)
   - Senior individual contributors with "Senior", "Principal", or "Lead" in their title

2. Collect ALL email addresses visible anywhere on the page (footer, contact section, team profiles, anywhere).

Rules for people:
- Only include real human names (First name + Last name minimum)
- Ignore nav links, footer items, button text, company names, section headings
- Ignore generic phrases like "Quick Links", "Book Appointment", "Our Services", "Read More"
- If someone has no clear title, skip them
- If the page has no team/people listing at all, return an empty people list

Rules for emails:
- Include every email address you can find on the page, whether personal or generic (info@, contact@, name@, etc.)
- Do not invent or guess emails — only include ones explicitly present in the text

Return ONLY a JSON object, no explanation, no markdown. Example:
{"people": [{"name": "Jude Okonkwo", "title": "Managing Partner"}, {"name": "Ada Eze", "title": "Head of Engineering"}], "emails_found": ["info@firm.com", "j.okonkwo@firm.com"]}

Page text:
"""


def extract_people_with_gemini(page_text: str, source_url: str) -> tuple[list[dict], list[str]]:
    """
    Sends page text to Gemini Flash and returns:
      - list of {person_name, title, source_page_url} dicts
      - list of email addresses found anywhere on the page

    Returns ([], []) on any error so the pipeline continues.
    """
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[GEMINI] GEMINI_API_KEY not set")
        return [], []

    # Truncate very long pages to avoid token limits (8000 chars is plenty)
    text = page_text[:8000].strip()
    if not text:
        return [], []

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=MODEL,
            contents=PROMPT + text,
            config=types.GenerateContentConfig(
                temperature=0,
                max_output_tokens=2048,
            ),
        )

        raw = response.text.strip()

        # Strip markdown code fences if Gemini wraps in ```json
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        data = json.loads(raw)
        if not isinstance(data, dict):
            logger.warning("[GEMINI] Unexpected response format from %s", source_url)
            return [], []

        # Parse people
        people = []
        for item in data.get("people", []):
            name  = str(item.get("name", "")).strip()
            title = str(item.get("title", "")).strip()
            if name and title and len(name.split()) >= 2:
                people.append({
                    "person_name":     name,
                    "title":           title,
                    "source_page_url": source_url,
                })

        # Parse emails found on the page
        emails_found = [
            str(e).strip().lower()
            for e in data.get("emails_found", [])
            if "@" in str(e)
        ]

        logger.info(
            "[GEMINI] %s => %d people, %d emails found",
            source_url, len(people), len(emails_found),
        )
        return people, emails_found

    except json.JSONDecodeError as exc:
        logger.warning("[GEMINI] JSON parse error for %s: %s", source_url, exc)
        return [], []
    except Exception as exc:
        logger.warning("[GEMINI] API error for %s: %s", source_url, exc)
        return [], []


def draft_email_with_gemini(
    person_name: str,
    title: str,
    business_name: str,
    page_text: str,
) -> dict | None:
    """
    Asks Gemini to write a personalised cold email for a specific person.

    Returns {"subject": "...", "body": "..."} or None on failure.
    """
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[GEMINI] GEMINI_API_KEY not set")
        return None

    prompt = DRAFT_PROMPT.format(
        person_name=person_name,
        title=title,
        business_name=business_name,
    )

    # Truncate page text — enough context without blowing tokens
    context = page_text[:6000].strip()

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt + context,
            config=types.GenerateContentConfig(
                temperature=0.7,   # a little warmth for email writing
                max_output_tokens=4096,
            ),
        )

        raw = response.text.strip()

        # Parse plain text format: SUBJECT: ... \n BODY: \n ...
        if "SUBJECT:" not in raw or "BODY:" not in raw:
            logger.warning("[GEMINI] Unexpected draft format for %s at %s", person_name, business_name)
            return None

        subject_part, body_part = raw.split("BODY:", 1)
        subject = subject_part.replace("SUBJECT:", "").strip()
        body    = body_part.strip()

        if not subject or not body:
            logger.warning("[GEMINI] Empty draft for %s at %s", person_name, business_name)
            return None

        logger.info("[GEMINI] Draft written for %s (%s) at %s", person_name, title, business_name)
        return {"subject": subject, "body": body}

    except json.JSONDecodeError as exc:
        logger.warning("[GEMINI] Draft JSON parse error for %s: %s", person_name, exc)
        return None
    except Exception as exc:
        logger.warning("[GEMINI] Draft API error for %s: %s", person_name, exc)
        return None
