"""
emailer/templates.py

Plain-text cold outreach template for law firms.
Single universal template used for all leads regardless of website quality.
"""

SUBJECT = "Custom Software Solutions for {business_name}"

BODY = """\
Dear {business_name} Team,

My name is Bosun. I build custom software for law firms, including client intake systems, case management tools, document automation, internal dashboards, and mobile apps.

I came across {business_name} online and wanted to introduce myself.

Many law firms have internal processes that are repetitive, slow, or still handled manually. That is where I am able to help, by building practical tools that fit the way the firm already works.

I am not pitching a specific product. Should your firm require any software services, please reply to this email and we can arrange a time to speak.

Yours sincerely,
Bosun
CEO
Blue Hydra Labs\
"""


def render(business_name: str) -> tuple[str, str]:
    """
    Returns (subject, body) with {business_name} substituted.
    Falls back to 'Your Firm' if business_name is blank.
    """
    name = business_name.strip().title() if business_name else "Your Firm"
    return SUBJECT.format(business_name=name), BODY.format(business_name=name)
