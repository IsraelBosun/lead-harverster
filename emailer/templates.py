"""
emailer/templates.py

Plain-text cold outreach template for law firms.
Single universal template used for all leads regardless of website quality.
"""

SUBJECT = "Is {business_name} losing clients after hours?"

BODY = """\
Dear {business_name} Team,

When a potential client visits your website at 9pm or during a hearing, is anyone available to answer their questions and take their details?

Most law firms lose those enquiries. The client moves on to the next firm on Google.

I have built a smart intake assistant that handles this automatically on your website. It engages visitors, finds out what they need, collects their contact details, and notifies you immediately. No manual effort on your end.

I would welcome the opportunity to show you a brief demo at your convenience.

Yours sincerely,
Bosun
Blue Hydra Labs\
"""


def render(business_name: str) -> tuple[str, str]:
    """
    Returns (subject, body) with {business_name} substituted.
    Falls back to 'Your Firm' if business_name is blank.
    """
    name = business_name.strip().title() if business_name else "Your Firm"
    return SUBJECT.format(business_name=name), BODY.format(business_name=name)
