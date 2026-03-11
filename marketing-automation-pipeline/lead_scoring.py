"""
Lead Scoring Engine

Computes a composite engagement score (0-100) for each lead
based on email engagement, website activity, and content interaction.
Segments leads into Hot/Warm/Cold categories for marketing prioritization.
"""


# Scoring weights for each engagement factor
WEIGHTS = {
    "email_open_rate": 0.20,
    "click_through_rate": 0.30,
    "page_visit_score": 0.25,
    "download_score": 0.25,
}

# Segment thresholds
HOT_THRESHOLD = 70
WARM_THRESHOLD = 40


def compute_lead_score(lead):
    """
    Compute engagement score for a single lead.

    Args:
        lead: dict with keys emails_sent, emails_opened, links_clicked,
              page_visits, content_downloads

    Returns:
        float: score between 0 and 100
    """
    emails_sent = max(lead.get("emails_sent", 0), 1)
    emails_opened = lead.get("emails_opened", 0)
    links_clicked = lead.get("links_clicked", 0)
    page_visits = lead.get("page_visits", 0)
    downloads = lead.get("content_downloads", 0)

    # Normalize each metric to 0-100 scale
    open_rate = min((emails_opened / emails_sent) * 100, 100)
    ctr = min((links_clicked / max(emails_opened, 1)) * 100, 100)
    visit_score = min((page_visits / 20) * 100, 100)  # 20+ visits = max
    download_score = min((downloads / 3) * 100, 100)  # 3+ downloads = max

    # Weighted composite
    score = (
        open_rate * WEIGHTS["email_open_rate"]
        + ctr * WEIGHTS["click_through_rate"]
        + visit_score * WEIGHTS["page_visit_score"]
        + download_score * WEIGHTS["download_score"]
    )

    return round(min(score, 100), 2)


def classify_segment(score):
    """Classify a lead into Hot/Warm/Cold segment based on score."""
    if score >= HOT_THRESHOLD:
        return "Hot"
    elif score >= WARM_THRESHOLD:
        return "Warm"
    else:
        return "Cold"


def score_leads(leads_df):
    """
    Score all leads in a DataFrame.

    Args:
        leads_df: pandas DataFrame with lead engagement columns

    Returns:
        DataFrame with added 'lead_score' and 'segment' columns
    """
    leads_df = leads_df.copy()
    leads_df["lead_score"] = leads_df.apply(
        lambda row: compute_lead_score(row.to_dict()), axis=1
    )
    leads_df["segment"] = leads_df["lead_score"].apply(classify_segment)
    return leads_df
