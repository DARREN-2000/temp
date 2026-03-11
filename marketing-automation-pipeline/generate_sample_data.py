"""
Generate sample marketing campaign data for pipeline testing.
Creates realistic CSV/JSON datasets simulating email campaigns,
web analytics, and CRM lead data.
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta

random.seed(42)

CAMPAIGNS = [
    {"id": "camp_001", "name": "Ubuntu Pro Launch", "channel": "email", "budget": 5000},
    {"id": "camp_002", "name": "Kubernetes Webinar", "channel": "webinar", "budget": 3000},
    {"id": "camp_003", "name": "Data Platform Blog Series", "channel": "content", "budget": 2000},
    {"id": "camp_004", "name": "Developer Newsletter", "channel": "email", "budget": 1500},
    {"id": "camp_005", "name": "Cloud Migration Guide", "channel": "content", "budget": 2500},
]

DOMAINS = [
    "techcorp.com", "dataworks.io", "cloudops.net", "devstudio.org",
    "infrateam.co", "mlplatform.ai", "sysadmin.dev", "linuxpro.com",
    "enterprise.tech", "opensrc.org",
]

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Drew",
    "Quinn", "Blake", "Avery", "Cameron", "Dakota", "Jamie", "Sage",
    "Rowan", "Parker", "Hayden", "Emerson", "Finley", "Reese",
]

LAST_NAMES = [
    "Chen", "Patel", "Mueller", "Kim", "Santos", "Nguyen", "Anderson",
    "Williams", "Kumar", "Singh", "Garcia", "Martinez", "Lee", "Brown",
    "Davis", "Wilson", "Moore", "Clark", "Hall", "Young",
]

JOB_TITLES = [
    "DevOps Engineer", "Data Engineer", "Platform Engineer",
    "SRE", "Software Developer", "Cloud Architect",
    "IT Manager", "CTO", "VP Engineering", "Data Scientist",
]


def generate_email(first, last, domain):
    patterns = [
        f"{first.lower()}.{last.lower()}@{domain}",
        f"{first[0].lower()}{last.lower()}@{domain}",
        f"{first.lower()}_{last.lower()}@{domain}",
    ]
    return random.choice(patterns)


def generate_leads(n=200):
    """Generate n synthetic lead records."""
    leads = []
    used_emails = set()

    for i in range(n):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        domain = random.choice(DOMAINS)
        email = generate_email(first, last, domain)

        # Allow ~5% duplicates for testing deduplication
        if email in used_emails and random.random() > 0.05:
            domain = random.choice(DOMAINS)
            email = generate_email(first, last, domain)
        used_emails.add(email)

        campaign = random.choice(CAMPAIGNS)
        base_date = datetime(2025, 1, 1)
        interaction_date = base_date + timedelta(days=random.randint(0, 180))

        lead = {
            "lead_id": f"lead_{i+1:04d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "company": domain.split(".")[0].title() + " Inc.",
            "job_title": random.choice(JOB_TITLES),
            "campaign_id": campaign["id"],
            "campaign_name": campaign["name"],
            "channel": campaign["channel"],
            "interaction_date": interaction_date.strftime("%Y-%m-%d"),
            "emails_sent": random.randint(1, 10),
            "emails_opened": 0,
            "links_clicked": 0,
            "page_visits": random.randint(0, 25),
            "content_downloads": random.randint(0, 5),
            "converted": False,
        }

        # Simulate realistic engagement patterns
        lead["emails_opened"] = random.randint(0, lead["emails_sent"])
        if lead["emails_opened"] > 0:
            lead["links_clicked"] = random.randint(0, lead["emails_opened"])

        # Higher engagement leads more likely to convert
        engagement = (
            lead["emails_opened"] / max(lead["emails_sent"], 1)
            + lead["links_clicked"] / max(lead["emails_opened"], 1)
            + lead["page_visits"] / 25
            + lead["content_downloads"] / 5
        ) / 4

        lead["converted"] = random.random() < engagement * 0.6

        # Introduce ~3% missing values for data quality testing
        if random.random() < 0.03:
            lead["email"] = ""
        if random.random() < 0.03:
            lead["job_title"] = ""

        leads.append(lead)

    return leads


def main():
    os.makedirs("data", exist_ok=True)

    leads = generate_leads(200)

    # Save as CSV
    csv_path = os.path.join("data", "campaign_leads.csv")
    fieldnames = leads[0].keys()
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)

    # Save campaign metadata as JSON
    json_path = os.path.join("data", "campaigns.json")
    with open(json_path, "w") as f:
        json.dump(CAMPAIGNS, f, indent=2)

    print(f"Generated {len(leads)} leads -> {csv_path}")
    print(f"Generated {len(CAMPAIGNS)} campaigns -> {json_path}")


if __name__ == "__main__":
    main()
