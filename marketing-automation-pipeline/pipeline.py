"""
Marketing Automation Data Pipeline

Main orchestrator that runs the full ETL process:
1. Ingest campaign data from CSV/JSON sources
2. Validate and clean the data
3. Compute lead scores and segments
4. Calculate campaign ROI metrics
5. Export reports
"""

import os
import re
import json
import logging
import sqlite3
from datetime import datetime

import pandas as pd

from lead_scoring import score_leads

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline_run.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

DATA_DIR = "data"
REPORTS_DIR = "reports"
DB_PATH = "marketing.db"
ESTIMATED_REVENUE_PER_CONVERSION = 50  # USD per converted lead


def ingest_leads(filepath):
    """Read lead data from CSV file."""
    logger.info(f"Ingesting leads from {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  Loaded {len(df)} raw records with {len(df.columns)} columns")
    return df


def ingest_campaigns(filepath):
    """Read campaign metadata from JSON file."""
    logger.info(f"Ingesting campaigns from {filepath}")
    with open(filepath, "r") as f:
        campaigns = json.load(f)
    logger.info(f"  Loaded {len(campaigns)} campaigns")
    return pd.DataFrame(campaigns)


def validate_email(email):
    """Check if email is a valid format."""
    if not email or not isinstance(email, str):
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, str(email)))


def clean_data(df):
    """Validate and clean the lead data."""
    logger.info("Starting data cleaning...")
    initial_count = len(df)

    # Remove rows with empty email
    invalid_emails = df[~df["email"].apply(validate_email)]
    logger.info(f"  Found {len(invalid_emails)} records with invalid emails")
    df = df[df["email"].apply(validate_email)].copy()

    # Remove duplicates based on email + campaign_id
    dupes = df.duplicated(subset=["email", "campaign_id"], keep="first")
    dupe_count = dupes.sum()
    logger.info(f"  Found {dupe_count} duplicate records")
    df = df.drop_duplicates(subset=["email", "campaign_id"], keep="first")

    # Fill missing job titles
    missing_titles = df["job_title"].isna().sum() + (df["job_title"] == "").sum()
    df["job_title"] = df["job_title"].replace("", "Unknown")
    df["job_title"] = df["job_title"].fillna("Unknown")
    logger.info(f"  Filled {missing_titles} missing job titles")

    # Ensure numeric columns are non-negative
    numeric_cols = [
        "emails_sent",
        "emails_opened",
        "links_clicked",
        "page_visits",
        "content_downloads",
    ]
    for col in numeric_cols:
        df[col] = df[col].clip(lower=0)

    # Compute derived metrics
    df["open_rate"] = (
        df["emails_opened"] / df["emails_sent"].replace(0, 1) * 100
    ).round(2)
    df["click_rate"] = (
        df["links_clicked"] / df["emails_opened"].replace(0, 1) * 100
    ).round(2)

    final_count = len(df)
    logger.info(
        f"  Cleaning complete: {initial_count} -> {final_count} records "
        f"({initial_count - final_count} removed)"
    )
    return df


def compute_campaign_roi(leads_df, campaigns_df):
    """Calculate ROI metrics per campaign."""
    logger.info("Computing campaign ROI metrics...")

    results = []
    for _, camp in campaigns_df.iterrows():
        camp_leads = leads_df[leads_df["campaign_id"] == camp["id"]]
        total_leads = len(camp_leads)
        conversions = int(camp_leads["converted"].sum())
        budget = camp["budget"]

        cpl = budget / max(total_leads, 1)
        conv_rate = (conversions / max(total_leads, 1)) * 100
        # Revenue estimate based on configurable per-conversion value
        revenue = conversions * ESTIMATED_REVENUE_PER_CONVERSION
        roi = ((revenue - budget) / max(budget, 1)) * 100

        result = {
            "campaign_id": camp["id"],
            "campaign_name": camp["name"],
            "channel": camp["channel"],
            "budget": budget,
            "total_leads": int(total_leads),
            "conversions": int(conversions),
            "cost_per_lead": round(cpl, 2),
            "conversion_rate_pct": round(conv_rate, 2),
            "estimated_revenue": round(revenue, 2),
            "roi_pct": round(roi, 2),
        }
        results.append(result)
        logger.info(
            f"  {camp['name']}: {total_leads} leads, "
            f"{conversions} conversions, ROI: {roi:.1f}%"
        )

    return results


def save_to_sqlite(leads_df, campaign_metrics, db_path):
    """Persist results to SQLite for downstream queries."""
    logger.info(f"Saving to SQLite database: {db_path}")
    conn = sqlite3.connect(db_path)

    leads_df.to_sql("scored_leads", conn, if_exists="replace", index=False)
    pd.DataFrame(campaign_metrics).to_sql(
        "campaign_roi", conn, if_exists="replace", index=False
    )

    conn.close()
    logger.info("  Database updated successfully")


def export_reports(scored_leads_df, campaign_metrics):
    """Export final reports to files."""
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # Scored leads CSV
    leads_path = os.path.join(REPORTS_DIR, "scored_leads.csv")
    scored_leads_df.to_csv(leads_path, index=False)
    logger.info(f"  Exported scored leads -> {leads_path}")

    # Campaign summary JSON
    summary_path = os.path.join(REPORTS_DIR, "campaign_summary.json")
    segment_breakdown = {
        k: int(v) for k, v in scored_leads_df["segment"].value_counts().to_dict().items()
    }
    summary = {
        "generated_at": datetime.now().isoformat(),
        "total_leads_processed": len(scored_leads_df),
        "segment_breakdown": segment_breakdown,
        "avg_lead_score": round(float(scored_leads_df["lead_score"].mean()), 2),
        "campaigns": campaign_metrics,
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"  Exported campaign summary -> {summary_path}")


def run_pipeline():
    """Execute the full ETL pipeline."""
    logger.info("=" * 60)
    logger.info("MARKETING AUTOMATION PIPELINE - START")
    logger.info("=" * 60)
    start_time = datetime.now()

    # Step 1: Ingest
    leads_path = os.path.join(DATA_DIR, "campaign_leads.csv")
    campaigns_path = os.path.join(DATA_DIR, "campaigns.json")

    if not os.path.exists(leads_path):
        logger.error(f"Data file not found: {leads_path}")
        logger.info("Run 'python generate_sample_data.py' first to create sample data")
        return

    leads_df = ingest_leads(leads_path)
    campaigns_df = ingest_campaigns(campaigns_path)

    # Step 2: Clean
    cleaned_df = clean_data(leads_df)

    # Step 3: Score
    logger.info("Computing lead scores...")
    scored_df = score_leads(cleaned_df)
    segment_counts = scored_df["segment"].value_counts()
    logger.info(f"  Segment distribution: {segment_counts.to_dict()}")

    # Step 4: Campaign ROI
    campaign_metrics = compute_campaign_roi(scored_df, campaigns_df)

    # Step 5: Persist & Export
    save_to_sqlite(scored_df, campaign_metrics, DB_PATH)
    export_reports(scored_df, campaign_metrics)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.2f}s")
    logger.info(f"  Processed: {len(scored_df)} leads across {len(campaigns_df)} campaigns")
    logger.info(f"  Avg lead score: {scored_df['lead_score'].mean():.1f}")
    logger.info(f"  Hot leads: {segment_counts.get('Hot', 0)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
