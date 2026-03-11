# Marketing Automation Data Pipeline

A data engineering pipeline for automating marketing campaign analytics and lead scoring. Built as a portfolio project demonstrating ETL, data transformation, and automated reporting skills relevant to growth engineering and marketing data operations.

## Overview

This pipeline ingests marketing campaign data from multiple sources (email platforms, web analytics, CRM exports), transforms it through a standardized ETL process, computes lead scores using a rule-based model, and outputs actionable reports for marketing teams.

## Architecture

```
[CSV/JSON Sources] → [Ingestion Layer] → [Transformation] → [Lead Scoring] → [Reports & Dashboards]
         ↓                    ↓                  ↓                 ↓                    ↓
    Email campaigns      Data validation     Deduplication     Score computation    CSV/JSON exports
    Web analytics        Schema mapping      Enrichment        Segmentation         Summary stats
    CRM exports          Type casting        Aggregation       Classification       Campaign metrics
```

## Features

- **Multi-source ingestion**: Reads campaign data from CSV, JSON, and simulated API responses
- **Data quality checks**: Validates email formats, removes duplicates, handles missing values
- **Lead scoring engine**: Rule-based scoring model using engagement metrics (open rates, click rates, page visits)
- **Campaign ROI calculation**: Computes cost-per-lead, conversion rates, and ROI per channel
- **Automated segmentation**: Classifies leads into Hot/Warm/Cold segments based on composite scores
- **Export & reporting**: Generates summary reports in CSV and JSON formats

## Tech Stack

- **Python 3.9+**
- **pandas** - Data transformation and analysis
- **SQLite** - Lightweight data warehouse
- **logging** - Pipeline observability

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Generate sample data
python generate_sample_data.py

# Run the full pipeline
python pipeline.py

# Check outputs in the reports/ directory
```

## Project Structure

```
marketing-automation-pipeline/
├── pipeline.py              # Main ETL pipeline orchestrator
├── generate_sample_data.py  # Sample data generator for testing
├── lead_scoring.py          # Lead scoring engine
├── requirements.txt         # Python dependencies
├── tests/
│   └── test_pipeline.py     # Pipeline tests
└── README.md
```

## How It Works

### 1. Ingestion
The pipeline reads campaign data from the `data/` directory. It supports CSV files from email platforms and JSON exports from web analytics tools.

### 2. Transformation
- Standardizes column names and data types
- Validates email formats using regex
- Removes duplicate records based on email + campaign_id
- Fills missing values with sensible defaults
- Computes derived metrics (engagement rate, cost efficiency)

### 3. Lead Scoring
Each lead receives a composite score (0-100) based on:
- Email open rate (weight: 0.2)
- Click-through rate (weight: 0.3)
- Website page visits (weight: 0.25)
- Content downloads (weight: 0.25)

Leads are then segmented:
- **Hot** (score >= 70): High intent, ready for sales outreach
- **Warm** (score 40-69): Engaged, needs nurturing
- **Cold** (score < 40): Low engagement, needs re-engagement campaign

### 4. Campaign ROI
For each campaign/channel, the pipeline calculates:
- Total spend vs. total conversions
- Cost per lead (CPL)
- Conversion rate
- Return on investment (ROI %)

### 5. Reporting
Outputs are saved to the `reports/` directory:
- `scored_leads.csv` - All leads with their scores and segments
- `campaign_summary.json` - ROI metrics per campaign
- `pipeline_run.log` - Execution log with data quality metrics

## License

MIT
