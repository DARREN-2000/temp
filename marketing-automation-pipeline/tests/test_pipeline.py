"""Tests for the marketing automation pipeline."""

import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lead_scoring import compute_lead_score, classify_segment, score_leads
from pipeline import validate_email, clean_data


class TestLeadScoring:
    """Tests for the lead scoring engine."""

    def test_high_engagement_lead(self):
        lead = {
            "emails_sent": 10,
            "emails_opened": 10,
            "links_clicked": 8,
            "page_visits": 20,
            "content_downloads": 4,
        }
        score = compute_lead_score(lead)
        assert score >= 70, f"High engagement lead should score >= 70, got {score}"

    def test_low_engagement_lead(self):
        lead = {
            "emails_sent": 10,
            "emails_opened": 1,
            "links_clicked": 0,
            "page_visits": 1,
            "content_downloads": 0,
        }
        score = compute_lead_score(lead)
        assert score < 40, f"Low engagement lead should score < 40, got {score}"

    def test_zero_engagement(self):
        lead = {
            "emails_sent": 5,
            "emails_opened": 0,
            "links_clicked": 0,
            "page_visits": 0,
            "content_downloads": 0,
        }
        score = compute_lead_score(lead)
        assert score == 0, f"Zero engagement should score 0, got {score}"

    def test_score_capped_at_100(self):
        lead = {
            "emails_sent": 1,
            "emails_opened": 10,
            "links_clicked": 10,
            "page_visits": 100,
            "content_downloads": 50,
        }
        score = compute_lead_score(lead)
        assert score <= 100, f"Score should be capped at 100, got {score}"

    def test_missing_keys_default_to_zero(self):
        lead = {}
        score = compute_lead_score(lead)
        assert score == 0


class TestSegmentation:
    """Tests for lead segmentation."""

    def test_hot_segment(self):
        assert classify_segment(85) == "Hot"
        assert classify_segment(70) == "Hot"

    def test_warm_segment(self):
        assert classify_segment(55) == "Warm"
        assert classify_segment(40) == "Warm"

    def test_cold_segment(self):
        assert classify_segment(20) == "Cold"
        assert classify_segment(0) == "Cold"
        assert classify_segment(39.99) == "Cold"


class TestEmailValidation:
    """Tests for email validation."""

    def test_valid_emails(self):
        assert validate_email("user@example.com") is True
        assert validate_email("first.last@company.io") is True
        assert validate_email("dev+tag@domain.org") is True

    def test_invalid_emails(self):
        assert validate_email("") is False
        assert validate_email(None) is False
        assert validate_email("notanemail") is False
        assert validate_email("@nodomain.com") is False


class TestDataCleaning:
    """Tests for data cleaning pipeline step."""

    def test_removes_invalid_emails(self):
        data = pd.DataFrame(
            {
                "email": ["valid@test.com", "", "also@valid.io"],
                "campaign_id": ["c1", "c1", "c2"],
                "job_title": ["Eng", "Eng", "Eng"],
                "emails_sent": [5, 5, 5],
                "emails_opened": [3, 3, 3],
                "links_clicked": [1, 1, 1],
                "page_visits": [2, 2, 2],
                "content_downloads": [0, 0, 0],
                "converted": [False, False, True],
            }
        )
        cleaned = clean_data(data)
        assert len(cleaned) == 2

    def test_removes_duplicates(self):
        data = pd.DataFrame(
            {
                "email": ["dup@test.com", "dup@test.com", "unique@test.com"],
                "campaign_id": ["c1", "c1", "c1"],
                "job_title": ["Eng", "Eng", "Eng"],
                "emails_sent": [5, 5, 5],
                "emails_opened": [3, 3, 3],
                "links_clicked": [1, 1, 1],
                "page_visits": [2, 2, 2],
                "content_downloads": [0, 0, 0],
                "converted": [False, False, True],
            }
        )
        cleaned = clean_data(data)
        assert len(cleaned) == 2

    def test_fills_missing_job_titles(self):
        data = pd.DataFrame(
            {
                "email": ["a@test.com"],
                "campaign_id": ["c1"],
                "job_title": [""],
                "emails_sent": [5],
                "emails_opened": [3],
                "links_clicked": [1],
                "page_visits": [2],
                "content_downloads": [0],
                "converted": [False],
            }
        )
        cleaned = clean_data(data)
        assert cleaned.iloc[0]["job_title"] == "Unknown"


class TestScoreLeadsIntegration:
    """Integration test for scoring a DataFrame of leads."""

    def test_score_leads_adds_columns(self):
        data = pd.DataFrame(
            {
                "emails_sent": [10, 5],
                "emails_opened": [8, 1],
                "links_clicked": [5, 0],
                "page_visits": [15, 2],
                "content_downloads": [3, 0],
            }
        )
        result = score_leads(data)
        assert "lead_score" in result.columns
        assert "segment" in result.columns
        assert len(result) == 2
