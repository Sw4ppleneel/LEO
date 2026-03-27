"""
Dataset Builder for constructing and merging review dataframes.

Creates company-level, review-level, and master datasets with proper schema.
"""

import logging
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class DatasetBuilder:
    """Builds structured datasets from scraped data."""

    # Define strict column ordering for datasets
    COMPANY_COLUMNS = [
        "company_id",
        "company_name",
        "industry",
        "overall_rating",
        "salary_rating",
        "culture_rating",
        "job_security_rating",
        "skill_development_rating",
        "work_life_balance_rating",
        "work_satisfaction_rating",
        "career_growth_rating",
        "total_reviews_on_platform",
        "last_updated",
        "scrape_datetime",
    ]

    REVIEW_COLUMNS = [
        "review_id",
        "company_id",
        "company_name",
        "page_num",
        "review_index",
        "overall_rating",
        "salary_rating",
        "culture_rating",
        "job_security_rating",
        "skill_development_rating",
        "work_life_balance_rating",
        "work_satisfaction_rating",
        "promotions_rating",
        "review_title",
        "likes",
        "dislikes",
        "employee_name",
        "employee_status",
        "designation",
        "employment_type",
        "department",
        "location",
        "work_policy",
        "travel_tags",
        "work_timing",
        "work_days",
        "review_date",
        "review_date_readable",
        "created_date",
        "modified_date",
        "verified",
        "helpful_count",
        "not_helpful_count",
        "scrape_datetime",
    ]

    def __init__(self):
        """Initialize dataset builder."""
        self.company_data: List[Dict[str, Any]] = []
        self.review_data: List[Dict[str, Any]] = []

    def add_company(self, company_info: Dict[str, Any]) -> None:
        """
        Add company-level information.
        
        Args:
            company_info: Dictionary with company data
        """
        self.company_data.append(company_info)
        logger.debug(f"Added company: {company_info.get('company_name')}")

    def add_reviews(self, reviews: List[Dict[str, Any]]) -> None:
        """
        Add review-level data.
        
        Args:
            reviews: List of review dictionaries
        """
        self.review_data.extend(reviews)
        logger.debug(f"Added {len(reviews)} reviews")

    def build_company_dataframe(self) -> pd.DataFrame:
        """
        Build company-level dataframe.
        
        Returns:
            DataFrame with company information
        """
        df = pd.DataFrame(self.company_data)

        # Ensure columns exist with proper ordering
        for col in self.COMPANY_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Reorder columns
        df = df[self.COMPANY_COLUMNS]

        logger.info(f"Built company dataframe with {len(df)} rows")
        return df

    def build_review_dataframe(self) -> pd.DataFrame:
        """
        Build review-level dataframe.
        
        Returns:
            DataFrame with review information
        """
        df = pd.DataFrame(self.review_data)

        # Ensure columns exist with proper ordering
        for col in self.REVIEW_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Reorder columns
        df = df[self.REVIEW_COLUMNS]

        # Remove duplicates based on review_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["review_id"], keep="first")
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate reviews")

        logger.info(f"Built review dataframe with {len(df)} rows")
        return df

    def build_master_dataframe(
        self, company_df: pd.DataFrame, review_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Build master dataframe by joining review and company data.
        
        Args:
            company_df: Company-level dataframe
            review_df: Review-level dataframe
            
        Returns:
            Merged master dataframe
        """
        # Left join reviews with company data
        master_df = review_df.merge(
            company_df.drop(columns=["company_name", "scrape_datetime"], errors="ignore"),
            on="company_id",
            how="left",
        )

        logger.info(f"Built master dataframe with {len(master_df)} rows")
        return master_df

    def validate_dataframes(
        self, company_df: pd.DataFrame, review_df: pd.DataFrame, master_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Validate dataframe integrity.
        
        Args:
            company_df: Company dataframe
            review_df: Review dataframe
            master_df: Master dataframe
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            "company_rows": len(company_df),
            "review_rows": len(review_df),
            "master_rows": len(master_df),
            "issues": [],
        }

        # Check for duplicate reviews
        duplicates = review_df["review_id"].duplicated().sum()
        if duplicates > 0:
            validation["issues"].append(f"Found {duplicates} duplicate review IDs")

        # Check for null review IDs
        null_ids = review_df["review_id"].isnull().sum()
        if null_ids > 0:
            validation["issues"].append(f"Found {null_ids} null review IDs")

        # Check for missing companies
        reviews_with_company = review_df["company_id"].notna().sum()
        if reviews_with_company < len(review_df):
            missing = len(review_df) - reviews_with_company
            validation["issues"].append(f"Found {missing} reviews with missing company_id")

        # Check master join integrity
        if len(master_df) != len(review_df):
            validation["issues"].append(
                f"Master join resulted in row count change: "
                f"{len(review_df)} -> {len(master_df)}"
            )

        validation["is_valid"] = len(validation["issues"]) == 0

        logger.info(
            f"Validation complete: {validation['company_rows']} companies, "
            f"{validation['review_rows']} reviews, {validation['master_rows']} master rows"
        )

        if validation["issues"]:
            for issue in validation["issues"]:
                logger.warning(f"Validation issue: {issue}")

        return validation

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of data collected.
        
        Returns:
            Dictionary with summary statistics
        """
        return {
            "total_companies": len(self.company_data),
            "total_reviews": len(self.review_data),
            "average_reviews_per_company": (
                len(self.review_data) / len(self.company_data)
                if self.company_data
                else 0
            ),
            "collection_time": datetime.now().isoformat(),
        }
