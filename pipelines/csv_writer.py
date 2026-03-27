"""
CSV Writer for exporting datasets.

Handles CSV writing with proper encoding and formatting.
"""

import logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class CSVWriter:
    """Writes dataframes to CSV files."""

    def __init__(self, output_dir: str = "data/raw"):
        """
        Initialize CSV writer.
        
        Args:
            output_dir: Directory to write CSV files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_company_csv(self, company_df: pd.DataFrame) -> Path:
        """
        Write company-level CSV.
        
        Args:
            company_df: Company dataframe
            
        Returns:
            Path to written file
        """
        filepath = self.output_dir / "company_level_data.csv"
        try:
            company_df.to_csv(
                filepath,
                index=False,
                encoding="utf-8",
                sep=",",
            )
            logger.info(f"Wrote company CSV: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to write company CSV: {e}")
            raise

    def write_review_csv(self, review_df: pd.DataFrame) -> Path:
        """
        Write review-level CSV.
        
        Args:
            review_df: Review dataframe
            
        Returns:
            Path to written file
        """
        filepath = self.output_dir / "review_level_data.csv"
        try:
            review_df.to_csv(
                filepath,
                index=False,
                encoding="utf-8",
                sep=",",
            )
            logger.info(f"Wrote review CSV: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to write review CSV: {e}")
            raise

    def write_master_csv(self, master_df: pd.DataFrame) -> Path:
        """
        Write master CSV.
        
        Args:
            master_df: Master dataframe
            
        Returns:
            Path to written file
        """
        filepath = self.output_dir / "master_employee_reviews.csv"
        try:
            master_df.to_csv(
                filepath,
                index=False,
                encoding="utf-8",
                sep=",",
            )
            logger.info(f"Wrote master CSV: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to write master CSV: {e}")
            raise

    def write_all_csvs(
        self,
        company_df: pd.DataFrame,
        review_df: pd.DataFrame,
        master_df: pd.DataFrame,
    ) -> Dict[str, Path]:
        """
        Write all three CSV files.
        
        Args:
            company_df: Company dataframe
            review_df: Review dataframe
            master_df: Master dataframe
            
        Returns:
            Dictionary mapping dataset names to file paths
        """
        results = {
            "company_level_data": self.write_company_csv(company_df),
            "review_level_data": self.write_review_csv(review_df),
            "master_employee_reviews": self.write_master_csv(master_df),
        }

        total_size = sum(f.stat().st_size for f in results.values() if f.exists())
        logger.info(f"All CSV files written, total size: {total_size / 1024 / 1024:.2f} MB")

        return results

    def get_csv_summary(self) -> Dict[str, Any]:
        """
        Get summary of written CSV files.
        
        Returns:
            Dictionary with file information
        """
        summary = {
            "output_directory": str(self.output_dir),
            "files": {},
        }

        csv_files = list(self.output_dir.glob("*.csv"))
        for filepath in csv_files:
            summary["files"][filepath.name] = {
                "size_bytes": filepath.stat().st_size,
                "size_mb": filepath.stat().st_size / 1024 / 1024,
                "path": str(filepath),
            }

        return summary
