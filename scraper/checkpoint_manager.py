"""
Checkpoint Manager for resumable scraping.

Persists scraping state to JSON to enable recovery from interruptions.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages checkpoint persistence and recovery."""

    def __init__(self, checkpoint_dir: str = "data/checkpoints"):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory to store checkpoint files
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "scraper_checkpoint.json"

    def load_checkpoint(self) -> Dict[str, Any]:
        """
        Load existing checkpoint or return empty dict.
        
        Returns:
            Dictionary with company checkpoint states
        """
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    checkpoint = json.load(f)
                logger.info(f"Loaded checkpoint with {len(checkpoint)} companies")
                return checkpoint
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}, starting fresh")
                return {}
        return {}

    def save_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """
        Save checkpoint to disk.
        
        Args:
            checkpoint: Dictionary with company states
        """
        try:
            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
            logger.debug(f"Checkpoint saved with {len(checkpoint)} companies")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def get_company_checkpoint(
        self, company_id: str, checkpoint: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get checkpoint state for a specific company.
        
        Args:
            company_id: Company identifier
            checkpoint: Full checkpoint dictionary
            
        Returns:
            Company-specific checkpoint state
        """
        return checkpoint.get(
            company_id,
            {
                "last_page_scraped": 0,
                "reviews_scraped": 0,
                "last_review_id": None,
            },
        )

    def update_company_checkpoint(
        self,
        checkpoint: Dict[str, Any],
        company_id: str,
        last_page: int,
        reviews_count: int,
        last_review_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update checkpoint for a specific company.
        
        Args:
            checkpoint: Current checkpoint dictionary
            company_id: Company identifier
            last_page: Last page scraped
            reviews_count: Total reviews scraped so far
            last_review_id: ID of last review extracted
            
        Returns:
            Updated checkpoint dictionary
        """
        checkpoint[company_id] = {
            "last_page_scraped": last_page,
            "reviews_scraped": reviews_count,
            "last_review_id": last_review_id,
        }
        return checkpoint

    def should_skip_company(self, company_id: str, checkpoint: Dict[str, Any]) -> bool:
        """
        Check if a company has been fully scraped.
        
        Args:
            company_id: Company identifier
            checkpoint: Full checkpoint dictionary
            
        Returns:
            True if company should be skipped
        """
        return company_id in checkpoint and checkpoint[company_id].get(
            "last_page_scraped", 0
        ) > 0

    def reset_checkpoint(self) -> None:
        """Reset checkpoint file completely."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            logger.info("Checkpoint reset")
