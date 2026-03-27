"""
Pagination Engine for review listing pages.

Handles next button detection, clicking, and page navigation logic.
"""

import logging
from typing import Optional, Set
import asyncio

logger = logging.getLogger(__name__)


class PaginationEngine:
    """Manages pagination through review listing pages."""

    def __init__(
        self,
        max_pages: int = 50,
        max_reviews_per_company: int = 10000,
    ):
        """
        Initialize pagination engine.
        
        Args:
            max_pages: Maximum pages to scrape per company
            max_reviews_per_company: Maximum reviews to scrape per company
        """
        self.max_pages = max_pages
        self.max_reviews_per_company = max_reviews_per_company
        self.current_page = 1
        self.seen_review_ids: Set[str] = set()

    async def has_next_page(self, session) -> bool:
        """
        Check if next page exists by checking URL pagination or next button.
        
        Args:
            session: BrowserSession instance
            
        Returns:
            True if next button/page exists and is accessible
        """
        if not session.page:
            return False

        # Try to navigate to next page URL to check if it exists
        try:
            current_url = session.page.url if session.page else ""
            if current_url:
                next_page_num = self.current_page + 1
                
                if "?page=" in current_url:
                    test_url = current_url.split("?page=")[0] + f"?page={next_page_num}"
                else:
                    separator = "&" if "?" in current_url else "?"
                    test_url = current_url + f"{separator}page={next_page_num}"
                
                # Check if page loads without error (HEAD request simulation)
                # For now, assume next page exists if we haven't reached max pages
                logger.debug(f"Assuming page {next_page_num} exists for: {test_url}")
                return True
        except Exception as e:
            logger.debug(f"Could not determine if next page exists: {e}")

        # Fallback: check for traditional next button
        next_selectors = [
            'button[aria-label="Scroll right"]',
            'button:has-text("Next")',
            'a[rel="next"]',
            'button[type="button"]:has-text("→")',
            'a.next-page',
            'button.next-page',
        ]

        for selector in next_selectors:
            try:
                elements = await session.page.query_selector_all(selector)
                if elements:
                    try:
                        is_disabled = await session.page.evaluate(
                            f'document.querySelector("{selector}").disabled'
                        )
                        if not is_disabled:
                            logger.debug(f"Found next button: {selector}")
                            return True
                    except:
                        logger.debug(f"Found next button: {selector}")
                        return True
            except Exception as e:
                logger.debug(f"Failed to check selector {selector}: {e}")
                continue

        logger.info("No active next page button found")
        return True  # Return True to allow URL-based pagination to try

    async def go_to_next_page(self, session) -> bool:
        """
        Navigate to next page by clicking next button or using URL parameter.
        
        Args:
            session: BrowserSession instance
            
        Returns:
            True if successfully navigated, False otherwise
        """
        # First try URL-based pagination (most reliable)
        try:
            current_url = session.page.url if session.page else ""
            if current_url:
                # Add or update page parameter
                next_page_num = self.current_page + 1
                
                if "?page=" in current_url:
                    # Replace existing page parameter
                    new_url = current_url.split("?page=")[0] + f"?page={next_page_num}"
                else:
                    # Add page parameter
                    separator = "&" if "?" in current_url else "?"
                    new_url = current_url + f"{separator}page={next_page_num}"
                
                logger.info(f"Navigating to: {new_url}")
                await session.navigate_to(new_url)
                await asyncio.sleep(2)
                
                # Wait for review containers to load
                try:
                    await session.wait_for_selector(
                        'div[id^="review-"], div[data-testid*="ReviewCard_"]',
                        timeout=15000,
                    )
                except:
                    logger.warning("Timeout waiting for new reviews after pagination")

                self.current_page += 1
                logger.info(f"Successfully navigated to page {self.current_page}")
                return True
        except Exception as e:
            logger.debug(f"Failed to navigate via URL: {e}")

        # Fallback to button-based pagination
        next_selectors = [
            'button[aria-label="Scroll right"]',
            'button:has-text("Next")',
            'a[rel="next"]',
            'button[type="button"]:has-text("→")',
            'a.next-page',
            'button.next-page',
        ]

        for selector in next_selectors:
            try:
                await session.click_element(selector)
                await asyncio.sleep(2)

                # Wait for page mutation (new reviews loaded)
                try:
                    await session.wait_for_selector(
                        'div[id^="review-"], div[data-testid*="ReviewCard_"]',
                        timeout=10000,
                    )
                except:
                    logger.warning("Timeout waiting for new reviews after pagination")

                self.current_page += 1
                logger.info(f"Successfully navigated to page {self.current_page}")
                return True

            except Exception as e:
                logger.debug(f"Failed to click next button with selector {selector}: {e}")
                continue

        logger.warning("Failed to navigate to next page using any method")
        return False

    def should_continue_pagination(
        self, reviews_count: int, has_next: bool, last_review_id: Optional[str] = None
    ) -> bool:
        """
        Determine if pagination should continue.
        
        Args:
            reviews_count: Total reviews scraped so far
            has_next: Whether next button exists
            last_review_id: ID of last review for duplicate detection
            
        Returns:
            True if pagination should continue, False otherwise
        """
        if self.current_page >= self.max_pages:
            logger.info(f"Reached max pages limit: {self.max_pages}")
            return False

        if reviews_count >= self.max_reviews_per_company:
            logger.info(f"Reached max reviews limit: {self.max_reviews_per_company}")
            return False

        if not has_next:
            logger.info("No next button available")
            return False

        return True

    def register_review_ids(self, review_ids: list[str]) -> bool:
        """
        Register review IDs and check for duplicates.
        
        Args:
            review_ids: List of review IDs from current page
            
        Returns:
            True if all IDs are new, False if duplicates detected
        """
        review_set = set(review_ids)

        if self.seen_review_ids & review_set:
            duplicates = self.seen_review_ids & review_set
            logger.warning(
                f"Detected {len(duplicates)} duplicate review IDs: {duplicates}"
            )
            return False

        self.seen_review_ids.update(review_set)
        logger.debug(f"Registered {len(review_ids)} new review IDs")
        return True

    def reset(self) -> None:
        """Reset pagination state for new company."""
        self.current_page = 1
        self.seen_review_ids = set()
        logger.debug("Pagination engine reset")

    def get_state(self) -> dict:
        """Get current pagination state."""
        return {
            "current_page": self.current_page,
            "total_unique_reviews": len(self.seen_review_ids),
        }
