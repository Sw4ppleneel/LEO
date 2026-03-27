"""
Review Extractor module.

Extracts structured review data from JSON embedded in page HTML.
Falls back to DOM parsing if JSON is unavailable.
"""

import logging
import json
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class ReviewExtractor:
    """Extracts structured review data from HTML content."""

    SUBRATING_CATEGORIES = [
        "Salary",
        "Company Culture",
        "Job Security",
        "Skill Development",
        "Work-Life Balance",
        "Work Satisfaction",
        "Promotions",
    ]

    def __init__(self):
        """Initialize review extractor."""
        self.extraction_stats = {
            "total_attempted": 0,
            "successful": 0,
            "failed": 0,
            "missing_fields": {},
        }

    def extract_reviews(
        self, html_content: str, company_id: str, page_num: int
    ) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
        """
        Extract all reviews from page HTML - combine JSON and DOM, deduplicate.
        
        Args:
            html_content: Page HTML content
            company_id: Company identifier
            page_num: Current page number
            
        Returns:
            Tuple of (reviews list, extraction stats)
        """
        soup = BeautifulSoup(html_content, "lxml")
        reviews = []
        seen_review_ids = set()

        # PRIMARY: Extract from JSON (most structured data)
        json_reviews = self._extract_reviews_from_json(soup, company_id, page_num)
        reviews.extend(json_reviews)
        for review in json_reviews:
            if "review_id" in review:
                seen_review_ids.add(review["review_id"])
        
        if json_reviews:
            logger.info(f"Extracted {len(json_reviews)} reviews from JSON on page {page_num}")
        
        # SECONDARY: Also extract from DOM to catch any additional reviews
        review_containers = self._find_review_containers(soup)
        dom_reviews = []
        
        if review_containers:
            logger.debug(f"Found {len(review_containers)} potential DOM review containers")
            for idx, container in enumerate(review_containers):
                try:
                    review = self._extract_single_review(
                        container, company_id, page_num, idx
                    )
                    if review:
                        # Check if this review is a duplicate
                        review_id = review.get("review_id")
                        if review_id and review_id not in seen_review_ids:
                            dom_reviews.append(review)
                            seen_review_ids.add(review_id)
                            self.extraction_stats["successful"] += 1
                        elif not review_id:
                            # No ID, add it anyway
                            dom_reviews.append(review)
                            self.extraction_stats["successful"] += 1
                except Exception as e:
                    logger.debug(
                        f"Failed to extract review {idx} on page {page_num}: {e}"
                    )
                    self.extraction_stats["failed"] += 1

        # Add non-duplicate DOM reviews to results
        reviews.extend(dom_reviews)
        self.extraction_stats["total_attempted"] = len(json_reviews) + len(dom_reviews)
        
        if dom_reviews:
            logger.info(f"Extracted {len(dom_reviews)} additional reviews from DOM (after dedup: {len(dom_reviews)} new)")
        
        logger.info(f"Page {page_num} total: {len(json_reviews)} from JSON + {len(dom_reviews)} from DOM = {len(reviews)} combined")
        return reviews, self.extraction_stats

    def _extract_reviews_from_json(self, soup: BeautifulSoup, company_id: str, page_num: int) -> list[Dict[str, Any]]:
        """
        Extract reviews directly from embedded JSON script tag.
        
        Args:
            soup: BeautifulSoup parsed document
            company_id: Company identifier
            page_num: Current page number
            
        Returns:
            List of review dictionaries
        """
        try:
            # Find the JSON script tag
            script_tag = soup.find('script', {'type': 'application/json'})
            if not script_tag or not script_tag.string:
                return []
            
            # Parse JSON
            data = json.loads(script_tag.string)
            reviews_data = data.get('props', {}).get('pageProps', {}).get('reviewsData', [])
            
            if not reviews_data:
                return []
            
            reviews = []
            for idx, raw_review in enumerate(reviews_data):
                try:
                    review = self._transform_json_review(raw_review, company_id, page_num, idx)
                    if review:
                        reviews.append(review)
                except Exception as e:
                    logger.debug(f"Failed to transform JSON review {idx}: {e}")
                    continue
            
            return reviews
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.debug(f"Failed to extract reviews from JSON: {e}")
            return []

    def _transform_json_review(self, raw_review: Dict[str, Any], company_id: str, page_num: int, idx: int) -> Optional[Dict[str, Any]]:
        """
        Transform raw JSON review data into our standard format.
        
        Args:
            raw_review: Raw review data from JSON
            company_id: Company identifier
            page_num: Page number
            idx: Index in page
            
        Returns:
            Transformed review dictionary
        """
        try:
            # Get scrape timestamp
            scrape_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            review = {
                "review_id": raw_review.get('id', self._generate_review_id(raw_review, company_id, page_num, idx)),
                "company_id": company_id,
                "page_num": page_num,
                "review_index": idx,
                
                # Core rating
                "overall_rating": raw_review.get('overallCompanyRating'),
                
                # Review content
                "review_title": raw_review.get('reviewTitle', ''),
                "likes": raw_review.get('likesText', ''),
                "dislikes": raw_review.get('disLikesText', ''),
                
                # Employee info
                "employee_name": raw_review.get('userName', 'Anonymous'),
                "employee_status": 'Current' if raw_review.get('currentJob') else 'Former',
                "designation": raw_review.get('jobProfile', {}).get('name', ''),
                "employment_type": raw_review.get('employmentType', ''),
                "department": raw_review.get('division', ''),
                "location": self._extract_location(raw_review.get('jobLocation', {})),
                
                # Work info
                "work_policy": raw_review.get('workPolicy', ''),
                "travel_tags": ', '.join(raw_review.get('workLabels', {}).get('travelTags', [])),
                "work_timing": ', '.join(raw_review.get('workLabels', {}).get('workTimeMonitor', [])),
                "work_days": ', '.join(raw_review.get('workLabels', {}).get('workDays', [])),
                
                # Dates and timestamps
                "review_date": raw_review.get('modifiedMachineReadable', raw_review.get('modified', '')),
                "review_date_readable": raw_review.get('modifiedHumanReadable', ''),
                "created_date": raw_review.get('created', ''),
                "modified_date": raw_review.get('modified', ''),
                "scrape_datetime": scrape_datetime,
                
                # Metadata
                "verified": raw_review.get('verified', False),
                "helpful_count": raw_review.get('helpfulCount', 0),
                "not_helpful_count": raw_review.get('notHelpfulCount', 0),
                
                # Category ratings (from ratingDistribution if available)
                "salary_rating": None,
                "culture_rating": None,
                "job_security_rating": None,
                "skill_development_rating": None,
                "work_life_balance_rating": None,
                "work_satisfaction_rating": None,
                "promotions_rating": None,
            }
            
            # Extract category ratings from ratingDistribution array
            # Format: [{'name': 'Salary', 'rating': 3}, {'name': 'Skill development', 'rating': 4}, ...]
            rating_dist = raw_review.get('ratingDistribution', [])
            for rating_item in rating_dist:
                category_name = rating_item.get('name', '').lower().strip()
                value = rating_item.get('rating')
                
                # Map category names to our field names
                if 'salary' in category_name:
                    review['salary_rating'] = value
                elif 'culture' in category_name:
                    review['culture_rating'] = value
                elif 'security' in category_name:
                    review['job_security_rating'] = value
                elif 'skill' in category_name:
                    review['skill_development_rating'] = value
                elif 'balance' in category_name:
                    review['work_life_balance_rating'] = value
                elif 'satisfaction' in category_name:
                    review['work_satisfaction_rating'] = value
                elif 'promotion' in category_name:
                    review['promotions_rating'] = value
            
            return review
            
        except Exception as e:
            logger.debug(f"Failed to transform JSON review: {e}")
            return None

    def _extract_location(self, job_location: Dict[str, Any]) -> str:
        """Extract location string from job location dict."""
        if not job_location:
            return ''
        
        # First try 'name' field (primary location name)
        if job_location.get('name'):
            return job_location['name']
        
        # Fallback to urlName
        if job_location.get('urlName'):
            return job_location['urlName']
        
        return ''


    def _find_review_containers(self, soup: BeautifulSoup) -> list:
        """
        Find all review card containers using multiple selectors.
        
        Args:
            soup: BeautifulSoup parsed document
            
        Returns:
            List of review container elements
        """
        containers = []

        # Primary: Find by data-testid pattern for ReviewCard
        containers = soup.find_all("div", {"data-testid": lambda x: x and "ReviewCard_" in x})

        if containers:
            logger.debug(f"Found {len(containers)} containers using data-testid")
            return containers

        # Fallback: Find by id pattern
        containers = soup.find_all("div", {"id": lambda x: x and x.startswith("review-")})

        if containers:
            logger.debug(f"Found {len(containers)} containers using id pattern")
            return containers

        logger.warning("No review containers found using any selector")
        return []

    def _extract_single_review(
        self, container, company_id: str, page_num: int, review_idx: int
    ) -> Optional[Dict[str, Any]]:
        """
        Extract a single review from a container element.
        
        Args:
            container: BeautifulSoup element
            company_id: Company identifier
            page_num: Current page number
            review_idx: Index of review on page
            
        Returns:
            Dictionary with review data or None if extraction fails
        """
        review = {
            "company_id": company_id,
            "page_number": page_num,
            "review_index_on_page": review_idx,
            "scrape_datetime": datetime.now().isoformat(),
        }

        # Extract overall rating
        overall_rating = self._extract_overall_rating(container)
        if overall_rating is not None:
            review["overall_rating"] = overall_rating
        else:
            self._track_missing_field("overall_rating")

        # Extract subratings
        subratings = self._extract_subratings(container)
        review.update(subratings)

        # Extract text fields
        review["likes"] = self._extract_field_with_selector(
            container,
            ["[data-testid$='_Likes']", "[data-testid$='_Likes'] + p"],
            "likes",
        )

        review["dislikes"] = self._extract_field_with_selector(
            container,
            ["[data-testid$='_Dislikes']", "[data-testid$='_Dislikes'] + p"],
            "dislikes",
        )

        review["work_details"] = self._extract_field_with_selector(
            container,
            ["[data-testid$='_WorkDetails']", "[data-testid$='_WorkDetails'] + span"],
            "work_details",
        )

        # Extract metadata
        review["job_designation"] = self._extract_field_with_selector(
            container,
            ["[data-testid$='_JobProfileName']"],
            "job_designation",
        )

        review["role_and_employment"] = self._extract_field_with_selector(
            container,
            ["[data-testid$='_RoleAndEmployment']"],
            "role_and_employment",
        )

        # Extract date
        review_date = self._extract_date(container)
        if review_date:
            review["review_date"] = review_date
        else:
            self._track_missing_field("review_date")

        # Extract or generate review ID
        review_id = self._extract_review_id(container)
        if not review_id:
            review_id = self._generate_review_id(review, company_id, page_num, review_idx)
        review["review_id"] = review_id

        # Extract source URL if available
        review["review_source_url"] = self._extract_source_url(container)

        return review

    def _extract_overall_rating(self, container) -> Optional[float]:
        """Extract overall rating from container."""
        selectors = [
            "[data-testid$='_RatingRow'] span.text-primary-text",
            "[data-testid$='_RatingRow'] span",
        ]
        for selector in selectors:
            try:
                element = container.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    # Extract numeric rating
                    for char in text:
                        if char.replace(".", "").isdigit():
                            return float(text.split()[0])
            except:
                continue
        return None

    def _extract_subratings(self, container) -> Dict[str, Optional[float]]:
        """Extract all subratings from container."""
        subratings = {}

        # Find rating carousel
        carousel = container.select_one("[data-testid='RatingCarousel']")
        if not carousel:
            # Fallback to general rating search
            carousel = container

        rating_spans = carousel.find_all("span", {"class": lambda x: x and "text-primary-text" in x})

        for i, span in enumerate(rating_spans):
            if i < len(self.SUBRATING_CATEGORIES):
                category = self.SUBRATING_CATEGORIES[i].lower().replace(" ", "_").replace("-", "_")
                try:
                    rating_text = span.get_text(strip=True)
                    rating = float(rating_text.split()[0]) if rating_text else None
                    subratings[f"rating_{category}"] = rating
                except:
                    subratings[f"rating_{category}"] = None
                    self._track_missing_field(f"rating_{category}")

        return subratings

    def _extract_field_with_selector(
        self, container, selectors: list, field_name: str
    ) -> Optional[str]:
        """
        Extract text field using multiple selector fallbacks.
        
        Args:
            container: BeautifulSoup element
            selectors: List of CSS selectors to try
            field_name: Field name for logging
            
        Returns:
            Extracted text or None
        """
        for selector in selectors:
            try:
                element = container.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text:
                        return text
            except:
                continue

        self._track_missing_field(field_name)
        return None

    def _extract_date(self, container) -> Optional[str]:
        """Extract review date."""
        # Try meta tag
        meta = container.find("meta", {"itemprop": "datePublished"})
        if meta:
            return meta.get("content")

        # Try time element
        time_elem = container.find("time")
        if time_elem:
            return time_elem.get("datetime", time_elem.get_text(strip=True))

        # Try text pattern in container
        text = container.get_text()
        for pattern in ["Mar 2026", "Mar 2025", "2026", "2025"]:
            if pattern in text:
                return pattern

        self._track_missing_field("review_date")
        return None

    def _extract_review_id(self, container) -> Optional[str]:
        """Extract unique review ID from container."""
        # Try div id
        review_div = container.find("div", {"id": lambda x: x and x.startswith("review-")})
        if review_div:
            return review_div.get("id")

        # Try data-testid
        testid = container.get("data-testid")
        if testid and "ReviewCard_" in testid:
            return testid.split("_")[-1]

        return None

    def _extract_source_url(self, container) -> Optional[str]:
        """Extract source URL from container if available."""
        link = container.find("a", href=True)
        if link:
            return link.get("href")
        return None

    def _generate_review_id(
        self, review: Dict, company_id: str, page_num: int, review_idx: int
    ) -> str:
        """
        Generate deterministic review ID from review content.
        
        Args:
            review: Review dictionary
            company_id: Company identifier
            page_num: Page number
            review_idx: Review index on page
            
        Returns:
            Generated review ID
        """
        content = f"{company_id}_{page_num}_{review_idx}"
        hash_obj = hashlib.md5(content.encode())
        return hash_obj.hexdigest()[:12]

    def _track_missing_field(self, field_name: str) -> None:
        """Track missing fields for reporting."""
        if field_name not in self.extraction_stats["missing_fields"]:
            self.extraction_stats["missing_fields"][field_name] = 0
        self.extraction_stats["missing_fields"][field_name] += 1

    def get_extraction_report(self) -> Dict[str, Any]:
        """
        Get extraction completeness report.
        
        Returns:
            Dictionary with extraction statistics
        """
        total = self.extraction_stats["total_attempted"]
        successful = self.extraction_stats["successful"]
        success_rate = (successful / total * 100) if total > 0 else 0

        return {
            "total_attempted": total,
            "successful": successful,
            "failed": self.extraction_stats["failed"],
            "success_rate_percent": success_rate,
            "missing_fields": self.extraction_stats["missing_fields"],
        }

    def reset_stats(self) -> None:
        """Reset extraction statistics."""
        self.extraction_stats = {
            "total_attempted": 0,
            "successful": 0,
            "failed": 0,
            "missing_fields": {},
        }
