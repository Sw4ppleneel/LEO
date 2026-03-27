"""
Company Extractor module.

Extracts company-level metadata and ratings from company review pages using JSON data.
"""

import logging
import json
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger(__name__)


class CompanyExtractor:
    """Extracts company-level information from review pages."""

    def __init__(self):
        """Initialize company extractor."""
        pass

    def extract_company_info(
        self, html_content: str, company_name: str, industry: str
    ) -> Dict[str, Any]:
        """
        Extract company-level information from page using JSON first, then DOM fallback.
        
        Args:
            html_content: Page HTML content
            company_name: Company name from config
            industry: Industry from config
            
        Returns:
            Dictionary with company information
        """
        soup = BeautifulSoup(html_content, "lxml")

        company_info = {
            "company_name": company_name,
            "industry": industry,
            "scrape_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Try JSON extraction first
        json_data = self._extract_from_json(soup)
        if json_data:
            company_info.update(json_data)
            logger.info(f"Extracted company info for {company_name} from JSON")
            return company_info
        
        # Fallback to DOM extraction
        logger.debug("JSON extraction for company info failed, falling back to DOM")
        
        # Extract overall rating
        overall_rating = self._extract_overall_rating(soup)
        if overall_rating is not None:
            company_info["overall_rating"] = overall_rating
            logger.info(f"Overall rating for {company_name}: {overall_rating}")
        else:
            logger.warning(f"Could not extract overall rating for {company_name}")
            company_info["overall_rating"] = None

        # Extract total number of reviews
        total_reviews = self._extract_total_reviews(soup)
        if total_reviews is not None:
            company_info["total_reviews_on_platform"] = total_reviews
            logger.info(f"Total reviews for {company_name}: {total_reviews}")
        else:
            company_info["total_reviews_on_platform"] = None

        # Extract category ratings
        category_ratings = self._extract_category_ratings(soup)
        company_info.update(category_ratings)

        logger.info(f"Extracted company info for {company_name}")
        return company_info

    def _extract_from_json(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Extract company-level data from embedded JSON.
        
        Args:
            soup: BeautifulSoup parsed document
            
        Returns:
            Dictionary with company data or None if extraction fails
        """
        try:
            # Find the JSON script tag
            script_tag = soup.find('script', {'type': 'application/json'})
            if not script_tag or not script_tag.string:
                return None
            
            # Parse JSON
            data = json.loads(script_tag.string)
            pageProps = data.get('props', {}).get('pageProps', {})
            
            if not pageProps:
                return None
            
            company_data = {}
            
            # Extract company name and ID
            if pageProps.get('companyName'):
                company_data['company_name'] = pageProps['companyName']
            
            if pageProps.get('companyId'):
                company_data['company_id'] = pageProps['companyId']
            
            # Extract overall ratings
            ratings_data = pageProps.get('ratingsData', {})
            if isinstance(ratings_data, dict):
                company_data['overall_rating'] = ratings_data.get('overallCompanyRating')
                company_data['salary_rating'] = ratings_data.get('compensationBenefitsRating')
                company_data['culture_rating'] = ratings_data.get('companyCultureRating')
                company_data['job_security_rating'] = ratings_data.get('jobSecurityRating')
                company_data['skill_development_rating'] = ratings_data.get('skillDevelopmentRating')
                company_data['work_life_balance_rating'] = ratings_data.get('workLifeRating')
                company_data['work_satisfaction_rating'] = ratings_data.get('workSatisfactionRating')
                company_data['career_growth_rating'] = ratings_data.get('careerGrowthRating')
            
            # Extract review counts
            rating_counts = pageProps.get('ratingCounts', {})
            if isinstance(rating_counts, dict):
                company_data['total_reviews_on_platform'] = rating_counts.get('overallCompanyRating', pageProps.get('reviewCount'))
            
            # Extract last updated time
            if pageProps.get('lastUpdatedAt'):
                company_data['last_updated'] = pageProps['lastUpdatedAt']
            
            return company_data if company_data else None
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.debug(f"Failed to extract company data from JSON: {e}")
            return None

    def _extract_overall_rating(self, soup: BeautifulSoup) -> Optional[float]:
        """
        Extract overall company rating.
        
        Args:
            soup: BeautifulSoup parsed document
            
        Returns:
            Overall rating as float or None
        """
        selectors = [
            "span.text-primary-text",  # Primary rating display
            "div:has-text('3.8') span",  # Common pattern
        ]

        for selector in selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    # Check if text contains a valid rating
                    if text and text[0].isdigit():
                        try:
                            rating = float(text.split()[0])
                            if 0 <= rating <= 5:
                                return rating
                        except ValueError:
                            continue
            except Exception as e:
                logger.debug(f"Error extracting rating with selector {selector}: {e}")
                continue

        return None

    def _extract_total_reviews(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract total number of reviews on platform.
        
        Args:
            soup: BeautifulSoup parsed document
            
        Returns:
            Total reviews count or None
        """
        patterns = [
            ("Reviews", "17k"),  # Common pattern from page
            ("based on", "reviews"),
        ]

        text = soup.get_text()

        # Look for "Reviews" section header with number
        for line in text.split("\n"):
            if "Reviews" in line and "k" in line:
                try:
                    # Extract number before 'k'
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.endswith("k"):
                            num = float(part[:-1]) * 1000
                            return int(num)
                except:
                    continue

        return None

    def _extract_category_ratings(self, soup: BeautifulSoup) -> Dict[str, Optional[float]]:
        """
        Extract category-wise ratings.
        
        Args:
            soup: BeautifulSoup parsed document
            
        Returns:
            Dictionary with category ratings
        """
        categories = {
            "salary": "Salary",
            "company_culture": "Company Culture",
            "job_security": "Job Security",
            "skill_development": "Skill Development",
            "work_life_balance": "Work-Life Balance",
            "work_satisfaction": "Work Satisfaction",
            "promotions": "Promotions",
        }

        category_ratings = {}

        # Look for category ratings section
        rating_elements = soup.find_all("div", class_=lambda x: x and "Category" in x)

        if rating_elements:
            text = soup.get_text()
            for key, label in categories.items():
                category_ratings[f"category_rating_{key}"] = self._extract_category_value(
                    text, label
                )
        else:
            # Initialize with None if not found
            for key in categories:
                category_ratings[f"category_rating_{key}"] = None

        return category_ratings

    def _extract_category_value(self, text: str, category_name: str) -> Optional[float]:
        """
        Extract rating value for a specific category.
        
        Args:
            text: Full page text
            category_name: Category name to search for
            
        Returns:
            Rating value or None
        """
        try:
            # Find the category name in text
            idx = text.find(category_name)
            if idx != -1:
                # Look for a number after the category name (within next 50 chars)
                substring = text[idx : idx + 50]
                for word in substring.split():
                    try:
                        rating = float(word)
                        if 0 <= rating <= 5:
                            return rating
                    except ValueError:
                        continue
        except Exception as e:
            logger.debug(f"Error extracting category {category_name}: {e}")

        return None
