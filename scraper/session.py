"""
Session Manager for Playwright-based browser automation.

Handles browser initialization, page navigation, explicit waits,
and user-agent management.
"""

import logging
from typing import Optional
from playwright.async_api import async_playwright, Browser, Page
import random
import asyncio

logger = logging.getLogger(__name__)


class BrowserSession:
    """Manages a Playwright browser session with realistic user behavior."""

    def __init__(self, headless: bool = True, timeout: int = 30000):
        """
        Initialize browser session.
        
        Args:
            headless: Run browser in headless mode
            timeout: Page timeout in milliseconds
        """
        self.headless = headless
        self.timeout = timeout
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None

    async def initialize(self) -> Page:
        """
        Initialize Playwright browser and create a new page.
        
        Returns:
            Playwright Page instance
        """
        try:
            self.playwright = await async_playwright().start()
            
            # Switch to Firefox to bypass basic bot protection
            # Add memory-saving arguments
            self.browser = await self.playwright.firefox.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--no-first-run",
                    "--disable-sync",
                ]
            )
            
            self.page = await self.browser.new_page()
            
            # Set realistic user agent
            await self.page.set_extra_http_headers({
                "User-Agent": self._get_user_agent()
            })
            
            # Set viewport for consistent rendering (smaller to save memory)
            await self.page.set_viewport_size({"width": 1280, "height": 720})
            
            # Set page timeout (60 seconds for slower connections)
            self.page.set_default_timeout(60000)
            
            logger.info("Browser session initialized successfully")
            return self.page

        except Exception as e:
            logger.error(f"Failed to initialize browser session: {e}")
            raise

    async def navigate_to(self, url: str) -> None:
        """
        Navigate to a URL with minimal wait - just navigate and let DOM render.
        
        Args:
            url: Target URL
        """
        if not self.page:
            raise RuntimeError("Browser session not initialized")
        
        try:
            # Navigate without waiting for full load - JS may never complete
            # Set a very short initial timeout
            try:
                await self.page.goto(url, timeout=15000)
            except Exception as goto_err:
                # Even if goto "times out", the page may have loaded enough
                logger.debug(f"goto timeout, but page may be partially loaded: {goto_err}")
            
            # Wait for page to settle and JS to execute
            await asyncio.sleep(8)
            
            logger.info(f"Navigated to {url}")
        except Exception as e:
            logger.error(f"Navigation encountered error for {url}: {e}")
            # Don't raise - page may still be usable

    async def wait_for_selector(self, selector: str, timeout: int = 10000) -> None:
        """
        Wait for a selector to be visible.
        
        Args:
            selector: CSS selector
            timeout: Wait timeout in milliseconds
        """
        if not self.page:
            raise RuntimeError("Browser session not initialized")
        
        try:
            await self.page.wait_for_selector(selector, timeout=timeout)
            logger.debug(f"Selector found: {selector}")
        except Exception as e:
            logger.warning(f"Timeout waiting for selector {selector}: {e}")
            raise

    async def scroll_into_view(self, selector: str) -> None:
        """
        Scroll element into view.
        
        Args:
            selector: CSS selector of element to scroll to
        """
        if not self.page:
            raise RuntimeError("Browser session not initialized")
        
        try:
            await self.page.locator(selector).scroll_into_view_if_needed()
            await asyncio.sleep(self._random_delay(0.5, 1.5))
            logger.debug(f"Scrolled into view: {selector}")
        except Exception as e:
            logger.warning(f"Failed to scroll selector {selector}: {e}")
            raise

    async def click_element(self, selector: str) -> None:
        """
        Click an element with scroll into view.
        
        Args:
            selector: CSS selector of element to click
        """
        if not self.page:
            raise RuntimeError("Browser session not initialized")
        
        try:
            await self.scroll_into_view(selector)
            await self.page.locator(selector).click()
            await asyncio.sleep(self._random_delay(1, 3))
            logger.debug(f"Clicked element: {selector}")
        except Exception as e:
            logger.warning(f"Failed to click element {selector}: {e}")
            raise

    async def get_page_content(self) -> str:
        """
        Get the current page's HTML content.
        
        Returns:
            HTML content as string
        """
        if not self.page:
            raise RuntimeError("Browser session not initialized")
        
        # Retry mechanism to handle intermediate navigation/redirects
        last_error = None
        for attempt in range(4):
            try:
                # Wait a bit for any mid-flight navigation to settle
                if attempt > 0:
                    await asyncio.sleep(2)
                return await self.page.content()
            except Exception as e:
                last_error = e
                logger.debug(f"Failed to get page content (attempt {attempt+1}): {e}")
        
        logger.warning(f"Could not retrieve page content cleanly: {last_error}")
        # One last desperate try
        return await self.page.content()

    async def close(self) -> None:
        """Close browser session gracefully."""
        try:
            if self.page:
                await self.page.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("Browser session closed successfully")
        except Exception as e:
            logger.error(f"Error closing browser session: {e}")

    @staticmethod
    def _get_user_agent() -> str:
        """Return a realistic user agent string."""
        user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        ]
        return random.choice(user_agents)

    @staticmethod
    def _random_delay(min_sec: float, max_sec: float) -> float:
        """Generate random delay between min and max seconds."""
        return random.uniform(min_sec, max_sec)
