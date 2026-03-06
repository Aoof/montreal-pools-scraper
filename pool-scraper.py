from bs4 import BeautifulSoup
from utils import *
import requests
import time
import re
from datetime import datetime, timezone, timedelta

logger = get_logger('scraper')

class PoolMyFingerScraper:
    # Selectors
    RESULTS_SELECTOR = "div#searchResultList div#spinLoader div.row div h2"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }
    POOLS_URL = "https://montreal.ca/en/places?mtl_content.lieux.installation.code="

    def __init__(self):
        self.db_handler = PoolMyFingerDB()

    def get_link(self, type : str, page : int):
        return f"{self.POOLS_URL}{type}&page={page}"

    def get_webpage(self, url: str) -> bytes:
        """
        Fetches webpage content, utilizing cache if available and not older than 1 week.
        If cache is stale or missing, fetches fresh content and updates cache.

        Args:
            url (str): The URL to fetch.

        Returns:
            bytes: The webpage content.
        """
        CACHE_TTL = timedelta(weeks=1)

        cached = self.db_handler.check_cache(url)
        if cached and len(cached) == 3:
            _url, content, last_scrape = cached
            if isinstance(content, bytes) and last_scrape is not None:
                if isinstance(last_scrape, str):
                    try:
                        last_scrape = datetime.fromisoformat(last_scrape)
                    except ValueError:
                        last_scrape = None
                if last_scrape and isinstance(last_scrape, datetime):
                    if last_scrape.tzinfo is None:
                        last_scrape = last_scrape.replace(tzinfo=timezone.utc)
                    age = datetime.now(timezone.utc) - last_scrape
                    if age < CACHE_TTL:
                        logger.info(f"Serving from cache: {url} (age: {age})")
                        return content
                    logger.info(f"Cache stale for {url} (age: {age}), fetching fresh")

        # Fetch fresh content
        logger.info(f"Fetching: {url}")
        res = requests.get(url, headers=self.HEADERS, timeout=15)
        res.raise_for_status()
        logger.info(f"Fetched {len(res.content)} bytes from {url} (status {res.status_code})")
        self.db_handler.store_site(url, res.content)
        return res.content

    def get_pools(self):
        pass

    def get_pages_for_tag(self, tag : str) -> int:
        tag_link = self.get_link(tag, 1)
        content = self.get_webpage(tag_link)
        soup = BeautifulSoup(content, features="html.parser")
        results_element = soup.select_one(self.RESULTS_SELECTOR)
        
        if results_element:
            text = results_element.get_text()
            match = re.search(r'(\d+) results', text)
            if match:
                total_results = int(match.group(1))
                # Assuming 100 results per page
                pages = (total_results + 99) // 100  # Ceiling division
                logger.info(f"Tag '{tag}': {total_results} results across {pages} page(s)")
                return pages

        logger.warning(f"Could not find results count for tag '{tag}'")
        return 0

if __name__ == "__main__":
    scraper = PoolMyFingerScraper()
