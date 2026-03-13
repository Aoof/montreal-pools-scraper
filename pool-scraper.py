from __future__ import annotations

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup, Tag

from utils import get_logger, PoolMyFingerDB, ListingPageParser, PoolDetailParser
from utils.scraper_types import Pool, PoolType, Schedule, TYPES

logger = get_logger("scraper")

_thread_local = threading.local()


class PoolMyFingerScraper:
    # Selectors

    HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xhtml+xml,"
            "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }
    POOLS_URL: str = (
        "https://montreal.ca/en/places?mtl_content.lieux.installation.code="
    )

    pools: list[Pool]

    def __init__(self) -> None:
        self._get_db()  # initialise DB connection for the main thread
        self.pools: list[Pool] = []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_db(self) -> PoolMyFingerDB:
        """Return a per-thread DB handler, creating one on first call per thread."""
        if not hasattr(_thread_local, "db_handler"):
            _thread_local.db_handler = PoolMyFingerDB()
        return _thread_local.db_handler

    def get_link(self, pool_type: PoolType, page: int = 1) -> str:
        """
        Build the listing URL for a given pool type and page number.

        Args:
            pool_type: The pool type (e.g. PISI).
            page:      Page number, 1-based (default: 1).

        Returns:
            The full URL string.
        """
        return f"{self.POOLS_URL}{pool_type!s}&page={page}"

    def get_webpage(self, url: str) -> bytes:
        """
        Return page content, serving from cache when it is less than 1 week old.

        Args:
            url: The URL to fetch.

        Returns:
            Raw page bytes.
        """
        CACHE_TTL = timedelta(weeks=1)

        cached = self._get_db().check_cache(url)
        if cached and len(cached) == 3:
            _url, content, last_scrape = cached  # types from db_controller (Any)
            if isinstance(content, bytes) and last_scrape is not None:
                if isinstance(last_scrape, str):
                    try:
                        last_scrape = datetime.fromisoformat(last_scrape)
                    except ValueError:
                        last_scrape = None
                if isinstance(last_scrape, datetime):
                    if last_scrape.tzinfo is None:
                        last_scrape = last_scrape.replace(tzinfo=timezone.utc)
                    age = datetime.now(timezone.utc) - last_scrape
                    if age < CACHE_TTL:
                        logger.info(f"Serving from cache: {url} (age: {age})")
                        return content
                    logger.info(f"Cache stale for {url} (age: {age}), fetching fresh")

        logger.info(f"Fetching: {url}")
        res = requests.get(url, headers=self.HEADERS, timeout=15)
        res.raise_for_status()
        logger.info(
            f"Fetched {len(res.content)} bytes from {url} (status {res.status_code})"
        )
        self._get_db().store_site(url, res.content)
        return res.content

    # ------------------------------------------------------------------
    # First pass: collect pool stubs
    # ------------------------------------------------------------------

    def get_pools(self, pool_type: PoolType, pages: int) -> list[Pool]:
        """
        FIRST PASS — scrape basic pool data from listing pages.

        Only name, url, geo_location, pool_type and map_link are populated;
        run `populate_pools()` afterwards to fill in the remaining fields.

        Args:
            pool_type: The pool type / tag to scrape.
            pages:     Number of listing pages available for this type.

        Returns:
            The accumulated list of Pool stubs (appends to self.pools).
        """
        logger.info(
            f"Fetching pool links for type '{pool_type}' across {pages} page(s)"
        )
        for page_num in range(1, pages + 1):
            url = self.get_link(pool_type, page_num)
            logger.info(f"Processing page {page_num}/{pages}: {url}")
            page = self.get_webpage(url)
            pools_on_page = ListingPageParser.get_pools(page, pool_type)
            self.pools.extend(pools_on_page)
            logger.debug(f"Found {len(pools_on_page)} pool(s) on page {page_num}")

        logger.info(f"Collected {len(self.pools)} pool(s) for type '{pool_type}'")
        return self.pools

    # ------------------------------------------------------------------
    # Second pass: populate detail fields
    # ------------------------------------------------------------------

    def _populate_one(self, pool: Pool) -> None:
        """Fetch a single pool's detail page and fill in all fields."""
        logger.info(f"Populating pool: {pool.name} ({pool.url})")
        try:
            content = self.get_webpage(pool.url)
            pool_soup = BeautifulSoup(content, "html.parser")

            pool.address = PoolDetailParser.parse_address(pool_soup)
            pool.phone = PoolDetailParser.parse_phone(pool_soup)
            pool.primary_image_url = PoolDetailParser.parse_primary_image_url(pool_soup)

            schedules = PoolDetailParser.parse_schedules(pool_soup)
            if schedules:
                pool.schedules = schedules
                pool.is_active = True
                logger.info(
                    f"  -> {len(schedules)} schedule(s) found, pool is active"
                )
            else:
                pool.is_active = False
                logger.info("  -> No schedules found, pool marked inactive")

        except Exception as exc:
            logger.error(f"Failed to populate pool '{pool.name}': {exc}")
            pool.is_active = False

    def populate_pools(self, max_workers: int = 10) -> None:
        """
        SECOND PASS — concurrently fetch each pool's detail page and fill in:
          - address, phone, primary_image_url
          - schedules  (via VARIANT_PARSERS)
          - is_active  (False when no parser matched)

        Args:
            max_workers: Maximum number of parallel threads (default: 10).
        """
        logger.info(
            f"Populating {len(self.pools)} pool(s) with up to {max_workers} workers"
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._populate_one, pool): pool
                for pool in self.pools
            }
            for future in as_completed(futures):
                future.result()  # surfaces any unexpected uncaught exception

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_pages_for_tag(self, pool_type: PoolType) -> int:
        """Return the number of listing pages for a given pool type."""
        tag_link = self.get_link(pool_type)
        content = self.get_webpage(tag_link)
        soup = BeautifulSoup(content, features="html.parser")
        results_element = soup.select_one(ListingPageParser.RESULTS_SELECTOR)

        if isinstance(results_element, Tag):
            text = results_element.get_text()
            match = re.search(r"(\d+) results", text)
            if match:
                total_results = int(match.group(1))
                pages = (total_results + 99) // 100   # ceiling division
                logger.info(
                    f"Tag '{pool_type}': {total_results} results across {pages} page(s)"
                )
                return pages

        logger.warning(f"Could not find results count for tag '{pool_type}'")
        return 0


if __name__ == "__main__":
    scraper = PoolMyFingerScraper()
    for type_key in TYPES:
        pool_type = PoolType(TYPES[type_key])
        page_count = scraper.get_pages_for_tag(pool_type)
        scraper.get_pools(pool_type, page_count)
    scraper.populate_pools()
