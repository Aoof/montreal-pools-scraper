from bs4 import BeautifulSoup, Tag
from utils import *
import requests
import json
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

    pools : list[Pool]

    def __init__(self):
        self.db_handler = PoolMyFingerDB()
        self.pools = []

    def get_link(self, pool_type : PoolType, page : int = 1):
        """
        Provides the pools link using `mtl_content.lieux.installation.code` search param
        and adding the type and page too

        Args:
            type (str): The type of pools like PISI
            page (int): The page number requested (default: 1)
        
        Returns:
            str: The link with all the right search params
        """
        return f"{self.POOLS_URL}{str(pool_type)}&page={page}"

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

    def get_pools(self, pool_type : PoolType, pages : int) -> list[Pool]:
        logger.info(f"Fetching pool links for type '{pool_type}' across {pages} page(s)")
        for page_num in range(1, pages + 1):
            url = self.get_link(pool_type, page_num)
            logger.info(f"Processing page {page_num}/{pages}: {url}")
            page = self.get_webpage(url)
            page_soup = BeautifulSoup(page, "html.parser")

            map_el = page_soup.select_one("div[data-map-map]")
            if (map_el and isinstance(map_el, Tag)):
                data = json.loads(str(map_el["data-map-map"]))
                features = data["coordinates"]["features"]
                logger.debug(f"Found {len(features)} feature(s) on page {page_num}")

                for feature in features:
                    lon, lat = feature["geometry"]["coordinates"]

                    desc_html = feature["properties"]["description"]
                    desc = BeautifulSoup(desc_html, "html.parser")

                    a = desc.select_one("a.link-list-element")
                    if a is None:
                        logger.warning(f"No link element found for feature at ({lat}, {lon}), skipping")
                        continue
                    name = a.get_text(strip=True)
                    slug = a["href"]

                    logger.debug(f"Found pool: {name} ({slug})")
                    self.pools.append(Pool(
                        name         = name,
                        url          = f"https://montreal.ca{slug}",
                        geo_location = f"{lat}:{lon}",
                        pool_type    = pool_type
                    ))
            else:
                logger.warning(f"No map element found on page {page_num} for type '{pool_type}'")

        logger.info(f"Collected {len(self.pools)} pool(s) for type '{pool_type}'")
        return self.pools

    def populate_pools(self):
        for pool in self.pools:
            content = self.get_webpage(pool.url)
            pool_soup = BeautifulSoup(content)

            # We got the following info 
            # pool.name
            # pool.url
            # pool.geo_location
            # pool.pool_type

            # Missing the following info
            # pool.address: str = "",
            # pool.primary_image_url: str = "",
            # pool.map_link: str = "",
            # pool.phone: str = "",
            # pool.createdAt: float = time.time(),
            # pool.is_active: bool = True,
            # pool.schedules: list[Unknown] = []
            # This is gonna be difficult... We need to take into account every possibility, but also we must see every possibility

    def get_pages_for_tag(self, pool_type : PoolType) -> int:
        tag_link = self.get_link(pool_type)
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
                logger.info(f"Tag '{pool_type}': {total_results} results across {pages} page(s)")
                return pages

        logger.warning(f"Could not find results count for tag '{pool_type}'")
        return 0

if __name__ == "__main__":
    scraper = PoolMyFingerScraper()
    for t in TYPES:
        page_count = scraper.get_pages_for_tag(t)
        pools = scraper.get_pools(t, page_count)
