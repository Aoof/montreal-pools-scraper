from __future__ import annotations

import argparse
import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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

    def _populate_one(self, pool: Pool, extract_fields: set[str]) -> None:
        """Fetch a single pool's detail page and fill in all fields."""
        logger.info(f"Populating pool: {pool.name} ({pool.url})")
        try:
            content = self.get_webpage(pool.url)
            pool_soup = BeautifulSoup(content, "html.parser")

            if "address" in extract_fields:
                pool.address = PoolDetailParser.parse_address(pool_soup)
            if "phone" in extract_fields:
                pool.phone = PoolDetailParser.parse_phone(pool_soup)
            if "image" in extract_fields:
                pool.primary_image_url = PoolDetailParser.parse_primary_image_url(pool_soup)

            if "schedules" in extract_fields:
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
            if "schedules" in extract_fields:
                pool.is_active = False

    def populate_pools(self, max_workers: int = 10, extract_fields: set[str] | None = None) -> None:
        """
        SECOND PASS — concurrently fetch each pool's detail page and fill in:
          - address, phone, primary_image_url
          - schedules  (via VARIANT_PARSERS)
          - is_active  (False when no parser matched)

        Args:
            max_workers: Maximum number of parallel threads (default: 10).
        """
        fields = extract_fields if extract_fields is not None else {
            "address",
            "phone",
            "image",
            "schedules",
        }
        logger.info(
            f"Populating {len(self.pools)} pool(s) with up to {max_workers} workers"
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._populate_one, pool, fields): pool
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Montreal pool listings, with optional detail extraction and JSON export."
        )
    )

    parser.add_argument(
        "--types",
        nargs="+",
        choices=list(TYPES),
        default=list(TYPES),
        help="Pool type code(s) to scrape. Defaults to all.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit pages fetched per selected type.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Maximum worker threads for detail-page scraping (default: 10).",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Only scrape listing pages (name/url/location/type), skip detail pages.",
    )
    parser.add_argument(
        "--extract",
        nargs="+",
        choices=["all", "address", "phone", "image", "schedules"],
        default=["all"],
        help=(
            "Detail fields to extract when detail scraping is enabled. "
            "Default: all."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Write scraped data to a JSON file.",
    )
    parser.add_argument(
        "--pretty-json",
        action="store_true",
        help="Pretty-print JSON output (used with --output-json).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Console log level for scraper-related loggers (default: INFO).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console logs (equivalent to --log-level CRITICAL).",
    )

    args = parser.parse_args()

    if args.max_pages is not None and args.max_pages < 1:
        parser.error("--max-pages must be >= 1")
    if args.workers < 1:
        parser.error("--workers must be >= 1")

    return args


def _configure_logging(level: str, quiet: bool) -> None:
    configured_level = logging.CRITICAL if quiet else getattr(logging, level)
    for logger_name in ("scraper", "db_controller"):
        target = logging.getLogger(logger_name)
        for handler in target.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                handler.setLevel(configured_level)


def _resolve_extract_fields(raw_extract: list[str]) -> set[str]:
    all_fields = {"address", "phone", "image", "schedules"}
    if "all" in raw_extract:
        return all_fields
    return set(raw_extract)


def _schedule_to_dict(schedule: Schedule) -> dict[str, Any]:
    return {
        "effective_date": schedule.effective_date,
        "end_date": schedule.end_date,
        "activity": schedule.activity,
        "time_blocks": [
            {
                "day": block.day,
                "start": block.start.strftime("%H:%M"),
                "end": block.end.strftime("%H:%M"),
                "label": block.label,
            }
            for block in schedule.time_blocks
        ],
    }


def _pool_to_dict(pool: Pool) -> dict[str, Any]:
    return {
        "name": pool.name,
        "pool_type": str(pool.pool_type),
        "pool_type_name": pool.pool_type.name,
        "url": pool.url,
        "address": pool.address,
        "primary_image_url": pool.primary_image_url,
        "map_link": pool.map_link,
        "geo_location": pool.geo_location,
        "phone": pool.phone,
        "created_at": pool.createdAt,
        "is_active": pool.is_active,
        "schedules": [_schedule_to_dict(s) for s in pool.schedules],
    }


def _write_json_output(path: Path, pools: list[Pool], pretty: bool) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pool_count": len(pools),
        "pools": [_pool_to_dict(pool) for pool in pools],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2 if pretty else None)
        f.write("\n")
    logger.info(f"Wrote JSON output to {path}")


if __name__ == "__main__":
    args = _parse_args()
    _configure_logging(args.log_level, args.quiet)

    extract_fields = _resolve_extract_fields(args.extract)
    scraper = PoolMyFingerScraper()

    for type_key in args.types:
        pool_type = PoolType(TYPES[type_key])
        page_count = scraper.get_pages_for_tag(pool_type)
        if args.max_pages is not None:
            page_count = min(page_count, args.max_pages)
        scraper.get_pools(pool_type, page_count)

    if not args.skip_details:
        scraper.populate_pools(max_workers=args.workers, extract_fields=extract_fields)

    if args.output_json is not None:
        _write_json_output(args.output_json, scraper.pools, args.pretty_json)
