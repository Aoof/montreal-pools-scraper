from __future__ import annotations

import argparse
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

from utils import (
    get_logger,
    PoolMyFingerDB,
    ListingPageParser,
    PoolDetailParser,
    ScraperApiClient,
)
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
        self._pools_by_url: dict[str, Pool] = {}

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
            for pool in pools_on_page:
                self._merge_pool_stub(pool)
            logger.debug(f"Found {len(pools_on_page)} pool(s) on page {page_num}")

        logger.info(f"Collected {len(self.pools)} pool(s) for type '{pool_type}'")
        return self.pools

    def _merge_pool_stub(self, candidate: Pool) -> None:
        existing = self._pools_by_url.get(candidate.url)
        if existing is None:
            self._pools_by_url[candidate.url] = candidate
            self.pools.append(candidate)
            return

        existing.add_pool_type(candidate.pool_type)

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
    parser.add_argument(
        "--write-api",
        action="store_true",
        help="Write scraped pools to the backend API using create/update upserts.",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.environ.get("SCRAPER_API_BASE_URL", "http://localhost/pool-my-finger/src/backend/api"),
        help="Backend API base URL (default: SCRAPER_API_BASE_URL or local API path).",
    )
    parser.add_argument(
        "--api-username",
        default=os.environ.get("SCRAPER_API_USERNAME"),
        help="Backend API username (default: SCRAPER_API_USERNAME).",
    )
    parser.add_argument(
        "--api-password",
        default=os.environ.get("SCRAPER_API_PASSWORD"),
        help="Backend API password (default: SCRAPER_API_PASSWORD).",
    )
    parser.add_argument(
        "--api-timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds for API requests (default: 20).",
    )
    parser.add_argument(
        "--api-retries",
        type=int,
        default=2,
        help="Retry attempts for transient API/network failures (default: 2).",
    )
    parser.add_argument(
        "--dry-run-write",
        action="store_true",
        help="Compute API writes without mutating backend data.",
    )
    parser.add_argument(
        "--type-aliases-file",
        type=Path,
        default=Path(__file__).with_name("type_aliases.json"),
        help="JSON file mapping scraper type aliases to canonical pool type names.",
    )
    parser.add_argument(
        "--continue-on-api-error",
        action="store_true",
        help="Continue processing remaining pools after API write failures.",
    )

    args = parser.parse_args()

    if args.max_pages is not None and args.max_pages < 1:
        parser.error("--max-pages must be >= 1")
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.api_timeout < 1:
        parser.error("--api-timeout must be >= 1")
    if args.api_retries < 0:
        parser.error("--api-retries must be >= 0")
    if args.write_api and (not args.api_username or not args.api_password):
        parser.error("--write-api requires --api-username and --api-password (or env vars)")

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
    def _parse_date(value: str) -> str | None:
        if not isinstance(value, str):
            return None
        clean = value.replace("\u00a0", " ").strip()
        if not clean:
            return None
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%B %d", "%b %d"):
            try:
                dt = datetime.strptime(clean, fmt)
                if "%Y" not in fmt:
                    dt = dt.replace(year=datetime.now(timezone.utc).year)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _normalize_dates(start_raw: str, end_raw: str) -> tuple[str, str]:
        start_iso = _parse_date(start_raw)
        end_iso = _parse_date(end_raw)

        if start_iso and end_iso:
            return start_iso, end_iso

        joined = (start_raw or "").replace("\u00a0", " ").strip()
        match = re.search(r"from\s+(.+?)\s+to\s+(.+)", joined, flags=re.IGNORECASE)
        if match:
            parsed_start = _parse_date(match.group(1))
            parsed_end = _parse_date(match.group(2))
            if parsed_start and parsed_end:
                return parsed_start, parsed_end

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if start_iso and not end_iso:
            return start_iso, start_iso
        if end_iso and not start_iso:
            return end_iso, end_iso
        return today, today

    start_iso, end_iso = _normalize_dates(schedule.effective_date, schedule.end_date)

    return {
        "effective_date": schedule.effective_date,
        "end_date": schedule.end_date,
        "activity": schedule.activity,
        "effective_date_iso": start_iso,
        "end_date_iso": end_iso,
        "activity_name": schedule.activity.strip() or "General",
        "time_blocks": [
            {
                "day": block.day,
                "start": block.start.strftime("%H:%M"),
                "end": block.end.strftime("%H:%M"),
                "label": block.label,
                "day_of_week": block.day.lower(),
                "start_time": block.start.strftime("%H:%M"),
                "end_time": block.end.strftime("%H:%M"),
            }
            for block in schedule.time_blocks
        ],
    }


def _pool_to_dict(pool: Pool) -> dict[str, Any]:
    digits = re.sub(r"\D+", "", pool.phone or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    normalized_phone = (
        f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}" if len(digits) == 10 else (digits[:12] or None)
    )

    pool_types = pool.pool_types if pool.pool_types else [pool.pool_type]
    primary_type = pool_types[0]

    return {
        "name": pool.name,
        "pool_type": str(primary_type),
        "pool_type_name": primary_type.name,
        "pool_types": [
            {
                "code": str(pool_type),
                "name": pool_type.name,
                "description": pool_type.description,
            }
            for pool_type in pool_types
        ],
        "url": pool.url,
        "address": pool.address,
        "primary_image_url": pool.primary_image_url,
        "map_link": pool.map_link,
        "geo_location": pool.geo_location,
        "phone": pool.phone,
        "created_at": pool.createdAt,
        "is_active": pool.is_active,
        "schedules": [_schedule_to_dict(s) for s in pool.schedules],
        "db_record": {
            "pool_type_name": (primary_type.name or "Unknown")[:50],
            "pool_type_description": (str(primary_type) or "")[:255] or None,
            "pool_type_names": [
                (pool_type.name or "Unknown")[:50]
                for pool_type in pool_types
            ],
            "name": (pool.name or "Unknown Pool")[:255],
            "full_address": pool.address or None,
            "primary_image_url": pool.primary_image_url or None,
            "website": pool.url or None,
            "map_link": pool.map_link or None,
            "phone": normalized_phone,
            "is_active": 1 if pool.is_active else 0,
            "schedules": [_schedule_to_dict(s) for s in pool.schedules],
        },
    }


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def _pool_identity_key(name: str | None, address: str | None) -> str:
    return f"{_normalize_text(name)}|{_normalize_text(address)}"


def _load_type_aliases(path: Path | None) -> dict[str, str]:
    aliases: dict[str, str] = {}

    for code, name in TYPES.items():
        aliases[_normalize_text(code)] = name
        aliases[_normalize_text(name)] = name

    if path is None or not path.exists():
        return aliases

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("Type aliases file must contain a JSON object")

    for alias, canonical in payload.items():
        if not isinstance(alias, str) or not isinstance(canonical, str):
            continue
        aliases[_normalize_text(alias)] = canonical

    return aliases


def _extract_canonical_type_names(pool: Pool, aliases: dict[str, str]) -> list[str]:
    extracted: list[str] = []
    pool_types = pool.pool_types if pool.pool_types else [pool.pool_type]

    for pool_type in pool_types:
        candidates = [str(pool_type), pool_type.name]
        for candidate in candidates:
            key = _normalize_text(candidate)
            if not key:
                continue
            canonical = aliases.get(key, candidate)
            normalized = canonical.strip()
            if normalized and normalized not in extracted:
                extracted.append(normalized)

    return extracted


def _extract_lat_lon(geo_location: str) -> tuple[float | None, float | None]:
    parts = geo_location.split(":")
    if len(parts) != 2:
        return None, None

    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def _pool_to_api_payload(pool: Pool, type_ids: list[int]) -> dict[str, Any]:
    latitude, longitude = _extract_lat_lon(pool.geo_location)
    return {
        "name": pool.name,
        "address": pool.address or None,
        "imageUrl": pool.primary_image_url or None,
        "website": pool.url or None,
        "map": pool.map_link or None,
        "latitude": latitude,
        "longitude": longitude,
        "phone": pool.phone or None,
        "active": bool(pool.is_active),
        "typeIds": type_ids,
    }


def _build_existing_pool_index(existing_pools: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for existing in existing_pools:
        name = existing.get("name")
        address = existing.get("address")
        if not isinstance(name, str):
            continue
        key = _pool_identity_key(name, address if isinstance(address, str) else None)
        indexed[key] = existing
    return indexed


def _write_pools_to_api(
    pools: list[Pool],
    client: ScraperApiClient,
    aliases: dict[str, str],
    dry_run: bool,
    fail_fast: bool,
) -> dict[str, int]:
    type_records = client.get_pool_types()
    type_ids_by_name: dict[str, int] = {
        _normalize_text(pool_type.name): pool_type.id
        for pool_type in type_records
    }

    existing_index = _build_existing_pool_index(client.get_pools())
    summary = {
        "created": 0,
        "updated": 0,
        "skipped_missing_type": 0,
        "failed_api": 0,
    }

    for pool in pools:
        canonical_names = _extract_canonical_type_names(pool, aliases)
        if not canonical_names:
            summary["skipped_missing_type"] += 1
            logger.error("Skipping '%s' because no pool types were extracted.", pool.name)
            continue

        unresolved = [
            name
            for name in canonical_names
            if _normalize_text(name) not in type_ids_by_name
        ]
        if unresolved:
            summary["skipped_missing_type"] += 1
            logger.error(
                "Skipping '%s' because pool type(s) do not exist in API: %s",
                pool.name,
                ", ".join(unresolved),
            )
            continue

        resolved_type_ids = [type_ids_by_name[_normalize_text(name)] for name in canonical_names]
        payload = _pool_to_api_payload(pool, resolved_type_ids)
        identity_key = _pool_identity_key(pool.name, pool.address)
        existing = existing_index.get(identity_key)

        try:
            if existing is None:
                if dry_run:
                    summary["created"] += 1
                    logger.info("DRY RUN create: %s", pool.name)
                    continue

                created = client.create_pool(payload)
                created_id = created.get("id") if isinstance(created, dict) else None
                if isinstance(created_id, int):
                    existing_index[identity_key] = created
                summary["created"] += 1
                logger.info("Created pool via API: %s", pool.name)
                continue

            existing_id = existing.get("id") if isinstance(existing, dict) else None
            if not isinstance(existing_id, int):
                raise RuntimeError(
                    f"Existing pool is missing a valid numeric id for key {identity_key}"
                )

            if dry_run:
                summary["updated"] += 1
                logger.info("DRY RUN update: %s (id=%s)", pool.name, existing_id)
                continue

            updated = client.update_pool(existing_id, payload)
            existing_index[identity_key] = updated
            summary["updated"] += 1
            logger.info("Updated pool via API: %s (id=%s)", pool.name, existing_id)
        except Exception as exc:
            summary["failed_api"] += 1
            logger.error("API write failed for '%s': %s", pool.name, exc)
            if fail_fast:
                raise

    return summary


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

    if args.write_api:
        type_aliases = _load_type_aliases(args.type_aliases_file)
        api_client = ScraperApiClient(
            base_url=args.api_base_url,
            username=args.api_username,
            password=args.api_password,
            timeout=args.api_timeout,
            retries=args.api_retries,
        )
        summary = _write_pools_to_api(
            pools=scraper.pools,
            client=api_client,
            aliases=type_aliases,
            dry_run=args.dry_run_write,
            fail_fast=not args.continue_on_api_error,
        )
        logger.info(
            "API sync summary: created=%s updated=%s skipped_missing_type=%s failed_api=%s",
            summary["created"],
            summary["updated"],
            summary["skipped_missing_type"],
            summary["failed_api"],
        )
