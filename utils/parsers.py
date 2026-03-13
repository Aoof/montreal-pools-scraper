from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import cast

from bs4 import BeautifulSoup, Tag

from .scraper_types import (
    WEEKDAYS,
    Pool,
    PoolType,
    Schedule,
    TimeBlock,
    _TIME_RE,
    _parse_date_range,
    _parse_time,
    _text,
)

# ---------------------------------------------------------------------------
# Abstract base for variant parsers
# ---------------------------------------------------------------------------

class ScheduleVariantParser(ABC):
    """
    Subclass this to add support for a new HTML schedule pattern.

    The pool-populating loop calls `can_parse(soup)` on each registered
    parser in order; the first one that returns True gets to call `parse`.

    To add a new variant:
      1. Subclass ScheduleVariantParser, implement `can_parse` and `parse`.
      2. Append an instance to VARIANT_PARSERS at the bottom of this file.
    """

    @abstractmethod
    def can_parse(self, soup: BeautifulSoup) -> bool:
        """Return True if this parser recognises the page structure."""

    @abstractmethod
    def parse(self, soup: BeautifulSoup) -> list[Schedule]:
        """Extract and return all Schedule objects from the page."""

# ---------------------------------------------------------------------------
# VARIANT 1 -- section#HRS0  (open swim / lane swim activity tables)
# ---------------------------------------------------------------------------

class Variant1Parser(ScheduleVariantParser):
    """
    Matches pages that contain a <section id="HRS0"> block.

    Structure:
      section#HRS0
        h2                              -> activity name, e.g. "Open swim"
        .wrapper.wrapper-complex        (one per period; hidden ones have d-none)
          .wrapper-header
            time[datetime]  x2          -> period start / end dates
          .wrapper-body
            h3                          -> audience label
            table
              tbody > tr
                td[0]   -> weekday
                td[1]   -> "HH:MM am to HH:MM pm"  (may contain multiple divs)

    All periods are parsed (not just the visible one) to capture the full
    season schedule.
    """

    def can_parse(self, soup: BeautifulSoup) -> bool:
        return bool(soup.find("section", id="HRS0"))

    def parse(self, soup: BeautifulSoup) -> list[Schedule]:
        section = soup.select_one("section#HRS0")
        if section is None or not isinstance(section, Tag):
            return []

        schedules: list[Schedule] = []

        # activity name
        raw_h2 = section.find("h2")
        activity_name = _text(raw_h2) if isinstance(raw_h2, Tag) else "swimming"

        # correct container
        container = section.select_one(".content-module-stacked")
        if not isinstance(container, Tag):
            return []

        wrappers = container.select(".wrapper.wrapper-complex")

        for wrapper in wrappers:

            header = wrapper.select_one(".wrapper-header .font-weight-bold")
            period_label = _text(header) if isinstance(header, Tag) else ""

            body = wrapper.select_one(".wrapper-body")
            if not isinstance(body, Tag):
                continue

            stacked_groups = body.select(".content-module-stacked")

            for stacked in stacked_groups:

                raw_h3 = stacked.find("h3")
                audience = _text(raw_h3) if isinstance(raw_h3, Tag) else activity_name

                tables = stacked.find_all("table")

                for table in tables:
                    blocks = self._parse_table(table)

                    if blocks:
                        schedules.append(
                            Schedule(
                                activity=audience,
                                effective_date=period_label,
                                end_date="",
                                time_blocks=blocks,
                            )
                        )

        return schedules

    @staticmethod
    def _parse_table(table: Tag) -> list[TimeBlock]:

        blocks: list[TimeBlock] = []

        for tr in table.select("tbody tr"):

            cells = tr.find_all("td")
            if len(cells) < 2:
                continue

            day = _text(cells[0])
            if day.lower() not in WEEKDAYS:
                continue

            text = _text(cells[1])

            if "closed" in text.lower():
                continue

            times = list(_TIME_RE.finditer(text))

            if len(times) >= 2:
                start = _parse_time(times[0].group(0))
                end = _parse_time(times[1].group(0))

                if start and end:
                    blocks.append(
                        TimeBlock(
                            day=day,
                            start=start,
                            end=end,
                        )
                    )

        return blocks

# ---------------------------------------------------------------------------
# VARIANT 2 -- div#section-horaire  (regular weekly opening hours)
# ---------------------------------------------------------------------------

class Variant2Parser(ScheduleVariantParser):
    """
    Matches pages that contain a <div id="section-horaire"> block.

    Structure:
      div#section-horaire
        h2                         -> "Opening hours"
        .list-item-icon-label      -> schedule label (e.g. "Regular schedule")
        ul
          li.row
            .schedule-day          -> weekday
            .schedule-data         -> "8:00 am to 9:00 pm"

    Produces a single schedule with weekly recurring hours.
    """

    def can_parse(self, soup: BeautifulSoup) -> bool:
        return bool(soup.find("div", id="section-horaire"))

    def parse(self, soup: BeautifulSoup) -> list[Schedule]:

        section = soup.select_one("#section-horaire")
        if not isinstance(section, Tag):
            return []

        schedules: list[Schedule] = []

        label_tag = section.select_one(".list-item-icon-label")
        activity_name = _text(label_tag) if isinstance(label_tag, Tag) else "Opening hours"

        blocks: list[TimeBlock] = []

        rows = section.select("ul li.row")

        for row in rows:

            day_tag = row.select_one(".schedule-day")
            data_tag = row.select_one(".schedule-data")

            if not isinstance(day_tag, Tag) or not isinstance(data_tag, Tag):
                continue

            day = _text(day_tag)

            if day.lower() not in WEEKDAYS:
                continue

            text = _text(data_tag)

            if "closed" in text.lower():
                continue

            times = list(_TIME_RE.finditer(text))

            if len(times) >= 2:

                start = _parse_time(times[0].group(0))
                end = _parse_time(times[1].group(0))

                if start and end:
                    blocks.append(
                        TimeBlock(
                            day=day,
                            start=start,
                            end=end,
                        )
                    )

        if blocks:
            schedules.append(
                Schedule(
                    activity=activity_name,
                    effective_date="",
                    end_date="",
                    time_blocks=blocks,
                )
            )

        return schedules

# ---------------------------------------------------------------------------
# Registry -- add new variant parser instances here
# ---------------------------------------------------------------------------

VARIANT_PARSERS: list[ScheduleVariantParser] = [
    Variant1Parser(),
    Variant2Parser(),
]

# ---------------------------------------------------------------------------
# Listing page parser
# ---------------------------------------------------------------------------

class ListingPageParser:
    RESULTS_SELECTOR: str = "div#searchResultList div#spinLoader div.row div h2"

    @staticmethod
    def get_pages_count(content: bytes) -> int:
        soup = BeautifulSoup(content, "html.parser")
        results_element = soup.select_one(ListingPageParser.RESULTS_SELECTOR)
        if isinstance(results_element, Tag):
            text = results_element.get_text()
            match = re.search(r"(\d+) results", text)
            if match:
                total_results = int(match.group(1))
                pages = (total_results + 99) // 100
                return pages
        return 0

    @staticmethod
    def get_pools(content: bytes, pool_type: PoolType) -> list[Pool]:
        soup = BeautifulSoup(content, "html.parser")
        pools = []
        map_el = soup.select_one("div[data-map-map]")
        if map_el is not None and isinstance(map_el, Tag):
            data: dict[str, object] = json.loads(str(map_el["data-map-map"]))
            features: list[dict[str, object]] = data["coordinates"]["features"]  # type: ignore[index]
            for feature in features:
                geometry: dict[str, object] = feature["geometry"]  # type: ignore[assignment]
                coords: list[float] = geometry["coordinates"]  # type: ignore[assignment]
                lon, lat = coords[0], coords[1]

                props: dict[str, object] = feature["properties"]  # type: ignore[assignment]
                desc_html = str(props["description"])
                desc = BeautifulSoup(desc_html, "html.parser")

                a = desc.select_one("a.link-list-element")
                if a is None:
                    continue

                name = a.get_text(strip=True)
                slug = str(a["href"])

                pools.append(Pool(
                    name=name,
                    url=f"https://montreal.ca{slug}",
                    geo_location=f"{lat}:{lon}",
                    pool_type=pool_type,
                ))
        return pools

# ---------------------------------------------------------------------------
# Pool detail parser
# ---------------------------------------------------------------------------

class PoolDetailParser:
    @staticmethod
    def parse_address(soup: BeautifulSoup) -> str:
        carte_tag = soup.select_one("#carte")
        if isinstance(carte_tag, Tag):
            address_parent = carte_tag.next_sibling
            if isinstance(address_parent, Tag):
                address_tag = address_parent.select_one(".list-item-content > div:first-child")
                if isinstance(address_tag, Tag):
                    return address_tag.get_text(" ", strip=True)
        return ""

    @staticmethod
    def parse_phone(soup: BeautifulSoup) -> str:
        phone_icon = soup.select_one(".icon.icon-phone")
        if isinstance(phone_icon, Tag):
            phone_tag = phone_icon.next_sibling
            if isinstance(phone_tag, Tag):
                return phone_tag.get_text(strip=True)
        return ""

    @staticmethod
    def parse_primary_image_url(soup: BeautifulSoup) -> str:
        heading_background = soup.select_one(".document-heading-background")
        if isinstance(heading_background, Tag) and 'style' in heading_background.attrs:
            style_attr = str(heading_background['style'])
            match = re.search(r'url\((.*?)\)', style_attr)
            if match:
                url_with_quotes = match.group(1)
                return url_with_quotes.strip('\'"')
        return ""

    @staticmethod
    def parse_schedules(soup: BeautifulSoup) -> list[Schedule]:
        all_schedules: list[Schedule] = []
        for parser in VARIANT_PARSERS:
            if parser.can_parse(soup):
                found = parser.parse(soup)
                all_schedules.extend(found)
        return all_schedules