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
        raw_section = soup.find("section", id="HRS0")
        if raw_section is None or isinstance(raw_section, Tag):
            return []
        section = cast(Tag, raw_section)

        schedules: list[Schedule] = []

        # Activity name from the section heading
        raw_h2 = section.find("h2")
        activity_name = _text(raw_h2) if isinstance(raw_h2, Tag) else "swimming"

        for wrapper in section.select(".wrapper.wrapper-complex"):
            # --- date range ---
            time_tags = wrapper.select(".wrapper-header time[datetime]")
            if len(time_tags) >= 2:
                eff = _text(time_tags[0]).replace("\u00a0", " ").strip()
                end = _text(time_tags[1]).replace("\u00a0", " ").strip()
            elif len(time_tags) == 1:
                eff = end = _text(time_tags[0]).replace("\u00a0", " ").strip()
            else:
                eff = end = ""

            # --- one sub-schedule per audience group (h3 + table) ---
            raw_body = wrapper.find(class_="wrapper-body")
            if not isinstance(raw_body, Tag):
                continue
            body = raw_body

            for stacked in body.find_all("div", class_="content-module-stacked", recursive=False):
                if not isinstance(stacked, Tag):
                    continue

                raw_h3 = stacked.find("h3")
                audience_label = _text(raw_h3) if isinstance(raw_h3, Tag) else activity_name

                for table in stacked.find_all("table"):
                    if not isinstance(table, Tag):
                        continue
                    blocks = self._parse_table(table)
                    if blocks:
                        schedules.append(Schedule(
                            time_blocks=blocks,
                            effective_date=eff,
                            end_date=end,
                            activity=audience_label,
                        ))

        return schedules

    @staticmethod
    def _parse_table(table: Tag) -> list[TimeBlock]:
        blocks: list[TimeBlock] = []
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue

            cell0 = cells[0]
            cell1 = cells[1]
            if not isinstance(cell0, Tag) or not isinstance(cell1, Tag):
                continue

            day = _text(cell0)
            if day.lower() not in WEEKDAYS:
                continue

            # A cell may contain multiple time-slot divs (e.g. two sessions on same day)
            slot_divs = cell1.find_all("div", recursive=False)
            raw_slots: list[str] = (
                [_text(d) for d in slot_divs if isinstance(d, Tag)]
                if slot_divs
                else [_text(cell1)]
            )

            for raw in raw_slots:
                if "closed" in raw.lower():
                    continue
                times = list(_TIME_RE.finditer(raw))
                if len(times) >= 2:
                    start = _parse_time(times[0].group(0))
                    end_t = _parse_time(times[1].group(0))
                    if start is not None and end_t is not None:
                        blocks.append(TimeBlock(day=day, start=start, end=end_t))

        return blocks


# ---------------------------------------------------------------------------
# Registry -- add new variant parser instances here
# ---------------------------------------------------------------------------

VARIANT_PARSERS: list[ScheduleVariantParser] = [
    Variant1Parser(),
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