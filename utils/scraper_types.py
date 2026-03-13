from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Iterator, Optional, overload

from bs4 import Tag


# ---------------------------------------------------------------------------
# Pool-type registry
# ---------------------------------------------------------------------------

class _TYPES(dict[str, str]):
    def __init__(self) -> None:
        super().__init__()
        self["PISI"] = "Indoor swimming pool"
        self["PIEX"] = "Outdoor swimming pool"
        self["PATA"] = "Wading pool"
        self["JEUD"] = "Play fountains"

    # Overloads let Pylance know the return is always `str`
    # regardless of whether the key is an int (positional) or str (named).
    @overload
    def __getitem__(self, key: int) -> str: ...
    @overload
    def __getitem__(self, key: str) -> str: ...
    def __getitem__(self, key: int | str) -> str:
        if isinstance(key, int):
            return list(self.keys())[key]
        return super().__getitem__(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())


TYPES = _TYPES()


# ---------------------------------------------------------------------------
# Regex helpers (used by parsers too — exported via __init__.py)
# ---------------------------------------------------------------------------

_TIME_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*(am|pm)",
    re.IGNORECASE,
)

_DATE_RANGE_TEXT_RE = re.compile(
    r"([A-Za-z]+ \d{1,2})\s+to\s+([A-Za-z]+ \d{1,2})",
    re.IGNORECASE,
)

_SINGLE_DATE_RE = re.compile(r"([A-Za-z]+ \d{1,2})")

WEEKDAYS: frozenset[str] = frozenset({
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
})


def _parse_time(raw: str) -> Optional[time]:
    """Parse '3:00 pm' / '11:00 am' -> datetime.time.  Returns None on failure."""
    m = _TIME_RE.search(raw)
    if not m:
        return None
    h, mn, period = int(m.group(1)), int(m.group(2)), m.group(3).lower()
    if period == "pm" and h != 12:
        h += 12
    elif period == "am" and h == 12:
        h = 0
    return time(h, mn)


def _text(tag: Tag) -> str:
    """Return the normalised text content of a BeautifulSoup Tag."""
    return tag.get_text(" ", strip=True)


def _parse_date_range(text: str) -> tuple[str, str]:
    """
    Extract (start_date, end_date) from strings like:
      "August 18 to August 24"
      "From June 21 to August 15, 2025"
    Returns ("", "") when nothing is found.
    """
    text = text.replace("\u00a0", " ")   # normalise non-breaking spaces
    m = _DATE_RANGE_TEXT_RE.search(text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    dates = _SINGLE_DATE_RE.findall(text)
    if len(dates) >= 2:
        return dates[0], dates[1]
    if len(dates) == 1:
        return dates[0], dates[0]
    return "", ""


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

class PoolType:
    def __init__(self, name: str = TYPES["PISI"], description: str = "") -> None:
        self.name: str = name
        self.description: str = description

    def __str__(self) -> str:
        for t in TYPES:
            if self.name == TYPES[t]:
                return t
        return self.name


class Pool:
    # Class-level annotations so Pylance knows the shape of every instance.
    name: str
    pool_type: PoolType
    url: str
    address: str
    primary_image_url: str
    map_link: str
    geo_location: str
    phone: str
    createdAt: float
    is_active: bool
    schedules: list[Schedule]

    def __init__(
        self,
        name: str,
        url: str,
        geo_location: str,
        pool_type: PoolType | None = None,
        address: str = "",
        primary_image_url: str = "",
        phone: str = "",
        is_active: bool = True,
        schedules: list[Schedule] | None = None,  # avoid mutable default
        createdAt: float | None = None,            # avoid evaluated-at-definition default
    ) -> None:
        geo_array = geo_location.split(":")
        if len(geo_array) != 2:
            raise ValueError(
                f"Bad geographic data: expected 'lat:lon', got {geo_location!r}"
            )

        self.name = name
        self.pool_type = pool_type if pool_type is not None else PoolType()
        self.url = url
        self.address = address
        self.primary_image_url = primary_image_url
        self.map_link = (
            f"https://www.openstreetmap.org/?lat={geo_array[0]}"
            f"&lon={geo_array[1]}&zoom=15"
        )
        self.geo_location = geo_location
        self.phone = phone
        self.createdAt = createdAt if createdAt is not None else datetime.now().timestamp()
        self.is_active = is_active
        self.schedules = schedules if schedules is not None else []


@dataclass
class TimeBlock:
    """A single open period on a given weekday."""
    day: str          # e.g. "Monday"
    start: time       # e.g. time(15, 0)
    end: time         # e.g. time(19, 0)
    label: str = ""   # optional audience label, e.g. "lane swimming"

    def __repr__(self) -> str:
        suffix = f", '{self.label}'" if self.label else ""
        return (
            f"TimeBlock({self.day}, "
            f"{self.start.strftime('%H:%M')}-{self.end.strftime('%H:%M')}"
            f"{suffix})"
        )


@dataclass
class Schedule:
    """
    A collection of TimeBlocks valid from effective_date through end_date.

    Dates are plain strings (e.g. "August 18") because the source HTML omits
    the year; callers can resolve them against the current season year if needed.
    """
    time_blocks: list[TimeBlock] = field(default_factory=list)
    effective_date: str = ""   # inclusive start, e.g. "August 18"
    end_date: str = ""         # inclusive end,   e.g. "August 24"
    activity: str = ""         # e.g. "open swim", "lane swimming"

    def __repr__(self) -> str:
        return (
            f"Schedule({self.effective_date!r} -> {self.end_date!r}, "
            f"activity={self.activity!r}, blocks={len(self.time_blocks)})"
        )