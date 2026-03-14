# Montreal Pools Scraper

A scraper for Montreal's public pool listings from `montreal.ca`, with a MySQL-backed HTML cache and structured pool data models.

## Prerequisites

- Python 3.8 or higher
- MySQL server running on port 3306 locally
- A MySQL database named 'test_pools_app' (for development) or 'pools_app' (for production)
- Copy `.env.example` to `.env` and fill in your database credentials
- Install Python dependencies: `pip install -r requirements.txt`

## CLI Usage

The scraper now supports command-line arguments for logging and selective info extraction:

```bash
python pool-scraper.py [options]
```

### Common examples

```bash
# Scrape all types with default behavior (all details)
python pool-scraper.py

# Only indoor and outdoor pools, max 2 pages each, debug logs to console
python pool-scraper.py --types PISI PIEX --max-pages 2 --log-level DEBUG

# Listing-only scrape (no detail pages)
python pool-scraper.py --skip-details

# Extract only schedules and phone, then export JSON
python pool-scraper.py --extract schedules phone --output-json data/pools.json --pretty-json

# Quiet console output
python pool-scraper.py --quiet
```

### Flags

- `--types {PISI,PIEX,PATA,JEUD} [...]`:
	choose one or more pool categories (default: all)
- `--max-pages N`:
	limit listing pages fetched per selected type
- `--workers N`:
	concurrent workers for detail pages (default: `10`)
- `--skip-details`:
	only collect listing-level fields (`name`, `url`, `geo_location`, etc.)
- `--extract {all,address,phone,image,schedules} [...]`:
	choose which detail fields to extract (default: `all`)
- `--output-json PATH`:
	write scraped pools to a JSON file
- `--pretty-json`:
	pretty-print JSON output when used with `--output-json`
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`:
	set console verbosity (default: `INFO`)
- `--quiet`:
	suppress most console logs

## Notes

### Types & Models
The scraper is built around three core types. `TYPES` is a singleton dict mapping short codes (`PISI`, `PIEX`, `PATA`, `JEUD`) to human-readable category names. `PoolType` wraps one of these categories and serializes back to its code for use in URL params. `Pool` is the main data model, holding everything from name and geo coordinates to schedules — most fields start empty and are meant to be filled in a second pass. `Schedule` is a simple day + time-range value object attached to a pool.

### Scraping Flow
The scraper works in two phases. The first phase (implemented) iterates over all pool types, determines the page count by parsing the results header on the listing page, then walks each page extracting pool stubs from the embedded GeoJSON map data (`data-map-map`). Each stub gives a name, URL slug, and coordinates. The second phase (`populate_pools`, not yet implemented) is meant to visit each pool's individual page to fill in the remaining fields: address, phone, primary image, and schedules.

All HTTP fetching goes through a cache layer backed by MySQL. Raw HTML is stored as chunked `LONGBLOB`s (1 MB/chunk) with a 1-week TTL — fresh content is served from cache; stale or missing entries trigger a real HTTP request and a cache write. This just seems logical especially in development since we'll probably be going through the html many times to understand it.
