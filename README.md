# Montreal Pools Scraper

A scraper for Montreal's public pool listings from `montreal.ca`, with a MySQL-backed HTML cache and structured pool data models.

## Notes

### Types & Models
The scraper is built around three core types. `TYPES` is a singleton dict mapping short codes (`PISI`, `PIEX`, `PATA`, `JEUD`) to human-readable category names. `PoolType` wraps one of these categories and serializes back to its code for use in URL params. `Pool` is the main data model, holding everything from name and geo coordinates to schedules — most fields start empty and are meant to be filled in a second pass. `Schedule` is a simple day + time-range value object attached to a pool.

### Scraping Flow
The scraper works in two phases. The first phase (implemented) iterates over all pool types, determines the page count by parsing the results header on the listing page, then walks each page extracting pool stubs from the embedded GeoJSON map data (`data-map-map`). Each stub gives a name, URL slug, and coordinates. The second phase (`populate_pools`, not yet implemented) is meant to visit each pool's individual page to fill in the remaining fields: address, phone, primary image, and schedules.

All HTTP fetching goes through a cache layer backed by MySQL. Raw HTML is stored as chunked `LONGBLOB`s (1 MB/chunk) with a 1-week TTL — fresh content is served from cache; stale or missing entries trigger a real HTTP request and a cache write. This just seems logical especially in development since we'll probably be going through the html many times to understand it.
