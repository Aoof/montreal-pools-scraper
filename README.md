# Montreal Pools Scraper

## Notes

- 
- connection details: MySQL root@localhost:3306 pw="" db_name=pools_app

- pool_types (
        id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
        name VARCHAR(50) NOT NULL,
        description VARCHAR(255) DEFAULT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_pool_types_name (name)
 )
    
- pool_types we need to confirm that they exist then use them in the following parts
  1. Indoor Swimming Pool
  2. Outdoor Swimming Pool
  3. Play fountains
  4. Wading Pool

- https://montreal.ca/en/places?mtl_content.lieux.installation.code=PISI,PIEX,JEUD,PATA
- PISI is Indoor Swimming Pools
- PIEX is Outdoor Swimming Pool
- PATA is Wading Pool
- JEUD is Play fountains

- We must loop through each individually to maintain the tag info because there is no tell on the website

- Pagination

- If the results are more than 100 it'll have multiple pages (page per 100)
- 1 to 39 of 39 results (div#searchResultList div#spinLoader div.row.align-items-center div.col-auto h2.h5.mb-0)
- e.g. if results are 380 then it'll have 4 pages

## Scraping Plan

Based on schema.php, the following tables need data insertion via scraping:

### pool_types
- Predefined types to insert if not exist:
  1. Indoor Swimming Pool
  2. Outdoor Swimming Pool
  3. Play fountains
  4. Wading Pool
- Fields: name, description (optional)

### pools
- Scrape from URLs for each type code (PISI, PIEX, JEUD, PATA)
- Fields to scrape/generate:
  - name: 
  - pool_type_id: map from URL code
  - is_active: default 1
  - created_at: auto
- Ignore nullables: full_address, primary_image_url, website, map_link, phone (manual input later)

### schedule_types
- Define types like "Public Swimming", "Lessons", etc.
- Fields: name, description

### schedules
- We can find schedule details on aria-label='Period selector'
- For each pool, scrape schedule details
- Fields to scrape/generate:
  - pool_id: link to inserted pool
  - day_of_week: 
  - start_time: 
  - end_time: 
  - schedule_type_id: 
  - notes: (optional)
  - created_at: auto

### Steps Outline
1. Connect to database
2. Insert pool_types if missing
3. Define/insert schedule_types
4. For each pool type code:
   - Fetch list of pools (handle pagination)
   - For each pool:
     - Extract name
     - Insert into pools with pool_type_id
     - Scrape schedule details
     - Insert schedules
5. 
