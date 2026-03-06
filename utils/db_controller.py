import os
from datetime import datetime, timezone
from typing import cast

from mysql.connector import MySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.types import RowType, RowItemType

from requests import Request, get
from .logger import get_logger

logger = get_logger('db_controller')

class PoolMyFingerDB:
    db : MySQLConnectionAbstract
    cursor : MySQLCursorAbstract

    def __init__(self):
        DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
        DB_PORT = os.environ.get("DB_PORT", "3306")
        ENV     = os.environ.get("ENV", "dev")
        DB_NAME = os.environ.get("DB_NAME", "test_pools_app" if ENV == "dev" else "pools_app")
        DB_USER = os.environ.get("DB_USER", "root")
        DB_PASS = os.environ.get("DB_PASS", "")

        db = MySQLConnection(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        self.db = db
        self.cursor = db.cursor()
        logger.info(f"Connected to database '{DB_NAME}' at {DB_HOST}:{DB_PORT}")

        # In testing environment, recreate tables every time
        # if ENV == "dev":
        #     self.cursor.execute("DROP TABLE IF EXISTS site_cache_blobs")
        #     self.cursor.execute("DROP TABLE IF EXISTS site_cache")

        # Create cache tables
        self.cursor.execute(
        "CREATE TABLE IF NOT EXISTS site_cache (" \
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT," \
            "url VARCHAR(767) NOT NULL UNIQUE," \
            "content_length BIGINT UNSIGNED," \
            "last_scrape DATETIME," \
            "PRIMARY KEY (id)," \
            "KEY idx_site_cache_url (url)" \
        ")")

        self.cursor.execute(
        "CREATE TABLE IF NOT EXISTS site_cache_blobs (" \
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT," \
            "cache_id BIGINT UNSIGNED NOT NULL," \
            "chunk_index INT UNSIGNED NOT NULL," \
            "data LONGBLOB," \
            "PRIMARY KEY (id)," \
            "KEY idx_site_cache_blobs_cache_id_chunk (cache_id, chunk_index)," \
            "CONSTRAINT fk_site_cache_blobs_cache_id FOREIGN KEY (cache_id) REFERENCES site_cache(id) ON DELETE CASCADE" \
        ")")

    def check_cache(self, url : str) -> tuple[str, bytes, datetime] | None:
        # Check if URL exists in site_cache
        sql = "SELECT id, url, content_length, last_scrape FROM site_cache WHERE url = %s"
        val = (url,)

        self.cursor.execute(sql, val)
        meta_row = cast(tuple[int, str, int, datetime], self.cursor.fetchone())

        if not meta_row:
            logger.debug(f"Cache miss: {url}")
            return None

        cache_id, url, content_length, last_scrape = meta_row

        # Retrieve blobs
        sql_blobs = "SELECT data FROM site_cache_blobs WHERE cache_id = %s ORDER BY chunk_index"
        self.cursor.execute(sql_blobs, (cache_id,))
        blobs = cast(list[tuple[bytes]], self.cursor.fetchall())

        # Concatenate blob data
        content = b''.join(blob[0] for blob in blobs)

        # Verify content length
        if len(content) != content_length:
            logger.warning(f"Cache content length mismatch for {url}: expected {content_length}, got {len(content)}")

        logger.debug(f"Cache hit: {url} (scraped {last_scrape})")
        return (url, content, last_scrape)

    def store_site(self, url : str, content : bytes):
        chunk_size = 1024 * 1024  # 1MB chunks
        content_length = len(content)

        # Check if URL already exists
        self.cursor.execute("SELECT id FROM site_cache WHERE url = %s", (url,))
        existing = self.cursor.fetchone()

        if existing:
            cache_id = cast(tuple[int], existing)[0]
            # Delete old blobs
            self.cursor.execute("DELETE FROM site_cache_blobs WHERE cache_id = %s", (cache_id,))
            # Update metadata
            self.cursor.execute(
                "UPDATE site_cache SET content_length = %s, last_scrape = %s WHERE id = %s",
                (content_length, datetime.now(timezone.utc), cache_id)
            )
            logger.debug(f"Updated cache for {url} ({content_length} bytes)")
        else:
            # Insert new metadata
            self.cursor.execute(
                "INSERT INTO site_cache (url, content_length, last_scrape) VALUES (%s, %s, %s)",
                (url, content_length, datetime.now(timezone.utc))
            )
            cache_id = self.cursor.lastrowid
            logger.debug(f"Inserted new cache entry for {url} ({content_length} bytes)")

        # Insert blobs
        chunks = [content[i:i + chunk_size] for i in range(0, content_length, chunk_size)]
        for idx, chunk in enumerate(chunks):
            sql_blob = "INSERT INTO site_cache_blobs (cache_id, chunk_index, data) VALUES (%s, %s, %s)"
            self.cursor.execute(sql_blob, (cache_id, idx, chunk))

        self.db.commit()
        logger.info(f"Stored {len(chunks)} blob chunk(s) for {url}")

