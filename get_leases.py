#!/usr/bin/env python3
"""
Fetch all current Sentinel leases from the API (with pagination)
and insert new/changed records into MySQL.
"""

import sys
import time
import base64
import struct
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

import scrtsxx


# ── Configuration ────────────────────────────────────────────────
API_BASE = "https://api.sentinel.mathnodes.com/sentinel/lease/v1/leases"
PAGE_LIMIT = 100  # max items per page

DB_CONFIG = {
    "host": scrtsxx.HOST,
    "port": scrtsxx.PORT,
    "user": scrtsxx.USERNAME,          # ← change to your MySQL user
    "password": scrtsxx.PASSWORD,  # ← change to your MySQL password
    "database": scrtsxx.DB,      # ← change to your database name
}

REQUEST_TIMEOUT = 30  # seconds per HTTP request
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries
# ─────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT IGNORE INTO leases (
    lease_id, prov_address, node_address, price_denom,
    price_base_value, price_quote_value, hours, max_hours,
    renewal_price_policy, start_at
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
"""

def parse_start_at(ts: str) -> str:
    """
    Parse an RFC-3339 timestamp with nanoseconds into a MySQL
    DATETIME(6) compatible string (microsecond precision).
    Example: '2026-04-11T04:20:28.652743928Z' → '2026-04-11 04:20:28.652743'
    """
    # Strip trailing 'Z' and split on '.'
    ts = ts.rstrip("Z")
    if "." in ts:
        dt_part, frac = ts.split(".", 1)
        # Truncate to 6 digits (microseconds)
        frac = frac[:6].ljust(6, "0")
        ts_clean = f"{dt_part}.{frac}"
    else:
        ts_clean = ts + ".000000"

    dt = datetime.strptime(ts_clean, "%Y-%m-%dT%H:%M:%S.%f")
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

def fetch_page(next_key: str = None) -> dict:
    """Fetch one page of leases from the API with retry logic."""
    params = {"pagination.limit": PAGE_LIMIT}
    if next_key:
        params["pagination.key"] = next_key

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                API_BASE, params=params, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            print(
                f"  [attempt {attempt}/{MAX_RETRIES}] "
                f"Error fetching page: {e}",
                file=sys.stderr,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise

def fetch_all_leases() -> list:
    """Paginate through the API and return all lease dicts."""
    all_leases = []
    next_key = None
    page_num = 0

    while True:
        page_num += 1
        data = fetch_page(next_key)
        leases = data.get("leases", [])
        all_leases.extend(leases)

        pagination = data.get("pagination", {})
        next_key = pagination.get("next_key")
        total = pagination.get("total", "?")

        print(
            f"  Page {page_num}: got {len(leases)} leases "
            f"(running total: {len(all_leases)}, "
            f"chain total: {total})"
        )

        if not next_key:
            break

    return all_leases

def lease_to_row(lease: dict) -> tuple:
    """Convert a lease JSON object into a tuple for INSERT."""
    price = lease["price"]
    return (
        int(lease["id"]),
        lease["prov_address"],
        lease["node_address"],
        price["denom"],
        price["base_value"],            # stored as DECIMAL string
        int(price["quote_value"]),
        int(lease["hours"]),
        int(lease["max_hours"]),
        lease["renewal_price_policy"],
        parse_start_at(lease["start_at"]),
    )

def main():
    start = time.time()
    print(f"[{datetime.utcnow().isoformat()}] Starting lease fetch...")

    # 1. Fetch all leases from API
    leases = fetch_all_leases()
    print(f"  Fetched {len(leases)} leases from API.")

    if not leases:
        print("  Nothing to insert.")
        return

    # 2. Convert to rows
    rows = [lease_to_row(l) for l in leases]

    # 3. Insert into MySQL
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        inserted = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(INSERT_SQL, batch)
            inserted += cursor.rowcount

        conn.commit()
        print(
            f"  Inserted {inserted} new rows "
            f"({len(rows) - inserted} duplicates skipped)."
        )

    except Error as e:
        print(f"  MySQL error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

    elapsed = time.time() - start
    print(f"  Done in {elapsed:.1f}s.")

if __name__ == "__main__":
    main()