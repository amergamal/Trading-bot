# populate_sc_gappers.py
# Run this from the same folder as app.py

import yfinance as yf
import requests
import time
import psycopg2
from psycopg2 import sql

# Import your existing config (same as app.py)
import config

print("Starting small-cap gappers scanner...")

# ────────────────────────────────────────────────
# 1. Fetch all listed tickers from NASDAQ API
# ────────────────────────────────────────────────
def get_tickers(exchange):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=0&download=true&exchange={exchange}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get('data', {}).get('rows', [])
        return [row['symbol'].strip().upper() for row in rows if row.get('symbol')]
    except Exception as e:
        print(f"Failed to fetch {exchange} tickers: {e}")
        return []

print("Fetching tickers from NASDAQ, NYSE, AMEX...")
nasdaq = get_tickers('nasdaq')
nyse   = get_tickers('nyse')
amex   = get_tickers('amex')

all_tickers = list(set(nasdaq + nyse + amex))  # remove duplicates
all_tickers.sort()
print(f"Total unique tickers collected: {len(all_tickers):,}")

# ────────────────────────────────────────────────
# 2. Filter for market cap <= 100 million + collect actual MC
# ────────────────────────────────────────────────
small_caps_data = []  # list of (ticker, mc) tuples

for i, ticker in enumerate(all_tickers, 1):
    if i % 200 == 0:
        print(f"  Processed {i:,} / {len(all_tickers):,} tickers...")

    try:
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        mc = info.get('marketCap')
        if mc is not None and isinstance(mc, (int, float)) and mc <= 100_000_000:
            mc_int = int(round(mc))  # store as integer dollars
            small_caps_data.append((ticker, mc_int))
            print(f"   → {ticker:<8}  market cap ≈ ${mc_int:,}")
    except Exception as e:
        pass  # silent on errors (delisted, no data, etc.)

    time.sleep(0.15)  # slightly increased to stay safer

print(f"\nFound {len(small_caps_data):,} tickers with market cap ≤ $100M")

# ────────────────────────────────────────────────
# 3. Insert/update PostgreSQL using the same config
# ────────────────────────────────────────────────
try:
    conn = psycopg2.connect(**config.DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    # Create table if missing (with mc column)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sc_gappers (
            id     SERIAL PRIMARY KEY,
            ticker TEXT UNIQUE NOT NULL,
            mc     BIGINT,                  -- market cap in dollars (integer)
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # If table already exists without 'mc' or 'updated_at', add them safely
    try:
        cur.execute("ALTER TABLE sc_gappers ADD COLUMN IF NOT EXISTS mc BIGINT;")
        cur.execute("ALTER TABLE sc_gappers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
    except psycopg2.errors.DuplicateColumn:
        pass  # already exists, ignore

    # Insert or update (refresh mc and updated_at if exists)
    inserted = 0
    updated   = 0
    for ticker, mc in small_caps_data:
        try:
            cur.execute("""
                INSERT INTO sc_gappers (ticker, mc, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker) 
                DO UPDATE SET 
                    mc = EXCLUDED.mc,
                    updated_at = CURRENT_TIMESTAMP
            """, (ticker, mc))
            if cur.rowcount == 1:
                inserted += 1
            elif cur.rowcount == 2:  # INSERT failed → UPDATE happened
                updated += 1
        except Exception as e:
            print(f"DB error for {ticker}: {e}")

    conn.commit()
    print(f"\nSuccessfully processed {len(small_caps_data):,} tickers:")
    print(f"  - New inserts: {inserted:,}")
    print(f"  - Updates (refreshed MC): {updated:,}")

except psycopg2.Error as e:
    print(f"Database error: {e}")
    if conn:
        conn.rollback()
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()

print("Done.")