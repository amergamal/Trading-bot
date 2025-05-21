import sqlite3
from datetime import datetime

# Database connection
DB_PATH = 'EOD_data.db'

# Connect to the database and create PMH_time table if not exists
def create_pmh_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PMH_time (
            ticker TEXT,
            price REAL,
            time TEXT,
            date TEXT,
            hod REAL,
            hod_time TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Fetch and insert highest prices before and after 9:30 AM for each ticker and date
def fetch_and_insert_high_prices():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Query to fetch the highest price before 9:30 AM for each ticker and date
    pre_market_query = '''
        SELECT ticker, MAX(high) AS pmh, substr(timestamp, 1, 10) AS date, substr(timestamp, 12, 5) AS pmh_time
        FROM ohlc_1min
        WHERE substr(timestamp, 12, 5) < '09:30'  -- Fetch entries before 9:30 AM
        GROUP BY ticker, date
    '''
    
    cursor.execute(pre_market_query)
    pre_market_results = cursor.fetchall()
    
    # Insert the results into PMH_time table with HOD values
    for row in pre_market_results:
        ticker, pmh, date, pmh_time = row
        
        # Fetch the highest price after 9:29 AM for the same ticker and date
        hod_query = '''
            SELECT MAX(high) AS hod, substr(timestamp, 12, 5) AS hod_time
            FROM ohlc_1min
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND substr(timestamp, 12, 5) >= '09:30'
        '''
        cursor.execute(hod_query, (ticker, date))
        hod_result = cursor.fetchone()
        
        # Extract HOD price and time, or set to None if no result
        hod, hod_time = hod_result if hod_result else (None, None)
        
        # Insert into PMH_time table
        cursor.execute('''
            INSERT INTO PMH_time (ticker, price, time, date, hod, hod_time)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (ticker, pmh, pmh_time, date, hod, hod_time))

    conn.commit()
    conn.close()

# Main function to create table and process data
def main():
    create_pmh_table()
    fetch_and_insert_high_prices()
    print("Data processing and insertion complete.")

if __name__ == "__main__":
    main()
