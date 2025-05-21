import sqlite3
from datetime import datetime

DB_PATH = 'EOD_data.db'

def update_lu_price(ticker, lu_price, last_price):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE TradeParameters
            SET LU = ?, LAST = ?
            WHERE ticker = ? AND date = ?
        """, (lu_price, last_price, ticker, datetime.now().strftime('%Y-%m-%d')))
        conn.commit()
        print(f"Updated LU price to {lu_price} and LAST price to {last_price} for {ticker}.")
    except sqlite3.Error as e:
        print(f"Error updating LU price: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Example update
    update_lu_price('TSLA', 226.05, 222.20)
