import sqlite3
import datetime
import logging
import threading
import time
import os
from api_connection import APIConnection  # Assuming the APIConnection class is in api_connection.py

class TickerMonitor:
    def __init__(self, db_path, api_client):
        self.db_path = db_path
        self.api_client = api_client
        self.logger = logging.getLogger('TickerMonitor')
        logging.basicConfig(level=logging.ERROR)  # Set to ERROR level for logging only errors
        self.connect_to_db()
        
        # Wait until the API connection is fully established before proceeding
        self.api_client.connection_ready_event.wait()
        self.update_account_equity()

    def connect_to_db(self):
        # Use an absolute path for the database
        abs_db_path = os.path.abspath(self.db_path)
        self.logger.debug(f"Connecting to database at: {abs_db_path}")
        self.conn = sqlite3.connect(abs_db_path)
        self.cursor = self.conn.cursor()
        self.logger.debug("Connected to database.")

    

    def get_todays_tickers(self):
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        query = f"SELECT TICKER FROM TradeParameters WHERE Date = '{today}'"
        self.cursor.execute(query)
        tickers = [row[0] for row in self.cursor.fetchall()]
        self.logger.debug(f"Retrieved tickers for today: {tickers}")
        return tickers

    def subscribe_to_level1(self, ticker):
        try:
            command = f"SB {ticker} Lv1"
            self.api_client.send_command(command)
            self.logger.debug(f"Subscribed to level 1 data for {ticker}")
        except Exception as e:
            self.logger.error(f"Error subscribing to level 1 data for {ticker}: {e}")

    def handle_quote_data(self, quote_data):
        self.logger.debug(f"Handling quote data: {quote_data}")
        fields = quote_data.split()
        ticker = fields[1] if fields[0] == '$Quote' else fields[0]
        last_price = None

        for field in fields[1:]:
            if field.startswith('L:'):
                last_price = float(field[2:])

        if last_price is not None:
            self.update_last_price(ticker, last_price)
        else:
            self.logger.error(f"Failed to update LAST price for {ticker} from quote data: {quote_data}")

    def handle_ldlu_data(self, ldlu_data):
        self.logger.debug(f"Handling LDLU data: {ldlu_data}")
        fields = ldlu_data.split()
        ticker = fields[1]
        limit_down = float(fields[2])
        limit_up = float(fields[3])
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        self.update_ldlu(ticker, limit_down, limit_up, timestamp)

    def update_last_price(self, ticker, last_price):
        query = "UPDATE TradeParameters SET LAST = ? WHERE TICKER = ?"
        try:
            self.cursor.execute(query, (last_price, ticker))
            self.conn.commit()
            self.logger.debug(f"Updated LAST price for {ticker} to {last_price}")
        except Exception as e:
            self.logger.error(f"Error updating LAST price for {ticker} to {last_price}: {e}")

    def update_ldlu(self, ticker, limit_down, limit_up, timestamp):
        query = "UPDATE TradeParameters SET LU = ?, LU_TIME = ? WHERE TICKER = ?"
        try:
            self.cursor.execute(query, (limit_up, timestamp, ticker))
            self.conn.commit()
            self.logger.debug(f"Updated LU value for {ticker} to {limit_up} at {timestamp}")
        except Exception as e:
            self.logger.error(f"Error updating LU value for {ticker} to {limit_up} at {timestamp}: {e}")

    def update_account_equity(self):
        try:
            command = "GET AccountInfo"
            self.api_client.send_command(command)
            response = self.api_client.receive_response()
            if response.startswith("$AccountInfo"):
                fields = response.split()
                account_equity = float(fields[2])
                query = "UPDATE TradeParameters SET ACCOUNT_EQUITY = ?"
                self.cursor.execute(query, (account_equity,))
                self.conn.commit()
                self.logger.debug(f"Updated account equity to {account_equity}")
        except Exception as e:
            self.logger.error(f"Error fetching account equity: {e}")

    def run(self):
        tickers = self.get_todays_tickers()

        for ticker in tickers:
            self.subscribe_to_level1(ticker)

        threading.Thread(target=self.api_client.continuously_receive, daemon=True).start()

        while True:
            if self.api_client.response_buffer:
                response = self.api_client.response_buffer.pop(0)
                if response.startswith("$Quote"):
                    self.handle_quote_data(response)
                elif response.startswith("$LDLU"):
                    self.handle_ldlu_data(response)
            time.sleep(0.1)

if __name__ == "__main__":
    api_client = APIConnection()
    try:
        api_client.login()
        if api_client.connected:
            threading.Thread(target=api_client.keep_alive, daemon=True).start()
            monitor = TickerMonitor('tms_data.db', api_client)
            monitor.run()
    except KeyboardInterrupt:
        api_client.quit()
