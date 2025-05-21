import sqlite3
import threading
import logging
import socket
import random
from datetime import datetime
import time

db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

def generate_token():
    return str(random.randint(100000, 999999))

class EndOfDay:
    def __init__(self):
        self.logger = logging.getLogger('EndOfDay')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.db_path = db_path
        self.trade_details = {}  # Dictionary to store trade details by token
        self.lock = threading.Lock()  # Lock for thread safety
        self.client_socket = None
        self.logger.info("EOD instance created and initialized.")

        # Start listening to the server for updates
        self.listen_to_server()

    def listen_to_server(self):
        """Start a thread to listen for server responses."""
        threading.Thread(target=self._listen_to_server, daemon=True).start()

    def _listen_to_server(self):
        """Continuously listen for server responses."""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.logger.info("Attempting to connect to server...")
            self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")

            while True:
                response = self.client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    self.handle_response(response)
                else:
                    self.logger.debug("No response received, waiting...")
                    time.sleep(0.5)
        except socket.error as e:
            self.logger.error(f"Socket error: {e}")
        except Exception as e:
            self.logger.error(f"Error listening to server: {e}")
        finally:
            self.client_socket.close()
            self.logger.info("Socket connection closed.")

    def receive_order_details(self, trade_details):
        """Receive trade details, store in the trade_details dictionary, and insert into the database."""
        trade_id = trade_details['tradeID']
        token = generate_token()
        with self.lock:
            self.trade_details[token] = trade_details
            trade_details['token'] = token
            self.insert_trade_details(trade_details)
            self.logger.info(f"Received and stored trade details for {trade_id}: {trade_details}")

    def insert_trade_details(self, trade_details):
        """Insert trade details into the TradeDetails table."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO TradeDetails (tradeID, time, strategy, ticker, shares, entry_price, stop_loss, sellOrderID, stopOrderID)
                VALUES (:tradeID, :time, :strategy, :ticker, :shares, :entry_price, :stop_loss, :sellOrderID, :stopOrderID)
            ''', trade_details)
            conn.commit()
            self.logger.info(f"Inserted trade details into TradeDetails table: {trade_details}")

    def send_command(self, command):
        """Send commands to the server."""
        with self.lock:
            if self.client_socket:
                self.client_socket.sendall(command.encode())
                self.logger.debug(f'Sent command: {command}')
            else:
                self.logger.error("Client socket not initialized.")

    def end_of_day_tasks(self):
        """Schedule to perform end of day tasks at the designated time."""
        while True:
            current_time = datetime.now()
            self.logger.debug(f"Checking time: {current_time}")
            if current_time.hour == 19 and current_time.minute == 56:  # Adjust time as needed
                self.logger.info("Designated time reached. Closing positions.")
                self.close_positions()
                break  # Exit after tasks are completed
            time.sleep(30)

    def close_positions(self):
        """Close all open positions at the end of the day."""
        self.logger.debug("Starting to close positions.")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, strategy, ticker, shares, entry_price, stop_loss, time 
                FROM ActiveTrades 
            """)
            open_trades = cursor.fetchall()
            self.logger.debug(f"Open trades fetched: {open_trades}")

            for trade in open_trades:
                tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time = trade
                self.logger.debug(f"Evaluating trade: {tradeID}, {strategy}, {ticker}")

                # Match with received trade details in the database
                cursor.execute("""
                    SELECT * FROM TradeDetails 
                    WHERE tradeID = ? AND strategy = ? AND ticker = ? AND shares = ?
                """, (tradeID, strategy, ticker, shares))
                trade_detail = cursor.fetchone()

                if trade_detail:
                    self.logger.info(f"Closing position for tradeID {tradeID}: {trade}")
                    token = self.trade_details.get(tradeID, {}).get('token')
                    if token:
                        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares} MKT TIF=DAY'
                        self.send_command(buy_command)
                else:
                    self.logger.debug(f"No matching trade details for tradeID {tradeID}.")

    def handle_response(self, response):
        """Process responses from the server and match tokens to trade details."""
        lines = response.strip().split('\n')
        for line in lines:
            parts = line.split()
            if len(parts) > 1:
                if parts[0] in ("%ORDER", "%ORDERACT"):
                    token = parts[2] if parts[0] == "%ORDER" else parts[-1]
                    if token in self.trade_details:
                        status = parts[11] if parts[0] == "%ORDER" else parts[2]
                        if status == 'Executed':
                            trade_details = self.trade_details.pop(token)
                            order_id = parts[1]  # Order ID from response
                            action = parts[4] if parts[0] == "%ORDER" else parts[3]
                            act_status = parts[2] if parts[0] == "%ORDERACT" else 'Executed'
                            notes = 'Order executed successfully'
                            self.process_executed_order(trade_details, parts, token, order_id, action, act_status, notes)

    def process_executed_order(self, trade_details, parts, token, order_id, action, act_status, notes):
        """Process an executed order."""
        trade_id = trade_details['tradeID']
        ticker = trade_details['ticker']
        shares = trade_details['shares']
        price = float(parts[9] if parts[0] == "%ORDER" else parts[6])
        time_executed = parts[12] if parts[0] == "%ORDER" else parts[8]

        self.logger.info(f"Order executed for trade_id {trade_id}. Moving trade to closed.")
        self.insert_buy_market(trade_id, time_executed, ticker, shares, price, token, order_id, action, 'Executed', act_status, notes)
        self.move_trade_to_closed(trade_id, ticker, price, time_executed)

    def insert_buy_market(self, trade_id, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes):
        """Insert the buy market order details into the BuyMarket table."""
        self.logger.debug(f"Inserting buy market order for trade_id: {trade_id}, ticker: {ticker}")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO BuyMarket (tradeID, time, ticker, shares, price, token, orderID, action, status, act_status, notes, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (trade_id, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes, datetime.now().strftime('%Y-%m-%d')))
            conn.commit()

    def move_trade_to_closed(self, trade_id, ticker, exit_price, exit_time):
        """Move the trade to the ClosedTrades table."""
        self.logger.debug(f"Moving trade to closed for trade_id: {trade_id}, ticker: {ticker}")
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT strategy, shares, entry_price, stop_loss, time 
                FROM ActiveTrades 
                WHERE tradeID = ?
            """, (trade_id,))
            trade = cursor.fetchone()

            if trade:
                strategy, shares, entry_price, stop_loss, entry_time = trade
                realized = (entry_price - exit_price) * shares

                cursor.execute("""
                    INSERT INTO ClosedTrades (tradeID, strategy, ticker, shares, entry_price, entry_time, 
                                              stop_loss, exit_price, exit_time, date, reason, realized)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (trade_id, strategy, ticker, shares, entry_price, entry_time,
                      stop_loss, exit_price, exit_time, datetime.now().strftime('%Y-%m-%d'), 'TakeProfit', realized))

                cursor.execute("DELETE FROM ActiveTrades WHERE tradeID = ?", (trade_id,))
                
                cursor.execute("""
                    UPDATE TradeStatus 
                    SET active_trade = 'closed' 
                    WHERE ticker = ? AND strategy = ? AND date = ?
                """, (ticker, strategy, datetime.now().strftime('%Y-%m-%d')))

                conn.commit()
                self.logger.info(f"Moved trade {trade_id} to ClosedTrades and updated TradeStatus")
        except sqlite3.Error as e:
            self.logger.error(f"Error moving trade to closed: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Create an EndOfDay instance
    eod = EndOfDay()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
