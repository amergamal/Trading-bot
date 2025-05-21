import socket
import sqlite3
import logging
import random
from datetime import datetime
import time
import threading

# Configuration variables
db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

def generate_token():
    """Generate a unique token for each order."""
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
        self.trade_details = {}
        self.lock = threading.Lock()  # Lock for thread safety
        self.client_socket = None
        self.keep_listening = True  # Flag to control listening
        self.logger.info("EOD instance created and initialized.")

    def connect_to_server(self):
        """Establish a connection to the server."""
        attempts = 0
        while attempts < 5:  # Try connecting up to 5 times
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                attempts += 1
                self.logger.info(f"Retrying connection in 5 seconds... (Attempt {attempts})")
                time.sleep(5)
        self.logger.error("Failed to connect to server after multiple attempts.")

    def disconnect_from_server(self):
        """Close the connection to the server."""
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
            self.logger.info("Socket connection closed.")

    def receive_order_details(self, trade_details):
        """Receive trade details, store in the trade_details dictionary, and insert into the database."""
        trade_id = trade_details['tradeID']
        with self.lock:
            self.trade_details[trade_id] = trade_details
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

    def end_of_day_tasks(self):
        """Schedule to perform end of day tasks at the designated time."""
        while True:
            current_time = datetime.now()
            self.logger.debug(f"Checking time: {current_time}")
            if current_time.hour == 15 and current_time.minute == 55:  # Check at 5:00 PM
                self.logger.info("Designated time reached. Closing positions.")
                self.close_positions()
                break  # Exit after tasks are completed
            time.sleep(30)  # Sleep for 30 seconds before checking the time again

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
                    self.send_buy_market_order(tradeID, ticker, shares)
                else:
                    self.logger.debug(f"No matching trade details for tradeID {tradeID}.")

    def send_buy_market_order(self, trade_id, ticker, shares):
        """Send a command to buy back the shares."""
        token = generate_token()
        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares} MKT TIF=DAY'

        # Ensure the socket is connected before sending
        if not self.client_socket:
            self.connect_to_server()

        if self.client_socket:
            try:
                # Send command
                self.logger.debug(f"Sending buy market order for trade_id {trade_id} with command: {buy_command}")
                self.send_command(buy_command)

                # Store the order details with the token
                self.trade_details[token] = {'trade_id': trade_id, 'ticker': ticker, 'shares': shares, 'token': token}

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.disconnect_from_server()

    def listen_to_server(self):
        """Continuously listen for server responses."""
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.handle_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
                except socket.error as e:
                    self.logger.error(f"Socket error while listening: {e}")
                    self.keep_listening = False
                except Exception as e:
                    self.logger.error(f"Error listening to server: {e}")
                    self.keep_listening = False
            else:
                self.logger.debug("Client socket is not connected.")
                time.sleep(1)  # Wait before retrying

    def handle_response(self, response):
        """Process responses from the server and match tokens to trade details."""
        lines = response.strip().split('\n')
        for line in lines:
            parts = line.split()
            if len(parts) > 1:
                if parts[0].upper() == "%ORDER":
                    token = parts[2]
                elif parts[0].upper() == "%ORDERACT":
                    token = parts[-1]
                else:
                    continue

                if token in self.trade_details:
                    trade_details = self.trade_details[token]
                    self.logger.debug(f"Processing buy response for token: {token}")
                    self.process_buy_response(line, trade_details['trade_id'], trade_details['ticker'], trade_details['shares'], trade_details['token'])

    def process_buy_response(self, line, trade_id, ticker, shares, token):
        """Process the response from the buy market order command."""
        self.logger.debug(f"Processing buy response for trade_id: {trade_id}, ticker: {ticker}")
        act_status = ""
        notes = ""
        executed = False  # Flag to track if the order was executed
        time_executed = None
        price = None

        order_id = None
        status = None

        parts = line.split()
        if parts[0].upper() == "%ORDER":
            order_id = parts[1]
            status = parts[11]
            shares = parts[6]
            ticker = parts[3]
            time_executed = parts[12]
            price = float(parts[9])
            action = parts[4]
            if status == 'Executed':
                executed = True

        elif parts[0].upper() == "%ORDERACT":
            act_status = parts[2]
            action = parts[3]
            ticker = parts[4]
            shares = parts[5]
            price = float(parts[6]) if parts[6].replace('.', '', 1).isdigit() else 0.0  # Handle price correctly
            time_executed = parts[8]
            notes = ' '.join(parts[9:-1])

        # Ensure we insert data even if some parts are missing
        if order_id or act_status:
            self.logger.debug(f"Order line found: {line}")
            self.insert_buy_market(trade_id, time_executed, ticker, shares, price, token, order_id or "N/A", action, status or act_status, act_status, notes)

        if executed:
            self.logger.info(f"Order executed for trade_id {trade_id}. Cancelling stop order.")
            self.cancel_stop_order(trade_id)
            self.move_trade_to_closed(trade_id, ticker, price, time_executed)
        else:
            self.logger.error(f"Close trade failed for {trade_id}, {ticker}.")

    def cancel_stop_order(self, trade_id):
        """Cancel the stop order for a given trade_id."""
        self.logger.debug(f"Attempting to cancel stop order for trade_id: {trade_id}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT stopOrderID FROM TradeDetails WHERE tradeID = ?", (trade_id,))
                result = cursor.fetchone()

                if result:
                    stop_order_id = result[0]
                    cancel_command = f'CANCEL {stop_order_id}'
                    self.logger.debug(f"Sending cancel command for stop order: {cancel_command}")
                    self.send_command(cancel_command)
                else:
                    self.logger.warning(f"No stop order found for trade_id: {trade_id}")

        except sqlite3.Error as e:
            self.logger.error(f"Error accessing database for canceling stop order: {e}")

    def insert_buy_market(self, trade_id, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes):
        """Insert the buy market order details into the BuyMarket table or update if it exists."""
        self.logger.debug(f"Processing buy market order for trade_id: {trade_id}, ticker: {ticker}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Check if the order already exists in the database
                cursor.execute("SELECT COUNT(*) FROM BuyMarket WHERE tradeID = ?", (trade_id,))
                exists = cursor.fetchone()[0]

                if exists:
                    # Update the existing order
                    self.logger.debug(f"Updating existing order for trade_id: {trade_id}")
                    cursor.execute("""
                        UPDATE BuyMarket
                        SET time = ?, ticker = ?, shares = ?, price = ?, token = ?, orderID = ?, action = ?, 
                            status = ?, act_status = ?, notes = ?, date = ?
                        WHERE tradeID = ?
                    """, (time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes, 
                          datetime.now().strftime('%Y-%m-%d'), trade_id))
                    self.logger.info(f"Updated order in BuyMarket for trade_id: {trade_id} with new status: {status}")
                else:
                    # Insert the new order
                    self.logger.debug(f"Inserting new order for trade_id: {trade_id}")
                    cursor.execute("""
                        INSERT INTO BuyMarket (tradeID, time, ticker, shares, price, token, orderID, action, status, act_status, notes, date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_id, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes, 
                         datetime.now().strftime('%Y-%m-%d')))
                    self.logger.info(f"Inserted order into BuyMarket for trade_id: {trade_id}")

                conn.commit()

        except sqlite3.Error as e:
            self.logger.error(f"Error inserting or updating in database: {e}")


    def send_command(self, command):
        """Send a command to the server."""
        with self.lock:  # Ensure thread safety
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

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

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    end_of_day = EndOfDay()

    # Connect to server before starting to listen
    end_of_day.connect_to_server()

    # Start listening to server in a separate thread
    listener_thread = threading.Thread(target=end_of_day.listen_to_server, daemon=True)
    listener_thread.start()

    # Start end of day tasks in a separate thread
    eod_thread = threading.Thread(target=end_of_day.end_of_day_tasks, daemon=True)
    eod_thread.start()

    # Keep the main program running to allow listening
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        end_of_day.keep_listening = False
        end_of_day.disconnect_from_server()
