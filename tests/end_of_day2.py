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
        self.trade_details = {}  # Dictionary to store trade details by tradeID
        self.lock = threading.Lock()  # Lock for thread safety
        self.client_socket = None
        self.logger.info("EOD instance created and initialized.")

    def receive_order_details(self, trade_details):
        """Receive trade details and store them in the trade_details dictionary."""
        trade_id = trade_details['tradeID']
        with self.lock:
            self.trade_details[trade_id] = trade_details
            self.logger.info(f"Received trade details for {trade_id}: {trade_details}")

    def send_command(self, command):
        """Send commands to the server."""
        with self.lock:  # Ensure thread safety when sending commands
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
            if current_time.hour == 14 and current_time.minute == 59:  # Adjust time as needed
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

                # Match with received trade details
                if tradeID in self.trade_details:
                    trade_detail = self.trade_details[tradeID]
                    if (trade_detail['strategy'] == strategy and
                        trade_detail['ticker'] == ticker and
                        trade_detail['shares'] == shares):
                        self.logger.info(f"Closing position for tradeID {tradeID}: {trade}")
                        self.send_buy_market_order(tradeID, ticker, shares)
                    else:
                        self.logger.debug(f"No matching trade details for tradeID {tradeID}.")
                else:
                    self.logger.debug(f"TradeID {tradeID} not found in received trade details.")

    def send_buy_market_order(self, trade_id, ticker, shares):
        """Send a command to buy back the shares and listen for the response."""
        token = generate_token()
        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares} MKT TIF=DAY'

        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.logger.info("Attempting to connect to server...")
            self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")

            # Send command and listen for response
            self.logger.debug(f"Sending buy market order for trade_id {trade_id} with command: {buy_command}")
            self.send_command(buy_command)
            self.listen_to_server()  # Start listening for the server's response

            # Store the order details with the token
            self.trade_details[token] = {'trade_id': trade_id, 'ticker': ticker, 'shares': shares, 'token': token}

        except socket.error as e:
            self.logger.error(f"Socket error: {e}")
        except Exception as e:
            self.logger.error(f"Error connecting to server: {e}")
        finally:
            if self.client_socket:
                self.client_socket.close()
                self.logger.info("Socket connection closed.")

    def listen_to_server(self):
        """Listen for server responses after sending a command."""
        try:
            while True:
                response = self.client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    self.handle_response(response)
                else:
                    self.logger.debug("No response received, waiting...")
                    time.sleep(0.5)
        except socket.error as e:
            self.logger.error(f"Socket error while listening: {e}")
        except Exception as e:
            self.logger.error(f"Error listening to server: {e}")

    def handle_response(self, response):
        """Process responses from the server and match tokens to trade details."""
        lines = response.strip().split('\n')
        for line in lines:
            parts = line.split()
            if len(parts) > 1:
                if parts[0] in ("%ORDER", "%ORDERACT"):
                    token = parts[2] if parts[0] == "%ORDER" else parts[-1]
                    if token in self.trade_details:
                        trade_details = self.trade_details[token]
                        self.logger.debug(f"Processing buy response for token: {token}")
                        self.process_buy_response(response, trade_details['trade_id'], trade_details['ticker'], trade_details['shares'], trade_details['token'])

    def process_buy_response(self, response, trade_id, ticker, shares, token):
        """Process the response from the buy market order command."""
        self.logger.debug(f"Processing buy response for trade_id: {trade_id}, ticker: {ticker}")
        act_status = ""
        notes = ""
        executed = False  # Flag to track if the order was executed
        time_executed = None
        price = None

        order_line = None
        order_act_line = None

        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%ORDER'):
                parts = line.split()
                if parts[2] == token:  # Match the token
                    order_line = parts
                    order_id = parts[1]
                    status = parts[11]
                    shares = parts[6]
                    ticker = parts[3]
                    time_executed = parts[12]
                    price = float(parts[9])
                    action = parts[4]

            elif line.startswith('%ORDERACT'):
                parts = line.split()
                if parts[-1] == token:  # Match the token
                    order_act_line = parts
                    act_status = parts[2]
                    action = parts[3]
                    ticker = parts[4]
                    shares = parts[5]
                    price = parts[6]
                    time_executed = parts[8]
                    notes = ' '.join(parts[9:-1])

        if order_line:
            self.logger.debug(f"Order line found: {order_line}")
            # Insert into BuyMarket using both %ORDER and %ORDERACT data
            self.insert_buy_market(trade_id, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes)

            if status == 'Executed':
                executed = True

        elif order_act_line:
            self.logger.debug(f"Order act line found: {order_act_line}")
            # Insert into BuyMarket using %ORDERACT data only
            self.insert_buy_market(trade_id, time_executed, ticker, shares, price, token, order_id, action, act_status, act_status, notes)

        if executed:
            self.logger.info(f"Order executed for trade_id {trade_id}. Moving trade to closed.")
            self.move_trade_to_closed(trade_id, ticker, price, time_executed)
        else:
            self.logger.error(f"Close trade failed for {trade_id}, {ticker}.")

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
    eod = EndOfDay()
    threading.Thread(target=eod.end_of_day_tasks, daemon=True).start()
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
