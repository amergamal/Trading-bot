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
LOCAL_SERVER_PORT = 5015

def generate_token():
    """Generate a unique token for each order."""
    return str(random.randint(100000, 999999))

class EndOfDay:
    def __init__(self, socketio=None):
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
        self.socketio = socketio  # Add SocketIO instance
        if not socketio:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        self.logger.info("EOD instance created and initialized.")

    def connect_to_server(self):
        """Establish a connection to the server."""
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.settimeout(5.0)  # Timeout for connection attempts
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server 5015.")
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                self.logger.info("Retrying connection in 5 seconds...")
                time.sleep(5)

    def disconnect_from_server(self):
        """Close the connection to the server."""
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
            self.logger.info("Socket connection closed.")

    def receive_order_details(self, trade_details):
        """Receive trade details, store in the trade_details dictionary, and insert into the database."""
        trade_id = trade_details['tradeID']
        stop_order_id = trade_details['stopOrderID']
        strategy = trade_details['strategy']
        
        if not stop_order_id:
            self.logger.error(f"Missing stopOrderID in trade details for trade_id {trade_id}: {trade_details}")
        with self.lock:
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

    def close_position(self, ticker, trade_id):
        """Close the open position for a specific ticker."""
        try:
            self.logger.debug(f"Starting to close position for ticker: {ticker}.")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT tradeID, strategy, ticker, shares, entry_price, stop_loss, time, stopOrderID 
                    FROM ActiveTrades 
                    WHERE ticker = ? AND tradeID = ?
                """, (ticker, trade_id))
                open_trade = cursor.fetchone()
                self.logger.debug(f"Open trades fetched: {open_trade}")

                if open_trade:
                    tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time, stopOrderID = open_trade
                    self.logger.info(f"Closing position for tradeID {tradeID}, ticker {ticker}")

                    # Match with received trade details in the database
                    cursor.execute("""
                        SELECT * FROM TradeDetails 
                        WHERE tradeID = ? AND strategy = ? AND ticker = ? AND shares = ? AND stopOrderID = ?
                    """, (tradeID, strategy, ticker, shares, stopOrderID))
                    trade_detail = cursor.fetchone()

                    if trade_detail:
                        # Send a buy market order to close the position
                        self.logger.info(f"Closing position for tradeID {tradeID}: {open_trade}")
                        self.send_buy_market_order(tradeID, strategy, ticker, shares, stopOrderID)
                    else:
                        self.logger.debug(f"No matching trade details for tradeID {tradeID}.")
                else:
                    self.logger.info(f"No open position found for ticker {ticker}.")

        except Exception as e:
            self.logger.error(f"Error while closing position for ticker {ticker}: {e}")

    def close_positions(self):
        """Close all open positions at the end of the day."""
        self.logger.debug("Starting to close positions.")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, strategy, ticker, shares, entry_price, stop_loss, time, stopOrderID 
                FROM ActiveTrades 
            """)
            open_trades = cursor.fetchall()
            self.logger.debug(f"Open trades fetched: {open_trades}")

            for trade in open_trades:
                tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time, stopOrderID = trade
                self.logger.debug(f"Evaluating trade: {tradeID}, {strategy}, {ticker}")

                # Match with received trade details in the database
                cursor.execute("""
                    SELECT * FROM TradeDetails 
                    WHERE tradeID = ? AND strategy = ? AND ticker = ? AND shares = ? AND stopOrderID = ?
                """, (tradeID, strategy, ticker, shares, stopOrderID))
                trade_detail = cursor.fetchone()

                if trade_detail:
                    self.logger.info(f"Closing position for tradeID {tradeID}: {trade}")
                    self.send_buy_market_order(tradeID, strategy, ticker, shares, stopOrderID)
                else:
                    self.logger.debug(f"No matching trade details for tradeID {tradeID}.")

    def send_buy_market_order(self, trade_id, strategy, ticker, shares, stopOrderID):
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
                self.trade_details[token] = {'trade_id': trade_id, 'strategy': strategy, 'ticker': ticker, 'shares': shares, 'stopOrderID': stopOrderID, 'token': token}

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.disconnect_from_server()

    def run_combined_tasks(self):
        """Run both server listening and end-of-day tasks in a single thread."""
        self.logger.info("Starting combined EndOfDay tasks")
        self.client_socket.settimeout(1.0)  # Set socket timeout for non-blocking reads
        while self.keep_listening:
            current_time = datetime.now()
            # Check time for end-of-day tasks (starting at 15:50)
            target_time = current_time.replace(hour=15, minute=50, second=0, microsecond=0)
            if current_time >= target_time and current_time.hour == 15 and current_time.minute == 57:
                self.logger.info("Designated time reached. Closing positions.")
                self.close_positions()
                break  # Exit after closing positions

            # Check for server responses
            try:
                response = self.client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    self.handle_response(response)
            except socket.timeout:
                pass  # No data available, continue loop
            except socket.error as e:
                self.logger.error(f"Socket error while listening: {e}")
                self.keep_listening = False
                break

            time.sleep(1)  # Short sleep to avoid busy-waiting
        self.disconnect_from_server()
        self.logger.info("Combined EndOfDay tasks completed")

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
                    stop_order_id = trade_details.get('stopOrderID', None) 
                    if stop_order_id is not None:
                        self.logger.debug(f"Processing buy response for token: {token}")
                        self.process_buy_response(line, trade_details['trade_id'], trade_details['ticker'], trade_details['shares'], trade_details['token'], stop_order_id)
                    else:
                        self.logger.error(f"Missing stop_order_id for trade_id: {trade_details['trade_id']}")

    def process_buy_response(self, line, trade_id, ticker, shares, token, stop_order_id):
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
            if act_status == 'Execute':
                executed = True

        # Ensure we insert data even if some parts are missing
        if order_id:
            self.logger.debug(f"Order line found: {line}")
            if executed or status == "Executed":
                # Get strategy from trade details
                strategy = self.trade_details[token]['strategy'] 
                self.logger.info(f"Order executed for trade_id {trade_id}. Cancelling stop order.")
                self.insert_buy_market(trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes)
                self.cancel_stop_order(trade_id, stop_order_id)
                self.move_trade_to_closed(trade_id, ticker, price, time_executed)
            else:
                self.logger.debug(f"Order {order_id} not yet executed. Status is: {status}. Will retry on next response.")

    def cancel_stop_order(self, trade_id, stop_order_id):
        """Cancel the stop order for a given trade_id."""
        self.logger.debug(f"Attempting to cancel stop order for trade_id: {trade_id}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT stopOrderID FROM TradeDetails WHERE tradeID = ? AND stopOrderID = ?", (trade_id, stop_order_id))
                result = cursor.fetchone()

                if result:
                    stop_order_id = result[0]
                    cancel_command = f'CANCEL {stop_order_id}'
                    self.logger.debug(f"Sending cancel command for stop order: {cancel_command}")
                    self.send_command(cancel_command)
                else:
                    self.logger.warning(f"No matching stop order found for trade_id: {trade_id} and stop_order_id: {stop_order_id}")

        except sqlite3.Error as e:
            self.logger.error(f"Error accessing database for canceling stop order: {e}")

    def insert_buy_market(self, trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes):
        """Insert the buy market order details into the BuyMarket table or update if it exists."""
        self.logger.debug(f"Processing buy market order for trade_id: {trade_id}, ticker: {ticker}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                buy_market_data = {
                    'tradeID': trade_id,
                    'strategy': strategy,
                    'time': time_executed,
                    'ticker': ticker,
                    'shares': shares,
                    'price': price,
                    'token': token,
                    'orderID': order_id,
                    'action': action,
                    'status': status,
                    'act_status': act_status,
                    'notes': notes,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
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
                        INSERT INTO BuyMarket (tradeID, strategy, time, ticker, shares, price, token, orderID, action, status, act_status, notes, date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes, 
                         datetime.now().strftime('%Y-%m-%d')))
                    self.logger.info(f"Inserted order into BuyMarket for trade_id: {trade_id}")
                    if self.socketio:
                        self.socketio.emit('buy_market_update', buy_market_data)
                        self.logger.info(f"Emitted buy_market_update: {buy_market_data}")
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
            # Join ActiveTrades and StopMarket on ticker and tradeID
            cursor.execute("""
                SELECT a.strategy, a.shares, a.entry_price, a.time, s.price AS original_stop_loss
                FROM ActiveTrades a
                LEFT JOIN StopMarket s ON a.ticker = s.ticker AND a.tradeID = s.TradeID
                WHERE a.tradeID = ? AND a.ticker = ?
            """, (trade_id, ticker))
            trade = cursor.fetchone()

            if trade:
                strategy, shares, entry_price, entry_time, original_stop_loss = trade
                if original_stop_loss is None:
                    self.logger.warning(f"No matching StopMarket record found for trade_id {trade_id}, ticker {ticker}, setting original_stop_loss to NULL")
                    r_gain_loss = 0.0
                else:
                    # Calculate R gain/loss
                    risk_per_share = entry_price - original_stop_loss
                    if risk_per_share != 0:  # Avoid division by zero
                        r_gain_loss = (entry_price - exit_price) / abs(risk_per_share)
                    else:
                        self.logger.warning(f"Risk per share is zero for trade_id {trade_id}, setting r_gain_loss to 0")
                        r_gain_loss = 0.0

                realized = (entry_price - exit_price) * shares
                closed_trade_data = {
                    'tradeID': trade_id,
                    'strategy': strategy,
                    'ticker': ticker,
                    'shares': shares,
                    'entry_price': entry_price,
                    'entry_time': entry_time,
                    'original_stop_loss': original_stop_loss,  # From StopMarket.price
                    'stop_loss': original_stop_loss,  # For take-profit, same as original
                    'sl_time': None,  # No stop-loss execution
                    'exit_price': exit_price,
                    'exit_time': exit_time,
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'reason': 'TakeProfit',
                    'realized': realized,
                    'r_gain_loss': r_gain_loss
                }

                cursor.execute("""
                    INSERT INTO ClosedTrades (tradeID, strategy, ticker, shares, entry_price, entry_time,
                                              original_stop_loss, stop_loss, sl_time, exit_price, exit_time,
                                              date, reason, realized, r_gain_loss)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (trade_id, strategy, ticker, shares, entry_price, entry_time,
                      original_stop_loss, original_stop_loss, None, exit_price, exit_time,
                      datetime.now().strftime('%Y-%m-%d'), 'TakeProfit', realized, r_gain_loss))

                cursor.execute("DELETE FROM ActiveTrades WHERE tradeID = ? AND ticker = ?", (trade_id, ticker))

                cursor.execute("""
                    UPDATE TradeStatus
                    SET active_trade = 'closed'
                    WHERE ticker = ? AND strategy = ? AND date = ?
                """, (ticker, strategy, datetime.now().strftime('%Y-%m-%d')))

                # Update BorrowedShares
                today_date = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("""
                    UPDATE BorrowedShares
                    SET available_shares = available_shares + ?, last_updated = ?
                    WHERE ticker = ? AND DATE(last_updated) = ?
                """, (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date))
                if cursor.rowcount > 0:
                    self.logger.info(f"Updated BorrowedShares for ticker {ticker}, increased available_shares by {shares}.")
                else:
                    self.logger.warning(f"No matching record found for ticker {ticker} on date {today_date}.")

                conn.commit()
                self.logger.info(f"Moved trade {trade_id} to ClosedTrades and updated TradeStatus")
                # Emit events
                if self.socketio:
                    self.socketio.emit('closed_trade_update', closed_trade_data)
                    self.logger.info(f"Emitted closed_trade_update: {closed_trade_data}")

                    active_trade_removal = {'tradeID': trade_id}
                    self.socketio.emit('active_trade_remove', active_trade_removal)
                    self.logger.info(f"Emitted active_trade_remove: {active_trade_removal}")

            else:
                self.logger.warning(f"No active trade found for trade_id {trade_id}, ticker {ticker}")

        except sqlite3.Error as e:
            self.logger.error(f"Error moving trade to closed: {e}")
        finally:
            conn.close()

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    end_of_day = EndOfDay()

    # Connect to server before starting tasks
    end_of_day.connect_to_server()

    # Start combined tasks in a single thread
    combined_thread = threading.Thread(target=end_of_day.run_combined_tasks, daemon=True)
    combined_thread.start()

    # Keep the main program running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        end_of_day.keep_listening = False
        end_of_day.disconnect_from_server()