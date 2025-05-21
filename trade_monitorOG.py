import sqlite3
import socket
import random
import logging
import time
import threading
from datetime import datetime

# Configuration
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015
DB_PATH = 'EOD_data.db'

# Initialize a global lock for managing access to the token_map
token_map_lock = threading.Lock()

def generate_token():
    return str(random.randint(100000, 999999))

class TradeMonitor:
    def __init__(self, db_path=DB_PATH, socketio=None):
        self.logger = logging.getLogger('TradeMonitor')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.debug(f"Received socketio instance: {socketio}, type: {type(socketio)}")
        self.db_path = db_path
        self.token_map = {}
        self.running = False
        self.threads = []  # Initialize the threads attribute
        self.client_socket = None
        self.keep_listening = True  # Flag to control listening
        
        # New dictionaries to store original stop loss and latest LU price
        
        

        self.stopOrderIDs = set()  # Initialize the set
        self.trade_details = {}  # Dictionary to store trade details
        self.socketio = socketio  # Add SocketIO instance
        if socketio is None:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        else:
            self.logger.info("SocketIO instance provided; real-time updates enabled.")
        
        self.start_listening_thread()
        
        self.logger.debug('TradeMonitor instance created.')

    def start_listening_thread(self):
        """Start the listen_to_server function in a separate thread."""
        listening_thread = threading.Thread(target=self.listen_to_server, daemon=True)
        listening_thread.start()

    def start_monitoring(self):
        self.running = True
        
        self.threads.append(threading.Thread(target=self.listen_to_server, daemon=True))
        
        for thread in self.threads:
            thread.start()
        self.logger.debug('TradeMonitor started monitoring.')

    def stop_monitoring(self):
        self.running = False
        self.keep_listening = False
        for thread in self.threads:
            if thread.is_alive():
                thread.join()
        self.logger.debug('TradeMonitor stopped monitoring.')

    def get_db_connection(self):
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None

    

    def insert_active_trade(self, trade_details):
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT LAST FROM TradeParameters 
                WHERE ticker = ? AND date = ?""",
                (trade_details['ticker'], datetime.now().strftime('%Y-%m-%d'))
            )
            last_price = cursor.fetchone()
            
            if last_price:
                last_price = float(last_price[0])  # Ensure last_price is a float
                entry_price = float(trade_details['entry_price'])  # Convert entry_price to float
                shares = float(trade_details['shares'])  # Convert shares to float
                unrealized = (entry_price - last_price) * shares
            else:
                unrealized = 0.0
                
            active_trade_data = {
                'tradeID': trade_details['tradeID'],
                'time': trade_details['time'],
                'strategy': trade_details['strategy'],
                'ticker': trade_details['ticker'],
                'shares': trade_details['shares'],
                'entry_price': trade_details['entry_price'],
                'stop_loss': trade_details['stop_loss'],
                'sellOrderID': trade_details['sellOrderID'],
                'stopOrderID': trade_details['stopOrderID'],
                'date': datetime.now().strftime('%Y-%m-%d'),
                'unrealized': unrealized
            }    
            
            cursor.execute("""
                INSERT INTO ActiveTrades (tradeID, time, strategy, ticker, shares, entry_price, stop_loss, sellOrderID, stopOrderID, date, unrealized) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_details['tradeID'], trade_details['time'], trade_details['strategy'], trade_details['ticker'], trade_details['shares'], trade_details['entry_price'], trade_details['stop_loss'], 
                trade_details['sellOrderID'], trade_details['stopOrderID'], datetime.now().strftime('%Y-%m-%d'), unrealized)
            )
            conn.commit()
            
            # Save the trade details in the dictionary using stopOrderID as the key
            self.trade_details[trade_details['stopOrderID']] = trade_details
            
            self.stopOrderIDs.add(trade_details['stopOrderID'])  # Add stopOrderID to the set
            self.logger.info(f"Inserted trade {trade_details['tradeID']} with sellOrderID {trade_details['sellOrderID']}, stopOrderID {trade_details['stopOrderID']} and unrealized {unrealized} into ActiveTrades")
            
            # Emit SocketIO event
            if self.socketio:
                self.socketio.emit('active_trade_update', active_trade_data)
                self.logger.info(f"Emitted active_trade_update: {active_trade_data}")
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting active trade: {e}")
        finally:
            conn.close()

    def update_trade_status(self, ticker, strategy):
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            update_date = datetime.now().strftime('%Y-%m-%d')
            self.logger.debug(f"Updating TradeStatus for ticker: {ticker}, strategy: {strategy}, date: {update_date}")
            
            cursor.execute("""
                UPDATE TradeStatus SET active_trade = 'open' 
                WHERE ticker = ? AND strategy = ? AND date = ?""",
                (ticker, strategy, update_date)
            )
            
            if cursor.rowcount == 0:
                self.logger.warning(f"No rows updated for ticker: {ticker}, strategy: {strategy}, date: {update_date}. Possible mismatch.")
            else:
                self.logger.info(f"Updated TradeStatus for {ticker} and strategy {strategy} to 'open'")
                 
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error updating trade status: {e}")
        finally:
            conn.close()

    def update_borrowed_shares(self, ticker, shares):
        """Update the available shares in the BorrowedShares table after a trade, matching today's date."""
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE BorrowedShares 
                SET available_shares = available_shares - ?, last_updated = ? 
                WHERE ticker = ? AND DATE(last_updated) = ?""",
                (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date)
            )
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Updated BorrowedShares for ticker {ticker}, reduced available_shares by {shares}.")
            else:
                self.logger.warning(f"No matching record found for ticker {ticker} on date {today_date}.")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating BorrowedShares: {e}")
        finally:
            conn.close()

    def get_lu_price_and_last_price(self, ticker):
        conn = self.get_db_connection()
        if not conn:
            return None, None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT LU, LAST FROM TradeParameters 
                WHERE ticker = ? AND date = ?""",
                (ticker, datetime.now().strftime('%Y-%m-%d'))
            )
            result = cursor.fetchone()
            if result:
                return result
            else:
                self.logger.warning(f"No LU or LAST price found for {ticker} on {datetime.now().strftime('%Y-%m-%d')}")
                return None, None
        except sqlite3.Error as e:
            self.logger.error(f"Error getting LU and LAST prices: {e}")
            return None, None
        finally:
            conn.close()

    def update_active_trades_with_lu_price_and_last_price(self, ticker, lu_price, last_price):
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ActiveTrades SET lu_price = ?, last_price = ? 
                WHERE ticker = ? AND date = ?""",
                (lu_price, last_price, ticker, datetime.now().strftime('%Y-%m-%d'))
            )
            conn.commit()
            self.logger.info(f"Updated LU and LAST prices for {ticker} in ActiveTrades to {lu_price}, {last_price}")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating active trades with LU and LAST prices: {e}")
        finally:
            conn.close()

    def receive_order_details(self, trade_details):
        self.logger.debug(f"TradeMonitor received trade details: {trade_details}")
    
        # Check the strategy type in trade_details
        strategy = trade_details.get('strategy')
    
        
        if strategy == 'marketl':  # Handle 'marketl' strategy specifically
            # Only insert active trade and update trade status
            self.insert_active_trade(trade_details)
            self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
            self.logger.info(f"Received and processed 'marketl' order details: {trade_details}")
        else:
            # Use the current function behavior
            self.insert_active_trade(trade_details)
            self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
            self.update_borrowed_shares(trade_details['ticker'], trade_details['shares'])  # Update BorrowedShares table
            lu_price, last_price = self.get_lu_price_and_last_price(trade_details['ticker'])
            if lu_price is not None:
                self.update_active_trades_with_lu_price_and_last_price(trade_details['ticker'], lu_price, last_price)
            self.logger.info(f"Received and processed order details: {trade_details}")    
            
        

        
    def receive_latest_lu_price(self, ticker, new_lu_price):
        """Receive the latest LU price, store it, and check if stop loss needs adjustment."""
        self.logger.info(f"Received new LU price for {ticker}: {new_lu_price}")
        
        # Skip processing if the LU price is zero
        if not new_lu_price or new_lu_price == 0:  # Handles None, empty string, and zero
            self.logger.warning(f"LU price for {ticker} is zero. Skipping any stop loss adjustment.")
            return
        
        # Call check_lu_price to handle stop loss adjustments
        self.get_stop_price(ticker, new_lu_price)
            

    def get_stop_price(self, ticker, lu_price):
        """Fetch stop prices for the given ticker, if any active trades are found. If no trades, ignore and proceed."""
        conn = self.get_db_connection()
        if not conn:
            self.logger.error("Failed to connect to the database.")
            return

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, ticker, shares, stop_loss, lu_price, stopOrderID 
                FROM ActiveTrades
                WHERE date = ?""",
                (datetime.now().strftime('%Y-%m-%d'),)
            )
            trades = cursor.fetchall()

            # Check if trades exist in the ActiveTrades table
            if not trades:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring LU price update for now.")
                return  # No active trades, so we simply ignore this LU price update

            # Process only the trades that match the specified ticker
            matching_trade_found = False
            for trade in trades:
                trade_id, trade_ticker, shares, stop_loss, trade_lu_price, stop_order_id = trade

                if trade_ticker == ticker:
                    matching_trade_found = True
                    # Process the matching trade by calling check_lu_price
                    threading.Thread(target=self.check_lu_price, args=(trade_id, trade_ticker, shares, stop_loss, lu_price, stop_order_id)).start()

            if not matching_trade_found:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring LU price update for now.")

        except sqlite3.Error as e:
            self.logger.error(f"Error fetching trades from ActiveTrades: {e}")

        finally:
            conn.close()


    def check_lu_price(self, trade_id, ticker, shares, stop_loss, lu_price, stop_order_id):
        # Check if the trade still exists in ActiveTrades
        if not self.trade_exists(trade_id, ticker, shares):
            self.logger.warning(f"Trade {trade_id} for {ticker} does not exist in ActiveTrades. Removing from monitoring.")
            self.remove_stop_order_id(stop_order_id)
            return
        
        # Check if LU price is None or zero
        if lu_price is None or lu_price == 0:
            self.logger.warning(f"LU price is missing or zero for {ticker}. Skipping stop loss adjustment.")
            return

        if lu_price is not None:
            # Round stop_loss and lu_price to two decimal places to avoid floating-point issues
            stop_loss = round(float(stop_loss), 2)
            lu_price = round(float(lu_price), 2)
        
            # Retrieve the original stop price from the StopMarket table
            original_stop_price = self.get_original_stop_price(stop_order_id)
            if original_stop_price is None:
                self.logger.error(f"Original stop price for stopOrderID {stop_order_id} could not be retrieved.")
                return

            potential_new_stop_price = round(lu_price - 0.02, 2)
            
            # Validate that potential_new_stop_price is positive
            if potential_new_stop_price <= 0:
                self.logger.warning(f"Invalid potential new stop price for {ticker}: {potential_new_stop_price}. Skipping stop loss adjustment.")
                return
        
            # Check if we need to restore the stop loss upward to the original price
            if stop_loss < original_stop_price and lu_price >= original_stop_price + 0.02:
                self.logger.info(f"Restoring stop loss for {ticker} to the original price. LU price: {lu_price}, old stop loss: {stop_loss}, restored stop loss: {original_stop_price}")
                self.send_replace_order(stop_order_id, ticker, shares, original_stop_price, trade_id)
        
            # Adjust the stop loss upward if the LU price goes back up and exceeds the $0.02 threshold, 
            # **BUT ensure the stop loss never exceeds the original stop price**
            elif stop_loss < potential_new_stop_price and lu_price - stop_loss > 0.02:
                new_stop_loss = min(potential_new_stop_price, original_stop_price)  # Limit the new stop loss to the original stop price
                if new_stop_loss > stop_loss:  # Only adjust if it's actually moving upward
                    self.logger.info(f"Adjusting stop loss upward for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {new_stop_loss}")
                    self.send_replace_order(stop_order_id, ticker, shares, new_stop_loss, trade_id)
        
            # Adjust the stop loss downward if the LU price is within the $0.02 threshold
            elif lu_price - stop_loss < 0.02:
                # Lower the stop loss to maintain the $0.02 threshold below LU price
                if potential_new_stop_price < stop_loss:
                    self.logger.info(f"Adjusting stop loss downward for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {potential_new_stop_price}")
                    self.send_replace_order(stop_order_id, ticker, shares, potential_new_stop_price, trade_id)
            else:
                self.logger.info(f"No adjustment needed for {ticker}. LU price: {lu_price}, stop loss remains at: {stop_loss}")         

        else:
            self.logger.warning(f"LU price is None for {ticker}, no adjustment possible.")
            
            

            
    def get_original_stop_price(self, stop_order_id):
        conn = self.get_db_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT price FROM StopMarket
                WHERE orderID = ?""",
                (stop_order_id,)
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            else:
                self.logger.warning(f"No original stop price found for stopOrderID {stop_order_id}.")
                return None
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving original stop price: {e}")
            return None
        finally:
            conn.close()
        


    def trade_exists(self, trade_id, ticker, shares):
        """Check if the trade exists in the ActiveTrades table."""
        conn = self.get_db_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM ActiveTrades
                WHERE tradeID = ? AND ticker = ? AND shares = ? AND date = ?""",
                           (trade_id, ticker, shares, datetime.now().strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            self.logger.error(f"Error checking trade existence in ActiveTrades: {e}")
            return False
        finally:
            conn.close()

    def remove_stop_order_id(self, stop_order_id):
        """Remove the stopOrderID from the active monitoring list."""
        if stop_order_id in self.stopOrderIDs:
            self.stopOrderIDs.remove(stop_order_id)
            self.logger.info(f"Removed stopOrderID {stop_order_id} from monitoring list.")

    def send_replace_order(self, stop_order_id, ticker, shares, new_stop_price, trade_id):
        # Before successfully sending the replace order, update the stop loss in ActiveTrades
        self.update_stop_loss_in_active_trades(stop_order_id, new_stop_price)
        
        command_replace = f"REPLACE {stop_order_id} {shares} STOPMKT {new_stop_price}"
        client_socket = self.send_command(command_replace)

        if client_socket:
            self.logger.info(f"Sent replace order for stopOrderID {ticker}: new stop price {new_stop_price}")
            
            
        else:
            self.logger.error(f"Failed to send replace order for stopOrderID {ticker}")
            
            
    def update_stop_loss_in_active_trades(self, stop_order_id, new_stop_price):
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ActiveTrades 
                SET stop_loss = ? 
                WHERE stopOrderID = ?""",
                (new_stop_price, stop_order_id)
            )
            conn.commit()
            self.logger.info(f"Updated stop loss in ActiveTrades for stopOrderID {stop_order_id} to {new_stop_price}")
        
            # Fetch updated row to emit
            cursor.execute("""
                SELECT tradeID, time, strategy, ticker, shares, entry_price, stop_loss, sellOrderID, stopOrderID, date, unrealized, lu_price, last_price
                FROM ActiveTrades WHERE stopOrderID = ?""",
                (stop_order_id,)
            )
            row = cursor.fetchone()
            if row:
                active_trade_data = {
                    'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3],
                    'shares': row[4], 'entry_price': row[5], 'stop_loss': row[6],
                    'sellOrderID': row[7], 'stopOrderID': row[8], 'date': row[9],
                    'unrealized': row[10], 'lu_price': row[11], 'last_price': row[12]
                }
                if self.socketio:
                    self.socketio.emit('active_trade_update', active_trade_data)
                    self.logger.info(f"Emitted active_trade_update: {active_trade_data}")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating stop loss in active trades: {e}")
        finally:
            conn.close()     

    def listen_to_server(self):
        """Continuously listen for server responses with an indefinite retry mechanism."""
        retry_delay = 1  # Start with 1 second delay

        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                self.keep_listening = True

                while self.keep_listening:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.process_replace_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
            
                break  # Exit retry loop if connection is successful

            except (socket.error, ConnectionRefusedError) as e:
                self.logger.error(f"Connection failed: {e}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60 seconds

        self.logger.info("Successfully connected to server.")

    def process_replace_response(self, response):
        """Process the response from the replace order command."""
        order_data = {}  # Temporary storage for the order data

        lines = response.strip().split('\n')
        for line in lines:
            

            if line.startswith('%OrderAct'):
                self.logger.debug(f"Processing %OrderAct response line: {line}")
                parts = line.split()
                try:
                    order_id = parts[1]  # Order ID is the second element
                    status = parts[2]  # Act Status is the third element
                    action = parts[3]
                    ticker = parts[4]
                    price = parts[6]  # Updated status price
                    time_executed = parts[8]  # Time executed
                    shares = parts[5]
                    
                    # Process only if the status is "Replaced"
                    if status == "Replaced":
                        self.logger.info(f"Order replaced successfully - Order ID: {order_id}, Ticker: {ticker}")
                        
                    
                        # Retrieve tradeID from TradeDetails table
                        trade_id, strategy = self.get_trade_id_from_stop_order_id(order_id)
                    
                        if not trade_id:
                            self.logger.warning(f"Could not retrieve tradeID for order ID {order_id}. Skipping.")
                            continue

                    
                    
                        self.store_replace_order(trade_id, strategy, time_executed, ticker, shares, price, order_id, action, status, status)
                    else:
                        self.logger.info(f"Order ID: {order_id} not yet replaced (status: {status}). Waiting for final replace.")

                    

                except IndexError as e:
                    self.logger.error(f'Error parsing %OrderAct response: {e}')
                    continue
                
                
    def store_replace_order(self, trade_id, strategy, time, ticker, shares, price, order_id, action, status, act_status):
        self.logger.debug(f"Storing or updating replace order - trade_id: {trade_id}, time: {time}, ticker: {ticker}, shares: {shares}, price: {price}, order_id: {order_id}, status: {status}, act_status: {act_status}")
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            replace_order_data = {
                'tradeID': trade_id,
                'strategy': strategy,
                'time': time,
                'ticker': ticker,
                'shares': shares,
                'price': price,
                'orderID': order_id,
                'action': action,
                'status': status,
                'act_status': act_status,
                'date': datetime.now().strftime('%Y-%m-%d')
            }
        
            # Check if the order_id already exists in the ReplaceStop table
            cursor.execute("""
                SELECT COUNT(*) FROM ReplaceStop WHERE orderID = ?
            """, (order_id,))
            exists = cursor.fetchone()[0]
        
            if exists:
                # Update the existing record
                cursor.execute("""
                    UPDATE ReplaceStop
                    SET time = ?, ticker = ?, shares = ?, price = ?, status = ?, act_status = ?, date = ?
                    WHERE orderID = ?
                """, (time, ticker, shares, price, status, act_status, datetime.now().strftime('%Y-%m-%d'), order_id))
                self.logger.info(f"Updated replace order {order_id} with status {status} and act_status {act_status} in ReplaceStop")
            else:
                # Insert a new record
                cursor.execute("""
                    INSERT INTO ReplaceStop (tradeID, strategy, time, ticker, shares, price, orderID, action, status, act_status, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (trade_id, strategy, time, ticker, shares, price, order_id, action, status, act_status, datetime.now().strftime('%Y-%m-%d')))
                self.logger.info(f"Inserted replace order {order_id} with status {status} and act_status {act_status} into ReplaceStop")
                # Emit insert event
                if self.socketio:
                    self.socketio.emit('replace_stop_update', replace_order_data)
                    self.logger.info(f"Emitted replace_stop_update: {replace_order_data}")
                
            conn.commit()
        
        except sqlite3.Error as e:
            self.logger.error(f"Error storing or updating replace order: {e}")
        finally:
            conn.close()            
                
                
    def get_trade_id_from_stop_order_id(self, stop_order_id):
        """Retrieve tradeID from TradeDetails table based on stopOrderID."""
        conn = self.get_db_connection()
        if not conn:
            return None, None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, strategy FROM TradeDetails 
                WHERE stopOrderID = ?
            """, (stop_order_id,))
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            else:
                self.logger.warning(f"No tradeID found for stopOrderID {stop_order_id}.")
                return None, None
        except sqlite3.Error as e:
            self.logger.error(f"Database error while retrieving tradeID: {e}")
            return None, None
        finally:
            conn.close()            

        

    def send_command(self, command):
        """Send a command to the server."""
        with token_map_lock:  # Ensure thread safety
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

# Example usage
if __name__ == "__main__":
    trade_monitor = TradeMonitor()
    trade_monitor.start_monitoring()

    # Keep the main thread alive to ensure the background threads keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        trade_monitor.stop_monitoring()
        print("Shutting down...")
