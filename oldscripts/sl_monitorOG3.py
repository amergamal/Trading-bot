import sqlite3
import threading
import logging
import socket
import time
from datetime import datetime


db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

class SLMonitor:
    def __init__(self):
        self.logger = logging.getLogger('SLMonitor')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.db_path = db_path
        self.stopOrderIDs = set()
        self.lock = threading.Lock()  # Initialize a lock to prevent race conditions
        self.logger.info("SLMonitor instance created and initialized.")

        # Start listening to the server for updates
        self.listen_to_server()

    def listen_to_server(self):
        """Start a thread to listen for server responses."""
        threading.Thread(target=self._listen_to_server, daemon=True).start()

    def _listen_to_server(self):
        """Continuously listen for server responses."""
        while True:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.logger.info("Attempting to connect to server...")
                self.wait_for_server()  # Wait until the server is ready
                
                self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")

                while True:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.process_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.logger.info("Retrying connection in 5 seconds...")
                time.sleep(5)  # Wait before retrying
            except Exception as e:
                self.logger.error(f"Error listening to server: {e}")
            finally:
                self.client_socket.close()
                self.logger.info("Socket connection closed.")
                
    def wait_for_server(self):
        """Keep checking if the server is up before attempting to connect."""
        while True:
            try:
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                break  # Successfully connected, exit the loop
            except socket.error:
                self.logger.info("Server not available, waiting 5 seconds before retrying...")
                time.sleep(5)  # Wait for a few seconds before retrying            
            
    def get_db_connection(self):
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None

    def receive_order_details(self, trade_details):
        """Receive trade details and track the stopOrderID."""
        self.logger.info(f"Received trade details: {trade_details}")
        stop_order_id = trade_details.get('stopOrderID')
        if stop_order_id:
            self.stopOrderIDs.add(stop_order_id)
            self.logger.info(f"Tracking stopOrderID: {stop_order_id}")
        
    def process_response(self, response):
        """Process server response lines."""
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%ORDER'):
                self.logger.debug(f"Processing response line: {line}")
                parts = line.split()
                try:
                    order_id = parts[1]  # Order ID is the second element
                    ticker = parts[3]    # Ticker is the fourth element
                    price = parts[9]     # Price is the ninth element
                    status = parts[11]   # Status is the eleventh element
                    update_status_time = parts[12] 
                    
                    self.logger.info(f"Processing order - Order ID: {order_id}, Status: {status}")
                    self.logger.info(f"Ticker: {ticker}, Order ID: {order_id}, Price: {price}, Status: {status}")
                    
                    # Check if order is executed and call process_executed_order
                    if status == "Executed" and self.is_stop_order_in_trade_details(order_id):
                        try:
                            self.process_executed_order(order_id, price, update_status_time)
                            self.logger.info(f"Processed executed order for Order ID: {order_id}.")
                        except Exception as e:
                            self.logger.error(f"Error in process_executed_order: {e}")
                        self.logger.info(f"Order ID: {order_id} has been executed. Processing executed order.")
                        try:
                            self.update_stop_market_status(order_id, status, update_status_time)
                            self.logger.info(f"Updated stop market status for Order ID: {order_id} to Executed.")
                        except Exception as e:
                            self.logger.error(f"Error in update_stop_market_status: {e}")
                        
                    # Check if order is canceled and call process_executed_order
                    elif status == "Canceled" and self.is_stop_order_in_trade_details(order_id):
                        self.logger.info(f"Match found for canceled order ID: {order_id}")    
                
                        try:
                            self.process_executed_order(order_id, price, update_status_time)
                            self.logger.info(f"Processed executed order for Order ID: {order_id}.")
                        except Exception as e:
                            self.logger.error(f"Error in process_executed_order: {e}")
                        self.logger.info(f"Order ID: {order_id} has been executed. Processing executed order.")
                        try:
                            self.update_stop_market_status(order_id, status, update_status_time)
                            self.logger.info(f"Updated stop market status for Order ID: {order_id} to Executed.")
                        except Exception as e:
                            self.logger.error(f"Error in update_stop_market_status: {e}")
                    
                    else:
                        self.logger.info(f"No match for order ID: {order_id} not Executed or Canceled, ignoring response.")
                except IndexError as e:
                    self.logger.error(f'Error parsing %ORDER response: {e}')
                    continue    
                    
            elif line.startswith('%OrderAct'):    
                self.logger.debug(f"Processing response line: {line}")    
                parts = line.split()
                try:
                    order_id = parts[1]  # Order ID is the second element
                    status = parts[2]    # Status is the third element
                    price = parts[6]     #updated status price
                    update_status_time = parts[8]      # Time is the ninth element
                
                    self.logger.info(f"Processing OrderAct - Order ID: {order_id}, Status: {status}, Price: {price}, Time: {update_status_time}")
                    
                    
                                        
                    self.update_stop_market_status(order_id, status, update_status_time)
                except IndexError as e:
                    self.logger.error(f'Error parsing %OrderAct response: {e}')
                continue    
            
            
    def is_stop_order_in_trade_details(self, stop_order_id):
        """Check if the stopOrderID exists in the TradeDetails table."""
        conn = self.get_db_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM TradeDetails 
                WHERE stopOrderID = ?
            """, (stop_order_id,))
            result = cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking stopOrderID: {e}")
            return False
        finally:
            conn.close()
        
                
                    
    def update_stop_market_status(self, order_id, status, update_status_time):
        """Update the status, act_status, and time in the StopMarket table."""
        self.logger.debug(f"Entered update_stop_market_status with order_id: {order_id}, status: {status}")
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
               UPDATE StopMarket
               SET status = ?, act_status = ?, time = ?
               WHERE orderID = ?""",
               (status, status, update_status_time, order_id)
            )
            conn.commit()
            self.logger.info(f"Updated stop market order status: orderID={order_id}, status={status}, time={update_status_time}")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating stop market status: {e}")
        finally:
            conn.close() 
            
    def update_stop_loss_in_active_trades(self, stop_order_id, new_stop_price):
        """Update the stop loss price in ActiveTrades to the new stop price after replacement using stopOrderID."""
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
            self.logger.info(f"Updated stop loss price in ActiveTrades for stopOrderID {stop_order_id} to {new_stop_price}")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating stop loss in active trades: {e}")
        finally:
            conn.close()
        
        
    
            
    def process_executed_order(self, order_id, price, update_status_time):
        with self.lock:
            conn = self.get_db_connection()
            if not conn:
                return
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT tradeID, strategy, ticker, shares, entry_price, stop_loss, time 
                    FROM ActiveTrades 
                    WHERE stopOrderID = ?""", 
                    (order_id,))
                trade = cursor.fetchone()

                if trade:
                    tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time = trade
                    realized = (entry_price - stop_loss) * shares

                    cursor.execute("""
                        INSERT INTO ClosedTrades (tradeID, strategy, ticker, shares, entry_price, entry_time, stop_loss, sl_time, reason, date, realized) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (tradeID, strategy, ticker, shares, entry_price, entry_time, price, update_status_time, 'Stop loss', datetime.now().strftime('%Y-%m-%d'), realized)
                    )

                    cursor.execute("DELETE FROM ActiveTrades WHERE stopOrderID = ?", (order_id,))
                    
                    cursor.execute("""
                        INSERT INTO ExecutedStop (tradeID, ticker, shares, stopOrderID, strategy, time, entry_price, stop_loss, executed_time) 
                        SELECT tradeID, ticker, shares, stopOrderID, strategy, time, entry_price, stop_loss, ? 
                        FROM TradeDetails 
                        WHERE stopOrderID = ?""", 
                        (update_status_time, order_id))

                    # Delete the processed record from TradeDetails
                    cursor.execute("DELETE FROM TradeDetails WHERE stopOrderID = ?", (order_id,))

                    
                    cursor.execute("""
                        UPDATE TradeStatus 
                        SET loss_count = loss_count + 1, active_trade = 'closed' 
                        WHERE ticker = ? AND strategy = ? AND date = ?""",
                        (ticker, strategy, datetime.now().strftime('%Y-%m-%d'))
                    )
                    
                    cursor.execute("""
                       UPDATE StopMarket
                       SET status = 'Executed', act_status = 'Execute'
                       WHERE ticker = ? AND orderID = ?""",
                       (ticker, order_id)
                    )
                    
                    
                    
                    # Update BorrowedShares (moved inside the same transaction)
                    today_date = datetime.now().strftime('%Y-%m-%d')
                    cursor.execute("""
                        UPDATE BorrowedShares 
                        SET available_shares = available_shares + ?, last_updated = ? 
                        WHERE ticker = ? AND DATE(last_updated) = ?""",
                        (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date)
                    )
                    if cursor.rowcount > 0:
                        self.logger.info(f"Updated BorrowedShares for ticker {ticker}, increased available_shares by {shares}.")
                    else:
                        self.logger.warning(f"No matching record found for ticker {ticker} on date {today_date}.")

                    conn.commit()
                    self.logger.info(f"Moved trade {tradeID} to ClosedTrades and updated TradeStatus")
                    self.stopOrderIDs.discard(order_id)  # Remove stopOrderID from the set                       
            except sqlite3.Error as e:
                self.logger.error(f"Error processing executed order: {e}")
            finally:
                conn.close() 

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Create an SLMonitor instance
    sl_monitor = SLMonitor()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
