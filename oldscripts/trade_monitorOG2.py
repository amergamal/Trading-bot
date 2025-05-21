import sqlite3
import socket
import random
import logging
import time
import threading
from datetime import datetime


# Configuration
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012
DB_PATH = 'EOD_data.db'

# Initialize a global lock for managing access to the token_map
token_map_lock = threading.Lock()

def generate_token():
    return str(random.randint(100000, 999999))



class TradeMonitor:
    def __init__(self, db_path=DB_PATH):
        self.logger = logging.getLogger('TradeMonitor')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.db_path = db_path
        self.token_map = {}
        self.running = False
        self.threads = []  # Initialize the threads attribute
        self.stopOrderIDs = set()  # Initialize the set
        self.trade_details = {}  # Dictionary to store trade details
        self.create_trigger()
        
        
        
        self.logger.debug('TradeMonitor instance created.')
        
        
    def start_monitoring(self):
        self.running = True
        self.threads.append(threading.Thread(target=self.monitor_lu_price, daemon=True))
        self.threads.append(threading.Thread(target=self.listen_to_server, daemon=True))
        
        for thread in self.threads:
            thread.start()
        self.logger.debug('TradeMonitor started monitoring.')
        
    def stop_monitoring(self):
        self.running = False
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

    def create_trigger(self):
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS update_lu_price_and_last_price_in_activetrades
            AFTER UPDATE OF LU, LAST ON TradeParameters
            FOR EACH ROW
            BEGIN
                UPDATE ActiveTrades
                SET lu_price = NEW.LU, last_price = NEW.LAST
                WHERE ticker = NEW.ticker AND date = NEW.date;
            END;
            """)
            
            cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS update_unrealized_in_activetrades
            AFTER UPDATE OF last_price ON ActiveTrades
            FOR EACH ROW
            BEGIN
               UPDATE ActiveTrades
               SET unrealized = (entry_price - NEW.last_price) * shares
               WHERE tradeID = NEW.tradeID AND date = NEW.date;
               
               -- Log the calculated unrealized value after the update
               INSERT INTO DebugLogs (log_message, created_at)
               VALUES ('After Update: tradeID=' || NEW.tradeID || ', unrealized=' || (entry_price - NEW.last_price) * shares, CURRENT_TIMESTAMP);

               
            END;
            """)
            
            conn.commit()
            self.logger.info("Created triggers to update LU and LAST prices and recalculate unrealized in ActiveTrades")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating triggers: {e}")
        finally:
            conn.close()

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
            self.loggerg.error(f"Error updating active trades with LU and LAST prices: {e}")
        finally:
            conn.close()

    def receive_order_details(self, trade_details):
        self.logger.debug(f"TradeMonitor received trade details: {trade_details}")
        self.insert_active_trade(trade_details)
        self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
        self.update_borrowed_shares(trade_details['ticker'], trade_details['shares'])  # Update BorrowedShares table
        lu_price, last_price = self.get_lu_price_and_last_price(trade_details['ticker'])
        if lu_price is not None:
            self.update_active_trades_with_lu_price_and_last_price(trade_details['ticker'], lu_price, last_price)
        self.logger.info(f"Received and processed order details: {trade_details}")

    def monitor_lu_price(self, check_interval=60):
        while True:
            time.sleep(check_interval)
            conn = self.get_db_connection()
            if not conn:
                continue
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT tradeID, ticker, shares, stop_loss, lu_price, stopOrderID FROM ActiveTrades
                    WHERE date = ?""",
                    (datetime.now().strftime('%Y-%m-%d'),)
                )
                trades = cursor.fetchall()
                for trade in trades:
                    trade_id, ticker, shares, stop_loss, lu_price, stop_order_id = trade
                    threading.Thread(target=self.check_lu_price, args=(trade_id, ticker, shares, stop_loss, lu_price, stop_order_id)).start()
            except sqlite3.Error as e:
                self.logger.error(f"Error monitoring LU prices: {e}")
            finally:
                conn.close()

    def check_lu_price(self, trade_id, ticker, shares, stop_loss, lu_price, stop_order_id):
         # Check if the trade still exists in ActiveTrades
        if not self.trade_exists(trade_id, ticker, shares):
            self.logger.warning(f"Trade {trade_id} for {ticker} does not exist in ActiveTrades. Removing from monitoring.")
            self.remove_stop_order_id(stop_order_id)
            return
        
        if lu_price is not None:
            new_stop_price = lu_price - 0.05
            
            if new_stop_price != stop_loss:  # Only update if the new stop price differs from the current one
                self.logger.info(f"Adjusting stop loss for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {new_stop_price}")
                self.send_replace_order(stop_order_id, ticker, shares, new_stop_price, trade_id)
            else:
                self.logger.info(f"No adjustment needed for {ticker}. LU price: {lu_price}, stop loss remains at: {stop_loss}")
            
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
        command_replace = f"REPLACE {stop_order_id} {shares} STOPMKT {new_stop_price}"
        client_socket = self.send_command(command_replace)
        if client_socket:
            self.logger.info(f"Sent replace order for stopOrderID {ticker}: new stop price {new_stop_price}")
        else:
            self.logger.error(f"Failed to send replace order for stopOrderID {ticker}")


    def process_replace_response(self, response, trade_details):
        parts = response.strip().split()
    
        try:
            order_id = parts[1]
            status = parts[2]
            action = 'Buy' 
            time = parts[8]
            new_stop_price = float(parts[6])
            ticker = trade_details['ticker']
            shares = trade_details['shares']
            trade_id = trade_details['tradeID']
            
            # Log the parsed information for debugging
            self.logger.debug(f"Parsed replace order response - order_id: {order_id}, status: {status}, time: {time}, new_stop_price: {new_stop_price}, ticker: {ticker}, shares: {shares}, trade_id: {trade_id}")


            # Store the replace order with all relevant details, ensuring all parts are captured
            self.store_replace_order(
                trade_id=trade_id,
                time=time,
                ticker=ticker,
                shares=shares,
                price=new_stop_price,
                order_id=order_id,
                action=action,
                status=status,
                act_status=status
            )

            if status == 'ReplaceRej':
                self.logger.warning(f"Replace order rejected for trade {trade_details['tradeID']}.")
    
        except IndexError as e:
            self.logger.error(f'Error parsing %OrderAct response: {e}')

        
    
    

    def store_replace_order(self, trade_id, time, ticker, shares, price, order_id, action, status, act_status):
        self.logger.debug(f"Storing replace order - trade_id: {trade_id}, time: {time}, ticker: {ticker}, shares: {shares}, price: {price}, order_id: {order_id}, action: {action}, status: {status}, act_status: {act_status}")
        conn = self.get_db_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO ReplaceStop (tradeID, time, ticker, shares, price, orderID, action, status, act_status, date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                           (trade_id, time, ticker, shares, price, order_id, action, status, act_status, datetime.now().strftime('%Y-%m-%d'))
                           )
            conn.commit()
            self.logger.info(f'Successfully inserted replace order {order_id} with status {status} into ReplaceStop')
        except sqlite3.Error as e:
            self.logger.error(f"Error storing replace order: {e}")
        finally:
            conn.close()
            
    def listen_to_server(self):
        threading.Thread(target=self._listen_to_server, daemon=True).start()
      

    def _listen_to_server(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")
        
            while True:
                response = client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    threading.Thread(target=self.handle_response, args=(response,)).start()
        except Exception as e:
            self.logger.error(f"Error listening to server: {e}")
        finally:
            client_socket.close()
            
    def handle_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%OrderAct'):
                parts = line.split()
                try:
                    stopOrderID = parts[1]
                    if stopOrderID in self.trade_details:
                        trade_details = self.trade_details[stopOrderID]
                        # Pass the correct parameters to process_replace_response
                        threading.Thread(target=self.process_replace_response, args=(response, trade_details)).start()
                except IndexError as e:
                    self.logger.error(f'Error parsing %OrderAct response: {e}')
        


            
    def send_command(self, command):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            client_socket.sendall(command.encode())
            self.logger.debug(f'Sent command: {command}')
            return client_socket
        except Exception as e:
            self.logger.error(f"Error sending command {command}: {e}")
            return None    

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