import sqlite3
import socket
import random
import logging
import time
import threading
from datetime import datetime
import requests

LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015
DB_PATH = 'EOD_data.db'
FLASK_SERVER_URL = 'http://localhost:5000'

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
        self.threads = []
        self.client_socket = None
        self.keep_listening = True
        self.stopOrderIDs = set()
        self.trade_details = {}
        self.latest_prices = {}  # Store latest last_price per ticker
        self.active_trades = {}  # Cache trade data for RR checks
        self.socketio = socketio
        self.last_sync_time = time.time()  # Track last sync for periodic cleanup
        if socketio is None:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        else:
            self.logger.info("SocketIO instance provided; real-time updates enabled.")
        self.start_listening_thread()
        self.logger.debug('TradeMonitor instance created.')

    def start_listening_thread(self):
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
                last_price = float(last_price[0])
                entry_price = float(trade_details['entry_price'])
                shares = float(trade_details['shares'])
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
            self.trade_details[trade_details['stopOrderID']] = trade_details
            self.stopOrderIDs.add(trade_details['stopOrderID'])
            # Cache trade data for RR checks
            self.active_trades[trade_details['tradeID']] = {
                'ticker': trade_details['ticker'],
                'entry_price': float(trade_details['entry_price']),
                'stop_loss': float(trade_details['stop_loss']),
                'shares': float(trade_details['shares'])
            }
            self.logger.info(f"Inserted trade {trade_details['tradeID']} with sellOrderID {trade_details['sellOrderID']}, stopOrderID {trade_details['stopOrderID']} and unrealized {unrealized} into ActiveTrades")
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

    def receive_latest_price(self, ticker, last_price):
        """Receive and process last_price updates from VwapFetch."""
        self.logger.debug(f"Received last price for {ticker}: {last_price}")
        if last_price is None or last_price == 0:
            self.logger.warning(f"Invalid last price for {ticker}: {last_price}. Skipping RR check.")
            return
        # Check if any active trades exist for the ticker
        if not self.has_active_trades_for_ticker(ticker):
            self.logger.info(f"No active trades found for ticker {ticker}. Ignoring last price update.")
            if ticker in self.latest_prices:
                del self.latest_prices[ticker]
                self.logger.debug(f"Removed last_price for {ticker} from latest_prices.")
            return
        # Store latest price and check RR
        self.latest_prices[ticker] = float(last_price)
        self.check_rr_and_close_trade(ticker)

    def has_active_trades_for_ticker(self, ticker):
        """Check if there are any active trades for the given ticker."""
        conn = self.get_db_connection()
        if not conn:
            self.logger.error("Failed to connect to the database.")
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM ActiveTrades
                WHERE ticker = ? AND date = ?""",
                (ticker, datetime.now().strftime('%Y-%m-%d'))
            )
            count = cursor.fetchone()[0]
            return count > 0
        except sqlite3.Error as e:
            self.logger.error(f"Error checking active trades for {ticker}: {e}")
            return False
        finally:
            conn.close()

    def check_rr_and_close_trade(self, ticker):
        """Check if any active trades for the ticker have reached 3:1 RR and close them."""
        if ticker not in self.latest_prices:
            self.logger.debug(f"No last price available for {ticker}. Skipping RR check.")
            return
        last_price = self.latest_prices[ticker]
        trades_to_remove = []
        for trade_id, trade in self.active_trades.items():
            if trade['ticker'] != ticker:
                continue
            # Check if trade still exists in ActiveTrades
            if not self.trade_exists(trade_id, trade['ticker'], trade['shares']):
                self.logger.warning(f"Trade {trade_id} for {ticker} no longer exists in ActiveTrades. Removing from active_trades.")
                trades_to_remove.append(trade_id)
                # Clean up stopOrderIDs and trade_details
                for stop_order_id, details in list(self.trade_details.items()):
                    if details['tradeID'] == trade_id:
                        self.remove_stop_order_id(stop_order_id)
                        del self.trade_details[stop_order_id]
                        self.logger.debug(f"Removed stopOrderID {stop_order_id} and trade_details for trade {trade_id}.")
                continue
            entry_price = trade['entry_price']
            stop_loss = trade['stop_loss']
            shares = trade['shares']
            # Calculate 2:1 RR threshold
            risk_per_share = stop_loss - entry_price
            profit_target = 3 * risk_per_share
            take_profit_price = entry_price - profit_target
            # Check if last_price has reached or fallen below take_profit_price
            if last_price <= take_profit_price:
                self.logger.info(f"Trade {trade_id} for {ticker} reached 3:1 RR. Closing trade.")
                try:
                    response = requests.post(
                        f"{FLASK_SERVER_URL}/close_trade",
                        json={'ticker': ticker, 'tradeID': trade_id}
                    )
                    if response.status_code == 200:
                        self.logger.info(f"Successfully closed trade {trade_id} for {ticker} at 3:1 RR.")
                        trades_to_remove.append(trade_id)
                        # Clean up stopOrderIDs and trade_details
                        for stop_order_id, details in list(self.trade_details.items()):
                            if details['tradeID'] == trade_id:
                                self.remove_stop_order_id(stop_order_id)
                                del self.trade_details[stop_order_id]
                                self.logger.debug(f"Removed stopOrderID {stop_order_id} and trade_details for trade {trade_id}.")
                    else:
                        self.logger.error(f"Failed to close trade {trade_id} for {ticker}: {response.text}")
                except requests.RequestException as e:
                    self.logger.error(f"Error calling close_trade for {trade_id} ({ticker}): {e}")
            else:
                self.logger.debug(f"Trade {trade_id} for {ticker} not at 2:1 RR yet. Last price: {last_price}, Target: {take_profit_price}")
        # Remove closed or invalid trades from active_trades
        for trade_id in trades_to_remove:
            if trade_id in self.active_trades:
                del self.active_trades[trade_id]
                self.logger.debug(f"Removed trade {trade_id} from active_trades.")
        # Clean up latest_prices if no active trades remain for the ticker
        if not any(trade['ticker'] == ticker for trade in self.active_trades.values()):
            if ticker in self.latest_prices:
                del self.latest_prices[ticker]
                self.logger.debug(f"Removed last_price for {ticker} from latest_prices as no active trades remain.")

    def receive_order_details(self, trade_details):
        self.logger.debug(f"TradeMonitor received trade details: {trade_details}")
        strategy = trade_details.get('strategy')
        if strategy == 'marketl':
            self.insert_active_trade(trade_details)
            self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
            self.logger.info(f"Received and processed 'marketl' order details: {trade_details}")
        else:
            self.insert_active_trade(trade_details)
            self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
            self.update_borrowed_shares(trade_details['ticker'], trade_details['shares'])
            lu_price, last_price = self.get_lu_price_and_last_price(trade_details['ticker'])
            if lu_price is not None:
                self.update_active_trades_with_lu_price_and_last_price(trade_details['ticker'], lu_price, last_price)
            self.logger.info(f"Received and processed order details: {trade_details}")

    def receive_latest_lu_price(self, ticker, new_lu_price):
        self.logger.info(f"Received new LU price for {ticker}: {new_lu_price}")
        if not new_lu_price or new_lu_price == 0:
            self.logger.warning(f"LU price for {ticker} is zero. Skipping any stop loss adjustment.")
            return
        # Check if any active trades exist for the ticker
        if not self.has_active_trades_for_ticker(ticker):
            self.logger.info(f"No active trades found for ticker {ticker}. Ignoring LU price update.")
            return
        self.get_stop_price(ticker, new_lu_price)

    def get_stop_price(self, ticker, lu_price):
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
            if not trades:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring LU price update for now.")
                return
            matching_trade_found = False
            for trade in trades:
                trade_id, trade_ticker, shares, stop_loss, trade_lu_price, stop_order_id = trade
                if trade_ticker == ticker:
                    matching_trade_found = True
                    threading.Thread(target=self.check_lu_price, args=(trade_id, trade_ticker, shares, stop_loss, lu_price, stop_order_id)).start()
            if not matching_trade_found:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring LU price update for now.")
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching trades from ActiveTrades: {e}")
        finally:
            conn.close()

    def check_lu_price(self, trade_id, ticker, shares, stop_loss, lu_price, stop_order_id):
        if not self.trade_exists(trade_id, ticker, shares):
            self.logger.warning(f"Trade {trade_id} for {ticker} does not exist in ActiveTrades. Removing from monitoring.")
            self.remove_stop_order_id(stop_order_id)
            return
        if lu_price is None or lu_price == 0:
            self.logger.warning(f"LU price is missing or zero for {ticker}. Skipping stop loss adjustment.")
            return
        if lu_price is not None:
            stop_loss = round(float(stop_loss), 2)
            lu_price = round(float(lu_price), 2)
            original_stop_price = self.get_original_stop_price(stop_order_id)
            if original_stop_price is None:
                self.logger.error(f"Original stop price for stopOrderID {stop_order_id} could not be retrieved.")
                return
            potential_new_stop_price = round(lu_price - 0.02, 2)
            if potential_new_stop_price <= 0:
                self.logger.warning(f"Invalid potential new stop price for {ticker}: {potential_new_stop_price}. Skipping stop loss adjustment.")
                return
            if stop_loss < original_stop_price and lu_price >= original_stop_price + 0.02:
                self.logger.info(f"Restoring stop loss for {ticker} to the original price. LU price: {lu_price}, old stop loss: {stop_loss}, restored stop loss: {original_stop_price}")
                self.send_replace_order(stop_order_id, ticker, shares, original_stop_price, trade_id)
            elif stop_loss < potential_new_stop_price and lu_price - stop_loss > 0.02:
                new_stop_loss = min(potential_new_stop_price, original_stop_price)
                if new_stop_loss > stop_loss:
                    self.logger.info(f"Adjusting stop loss upward for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {new_stop_loss}")
                    self.send_replace_order(stop_order_id, ticker, shares, new_stop_loss, trade_id)
            elif lu_price - stop_loss < 0.02:
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
        if stop_order_id in self.stopOrderIDs:
            self.stopOrderIDs.remove(stop_order_id)
            self.logger.info(f"Removed stopOrderID {stop_order_id} from monitoring list.")

    def send_replace_order(self, stop_order_id, ticker, shares, new_stop_price, trade_id):
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

    def sync_state_with_active_trades(self):
        """Periodically sync stopOrderIDs, trade_details, and latest_prices with ActiveTrades."""
        conn = self.get_db_connection()
        if not conn:
            self.logger.error("Failed to connect to the database for state sync.")
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, ticker, shares, stopOrderID 
                FROM ActiveTrades 
                WHERE date = ?""",
                (datetime.now().strftime('%Y-%m-%d'),)
            )
            active_trades = {(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()}
            # Remove stale stopOrderIDs
            for stop_order_id in list(self.stopOrderIDs):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    self.remove_stop_order_id(stop_order_id)
                    self.logger.debug(f"Removed stale stopOrderID {stop_order_id} during sync.")
            # Remove stale trade_details
            for stop_order_id in list(self.trade_details.keys()):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    trade_id = self.trade_details[stop_order_id]['tradeID']
                    del self.trade_details[stop_order_id]
                    self.logger.debug(f"Removed stale trade_details for trade {trade_id} (stopOrderID {stop_order_id}) during sync.")
            # Remove stale latest_prices
            active_tickers = {trade[1] for trade in active_trades}
            for ticker in list(self.latest_prices.keys()):
                if ticker not in active_tickers:
                    del self.latest_prices[ticker]
                    self.logger.debug(f"Removed stale last_price for {ticker} during sync.")
        except sqlite3.Error as e:
            self.logger.error(f"Error syncing state with ActiveTrades: {e}")
        finally:
            conn.close()

    def listen_to_server(self):
        """Continuously listen for server responses and sync state periodically."""
        retry_delay = 1
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                self.keep_listening = True
                while self.keep_listening:
                    # Sync state every 60 seconds
                    if time.time() - self.last_sync_time > 60:
                        self.sync_state_with_active_trades()
                        self.last_sync_time = time.time()
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.process_replace_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
                break
            except (socket.error, ConnectionRefusedError) as e:
                self.logger.error(f"Connection failed: {e}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    def process_replace_response(self, response):
        order_data = {}
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%OrderAct'):
                self.logger.debug(f"Processing %OrderAct response line: {line}")
                parts = line.split()
                try:
                    order_id = parts[1]
                    status = parts[2]
                    action = parts[3]
                    ticker = parts[4]
                    price = parts[6]
                    time_executed = parts[8]
                    shares = parts[5]
                    if status == "Replaced":
                        self.logger.info(f"Order replaced successfully - Order ID: {order_id}, Ticker: {ticker}")
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
            cursor.execute("""
                SELECT COUNT(*) FROM ReplaceStop WHERE orderID = ?""",
                (order_id,)
            )
            exists = cursor.fetchone()[0]
            if exists:
                cursor.execute("""
                    UPDATE ReplaceStop
                    SET time = ?, ticker = ?, shares = ?, price = ?, status = ?, act_status = ?, date = ?
                    WHERE orderID = ?""",
                    (time, ticker, shares, price, status, act_status, datetime.now().strftime('%Y-%m-%d'), order_id)
                )
                self.logger.info(f"Updated replace order {order_id} with status {status} and act_status {act_status} in ReplaceStop")
            else:
                cursor.execute("""
                    INSERT INTO ReplaceStop (tradeID, strategy, time, ticker, shares, price, orderID, action, status, act_status, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (trade_id, strategy, time, ticker, shares, price, order_id, action, status, act_status, datetime.now().strftime('%Y-%m-%d'))
                )
                self.logger.info(f"Inserted replace order {order_id} with status {status} and act_status {act_status} into ReplaceStop")
                if self.socketio:
                    self.socketio.emit('replace_stop_update', replace_order_data)
                    self.logger.info(f"Emitted replace_stop_update: {replace_order_data}")
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error storing or updating replace order: {e}")
        finally:
            conn.close()

    def get_trade_id_from_stop_order_id(self, stop_order_id):
        conn = self.get_db_connection()
        if not conn:
            return None, None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, strategy FROM TradeDetails 
                WHERE stopOrderID = ?""",
                (stop_order_id,)
            )
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
        with token_map_lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

if __name__ == "__main__":
    trade_monitor = TradeMonitor()
    trade_monitor.start_monitoring()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        trade_monitor.stop_monitoring()
        print("Shutting down...")