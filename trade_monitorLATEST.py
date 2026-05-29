import psycopg2
from psycopg2 import pool
import config
import socket
import random
import logging
import time
import pytz
import threading
from datetime import datetime, time as dt_time
import requests
import os


LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015
FLASK_SERVER_URL = 'http://localhost:5001'

token_map_lock = threading.Lock()

def generate_token():
    return str(random.randint(100000, 999999))

class TradeMonitor:
    def __init__(self, db_pool, socketio=None, order_execution=None):
        self.order_execution = order_execution
        self.multi_addon_cutoff = dt_time(10, 30)
        self._multi_cutoff_logged = set()
        self.logger = logging.getLogger('TradeMonitor')
        self.logger.debug("TradeMonitor initialized")
        self.logger.debug(f"TradeMonitor logger handlers: {self.logger.handlers}")
        self.db_pool = db_pool
        self.socketio = socketio
        self.logger.debug(f"SocketIO instance: {socketio}, type: {type(socketio)}")
        self.logger.debug(f"SocketIO id: {id(socketio)}")
        self.token_map = {}
        self.running = False
        self.threads = []
        self.client_socket = None
        self.keep_listening = True
        self.stopOrderIDs = set()
        self.trade_details = {}
        self.processed_trades = set()
        self.latest_prices = {}
        self.active_trades = {}
        self.multi_pivots = {}                   # ticker -> {pivot_high, add_count, broken, last_trigger_ts}
        self.multi_original_risk = {}            # ticker -> original dollar risk from first manual order
        self.multi_current_stop_level = {}       # ticker -> current stop level (new high after break)
        self.last_sync_time = time.time()
        self.db_lock = threading.Lock()          # ← For generate_auto_id
        self.logger.info("TradeMonitor initialized with db_lock for trade ID generation")
        if socketio is None:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        else:
            self.logger.info("SocketIO instance provided; real-time updates enabled.")
        self.start_listening_thread()
        self.logger.debug('TradeMonitor instance created.')
        
    def generate_auto_id(self):
        """Copy of StrategyLogic.generate_auto_id - generates next trade ID safely"""
        conn = None
        cursor = None
        try:
            with self.db_lock:          # We'll add this lock in __init__
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                
                # Get current last_trade_id
                cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
                row = cursor.fetchone()
                last_trade_id = row[0] if row else 0
                
                # Increment
                new_trade_id = last_trade_id + 1
                
                # Update counter
                cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (new_trade_id,))
                conn.commit()
                
                self.logger.info(f"TradeMonitor generated trade ID: {new_trade_id}")
                return str(new_trade_id)
                
        except psycopg2.Error as e:
            self.logger.error(f"Error generating trade ID in TradeMonitor: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)    

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

    def insert_active_trade(self, trade_details):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT last FROM tradeparameters 
                WHERE ticker = %s AND date = %s
            """, (trade_details['ticker'], datetime.now().strftime('%Y-%m-%d')))
            last_price = cursor.fetchone()
            if last_price:
                last_price = float(last_price[0])
                entry_price = float(trade_details['entry_price'])
                shares = float(trade_details['shares'])
                unrealized = (entry_price - last_price) * shares
            else:
                unrealized = 0.0
            target = float(trade_details['target_price']) if trade_details.get('target_price') else None
            active_trades = {
                'tradeID': trade_details['tradeID'],
                'time': trade_details['time'],
                'strategy': trade_details['strategy'],
                'ticker': trade_details['ticker'],
                'shares': trade_details['shares'],
                'entry_price': trade_details['entry_price'],
                'stop_loss': trade_details['stop_loss'],
                'target': target,
                'risk': trade_details['risk'],
                'sellOrderID': trade_details['sellOrderID'],
                'stopOrderID': trade_details['stopOrderID'],
                'date': datetime.now().strftime('%Y-%m-%d'),
                'last_price': last_price,
                'unrealized': unrealized
                
            }
            cursor.execute("""
                INSERT INTO activetrades (
                    tradeid, time, strategy, ticker, shares, entry_price, stop_loss, 
                    target, risk, sellorderid, stoporderid, date, last_price, unrealized
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                active_trades['tradeID'],
                active_trades['time'],
                active_trades['strategy'],
                active_trades['ticker'],
                active_trades['shares'],
                active_trades['entry_price'],
                active_trades['stop_loss'],
                active_trades['target'],
                active_trades['risk'],
                active_trades['sellOrderID'],
                active_trades['stopOrderID'],
                active_trades['date'],
                active_trades['last_price'],
                active_trades['unrealized']
            ))
            conn.commit()
            self.trade_details[trade_details['stopOrderID']] = trade_details
            self.stopOrderIDs.add(trade_details['stopOrderID'])
            self.active_trades[trade_details['tradeID']] = {
                'ticker': trade_details['ticker'],
                'entry_price': float(trade_details['entry_price']),
                'stop_loss': float(trade_details['stop_loss']),
                'target': target,
                'shares': float(trade_details['shares']),
                'strategy': trade_details['strategy']
            }
            self.logger.info(f"Inserted trade {trade_details['tradeID']} with sellorderid {trade_details['sellOrderID']}, "
                           f"stoporderid {trade_details['stopOrderID']}, "
                           f"target {target}, unrealized {unrealized} into activetrades")
            if self.socketio:
                
                all_trades = self.get_all_active_trades_as_list()
                self.socketio.emit('active_trade_update', all_trades)
                self.logger.info(f"EMIT  active_trade_update: {len(all_trades)} trades")  
                
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting active trade: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)
                
    def get_all_active_trades_as_list(self):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, time, strategy, ticker, shares, entry_price, stop_loss,
                       target, risk, sellorderid, stoporderid, date, unrealized, lu_price, last_price
                FROM activetrades
                WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            rows = cursor.fetchall()
            trades = []
            for r in rows:
                trades.append({
                    'tradeID': r[0],
                    'time': r[1],
                    'strategy': r[2],
                    'ticker': r[3],
                    'shares': r[4],
                    'entry_price': float(r[5]) if r[5] else None,
                    'stop_loss': float(r[6]) if r[6] else None,
                    'target': float(r[7]) if r[7] else None,
                    'risk': float(r[8]) if r[8] else 0.0,
                    'sellOrderID': r[9],
                    'stopOrderID': r[10],
                    'date': r[11],
                    'unrealized': float(r[12]) if r[12] else 0.0,
                    'lu_price': float(r[13]) if r[13] else None,
                    'last_price': float(r[14]) if r[14] else None
                })
            return trades
        except Exception as e:
            self.logger.error(f"DB error: {e}")
            return []
        finally:
            if conn:
                self.db_pool.putconn(conn)            

    def update_trade_status(self, ticker, strategy):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            update_date = datetime.now().strftime('%Y-%m-%d')
            
            cursor.execute("""
                UPDATE tradestatus SET active_trade = 'open' 
                WHERE ticker = %s AND strategy = %s AND date = %s
            """, (ticker, strategy, update_date))
            if cursor.rowcount == 0:
                self.logger.warning(f"No rows updated for ticker: {ticker}, strategy: {strategy}, date: {update_date}. Possible mismatch.")
            else:
                self.logger.info(f"Updated tradestatus for {ticker} and strategy {strategy} to 'open'")
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error updating tradestatus: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def update_borrowed_shares(self, ticker, shares):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE borrowedshares 
                SET available_shares = available_shares - %s, last_updated = %s 
                WHERE ticker = %s AND DATE(last_updated) = %s
            """, (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date))
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Updated borrowedshares for ticker {ticker}, reduced available_shares by {shares}")
            else:
                self.logger.warning(f"No matching record found for ticker {ticker} on date {today_date}")
        except psycopg2.Error as e:
            self.logger.error(f"Error updating borrowedshares: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def get_lu_price_and_last_price(self, ticker):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT lu, last FROM tradeparameters 
                WHERE ticker = %s AND date = %s
            """, (ticker, datetime.now().strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            if result:
                return result
            else:
                self.logger.warning(f"No lu or last price found for {ticker} on {datetime.now().strftime('%Y-%m-%d')}")
                return None, None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting lu and last prices: {e}")
            return None, None
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def update_active_trades_with_lu_price_and_last_price(self, ticker, lu_price, last_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE activetrades SET lu_price = %s, last_price = %s 
                WHERE ticker = %s AND date = %s
            """, (lu_price, last_price, ticker, datetime.now().strftime('%Y-%m-%d')))
            conn.commit()
            self.logger.info(f"Updated lu and last prices for {ticker} in activetrades to {lu_price}, {last_price}")
            #if self.socketio:
                #all_trades = self.get_all_active_trades_as_list()
                #self.socketio.emit('active_trade_update', all_trades)
                #self.logger.info(f"EMIT  active_trade_update: {len(all_trades)} trades")
        except psycopg2.Error as e:
            self.logger.error(f"Error updating activetrades with lu and last prices: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def receive_latest_price(self, ticker, last_price):
        
        if last_price is None or last_price == 0:
            self.logger.warning(f"Invalid last price for {ticker}: {last_price}. Skipping update.")
            return
        self.latest_prices[ticker] = float(last_price)  # Set latest_prices
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            
            cursor.execute("""
                SELECT tradeid, time, strategy, ticker, shares, entry_price, stop_loss, 
                       target, sellorderid, stoporderid, date, unrealized, lu_price, last_price
                FROM activetrades 
                WHERE ticker = %s AND date = %s
            """, (ticker, today_date))
            trades = cursor.fetchall()
            
            if not trades:
                
                return
            for trade in trades:
                trade_id, time, strategy, ticker, shares, entry_price, stop_loss, target, sellorderid, stoporderid, date, unrealized, lu_price, current_last_price = trade
                
                if not trade_id or trade_id.strip() == '':
                    self.logger.error(f"Invalid or empty trade_id for ticker {ticker}: {trade_id}. Full trade data: {trade}")
                    continue
                last_price_float = float(last_price)
                entry_price_float = float(entry_price) if entry_price is not None else 0.0
                shares_float = float(shares) if shares is not None else 0.0
                unrealized = round((entry_price_float - last_price_float) * shares_float, 2)
                
                cursor.execute("""
                    UPDATE activetrades 
                    SET last_price = %s, unrealized = %s 
                    WHERE tradeid = %s AND ticker = %s
                """, (last_price_float, unrealized, trade_id, ticker))
                conn.commit()
                
                active_trade_data = {
                    'tradeID': trade_id,
                    'time': time,
                    'strategy': strategy,
                    'ticker': ticker,
                    'shares': shares,
                    'entry_price': entry_price_float,
                    'stop_loss': float(stop_loss) if stop_loss is not None else None,
                    'target': float(target) if target is not None else None,
                    'sellOrderID': sellorderid,
                    'stopOrderID': stoporderid,
                    'date': date,
                    'unrealized': unrealized,
                    'lu_price': float(lu_price) if lu_price is not None else None,
                    'last_price': last_price_float
                }
                #if self.socketio:
                    
                    #all_trades = self.get_all_active_trades_as_list()
                    #self.socketio.emit('active_trade_update', all_trades)
                    #self.logger.info(f"EMIT  active_trade_update: {len(all_trades)} trades")
                
                    
            # Check if the trade should be closed due to target price
            self.check_target_price_and_close_trade(ticker) 
            
            # Check for automated partial close at -0.5R loss
            self.check_partial_close_at_minus_half_r(ticker, last_price)   # ← This call
                    
            # === MULTI PYRAMIDING CHECK ===
            # This must run on every price update for tickers that have active Multi trades
            if ticker in self.multi_pivots:
                last_price_float = float(last_price)
                self.check_multi_pyramiding(ticker, last_price_float)            
                    
        except psycopg2.Error as e:
            self.logger.error(f"Error processing last price update for {ticker}: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def has_active_trades_for_ticker(self, ticker):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM activetrades
                WHERE ticker = %s AND date = %s
            """, (ticker, datetime.now().strftime('%Y-%m-%d')))
            count = cursor.fetchone()[0]
            return count > 0
        except psycopg2.Error as e:
            self.logger.error(f"Error checking active trades for {ticker}: {e}")
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)
                
    def check_partial_close_at_minus_half_r(self, ticker, last_price):
        """Trigger partial close (half position) when total unrealized loss reaches half the original risk (-0.5R).
        Applies to ALL active trades (no strategy restriction)."""
        if not ticker or ticker not in self.latest_prices:
            return

        
        today = datetime.now().strftime('%Y-%m-%d')

        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            # Fetch ALL active trades eligible for partial close (no strategy filter)
            cursor.execute("""
                SELECT tradeid, shares, entry_price, risk
                FROM activetrades
                WHERE ticker = %s 
                  AND date = %s 
                  AND partial_closed = FALSE
                  AND risk > 0
                  AND shares > 0
            """, (ticker, today))

            trades = cursor.fetchall()
            if not trades:
                return

            for trade_id, shares, entry_price, risk_total in trades:
                shares = int(shares)
                entry_price = float(entry_price)
                risk_total = float(risk_total)

                # Target: trigger when total unrealized loss >= half original risk
                target_loss = risk_total / 2.0

                # Current total unrealized P/L (for short: positive when in profit)
                unrealized_total = (entry_price - last_price) * shares

                # Check if we're in loss by at least half the original risk
                if -unrealized_total >= target_loss:
                    

                    if shares < 2:
                        self.logger.warning(f"Skipping partial close {trade_id}: only {shares} share(s)")
                        continue

                    self.logger.info(
                        f"AUTOMATED PARTIAL CLOSE AT -0.5R TOTAL LOSS: "
                        f"{ticker} | trade_id={trade_id} | "
                        f"unrealized=${unrealized_total:+.2f} (loss >= ${target_loss:.2f}) | "
                        f"requesting 50% close via /partial_close"
                    )

                    # Just call the existing route — it handles shares calculation safely
                    payload = {
                        'tradeID': trade_id,
                        'ticker': ticker,
                        'percent': 50
                    }

                    success = False
                    for attempt in range(3):
                        try:
                            response = requests.post(
                                f"{FLASK_SERVER_URL}/partial_close",
                                json=payload,
                                timeout=10
                            )
                            if response.status_code == 200:
                                self.logger.info(f"Partial close SUCCESS for {trade_id}")
                                success = True
                                break
                            else:
                                self.logger.error(f"Attempt {attempt+1}: {response.status_code} {response.text}")
                        except requests.RequestException as e:
                            self.logger.error(f"Attempt {attempt+1} failed: {e}")

                        if attempt < 2:
                            time.sleep(5)

                    if not success:
                        self.logger.error(f"Failed to trigger partial close for {trade_id} after 3 attempts")

        except Exception as e:
            self.logger.error(f"Error in check_partial_close_at_minus_half_r for {ticker}: {e}", exc_info=True)
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)            

    def check_target_price_and_close_trade(self, ticker):
        """Check if last price reaches target price for target strategy trades and close them."""
        if ticker not in self.latest_prices:
            self.logger.debug(f"No last price available for {ticker}. Skipping target price check.")
            return
        last_price = self.latest_prices[ticker]
        self.logger.debug(f"Checking target price for {ticker}, last_price: {last_price}")
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, ticker, shares, target, strategy, stoporderid
                FROM activetrades
                WHERE ticker = %s AND strategy IN (%s, %s) AND date = %s AND target IS NOT NULL
            """, (ticker, 'market', 'target', datetime.now().strftime('%Y-%m-%d')))
            trades = cursor.fetchall()
            if not trades:
                self.logger.debug(f"No active target strategy trades for {ticker}.")
                if ticker in self.latest_prices:
                    del self.latest_prices[ticker]
                    self.logger.debug(f"Removed last_price for {ticker} from latest_prices.")
                return
            trades_to_remove = []
            for trade in trades:
                trade_id, trade_ticker, shares, target, strategy, stop_order_id = trade
                target = float(target)
                if trade_id in self.processed_trades:
                    self.logger.debug(f"Skipping trade {trade_id} for {ticker}: already processed")
                    continue
                self.logger.debug(f"Comparing last_price={last_price} with target={target} for trade {trade_id}")
                if last_price <= target:
                    self.logger.info(f"Trade {trade_id} for {ticker} reached target price {target}. Last price: {last_price}. Closing trade.")
                    self.processed_trades.add(trade_id)
                    # Retry HTTP POST up to 3 times
                    for attempt in range(3):
                        try:
                            response = requests.post(
                                f"{FLASK_SERVER_URL}/close_trade",
                                json={'ticker': ticker, 'tradeID': trade_id, 'reason': 'Target'},
                                timeout=5
                            )
                            if response.status_code == 200:
                                self.logger.info(f"Successfully closed trade {trade_id} for {ticker} at target price.")
                                trades_to_remove.append(trade_id)
                                if stop_order_id:
                                    self.remove_stop_order_id(stop_order_id)
                                    if stop_order_id in self.trade_details:
                                        del self.trade_details[stop_order_id]
                                        self.logger.debug(f"Removed stopOrderID {stop_order_id} and trade_details for trade {trade_id}.")
                                break
                            else:
                                self.logger.error(f"Attempt {attempt + 1} failed to close trade {trade_id} for {ticker}: {response.text}")
                                if attempt == 2:
                                    self.logger.error(f"Failed to close trade {trade_id} for {ticker} after 3 attempts")
                                time.sleep(5)  # Wait 1 second before retrying
                        except requests.RequestException as e:
                            self.logger.error(f"Attempt {attempt + 1} failed to close trade {trade_id} for {ticker}: {e}")
                            if attempt == 2:
                                self.logger.error(f"Failed to close trade {trade_id} for {ticker} after 3 attempts")
                            time.sleep(5)  # Wait 1 second before retrying
                else:
                    self.logger.debug(f"Trade {trade_id} for {ticker} not at target price. Last price: {last_price}, Target: {target}")
        except psycopg2.Error as e:
            self.logger.error(f"Error checking target price: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def receive_order_details(self, trade_details):
        self.logger.debug(f"TradeMonitor received trade details: {trade_details}")
        required_fields = ['tradeID', 'ticker', 'shares', 'strategy', 'entry_price', 'stop_loss', 'sellOrderID', 'stopOrderID', 'risk']
        missing_fields = [field for field in required_fields if field not in trade_details]
        if missing_fields:
            self.logger.error(f"Missing required fields in trade_details: {missing_fields}")
            return
        valid_strategies = ['Multi', '1Min', '5Min', 'market', 'limit', 'target', '1Min-below_sma', '5Min-below_sma', '1Min-2g2r', '5Min-2g2r', 'spara', 'stop', '1Min-below_pmh', '5Min-below_pmh', '1Min-vwap_crossover', '5Min-vwap_crossover', '1Min-vwap_dev']
        if trade_details['strategy'] not in valid_strategies:
            self.logger.error(f"Invalid strategy: {trade_details['strategy']}. Must be one of {valid_strategies}")
            return
        self.insert_active_trade(trade_details)
        if trade_details.get('strategy') == 'Multi':
            if trade_details['ticker'] not in self.multi_pivots:
                self.handle_first_multi_entry(trade_details)
        self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
        self.update_borrowed_shares(trade_details['ticker'], trade_details['shares'])
        lu_price, last_price = self.get_lu_price_and_last_price(trade_details['ticker'])
        if lu_price is not None:
            self.update_active_trades_with_lu_price_and_last_price(trade_details['ticker'], lu_price, last_price)
        self.logger.info(f"Received and processed {trade_details['strategy']} order details: {trade_details}")

    def receive_latest_lu_price(self, ticker, new_lu_price):
        
        if not new_lu_price or new_lu_price == 0:
            self.logger.warning(f"LU price for {ticker} is zero. Skipping any stop loss adjustment.")
            return
        # Check if any active trades exist for the ticker
        if not self.has_active_trades_for_ticker(ticker):
            
            return
        self.get_stop_price(ticker, new_lu_price)

    def get_stop_price(self, ticker, lu_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, ticker, shares, stop_loss, lu_price, stoporderid 
                FROM activetrades
                WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            trades = cursor.fetchall()
            if not trades:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring lu price update for now")
                return
            matching_trade_found = False
            for trade in trades:
                trade_id, trade_ticker, shares, stop_loss, trade_lu_price, stop_order_id = trade
                if trade_ticker == ticker:
                    matching_trade_found = True
                    threading.Thread(target=self.check_lu_price, args=(trade_id, trade_ticker, shares, stop_loss, lu_price, stop_order_id)).start()
            if not matching_trade_found:
                self.logger.info(f"No active trades found for ticker {ticker}. Ignoring lu price update for now")
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching trades from activetrades: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

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
                
                return
            if stop_loss < original_stop_price and lu_price >= original_stop_price + 0.02:
                self.logger.info(f"Restoring stop loss for {ticker} to the original price. LU price: {lu_price}, old stop loss: {stop_loss}, restored stop loss: {original_stop_price}")
                self.send_replace_order(stop_order_id, ticker, shares, original_stop_price, trade_id, source='lu')
            elif stop_loss < potential_new_stop_price and lu_price - stop_loss > 0.02:
                new_stop_loss = min(potential_new_stop_price, original_stop_price)
                if new_stop_loss > stop_loss:
                    self.logger.info(f"Adjusting stop loss upward for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {new_stop_loss}")
                    self.send_replace_order(stop_order_id, ticker, shares, new_stop_loss, trade_id, source='lu')
            elif lu_price - stop_loss < 0.02:
                if potential_new_stop_price < stop_loss:
                    self.logger.info(f"Adjusting stop loss downward for {ticker}. LU price: {lu_price}, old stop loss: {stop_loss}, new stop loss: {potential_new_stop_price}")
                    self.send_replace_order(stop_order_id, ticker, shares, potential_new_stop_price, trade_id, source='lu')
            #else:
                #self.logger.info(f"No adjustment needed for {ticker}. LU price: {lu_price}, stop loss remains at: {stop_loss}")
        else:
            self.logger.warning(f"LU price is None for {ticker}, no adjustment possible.")

    def get_original_stop_price(self, stop_order_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT price FROM stopmarket
                WHERE orderid = %s
            """, (stop_order_id,))
            row = cursor.fetchone()
            if row:
                return row[0]
            else:
                self.logger.warning(f"No original stop price found for stoporderid {stop_order_id}")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"Error retrieving original stop price: {e}")
            return None
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def trade_exists(self, trade_id, ticker, shares):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM activetrades
                WHERE tradeid = %s AND ticker = %s AND shares = %s AND date = %s
            """, (trade_id, ticker, shares, datetime.now().strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            return result is not None
        except psycopg2.Error as e:
            self.logger.error(f"Error checking trade existence in activetrades: {e}")
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def remove_stop_order_id(self, stop_order_id):
        if stop_order_id in self.stopOrderIDs:
            self.stopOrderIDs.remove(stop_order_id)
            self.logger.info(f"Removed stopOrderID {stop_order_id} from monitoring list.")

    def send_replace_order(self, stop_order_id, ticker, shares, new_stop_price, trade_id, source='lu'):
        self.update_stop_loss_in_active_trades(stop_order_id, new_stop_price)
        
        strategy = self.get_strategy_from_trade_id(trade_id)
        if not strategy:
            strategy = 'limit'  # your fallback

        shares = str(int(float(shares)))
        new_stop_price = round(float(new_stop_price), 2)

        # Store pending info
        self.pending_replaces = getattr(self, 'pending_replaces', {})
        self.pending_replaces[stop_order_id] = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'price': new_stop_price,
            'source': source
        }

        if strategy == 'market':
            command = f"REPLACE {stop_order_id} {shares} STOPMKT {new_stop_price:.2f}"
            self.logger.info(f"[{ticker}] REPLACE STOPMKT {new_stop_price:.2f}")
        else:
            stop_trigger = round(new_stop_price - 0.02, 2)
            command = f"REPLACE {stop_order_id} {shares} STOPLMT {stop_trigger:.2f} {new_stop_price:.2f}"
            self.logger.info(f"[{ticker}] REPLACE STOPLMT -> trigger {stop_trigger:.2f} | limit {new_stop_price:.2f}")

        # ← THIS WAS THE BUG: you sent command_replace instead of command
        self.send_command(command) 
                
    def get_strategy_from_trade_id(self, trade_id):
        """Fetch strategy from activetrades using trade_id"""
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT strategy FROM activetrades 
                WHERE tradeid = %s AND date = %s
            """, (trade_id, datetime.now().strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                self.logger.warning(f"Strategy not found for trade_id {trade_id}")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching strategy for trade_id {trade_id}: {e}")
            return None
        finally:
            if conn:
                self.db_pool.putconn(conn)             

    def update_stop_loss_in_active_trades(self, stop_order_id, new_stop_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE activetrades 
                SET stop_loss = %s 
                WHERE stoporderid = %s
            """, (new_stop_price, stop_order_id))
            conn.commit()
            self.logger.info(f"Updated stop loss in activetrades for stoporderid {stop_order_id} to {new_stop_price}")
            cursor.execute("""
                SELECT tradeid, time, strategy, ticker, shares, entry_price, stop_loss, sellorderid, stoporderid, date, unrealized, lu_price, last_price
                FROM activetrades WHERE stoporderid = %s
            """, (stop_order_id,))
            row = cursor.fetchone()
            if row:
                active_trades = {
                    'tradeID': row[0],
                    'time': row[1],
                    'strategy': row[2],
                    'ticker': row[3],
                    'shares': float(row[4]) if row[4] is not None else None,
                    'entry_price': float(row[5]) if row[5] is not None else None,
                    'stop_loss': float(row[6]) if row[6] is not None else None,
                    'sellOrderID': row[7],
                    'stopOrderID': row[8],
                    'date': row[9],
                    'unrealized': float(row[10]) if row[10] is not None else None,
                    'lu_price': float(row[11]) if row[11] is not None else None,
                    'last_price': float(row[12]) if row[12] is not None else None
                }
                if self.socketio:
                    all_trades = self.get_all_active_trades_as_list()
                    self.socketio.emit('active_trade_update', all_trades)
                    self.logger.info(f"EMIT  active_trade_update: {len(all_trades)} trades")
                
        except psycopg2.Error as e:
            self.logger.error(f"Error updating stop loss in activetrades: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def sync_state_with_active_trades(self):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, ticker, shares, stoporderid 
                FROM activetrades 
                WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            active_trades = {(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()}
            for stop_order_id in list(self.stopOrderIDs):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    self.remove_stop_order_id(stop_order_id)
                    self.logger.debug(f"Removed stale stoporderid {stop_order_id} during sync")
            for stop_order_id in list(self.trade_details.keys()):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    trade_id = self.trade_details[stop_order_id]['tradeID']
                    del self.trade_details[stop_order_id]
                    self.logger.debug(f"Removed stale trade_details for trade {trade_id} (stoporderid {stop_order_id}) during sync")
            active_tickers = {trade[1] for trade in active_trades}
            for ticker in list(self.latest_prices.keys()):
                if ticker not in active_tickers:
                    del self.latest_prices[ticker]
                    self.logger.debug(f"Removed stale last_price for {ticker} during sync")
        except psycopg2.Error as e:
            self.logger.error(f"Error syncing state with activetrades: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def listen_to_server(self):
        """Continuously listen for server responses and sync state periodically."""
        retry_delay = 1
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                self.keep_listening = True
                retry_delay = 1
                while self.keep_listening:
                    if time.time() - self.last_sync_time > 60:
                        self.sync_state_with_active_trades()
                        self.last_sync_time = time.time()

                    try:
                        response = self.client_socket.recv(4096).decode('utf-8', errors='ignore')
                        if response:
                            self.process_replace_response(response)
                        else:
                            time.sleep(0.1)
                    except (socket.error, ConnectionResetError):
                        self.logger.warning("Socket read error. Reconnecting...")
                        break  # Exit inner loop-> reconnect

                self.client_socket.close()
                self.logger.warning("Disconnected from server. Reconnecting in 1s...")
            except (socket.error, ConnectionRefusedError) as e:
                self.logger.error(f"Connection failed: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    def process_replace_response(self, response):
        order_data = {}
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%OrderAct'):
                
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
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            # 1. Get source FIRST
            source = 'unknown'
            if hasattr(self, 'pending_replaces') and order_id in self.pending_replaces:
                source = self.pending_replaces[order_id]['source']
            if source == 'lu':    
            
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
                    'notes': '',  # ADD THIS
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
                cursor.execute("""
                    SELECT COUNT(*) FROM replacestop WHERE orderid = %s
                """, (order_id,))
                exists = cursor.fetchone()[0]
                if exists:
                    cursor.execute("""
                        UPDATE replacestop
                        SET time = %s, ticker = %s, shares = %s, price = %s, status = %s, act_status = %s, date = %s
                        WHERE orderid = %s
                    """, (time, ticker, shares, price, status, act_status, datetime.now().strftime('%Y-%m-%d'), order_id))
                    self.logger.info(f"Updated replace order {order_id} with status {status} and act_status {act_status} in replacestop")
                else:
                    cursor.execute("""
                        INSERT INTO replacestop 
                        (tradeid, strategy, time, ticker, shares, price, orderid, action, status, act_status, date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (trade_id, strategy, time, ticker, shares, price, order_id, action, status, act_status, datetime.now().strftime('%Y-%m-%d')))
                    self.logger.info(f"Inserted replace order {order_id} with status {status} and act_status {act_status} into replacestop")
                    if self.socketio:
                        self.socketio.emit('replace_stop_update', replace_order_data)
                        self.logger.info(f"Emitted replace_stop_update: {replace_order_data}")
                        
            elif source == 'user':     
                cursor.execute("""
                    UPDATE stopmarket 
                    SET price = %s, time = %s, status = %s, act_status = %s
                    WHERE orderid = %s
                """, (price, time, status, act_status, order_id))  
                
                # Emit stop_market_update
                stop_data = {
                    'tradeID': trade_id,
                    'strategy': strategy,
                    'time': time,
                    'ticker': ticker,
                    'shares': shares,
                    'price': price,
                    'action': action,
                    'status': status,
                    'act_status': act_status,
                    'notes': '',
                    'orderID': order_id
                }
                if self.socketio:
                    self.socketio.emit('stop_market_update', stop_data)     
            conn.commit()
            # 4. CLEAN UP pending
            if hasattr(self, 'pending_replaces'):
                self.pending_replaces.pop(order_id, None)
        except psycopg2.Error as e:
            self.logger.error(f"Error storing or updating replace order: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def get_trade_id_from_stop_order_id(self, stop_order_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, strategy FROM tradedetails 
                WHERE stoporderid = %s
            """, (stop_order_id,))
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            else:
                self.logger.warning(f"No tradeid found for stoporderid {stop_order_id}")
                return None, None
        except psycopg2.Error as e:
            self.logger.error(f"Database error while retrieving tradeid: {e}")
            return None, None
        finally:
            if conn:
                self.db_pool.putconn(conn)

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
                
    def handle_first_multi_entry(self, trade_details):
        ticker = trade_details['ticker']
        risk = float(trade_details.get('risk', 0.0))
        
        self.multi_original_risk[ticker] = risk
        
        stop_level = self._get_highest_high_so_far(ticker)
        if stop_level is None:
            stop_level = float(trade_details.get('entry_price', 0)) + 0.5
        
        self.multi_current_stop_level[ticker] = float(stop_level)
        
        self.multi_pivots[ticker] = {
            'pivot_high': float(stop_level),
            'add_count': 0,
            'broken': False,
            'last_trigger_ts': None
        }
        
        self.logger.info(f"MULTI INIT: {ticker} | tradeID={trade_details.get('tradeID')} | "
                        f"initial_stop={stop_level:.2f} | original_risk=${risk:.2f}")

    def _get_highest_high_so_far(self, ticker):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_str = datetime.now().strftime('%Y/%m/%d')
            cursor.execute("""
                SELECT MAX(high) FROM ohlc_5min
                WHERE ticker = %s AND timestamp LIKE %s
            """, (ticker, f"{today_str}%"))
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] is not None else None
        except Exception as e:
            self.logger.error(f"Error getting highest high for {ticker}: {e}")
            return None
        finally:
            if 'conn' in locals() and conn:
                self.db_pool.putconn(conn)

    def _get_latest_red_5min_candle(self, ticker):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y/%m/%d')
            cursor.execute("""
                SELECT timestamp, open, close
                FROM ohlc_5min
                WHERE ticker = %s AND timestamp LIKE %s
                ORDER BY timestamp DESC LIMIT 1
            """, (ticker, f"{today}%"))
            row = cursor.fetchone()
            if row and float(row[2]) < float(row[1]):
                return {'timestamp': row[0], 'close': float(row[2])}
            return None
        except Exception as e:
            self.logger.error(f"Error checking red candle for {ticker}: {e}")
            return None
        finally:
            if 'conn' in locals() and conn:
                self.db_pool.putconn(conn)

    def check_multi_pyramiding(self, ticker, last_price):
        if ticker not in self.multi_pivots:
            return
        
        
        # === TIME CUTOFF: No new Multi add-ons after 10:30 AM EST ===
        now_est = datetime.now(pytz.timezone('US/Eastern'))
        if now_est.time() > self.multi_addon_cutoff:
            
            
            if ticker not in self._multi_cutoff_logged:
                self.logger.info(f"MULTI ADD-ON CUTOFF: No more add-ons allowed for {ticker} after 10:30 AM EST")
                self._multi_cutoff_logged.add(ticker)
            return
        
        data = self.multi_pivots[ticker]
        if data['add_count'] >= 3:
            return

        current_stop = self.multi_current_stop_level.get(ticker, 0)

        if last_price > current_stop and not data['broken']:
            data['broken'] = True
            self.logger.info(f"MULTI BREAKOUT {ticker}: {last_price:.2f} > {current_stop:.2f}")
            return

        if not data['broken']:
            return

        red_candle = self._get_latest_red_5min_candle(ticker)
        if not red_candle:
            return

        close_price = red_candle['close']
        candle_ts = red_candle['timestamp']

        if data.get('last_trigger_ts') != candle_ts:
            self._place_multi_add_on(ticker, close_price)
            
            # Update stop level to new high after this break
            new_stop = self._get_highest_high_so_far(ticker)
            if new_stop and new_stop > current_stop:
                self.multi_current_stop_level[ticker] = new_stop
                self.logger.info(f"MULTI STOP UPDATED {ticker}: {new_stop:.2f}")
            
            data['last_trigger_ts'] = candle_ts
            data['broken'] = False
            data['add_count'] += 1

    def _place_multi_add_on(self, ticker, limit_price):
        if ticker not in self.multi_original_risk or ticker not in self.multi_current_stop_level:
            return

        entry_price = float(limit_price)
        stop_loss = self.multi_current_stop_level[ticker]
        original_risk = self.multi_original_risk[ticker]
        target_risk = original_risk * 2

        distance = stop_loss - entry_price
        if distance <= 0:
            self.logger.error(f"Invalid distance for {ticker}: stop={stop_loss}, entry={entry_price}")
            return

        shares = max(1, int(target_risk / distance))
        new_risk = round(target_risk, 2)

        try:
            new_trade_id = self.generate_auto_id()
        except Exception as e:
            self.logger.error(f"Failed to generate trade ID for Multi add-on: {e}")
            return

        self.logger.info(f"MULTI ADD #{self.multi_pivots[ticker]['add_count']+1} | {ticker} | "
                        f"entry={entry_price:.2f} | stop={stop_loss:.2f} | shares={shares} | risk=${new_risk}")

        try:
            if not self.order_execution:
                self.logger.error("order_execution not available in TradeMonitor")
                return
            self.order_execution.execute_multi_add_on(
                new_trade_id, ticker, shares, entry_price, stop_loss, new_risk, 'Multi'
            )
        except Exception as e:
            self.logger.error(f"Failed to place Multi add-on for {ticker}: {e}")            
                
    def shutdown(self):
        self.keep_listening = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
        self.logger.info("TradeMonitor shutdown.")            