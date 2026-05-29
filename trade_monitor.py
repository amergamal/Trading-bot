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
        self.multi_addon_cutoff = dt_time(11, 30)
        self._multi_cutoff_logged = set()

        self.logger = logging.getLogger('TradeMonitor')
        self.multi_log = logging.getLogger('MultiStrategy')
        self.db_pool = db_pool
        self.socketio = socketio

        # Core monitoring
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

        # Multi Pyramid State (Persistent)
        self.multi_pivots = {}
        self.multi_original_risk = {}
        self.multi_current_stop_level = {}

        self.last_sync_time = time.time()
        self.db_lock = threading.Lock()

        # Stop-limit gap upgrade tracking
        self.upgraded_stops = set()   # stop_order_ids already upgraded to STOPMKT
        self.pending_upgrades = {}    # token -> upgrade metadata

        self.logger.info("TradeMonitor initialized with Persistent Multi Pyramid support")

        if socketio is None:
            self.logger.warning("No SocketIO instance provided")
        else:
            self.logger.info("SocketIO instance provided")

        self.load_active_multi_trades()
        self.logger.info("=== MULTI STATE AFTER LOAD ===")
        self.logger.info(f"Loaded tickers: {list(self.multi_pivots.keys())}")
        self.start_listening_thread()
        self.logger.debug('TradeMonitor instance created.')

    # ========================== PERSISTENCE ==========================

    def save_multi_state(self, ticker):
        if ticker not in self.multi_pivots:
            return
        state = self.multi_pivots[ticker]
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            # Stable trade_id for today
            trade_id = f"MULTI_{ticker}_{datetime.now().strftime('%Y%m%d')}"

            cursor.execute("""
                INSERT INTO multi_pyramid 
                (trade_id, ticker, initial_stop, original_risk, current_stop_level, 
                 pivot_high, add_count, broken, last_trigger_ts, status, date, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE', CURRENT_DATE, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    current_stop_level = EXCLUDED.current_stop_level,
                    pivot_high = EXCLUDED.pivot_high,
                    add_count = EXCLUDED.add_count,
                    broken = EXCLUDED.broken,
                    last_trigger_ts = EXCLUDED.last_trigger_ts,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                trade_id,
                ticker,
                self.multi_current_stop_level.get(ticker),     # initial_stop
                self.multi_original_risk.get(ticker),
                self.multi_current_stop_level.get(ticker),
                state.get('pivot_high', 0),
                state.get('add_count', 0),
                state.get('broken', False),
                state.get('last_trigger_ts')
            ))
            conn.commit()
            self.logger.info(f" SAVED Multi state - {ticker} | Adds: {state.get('add_count')} | Stop: ${self.multi_current_stop_level.get(ticker):.2f} | trade_id={trade_id}")
        except Exception as e:
            self.logger.error(f"Failed to save Multi state for {ticker}: {e}", exc_info=True)
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): self.db_pool.putconn(conn)


    def load_active_multi_trades(self):
        """Improved loading with better logging"""
        self.multi_pivots.clear()
        self.multi_original_risk.clear()
        self.multi_current_stop_level.clear()

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ticker, initial_stop, original_risk, current_stop_level, 
                       pivot_high, add_count, broken, last_trigger_ts
                FROM multi_pyramid 
                WHERE status = 'ACTIVE' 
                  AND date = CURRENT_DATE
            """)
            rows = cursor.fetchall()
            
            self.logger.info(f"Found {len(rows)} active Multi records in DB for today")

            for row in rows:
                ticker = row[0]
                self.multi_pivots[ticker] = {
                    'pivot_high': float(row[4]) if row[4] else 0,
                    'add_count': int(row[5]),
                    'broken': bool(row[6]),
                    'last_trigger_ts': row[7],
                    'broken_at': None,  # not persisted; safe to reset on restart
                }
                self.multi_original_risk[ticker] = float(row[2]) if row[2] else 0
                self.multi_current_stop_level[ticker] = float(row[3]) if row[3] else 0
                
                self.logger.info(f"Loaded Multi: {ticker} | Adds: {row[5]} | Stop: ${row[3]}")

        except Exception as e:
            self.logger.error(f"Failed to load Multi trades: {e}", exc_info=True)
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): self.db_pool.putconn(conn)

    # ========================== MULTI LOGIC ==========================

    def handle_first_multi_entry(self, trade_details):
        ticker = trade_details['ticker']
        risk = float(trade_details.get('risk', 0.0))
        stop_level = float(trade_details.get('stop_loss', 0))

        self.multi_original_risk[ticker] = risk
        self.multi_current_stop_level[ticker] = stop_level

        self.multi_pivots[ticker] = {
            'pivot_high': stop_level,
            'add_count': 0,
            'broken': False,
            'last_trigger_ts': None,
            'broken_at': None,      # wall-clock time when breakout was first detected
        }

        self.logger.info(f"MULTI INITIALIZED: {ticker} | Stop=${stop_level:.2f} | Risk=${risk:.2f}")
        self.multi_log.info(f"[INIT] {ticker} | Stop=${stop_level:.2f} | Risk=${risk:.2f} | Initial state loaded, watching for breakout")
        self.save_multi_state(ticker)
        
        

    def on_multi_breakout_confirmed(self, ticker, stop_exec_price):
        """Called by sl_monitor when the DAS stop order executes for a Multi trade.
        The stop executing is definitive proof price breached the stop — more reliable
        than waiting for a quote tick to show last_price > stop."""
        if ticker not in self.multi_pivots:
            self.logger.warning(f"on_multi_breakout_confirmed: {ticker} not in multi_pivots, ignoring")
            return
        data = self.multi_pivots[ticker]
        if data['broken']:
            return  # already flagged from quote feed, nothing to do
        if data['add_count'] >= 3:
            self.logger.info(f"MULTI BREAKOUT confirmed for {ticker} but add_count={data['add_count']} — no more add-ons")
            self.multi_log.info(f"[BREAKOUT-STOP] {ticker} | Stop executed at ${stop_exec_price:.2f} | add_count={data['add_count']} — max add-ons reached, no new add-on")
            return
        data['broken'] = True
        data['broken_at'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        self.logger.info(f"MULTI BREAKOUT confirmed via stop execution: {ticker} stopped at ${stop_exec_price:.2f}")
        self.multi_log.info(f"[BREAKOUT-STOP] {ticker} | Stop executed at ${stop_exec_price:.2f} | broken=True, waiting for red candle")
        self.save_multi_state(ticker)

    def check_multi_pyramiding(self, ticker, last_price):
        if ticker not in self.multi_pivots:
            return

        now_est = datetime.now(pytz.timezone('US/Eastern'))
        if now_est.time() > self.multi_addon_cutoff:
            if ticker not in self._multi_cutoff_logged:
                self.logger.info(f"MULTI ADD-ON CUTOFF for {ticker}")
                self.multi_log.info(f"[CUTOFF] {ticker} | Time {now_est.strftime('%H:%M:%S')} ET past 10:30 — no more add-ons allowed")
                self._multi_cutoff_logged.add(ticker)
            return

        data = self.multi_pivots[ticker]
        if data['add_count'] >= 3:
            return

        current_stop = self.multi_current_stop_level.get(ticker, 0)

        # === 1. Detect breakout above current stop ===
        if last_price > current_stop and not data['broken']:
            data['broken'] = True
            data['broken_at'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            self.logger.info(f"MULTI BREAKOUT DETECTED {ticker}: {last_price:.2f} > Stop {current_stop:.2f}")
            self.multi_log.info(f"[BREAKOUT-QUOTE] {ticker} | Quote ${last_price:.2f} > Stop ${current_stop:.2f} | broken=True, waiting for red candle")
            self.save_multi_state(ticker)
            return

        # === 2. Only proceed if we have broken the stop ===
        if not data['broken']:
            return

        # === 3. Get HOD now — this becomes the new stop for the add-on ===
        new_stop = self._get_highest_high_so_far(ticker)
        if not new_stop or new_stop <= current_stop:
            new_stop = last_price  # fallback: use current price as new stop

        # === 4. Wait for first red candle AFTER breakout ===
        red_candle = self._get_latest_red_5min_candle(ticker, after_ts=data.get('broken_at'))
        if not red_candle:
            self.multi_log.debug(f"[WAIT-RED] {ticker} | HOD=${new_stop:.2f} | No red 5min candle yet after {data.get('broken_at')} — waiting")
            return

        # === 5. STRICT PROTECTION: Only one add-on per breakout cycle ===
        last_trigger = data.get('last_trigger_ts')
        if last_trigger == red_candle['timestamp']:
            return  # Already triggered on this exact red candle

        # Place the add-on using the HOD as the new stop so distance is positive
        self.multi_log.info(
            f"[ADD-ON-TRIGGER] {ticker} | Add-on #{data['add_count']+1} | "
            f"Red candle close=${red_candle['close']:.2f} @ {red_candle['timestamp']} | "
            f"New stop (HOD)=${new_stop:.2f} | Sending to OrderExecution"
        )
        self._place_multi_add_on(ticker, red_candle['close'], new_stop)

        # Persist new stop
        self.multi_current_stop_level[ticker] = new_stop
        self.logger.info(f"MULTI New Stop Updated {ticker} - ${new_stop:.2f}")

        # Reset for next cycle
        data['last_trigger_ts'] = red_candle['timestamp']
        data['broken'] = False
        data['broken_at'] = None
        data['add_count'] += 1

        self.save_multi_state(ticker)
        self.logger.info(f"MULTI ADD-ON #{data['add_count']} PLACED for {ticker} | New Stop: ${new_stop:.2f}")
        self.multi_log.info(f"[ADD-ON-COMPLETE] {ticker} | Add-on #{data['add_count']} registered | New Stop=${new_stop:.2f} | broken reset, watching for next breakout")


    def _place_multi_add_on(self, ticker, limit_price, new_stop=None):
        if ticker not in self.multi_original_risk:
            return

        entry_price = float(limit_price)
        # Use the HOD passed in as new_stop (above the red-candle close).
        # Falling back to current_stop_level would give a negative distance
        # because the candle close is above the old stop that was just broken.
        stop_loss = float(new_stop) if new_stop else self.multi_current_stop_level.get(ticker, 0)
        original_risk = self.multi_original_risk[ticker]
        target_risk = original_risk * 2

        distance = stop_loss - entry_price
        if distance <= 0:
            self.logger.warning(f"Invalid distance for {ticker} add-on: entry={entry_price:.2f} stop={stop_loss:.2f} distance={distance:.4f}")
            self.multi_log.warning(f"[ADD-ON-SKIP] {ticker} | Invalid distance: entry=${entry_price:.2f} stop=${stop_loss:.2f} distance={distance:.4f} — add-on aborted")
            return

        shares = max(1, int(target_risk / distance))
        new_risk = round(target_risk, 2)

        try:
            new_trade_id = self.generate_auto_id()
        except Exception as e:
            self.logger.error(f"Failed to generate trade ID: {e}")
            return

        self.logger.info(f" MULTI ADD-ON #{self.multi_pivots[ticker]['add_count']+1} | "
                        f"{ticker} | Entry≈{entry_price:.2f} | Stop={stop_loss:.2f} | Shares={shares} | Risk=${new_risk}")
        self.multi_log.info(
            f"[PLACE-ADD-ON] {ticker} | Add-on #{self.multi_pivots[ticker]['add_count']+1} | "
            f"Entry≈${entry_price:.2f} | Stop=${stop_loss:.2f} | Shares={shares} | Risk=${new_risk:.2f} | TradeID={new_trade_id}"
        )

        try:
            self.order_execution.execute_multi_add_on(
                new_trade_id, ticker, shares, entry_price, stop_loss, new_risk, 'Multi'
            )
        except Exception as e:
            self.logger.error(f"Failed to place Multi add-on: {e}")
            
    def get_multi_status(self):
        """Return current Multi state for frontend"""
        status = {}
        for ticker in self.multi_pivots:
            state = self.multi_pivots[ticker]
            status[ticker] = {
                'add_count': state['add_count'],
                'current_stop': self.multi_current_stop_level.get(ticker, 0),
                'original_risk': self.multi_original_risk.get(ticker, 0),
                'broken': state['broken'],
                'last_trigger': state.get('last_trigger_ts')
            }
        return status        
            
    def cancel_multi_pyramid(self, ticker):
        """Fully cancel and reset Multi pyramid for a ticker (user requested)"""
        ticker = ticker.upper()
        if ticker not in self.multi_pivots:
            self.logger.info(f"No active Multi pyramid for {ticker} to cancel")
            return False

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            # Delete from persistent table
            cursor.execute("""
                DELETE FROM multi_pyramid 
                WHERE ticker = %s AND status = 'ACTIVE' AND date = CURRENT_DATE
            """, (ticker,))
            
            conn.commit()

            # Clear in-memory state
            if ticker in self.multi_pivots:
                del self.multi_pivots[ticker]
            if ticker in self.multi_original_risk:
                del self.multi_original_risk[ticker]
            if ticker in self.multi_current_stop_level:
                del self.multi_current_stop_level[ticker]

            self.logger.info(f" Multi Pyramid CANCELLED for {ticker} by user")
            
            if self.socketio:
                self.socketio.emit('multi_status_update', self.get_multi_status())
                
            return True

        except Exception as e:
            self.logger.error(f"Error canceling Multi pyramid for {ticker}: {e}")
            return False
        finally:
            if conn:
                self.db_pool.putconn(conn)        

    # ========================== ORIGINAL METHODS ==========================

    def generate_auto_id(self):
        conn = None
        cursor = None
        try:
            with self.db_lock:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
                row = cursor.fetchone()
                last_trade_id = row[0] if row else 0
                new_trade_id = last_trade_id + 1
                cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (new_trade_id,))
                conn.commit()
                return str(new_trade_id)
        except psycopg2.Error as e:
            self.logger.error(f"Error generating trade ID: {e}")
            raise
        finally:
            if cursor: cursor.close()
            if conn: self.db_pool.putconn(conn)

    def start_listening_thread(self):
        listening_thread = threading.Thread(target=self.listen_to_server, daemon=True)
        listening_thread.start()

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
            target = float(trade_details.get('target_price')) if trade_details.get('target_price') else None

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
                INSERT INTO activetrades (tradeid, time, strategy, ticker, shares, entry_price, stop_loss, 
                    target, risk, sellorderid, stoporderid, date, last_price, unrealized)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                active_trades['tradeID'], active_trades['time'], active_trades['strategy'],
                active_trades['ticker'], active_trades['shares'], active_trades['entry_price'],
                active_trades['stop_loss'], active_trades['target'], active_trades['risk'],
                active_trades['sellOrderID'], active_trades['stopOrderID'], active_trades['date'],
                active_trades['last_price'], active_trades['unrealized']
            ))
            conn.commit()

            self.trade_details[trade_details.get('stopOrderID')] = trade_details
            if trade_details.get('stopOrderID'):
                self.stopOrderIDs.add(trade_details['stopOrderID'])

            if self.socketio:
                self.socketio.emit('active_trade_update', active_trades)

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
                FROM activetrades WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            rows = cursor.fetchall()
            trades = []
            for r in rows:
                trades.append({
                    'tradeID': r[0], 'time': r[1], 'strategy': r[2], 'ticker': r[3],
                    'shares': r[4], 'entry_price': float(r[5]) if r[5] else None,
                    'stop_loss': float(r[6]) if r[6] else None,
                    'target': float(r[7]) if r[7] else None,
                    'risk': float(r[8]) if r[8] else 0.0,
                    'sellOrderID': r[9], 'stopOrderID': r[10], 'date': r[11],
                    'unrealized': float(r[12]) if r[12] else 0.0,
                    'lu_price': float(r[13]) if r[13] else None,
                    'last_price': float(r[14]) if r[14] else None
                })
            return trades
        except Exception as e:
            self.logger.error(f"DB error: {e}")
            return []
        finally:
            if conn: self.db_pool.putconn(conn)

    def update_trade_status(self, ticker, strategy):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            update_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE tradestatus SET active_trade = 'open' 
                WHERE ticker = %s AND strategy = %s AND date = %s
            """, (ticker, strategy, update_date))
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error updating tradestatus: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

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
        except psycopg2.Error as e:
            self.logger.error(f"Error updating borrowedshares: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

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
                return None, None
        except psycopg2.Error as e:
            self.logger.error(f"Error getting lu and last prices: {e}")
            return None, None
        finally:
            if conn: self.db_pool.putconn(conn)

    def update_active_trades_with_lu_price_and_last_price(self, ticker, lu_price, last_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE activetrades SET lu_price = %s, last_price = %s 
                WHERE ticker = %s AND date = %s
            """, (lu_price, last_price, ticker, datetime.now().strftime('%Y-%m-%d')))
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error updating activetrades with lu and last prices: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

    def receive_latest_price(self, ticker, last_price):
        if last_price is None or last_price == 0:
            return
        last_price_float = float(last_price)
        self.latest_prices[ticker] = last_price_float
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
            
            for trade in trades:
                trade_id, time, strategy, ticker, shares, entry_price, stop_loss, target, sellorderid, stoporderid, date, unrealized, lu_price, current_last_price = trade
                entry_price_float = float(entry_price) if entry_price is not None else 0.0
                shares_float = float(shares) if shares is not None else 0.0
                unrealized = round((entry_price_float - last_price_float) * shares_float, 2)
                
                cursor.execute("""
                    UPDATE activetrades 
                    SET last_price = %s, unrealized = %s 
                    WHERE tradeid = %s AND ticker = %s
                """, (last_price_float, unrealized, trade_id, ticker))
                conn.commit()

            self.check_target_price_and_close_trade(ticker)
            self.check_partial_close_at_minus_half_r(ticker, last_price)
            self.check_stop_limit_gap(ticker, last_price_float)
                    
            if ticker in self.multi_pivots:
                self.check_multi_pyramiding(ticker, last_price_float)            
        except psycopg2.Error as e:
            self.logger.error(f"Error processing last price update for {ticker}: {e}")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): self.db_pool.putconn(conn)

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
            if conn: self.db_pool.putconn(conn)

    def check_partial_close_at_minus_half_r(self, ticker, last_price):
        """Trigger partial close at -0.5R. Restored robustness from original."""
        if not ticker or ticker not in self.latest_prices:
            return

        today = datetime.now().strftime('%Y-%m-%d')

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
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
                target_loss = risk_total / 2.0
                unrealized_total = (entry_price - last_price) * shares

                if -unrealized_total >= target_loss:
                    if shares < 2:
                        continue

                    self.logger.info(f"PARTIAL CLOSE TRIGGERED at -0.5R for {ticker} trade {trade_id}")

                    payload = {'tradeID': trade_id, 'ticker': ticker, 'percent': 50, 'last_price': last_price}

                    # Retry logic (restored)
                    success = False
                    for attempt in range(3):
                        try:
                            response = requests.post(
                                f"{FLASK_SERVER_URL}/partial_close",
                                json=payload,
                                timeout=10
                            )
                            if response.status_code == 200:
                                self.logger.info(f"Partial close SUCCESS for trade {trade_id}")
                                success = True
                                break
                            else:
                                self.logger.warning(f"Partial close attempt {attempt+1} failed: {response.text}")
                        except Exception as e:
                            self.logger.error(f"Partial close attempt {attempt+1} error: {e}")

                        if not success and attempt < 2:
                            time.sleep(2)

                    if not success:
                        self.logger.error(f"Failed to partial close trade {trade_id} after 3 attempts")

        except Exception as e:
            self.logger.error(f"Error in check_partial_close_at_minus_half_r for {ticker}: {e}", exc_info=True)
        finally:
            if 'conn' in locals():
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
        if trade_details.get('strategy') == 'Multi':
            if trade_details['ticker'] not in self.multi_pivots:
                self.handle_first_multi_entry(trade_details)
        self.insert_active_trade(trade_details)
        self.update_trade_status(trade_details['ticker'], trade_details['strategy'])
        self.update_borrowed_shares(trade_details['ticker'], trade_details['shares'])
        lu_price, last_price = self.get_lu_price_and_last_price(trade_details['ticker'])
        if lu_price is not None:
            self.update_active_trades_with_lu_price_and_last_price(trade_details['ticker'], lu_price, last_price)

    def receive_latest_lu_price(self, ticker, new_lu_price):
        if not new_lu_price or new_lu_price == 0:
            return
        if not self.has_active_trades_for_ticker(ticker):
            return
        self.get_stop_price(ticker, new_lu_price)

    def get_stop_price(self, ticker, lu_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, ticker, shares, stop_loss, lu_price, stoporderid 
                FROM activetrades WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            trades = cursor.fetchall()
            for trade in trades:
                trade_id, trade_ticker, shares, stop_loss, trade_lu_price, stop_order_id = trade
                if trade_ticker == ticker:
                    threading.Thread(target=self.check_lu_price, 
                                   args=(trade_id, trade_ticker, shares, stop_loss, lu_price, stop_order_id)).start()
        except Exception as e:
            self.logger.error(f"Error in get_stop_price: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

    def check_lu_price(self, trade_id, ticker, shares, stop_loss, lu_price, stop_order_id):
        if not self.trade_exists(trade_id, ticker, shares):
            self.remove_stop_order_id(stop_order_id)
            return
        if lu_price is None or lu_price == 0:
            return
        stop_loss = round(float(stop_loss), 2)
        lu_price = round(float(lu_price), 2)
        original_stop_price = self.get_original_stop_price(stop_order_id)
        if original_stop_price is None:
            return
        potential_new_stop_price = round(lu_price - 0.02, 2)
        if potential_new_stop_price <= 0:
            return
        if stop_loss < original_stop_price and lu_price >= original_stop_price + 0.02:
            self.send_replace_order(stop_order_id, ticker, shares, original_stop_price, trade_id, source='lu')
        elif stop_loss < potential_new_stop_price and lu_price - stop_loss > 0.02:
            new_stop_loss = min(potential_new_stop_price, original_stop_price)
            if new_stop_loss > stop_loss:
                self.send_replace_order(stop_order_id, ticker, shares, new_stop_loss, trade_id, source='lu')
        elif lu_price - stop_loss < 0.02:
            if potential_new_stop_price < stop_loss:
                self.send_replace_order(stop_order_id, ticker, shares, potential_new_stop_price, trade_id, source='lu')

    def get_original_stop_price(self, stop_order_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT price FROM stopmarket WHERE orderid = %s", (stop_order_id,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            self.logger.error(f"Error retrieving original stop price: {e}")
            return None
        finally:
            if conn: self.db_pool.putconn(conn)

    def trade_exists(self, trade_id, ticker, shares):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM activetrades
                WHERE tradeid = %s AND ticker = %s AND shares = %s AND date = %s
            """, (trade_id, ticker, shares, datetime.now().strftime('%Y-%m-%d')))
            return cursor.fetchone() is not None
        except Exception:
            return False
        finally:
            if conn: self.db_pool.putconn(conn)

    def remove_stop_order_id(self, stop_order_id):
        if stop_order_id in self.stopOrderIDs:
            self.stopOrderIDs.remove(stop_order_id)

    def send_replace_order(self, stop_order_id, ticker, shares, new_stop_price, trade_id, source='lu'):
        self.update_stop_loss_in_active_trades(stop_order_id, new_stop_price)
        strategy = self.get_strategy_from_trade_id(trade_id) or 'limit'
        shares = str(int(float(shares)))
        new_stop_price = round(float(new_stop_price), 2)

        self.pending_replaces = getattr(self, 'pending_replaces', {})
        self.pending_replaces[stop_order_id] = {
            'trade_id': trade_id, 'ticker': ticker, 'shares': shares,
            'price': new_stop_price, 'source': source
        }

        if strategy == 'market':
            command = f"REPLACE {stop_order_id} {shares} STOPMKT {new_stop_price:.2f}"
        else:
            stop_trigger = round(new_stop_price - 0.02, 2)
            command = f"REPLACE {stop_order_id} {shares} STOPLMT {stop_trigger:.2f} {new_stop_price:.2f}"
        self.send_command(command)

    def get_strategy_from_trade_id(self, trade_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT strategy FROM activetrades 
                WHERE tradeid = %s AND date = %s
            """, (trade_id, datetime.now().strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception:
            return None
        finally:
            if conn: self.db_pool.putconn(conn)

    def update_stop_loss_in_active_trades(self, stop_order_id, new_stop_price):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE activetrades SET stop_loss = %s WHERE stoporderid = %s
            """, (new_stop_price, stop_order_id))
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error updating stop loss: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

    def sync_state_with_active_trades(self):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, ticker, shares, stoporderid 
                FROM activetrades WHERE date = %s
            """, (datetime.now().strftime('%Y-%m-%d'),))
            active_trades = {(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()}
            for stop_order_id in list(self.stopOrderIDs):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    self.remove_stop_order_id(stop_order_id)
            for stop_order_id in list(self.trade_details.keys()):
                if not any(stop_order_id == trade[3] for trade in active_trades):
                    del self.trade_details[stop_order_id]
            active_tickers = {trade[1] for trade in active_trades}
            for ticker in list(self.latest_prices.keys()):
                if ticker not in active_tickers:
                    del self.latest_prices[ticker]
        except Exception as e:
            self.logger.error(f"Error syncing state: {e}")
        finally:
            if conn: self.db_pool.putconn(conn)

    def listen_to_server(self):
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
                        break
                self.client_socket.close()
            except (socket.error, ConnectionRefusedError) as e:
                self.logger.error(f"Connection failed: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    def process_replace_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%ORDER'):
                parts = line.split()
                try:
                    order_id = parts[1]
                    token    = parts[2]
                    status   = parts[11] if len(parts) > 11 else ''
                    time_ex  = parts[12] if len(parts) > 12 else ''
                    if status == 'Accepted' and token in self.pending_upgrades:
                        upgrade = self.pending_upgrades.pop(token)
                        threading.Thread(
                            target=self._finalize_stop_upgrade,
                            args=(order_id, upgrade, time_ex),
                            daemon=True
                        ).start()
                except IndexError:
                    continue
            elif line.startswith('%OrderAct'):
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
                        trade_id, strategy = self.get_trade_id_from_stop_order_id(order_id)
                        if trade_id:
                            self.store_replace_order(trade_id, strategy, time_executed, ticker, shares, price, order_id, action, status, status)
                except IndexError:
                    continue

    def store_replace_order(self, trade_id, strategy, time, ticker, shares, price, order_id, action, status, act_status):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            # 1. Determine source
            source = 'unknown'
            if hasattr(self, 'pending_replaces') and order_id in self.pending_replaces:
                source = self.pending_replaces[order_id].get('source', 'unknown')

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
                    'notes': '',
                    'date': datetime.now().strftime('%Y-%m-%d')
                }

                cursor.execute("SELECT COUNT(*) FROM replacestop WHERE orderid = %s", (order_id,))
                exists = cursor.fetchone()[0]

                if exists:
                    cursor.execute("""
                        UPDATE replacestop
                        SET time = %s, ticker = %s, shares = %s, price = %s, 
                            status = %s, act_status = %s, date = %s
                        WHERE orderid = %s
                    """, (time, ticker, shares, price, status, act_status, 
                          datetime.now().strftime('%Y-%m-%d'), order_id))
                    self.logger.info(f"Updated replace order {order_id}")
                else:
                    cursor.execute("""
                        INSERT INTO replacestop 
                        (tradeid, strategy, time, ticker, shares, price, orderid, action, 
                         status, act_status, date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (trade_id, strategy, time, ticker, shares, price, order_id, 
                          action, status, act_status, datetime.now().strftime('%Y-%m-%d')))
                    self.logger.info(f"Inserted replace order {order_id}")

                if self.socketio:
                    self.socketio.emit('replace_stop_update', replace_order_data)

            elif source == 'user':
                cursor.execute("""
                    UPDATE stopmarket 
                    SET price = %s, time = %s, status = %s, act_status = %s
                    WHERE orderid = %s
                """, (price, time, status, act_status, order_id))
                
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

            # Clean up pending
            if hasattr(self, 'pending_replaces'):
                self.pending_replaces.pop(order_id, None)

        except psycopg2.Error as e:
            self.logger.error(f"Error storing replace order: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def get_trade_id_from_stop_order_id(self, stop_order_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT tradeid, strategy FROM tradedetails WHERE stoporderid = %s", (stop_order_id,))
            result = cursor.fetchone()
            return result if result else (None, None)
        finally:
            if conn: self.db_pool.putconn(conn)

    def check_stop_limit_gap(self, ticker, current_price):
        """Upgrade a stop-limit to stop-market when price has gapped above the fill window."""
        LIMIT_BUFFER = 0.03   # matches send_stop_limit_order
        UPGRADE_TRIGGER = LIMIT_BUFFER + 0.03  # 6 cents above stop — safely past the limit

        today = datetime.now().strftime('%Y-%m-%d')
        conn = cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, shares, stop_loss, stoporderid, strategy
                FROM activetrades
                WHERE ticker = %s AND date = %s AND shares > 0
            """, (ticker, today))
            trades = cursor.fetchall()
        except Exception as e:
            self.logger.error(f"check_stop_limit_gap DB error: {e}")
            return
        finally:
            if cursor: cursor.close()
            if conn: self.db_pool.putconn(conn)

        for trade_id, shares, stop_price, stop_order_id, strategy in trades:
            if not stop_order_id or stop_order_id in self.upgraded_stops:
                continue
            stop_price = float(stop_price)
            if current_price <= stop_price + UPGRADE_TRIGGER:
                continue

            self.logger.warning(
                f"Stop-limit gap: {ticker} price={current_price:.2f} > stop={stop_price:.2f}+{UPGRADE_TRIGGER:.2f} "
                f"— upgrading {stop_order_id} to STOPMKT"
            )
            self.upgraded_stops.add(stop_order_id)

            self.send_command(f'CANCEL {stop_order_id}')
            time.sleep(0.5)

            new_token = generate_token()
            shares_int = int(float(shares))
            self.send_command(
                f'NEWORDER {new_token} B {ticker} SMAT {shares_int} STOPMKT {stop_price:.2f} {trade_id}'
            )
            self.pending_upgrades[new_token] = {
                'trade_id': trade_id, 'ticker': ticker, 'shares': shares_int,
                'stop_price': stop_price, 'old_order_id': stop_order_id,
                'strategy': strategy or 'limit'
            }
            self.logger.info(f"STOPMKT sent: {ticker} token={new_token} stop={stop_price:.2f}")

            if self.socketio:
                self.socketio.emit('stop_upgraded', {
                    'ticker': ticker, 'tradeID': trade_id,
                    'stop_price': stop_price, 'old_order_id': stop_order_id,
                    'message': f'{ticker} stop-limit upgraded to market @ ${stop_price:.2f}'
                })

    def _finalize_stop_upgrade(self, new_order_id, upgrade, time_executed):
        """Update DB and monitors after a stop-limit → stop-market upgrade is confirmed."""
        trade_id    = upgrade['trade_id']
        ticker      = upgrade['ticker']
        old_order_id = upgrade['old_order_id']
        stop_price  = upgrade['stop_price']
        conn = cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE activetrades SET stoporderid = %s WHERE tradeid = %s AND ticker = %s",
                (new_order_id, trade_id, ticker)
            )
            cursor.execute(
                "UPDATE stopmarket SET orderid = %s, act_status = 'Upgraded-STOPMKT', time = %s WHERE orderid = %s",
                (new_order_id, time_executed, old_order_id)
            )
            conn.commit()
            self.stopOrderIDs.discard(old_order_id)
            self.stopOrderIDs.add(new_order_id)
            self.logger.info(f"Stop upgrade finalized: {ticker} trade={trade_id} new_order={new_order_id}")
            if self.socketio:
                self.socketio.emit('active_trade_update', {
                    'tradeID': trade_id, 'ticker': ticker,
                    'stopOrderID': new_order_id, 'stop_loss': stop_price
                })
        except Exception as e:
            self.logger.error(f"_finalize_stop_upgrade error: {e}")
            if conn: conn.rollback()
        finally:
            if cursor: cursor.close()
            if conn: self.db_pool.putconn(conn)

    def send_command(self, command):
        with token_map_lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                except socket.error as e:
                    self.logger.error(f"Socket error: {e}")

    def _get_highest_high_so_far(self, ticker):
        """Return the live intraday high from tradeparameters (updated every quote tick)."""
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT high FROM tradeparameters WHERE ticker = %s AND date = %s",
                (ticker, datetime.now().strftime('%Y-%m-%d'))
            )
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else None
        except Exception as e:
            self.logger.error(f"Error getting HOD for {ticker}: {e}")
            return None
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def _get_latest_red_5min_candle(self, ticker, after_ts=None):
        """Return the first red 5-min candle after after_ts (close < open, filtered in SQL).
        Uses ASC order so a later green candle doesn't hide the red one we already want to act on."""
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y/%m/%d')
            if after_ts:
                cursor.execute("""
                    SELECT timestamp, open, close FROM ohlc_5min
                    WHERE ticker = %s AND timestamp LIKE %s AND timestamp > %s
                      AND close < open
                    ORDER BY timestamp ASC LIMIT 1
                """, (ticker, f"{today}%", after_ts))
            else:
                cursor.execute("""
                    SELECT timestamp, open, close FROM ohlc_5min
                    WHERE ticker = %s AND timestamp LIKE %s
                      AND close < open
                    ORDER BY timestamp ASC LIMIT 1
                """, (ticker, f"{today}%"))
            row = cursor.fetchone()
            if row:
                return {'timestamp': row[0], 'close': float(row[2])}
            return None
        except Exception as e:
            self.logger.debug(f"_get_latest_red_5min_candle error for {ticker}: {e}")
            return None
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def shutdown(self):
        self.keep_listening = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass
        self.logger.info("TradeMonitor shutdown.")