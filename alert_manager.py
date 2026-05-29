# alert_manager.py
import logging
import psycopg2
from datetime import datetime
import time
from decimal import Decimal
import queue
import pyttsx3
import threading

class AlertManager:
    def __init__(self, db_pool, socketio=None):
        self.logger = logging.getLogger('AlertManager')
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            handler = logging.FileHandler('alertmanager.log')
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            console.setFormatter(formatter)
            self.logger.addHandler(console)

        # Voice engine
        self.engine = pyttsx3.init()
        self.engine.setProperty('rate', 150)
        self.engine.setProperty('volume', 1.0)
        voices = self.engine.getProperty('voices')
        if voices:
            self.engine.setProperty('voice', voices[0].id)
        self.logger.info("pyttsx3 voice engine initialized")    

        self.speech_queue = queue.Queue()
        self.speech_thread = threading.Thread(target=self._speech_worker, daemon=True)
        self.speech_thread.start()
        self.logger.info("Speech worker thread started")

        self.db_pool = db_pool
        self.socketio = socketio
        self.db_lock = threading.Lock()

        # In-memory real-time data
        self.last_price = {}      # ticker -> float

        self.logger.info("AlertManager initialized")
        
    def speak(self, text):
        if text:
            self.speech_queue.put(text.strip())

    def _speech_worker(self):
        while True:
            try:
                text = self.speech_queue.get()
                if text is None:
                    break
                self.logger.info(f"SPEAKING: {text}")
                self.engine.say(text)
                self.engine.runAndWait()
                self.speech_queue.task_done()
            except Exception as e:
                self.logger.error(f"Error in speech worker: {e}")    
        
    def start_alert_loop(self):
        """Start both 5-minute and 1-minute loops"""
        def run_5min_loop():
            self.logger.info("5-minute alert loop started")
            while True:
                now = datetime.now()
                minute = now.minute
                second = now.second
                if minute % 5 == 0 and second == 4:
                    self.process_5min_alerts()
                    time.sleep(1)
                time.sleep(0.5)

        def run_1min_loop():
            self.logger.info("1-minute high update loop started")
            while True:
                now = datetime.now()
                second = now.second
                if second == 4:  # 4 seconds after top of every minute
                    self.update_pmh_hod_from_ohlc_1min()
                    time.sleep(1)
                time.sleep(0.5)

        threading.Thread(target=run_5min_loop, daemon=True).start()
        threading.Thread(target=run_1min_loop, daemon=True).start()
        self.logger.info("Alert loops started")

    def update_pmh_hod_from_ohlc_1min(self):
        """Run every minute at :04 - update PMH pre-market, set HOD once post-open"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_slash = today.replace('-', '/')
        current_minute_str = datetime.now().strftime('%Y/%m/%d-%H:%M')

        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()

                cursor.execute("SELECT ticker FROM ticker_alert_states WHERE alert_active = TRUE")
                active_tickers = [row[0] for row in cursor.fetchall()]

                updated = False

                for ticker in active_tickers:
                    # Pre-market: keep updating PMH
                    if datetime.now().hour < 9 or (datetime.now().hour == 9 and datetime.now().minute < 30):
                        cursor.execute("""
                            SELECT MAX(high) FROM ohlc_1min 
                            WHERE ticker = %s 
                              AND "timestamp" LIKE %s 
                              AND "timestamp" < %s
                        """, (ticker, f"{today_slash}%", f"{today_slash}-09:30"))
                        result = cursor.fetchone()
                        new_pmh = float(result[0]) if result and result[0] else None

                        if new_pmh:
                            cursor.execute("""
                                UPDATE ticker_alert_states 
                                SET pmh = GREATEST(COALESCE(pmh, 0), %s), last_updated = CURRENT_TIMESTAMP
                                WHERE ticker = %s AND (pmh IS NULL OR pmh < %s)
                            """, (new_pmh, ticker, new_pmh))
                            if cursor.rowcount:
                                updated = True
                                self.logger.info(f"PMH updated for {ticker} -> {new_pmh:.2f}")

                    # Regular trading hours: initialize HOD once from completed 1-min candles
                    else:
                        cursor.execute("SELECT hod FROM ticker_alert_states WHERE ticker = %s", (ticker,))
                        current_hod_row = cursor.fetchone()
                        current_hod = current_hod_row[0] if current_hod_row else None

                        if current_hod is None:
                            cursor.execute("""
                                SELECT MAX(high) FROM ohlc_1min 
                                WHERE ticker = %s 
                                  AND "timestamp" LIKE %s 
                                  AND "timestamp" >= %s
                                  AND "timestamp" < %s  -- exclude current incomplete minute
                            """, (ticker, f"{today_slash}%", f"{today_slash}-09:30", current_minute_str))
                            result = cursor.fetchone()
                            new_hod = float(result[0]) if result and result[0] else None

                            if new_hod:
                                cursor.execute("""
                                    UPDATE ticker_alert_states 
                                    SET hod = %s, last_updated = CURRENT_TIMESTAMP
                                    WHERE ticker = %s
                                """, (new_hod, ticker))
                                updated = True
                                self.logger.info(f"HOD initialized for {ticker} -> {new_hod:.2f}")
                                
                        # === POST-OPEN PMH FALLBACK FOR LATE DATA OR LATE ACTIVATION ===
                        cursor.execute("SELECT pmh FROM ticker_alert_states WHERE ticker = %s", (ticker,))
                        pmh_row = cursor.fetchone()
                        current_pmh = pmh_row[0] if pmh_row else None

                        if current_pmh is None:
                            cursor.execute("""
                                SELECT MAX(high) FROM ohlc_1min 
                                WHERE ticker = %s 
                                  AND "timestamp" LIKE %s 
                                  AND "timestamp" < %s
                            """, (ticker, f"{today_slash}%", f"{today_slash}-09:30"))
                            pmh_result = cursor.fetchone()
                            fallback_pmh = float(pmh_result[0]) if pmh_result and pmh_result[0] else None

                            if fallback_pmh:
                                cursor.execute("""
                                    UPDATE ticker_alert_states 
                                    SET pmh = %s, last_updated = CURRENT_TIMESTAMP
                                    WHERE ticker = %s
                                """, (fallback_pmh, ticker))
                                updated = True
                                self.logger.info(f"PMH fallback set for {ticker} -> {fallback_pmh:.2f} (post-open late data)")           

                if updated:
                    conn.commit()

            except psycopg2.Error as e:
                self.logger.error(f"DB error in update_pmh_hod_from_ohlc_1min: {e}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

    def process_5min_alerts(self):
        """Main method: run once per 5-min candle close"""
        self.logger.info("=== Processing 5-minute alerts ===")

        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()

                # Get all active tickers
                cursor.execute("SELECT ticker FROM ticker_alert_states WHERE alert_active = TRUE")
                active_tickers = [row[0] for row in cursor.fetchall()]

                for ticker in active_tickers:
                    self.process_ticker(ticker, cursor, conn)

                conn.commit()
            except psycopg2.Error as e:
                self.logger.error(f"DB error in process_5min_alerts: {e}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)   
                    
    def process_ticker(self, ticker, cursor, conn):
        """Process one ticker's latest 5-min candle and update state"""
        today_slash = datetime.now().strftime('%Y/%m/%d')

        # Fetch latest candle from candleconditions_5min
        cursor.execute("""
            SELECT timestamp, open, close, high, vwap, sma_10, is_green
            FROM candleconditions_5min 
            WHERE ticker = %s 
            ORDER BY timestamp DESC LIMIT 2
        """, (ticker,))
        rows = cursor.fetchall()
        if not rows:
            return  # No data yet

        latest = rows[0]  # Most recent closed candle
        prev = rows[1] if len(rows) > 1 else None

        ts, open_p, close, high, vwap, sma10, is_green = latest

        # Get current state and data (including pmh and hod)
        cursor.execute("""
            SELECT current_state, streak_count, last_green_high, first_red_type,
                   last_sma_approach_alert, pmh, hod
            FROM ticker_alert_states WHERE ticker = %s
        """, (ticker,))
        state_row = cursor.fetchone()
        if not state_row:
            return

        current_state, streak_count, last_green_high, first_red_type, sma_approach_spoken, pmh, hod = state_row

        last_price = self.last_price.get(ticker)

        if not all([last_price, vwap, sma10]):
            return  # Missing data

                # === HOD / PMH independent alerts (only if hod is set) ===
        if hod is not None:
            self.check_hod_pmh_alerts(ticker, last_price, hod, pmh, cursor, conn)

        # === VWAP DEVIATION ALERT: 15% or more ABOVE VWAP ===
        if last_price and vwap is not None and float(vwap) > 0:
            vwap = float(vwap)
            pct_above = (last_price - vwap) / vwap

            cursor.execute("""
                SELECT vwap_deviation_spoken
                FROM ticker_alert_states WHERE ticker = %s
            """, (ticker,))
            row = cursor.fetchone()
            deviation_spoken = row[0] if row else False

            spoken = False

            if pct_above >= 0.15 and not deviation_spoken:
                self.speak(f"{ticker} VWAP deviation")
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET vwap_deviation_spoken = TRUE
                    WHERE ticker = %s
                """, (ticker,))
                spoken = True
            elif pct_above < 0.15 and deviation_spoken:
                # Reset when price returns below 15%
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET vwap_deviation_spoken = FALSE
                    WHERE ticker = %s
                """, (ticker,))
                # spoken = True  # Optional: uncomment if you want a "back near VWAP" alert

            if spoken:
                conn.commit()

        # === State Machine ===
        if current_state == 'IDLE':
            ...

        # === State Machine ===
        if current_state == 'IDLE':
            if is_green and close > vwap and prev and prev[6]:  # prev.is_green
                # Two consecutive green candles above VWAP
                self.speak(f"{ticker} green candle streak started")
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET current_state = 'GREEN_STREAK', streak_count = 2, 
                        last_green_high = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s
                """, (high, ticker))

        elif current_state == 'GREEN_STREAK':
            if is_green and close > vwap:
                # Continue streak — no speech
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET streak_count = streak_count + 1, last_green_high = %s
                    WHERE ticker = %s
                """, (high, ticker))

            elif not is_green:  # First red candle
                lower_high = high < last_green_high if last_green_high else False
                alert_text = (
                    f"{ticker} first red candle with lower high"
                    if lower_high else
                    f"{ticker} first red candle with higher high wick"
                )
                self.speak(alert_text)

                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET current_state = 'FIRST_RED', 
                        first_red_type = %s,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s
                """, ('lower_high' if lower_high else 'higher_high_wick', ticker))

        elif current_state == 'FIRST_RED':
            if last_price <= sma10 + Decimal('0.10') and not sma_approach_spoken:
                self.speak(f"{ticker} approaching 10 SMA after signal")
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET last_sma_approach_alert = TRUE
                    WHERE ticker = %s
                """, (ticker,))

            if close < sma10:
                self.speak(f"{ticker} closed below 10 SMA after signal")
                # Reset to IDLE
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET current_state = 'IDLE', streak_count = 0, 
                        last_green_high = NULL, first_red_type = NULL,
                        last_sma_approach_alert = FALSE,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s
                """, (ticker,))
                
                

        # Note: Your original had some duplicated/redundant blocks for APPROACHING_SMA - keeping logic as-is

    def check_hod_pmh_alerts(self, ticker, last_price, hod, pmh, cursor, conn):
        """Independent HOD and PMH approaching/break alerts - fixed type issues"""
        if last_price is None or hod is None:
            return

        cursor.execute("""
            SELECT approaching_hod_spoken, approaching_pmh_spoken,
                   last_hod_alert_price, last_pmh_alert_price
            FROM ticker_alert_states WHERE ticker = %s
        """, (ticker,))
        row = cursor.fetchone()
        if not row:
            return
        hod_spoken, pmh_spoken, last_hod_price, last_pmh_price = row

        spoken = False

        # HOD alerts - all float
        if last_price >= hod:
            self.speak(f"{ticker} new high at {last_price:.2f}")
            cursor.execute("""
                UPDATE ticker_alert_states 
                SET last_hod_alert_price = %s, approaching_hod_spoken = FALSE
                WHERE ticker = %s
            """, (last_price, ticker))
            spoken = True
        elif last_price >= hod - 0.10 and not hod_spoken:
            if last_hod_price is None or last_price > float(last_hod_price or 0) + 0.20:
                self.speak(f"{ticker} approaching HOD")
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET approaching_hod_spoken = TRUE, last_hod_alert_price = %s
                    WHERE ticker = %s
                """, (last_price, ticker))
                spoken = True

        # PMH alerts
        if pmh is not None:
            pmh_f = float(pmh)
            if last_price > pmh_f:
                self.speak(f"{ticker} broke pre-market high at {last_price:.2f}")
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET last_pmh_alert_price = %s, approaching_pmh_spoken = FALSE
                    WHERE ticker = %s
                """, (last_price, ticker))
                spoken = True
            elif last_price >= pmh_f - 0.10 and not pmh_spoken:
                if last_pmh_price is None or last_price > float(last_pmh_price or 0) + 0.20:
                    self.speak(f"{ticker} approaching pre-market high")
                    cursor.execute("""
                        UPDATE ticker_alert_states 
                        SET approaching_pmh_spoken = TRUE, last_pmh_alert_price = %s
                        WHERE ticker = %s
                    """, (last_price, ticker))
                    spoken = True

        if spoken:
            conn.commit()

    def receive_latest_price(self, ticker, price):
        """Called by VwapFetch on every new last price"""
        ticker = ticker.upper()
        if price is None:
            return

        price = float(price)
        self.last_price[ticker] = price

        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT pmh, hod FROM ticker_alert_states 
                    WHERE ticker = %s AND alert_active = TRUE
                """, (ticker,))
                row = cursor.fetchone()
                if not row:
                    return
                pmh, hod = row

                updated = False

                # ONLY update PMH during pre-market hours
                if pmh is not None and price > pmh:
                    now = datetime.now()
                    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
                        # Still pre-market → allow live PMH updates
                        cursor.execute("UPDATE ticker_alert_states SET pmh = %s WHERE ticker = %s", (price, ticker))
                        self.logger.info(f"PMH updated (pre-market) -> {price:.2f} for {ticker}")
                    else:
                        # After 9:30 → PMH is frozen, do NOT update the column
                        # But still trigger the ONE-TIME "broke PMH" voice alert via check_hod_pmh_alerts()
                        pass  # Do nothing here for PMH column

                # Update HOD if broken (only after initialized)
                if hod is not None and price > hod:
                    cursor.execute("""
                        UPDATE ticker_alert_states SET hod = %s WHERE ticker = %s
                    """, (price, ticker))
                    updated = True
                    self.logger.info(f"HOD broken -> updated to {price:.2f} for {ticker}")

                if updated:
                    conn.commit()

            except psycopg2.Error as e:
                self.logger.error(f"Error in receive_latest_price for {ticker}: {e}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

    def activate_ticker_alerts(self, ticker):
        """Called when ticker is added via frontend"""
        ticker = ticker.upper()
        today_slash = datetime.now().strftime('%Y/%m/%d')

        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()

                # Try to get current PMH from pre-market 1min data
                cursor.execute("""
                    SELECT MAX(high) FROM ohlc_1min 
                    WHERE ticker = %s 
                      AND "timestamp" LIKE %s 
                      AND "timestamp" < %s
                """, (ticker, f"{today_slash}%", f"{today_slash}-09:30"))
                pmh_res = cursor.fetchone()
                pmh = float(pmh_res[0]) if pmh_res and pmh_res[0] else None

                # Insert or update row - hod starts NULL
                cursor.execute("""
                    INSERT INTO ticker_alert_states 
                        (ticker, alert_active, current_state, pmh, hod)
                    VALUES (%s, TRUE, 'IDLE', %s, NULL)
                    ON CONFLICT (ticker) DO UPDATE SET
                        alert_active = TRUE,
                        current_state = 'IDLE',
                        pmh = EXCLUDED.pmh,
                        hod = NULL,
                        last_updated = CURRENT_TIMESTAMP
                """, (ticker, pmh))

                conn.commit()
                self.logger.info(f"Alerts activated for {ticker} | PMH: {pmh} | HOD: null (will be set post-9:30)")

            except psycopg2.Error as e:
                self.logger.error(f"Error activating alerts for {ticker}: {e}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

    def deactivate_ticker_alerts(self, ticker):
        """Called when ticker is removed"""
        ticker = ticker.upper()
        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE ticker_alert_states 
                    SET alert_active = FALSE, last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s
                """, (ticker,))
                conn.commit()
                self.logger.info(f"Alerts deactivated for {ticker}")
            except psycopg2.Error as e:
                self.logger.error(f"Error deactivating alerts for {ticker}: {e}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

        # Clean in-memory data
        self.last_price.pop(ticker, None)