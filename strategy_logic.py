import time as tm
import pandas as pd
import logging
import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
from datetime import datetime, time
import threading
from flask_socketio import SocketIO, emit
from sl_monitor import SLMonitor
from end_of_day import EndOfDay
from risk_management import RiskManagement
from order_execution import OrderExecution
from trade_monitor import TradeMonitor
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv  # Added for .env
import os

class StrategyLogic:
    def __init__(self, risk_management, socketio=None, signal_end=time(14, 0), trading_end=time(18, 0), strategy_mode='both'):
        self.logger = logging.getLogger('StrategyLogic')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.debug("Initializing StrategyLogic")
        self.logger.debug(f"StrategyLogic logger handlers: {self.logger.handlers}")
        self.socketio = socketio or SocketIO()
        self.tickers = {}
        self.bot_enabled = True

        
        self.risk_management = risk_management
        self.enabled_strategies = {
            # '1Min-2g2r',          # <-- disabled
            # '1Min-below_sma',     # <-- disabled
            # '5Min-2g2r',          # <-- disabled
            '5Min-below_sma',       # <-- keep this one
            '1Min-below_pmh',     
            # '5Min-below_pmh',     # <-- disabled
            '1Min-vwap_crossover',
            '1Min-vwap_dev',
            # '5Min-vwap_crossover',# <-- disabled
            
        }
        self.signal_end  = signal_end
        self.trading_end = trading_end
        self.db_lock = threading.Lock()
        self.lock = threading.Lock()
        self.ticker_threads = {}
        self.last_processed = {}  # Track last processed timestamp
        self.processing_lock = threading.Lock()  # Lock for processing
        self.logger.debug("Initialized last_processed dictionary")
        
        self.strategy_mode = strategy_mode  # 'both', '2g2r', or 'below_sma'
        if strategy_mode not in ['both', '2g2r', 'below_sma']:
            self.logger.error(f"Invalid strategy_mode: {strategy_mode}. Defaulting to 'both'.")
            self.strategy_mode = 'both'
        # Load environment variables from .env
        current_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(current_dir, '.env')
        if not os.path.exists(env_path):
            self.logger.error(f".env file not found at path: {env_path}")
            self.smtp_server = None
        else:
            load_dotenv(env_path)
            smtp_server = os.getenv('SMTP_SERVER')
            smtp_port = os.getenv('SMTP_PORT')
            email_user = os.getenv('EMAIL_USER')
            email_pass = os.getenv('EMAIL_PASS')
            self.sender_email = os.getenv('SENDER_EMAIL')
            self.recipient_email = os.getenv('RECIPIENT_EMAIL')
            if not all([smtp_server, smtp_port, email_user, email_pass, self.sender_email, self.recipient_email]):
                self.logger.error("One or more email environment variables are missing.")
                self.smtp_server = None
            else:
                # Initialize SMTP server
                try:
                    self.smtp_server = smtplib.SMTP(smtp_server, int(smtp_port))
                    self.smtp_server.starttls()
                    self.smtp_server.login(email_user, email_pass)
                    self.logger.info("SMTP server initialized for email notifications")
                except Exception as e:
                    self.logger.error(f"Failed to initialize SMTP server: {e}")
                    self.smtp_server = None
        
        try:
            # Initialize SQLAlchemy engine
            db_uri = f"postgresql+psycopg2://{config.DB_CONFIG['user']}:{config.DB_CONFIG['password']}@{config.DB_CONFIG['host']}:{config.DB_CONFIG['port']}/{config.DB_CONFIG['dbname']}"
            self.engine = create_engine(db_uri)
            self.logger.info("SQLAlchemy engine initialized")
            # Keep psycopg2 pool for other queries
            self.db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=50,
                **config.DB_CONFIG
            )
            self.logger.info("PostgreSQL connection pool initialized")
            self.initialize_trade_id_counter()
            self.initialize_candle_conditions_table()
        except Exception as e:
            self.logger.error(f"Failed to initialize database connections: {e}")
            raise
        
        if not self.socketio:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        
        self.max_signal_age_1min = pd.Timedelta(minutes=2)
        self.max_signal_age_5min = pd.Timedelta(minutes=6)
            
            
    def initialize_candle_conditions_table(self):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.candleconditions_1min (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open NUMERIC(10,2) NOT NULL,
                    high NUMERIC(10,2) NOT NULL,
                    close NUMERIC(10,2) NOT NULL,
                    vwap NUMERIC(10,2) NOT NULL,
                    is_green BOOLEAN NOT NULL,
                    sma_10 NUMERIC(10,2),  -- Added SMA column
                    state TEXT NOT NULL,
                    state_2g2r TEXT,  -- Separate state for 2G2R
                    state_sma TEXT,   -- Separate state for below SMA
                    state_below_pmh TEXT,  -- Separate state for below PMH
                    state_vwap_crossover TEXT,  -- Separate state for VWAP crossover
                    state_vwap_dev TEXT,  -- Separate state for VWAP deviation
                    CONSTRAINT unique_candleconditions_1min_ticker_timestamp UNIQUE (ticker, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_candleconditions_1min_ticker_timestamp
                ON public.candleconditions_1min (ticker, timestamp);
                CREATE INDEX IF NOT EXISTS idx_candleconditions_1min_state
                ON public.candleconditions_1min (state);

                CREATE TABLE IF NOT EXISTS public.candleconditions_5min (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open NUMERIC(10,2) NOT NULL,
                    high NUMERIC(10,2) NOT NULL,
                    close NUMERIC(10,2) NOT NULL,
                    vwap NUMERIC(10,2) NOT NULL,
                    is_green BOOLEAN NOT NULL,
                    sma_10 NUMERIC(10,2),  -- Added SMA column
                    state TEXT NOT NULL,
                    state_2g2r TEXT,  -- Separate state for 2G2R
                    state_sma TEXT,   -- Separate state for below SMA
                    state_below_pmh TEXT,  -- Separate state for below PMH
                    state_vwap_crossover TEXT,  -- Separate state for VWAP crossover
                    state_vwap_dev TEXT,
                    CONSTRAINT unique_candleconditions_5min_ticker_timestamp UNIQUE (ticker, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_candleconditions_5min_ticker_timestamp
                ON public.candleconditions_5min (ticker, timestamp);
                CREATE INDEX IF NOT EXISTS idx_candleconditions_5min_state
                ON public.candleconditions_5min (state);
            """)
            conn.commit()
            self.logger.info("Initialized candleconditions_1min and candleconditions_5min tables with separate states")
        except psycopg2.Error as e:
            self.logger.error(f"Error initializing CandleConditions tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
        
        
    def fetch_tickers_from_db(self):
        now = tm.time()
        today = datetime.now().strftime('%Y-%m-%d')
        conn = None
        cursor = None
        retries = 3
        delay = 1
        for attempt in range(retries):
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                query = """
                    SELECT ticker, rsi_1min, rsi_5min FROM tradeparameters
                    WHERE date = %s
                """
                cursor.execute(query, (today,))
                rows = cursor.fetchall()
                if not rows:
                    self.logger.warning(f"No tickers found in the database for date {today}.")
                else:
                    self.logger.info(f"Fetched tickers: {rows}")
                current_tickers = set(self.tickers.keys())
                for row in rows:
                    ticker, rsi_1m, rsi_5m = row
                    if ticker not in current_tickers:
                        risks = self.get_strategy_risks(ticker)
                        self.logger.info(f"Fetched strategy_risks for {ticker}: {risks}")  # <-- ADD THIS
                        selected = list(risks.keys())
                        self.logger.info(f"Selected strategies for {ticker}: {selected}")  # <-- ADD THIS
                        self.add_ticker(ticker, rsi_1m, rsi_5m, selected_strategies=selected)
                    else:
                        self.logger.info(f"Ticker {ticker} already in memory — skipping add")    
                self.logger.debug(f"Fetched and processed {len(rows)} tickers from database")
                return rows
            except psycopg2.Error as e:
                self.logger.error(f"Attempt {attempt + 1}/{retries} - Database error: {e}")
                if attempt < retries - 1:
                    tm.sleep(delay)
                continue
            except Exception as e:
                self.logger.error(f"Error fetching tickers from db: {e}")
                break
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)
        return []
    
    def get_pml_from_ohlc(self, ticker):
        """
        Calculate Pre-Market Low (PML) from ohlc_1min table.
        Uses all 1min candles before 09:30 AM on the current day.
        Returns float PML or None if no pre-market data.
        """
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            today_prefix = datetime.now().strftime('%Y/%m/%d')
            market_open = f"{today_prefix}-09:30"  # Exclude the regular hours open candle
            
            query = """
                SELECT MIN(low)
                FROM ohlc_1min
                WHERE ticker = %s
                  AND timestamp LIKE %s
                  AND timestamp < %s
            """
            cursor.execute(query, (ticker.upper(), today_prefix + "-%", market_open))
            
            result = cursor.fetchone()[0]
            if result is not None:
                pml = float(result)
                self.logger.debug(f"Calculated PML for {ticker}: {pml}")
                return pml
            else:
                self.logger.info(f"No pre-market data for {ticker} today")
                return None
                
        except psycopg2.Error as e:
            self.logger.error(f"Error calculating PML for {ticker}: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def handle_ticker_update(self, message):
        ticker = message['ticker']
        action = message['action']
        if action == 'add':
            rsi_1m = message['rsi_1m']
            rsi_5m = message['rsi_5m']
            self.add_ticker(ticker, rsi_1m, rsi_5m)

    def initialize_trade_id_counter(self):
        """Initialize tradeidcounter with the highest numeric tradeid."""
        conn = None
        cursor = None
        try:
            with self.lock:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tradeidcounter (
                        id INTEGER PRIMARY KEY,
                        last_trade_id INTEGER NOT NULL
                    )
                """)
                # Filter for numeric tradeid
                cursor.execute("""
                    SELECT MAX(CAST(tradeid AS INTEGER)) FROM (
                        SELECT tradeid FROM tradesignal WHERE tradeid ~ '^[0-9]+$'
                        UNION
                        SELECT tradeid FROM tradedetails WHERE tradeid ~ '^[0-9]+$'
                        UNION
                        SELECT tradeid FROM activetrades WHERE tradeid ~ '^[0-9]+$'
                        UNION
                        SELECT tradeid FROM closedtrades WHERE tradeid ~ '^[0-9]+$'
                        UNION
                        SELECT tradeid FROM buymarket WHERE tradeid ~ '^[0-9]+$'
                    )
                """)
                max_trade_id = cursor.fetchone()[0]
                max_trade_id = int(max_trade_id) if max_trade_id else 0
                cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
                if cursor.fetchone():
                    cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (max_trade_id,))
                else:
                    cursor.execute("INSERT INTO tradeidcounter (id, last_trade_id) VALUES (1, %s)", (max_trade_id,))
                conn.commit()
                self.logger.info(f"Initialized tradeID counter to {max_trade_id}")
        except psycopg2.Error as e:
            self.logger.error(f"Error initializing tradeID counter: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def generate_auto_id(self):
        conn = None
        cursor = None
        try:
            with self.lock:  # Ensure thread safety
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                # Get current last_trade_id
                cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
                
                last_trade_id = cursor.fetchone()[0]
                # Increment ID
                new_trade_id = last_trade_id + 1
                # Update tradeidcounter
                cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (new_trade_id,))
                conn.commit()
                self.logger.debug(f"Generated trade ID: {new_trade_id}")
                return str(new_trade_id)  # Return as string for tradesignal.tradeid
        except psycopg2.Error as e:
            self.logger.error(f"Error generating trade ID: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
    
    def get_strategy_risks(self, ticker):
        """Fetch the strategy_risks JSON for a ticker from tradeparameters"""
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT strategy_risks FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """, (ticker.upper(), today))
            row = cursor.fetchone()
            return row[0] if row else {}
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching strategy_risks for {ticker}: {e}")
            return {}
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def add_ticker(self, ticker, rsi_1m, rsi_5m, selected_strategies=None):
        self.logger.info(f"ENTERING add_ticker for {ticker} with selected_strategies={selected_strategies}")
        with self.lock:
            # Clean up any existing threads/state for this ticker
            self.remove_ticker(ticker)
            self.logger.info(f"After remove_ticker, self.tickers keys: {list(self.tickers.keys())}")

            # If no strategies provided, do nothing
            if not selected_strategies:
                self.logger.info(f"No strategies selected for {ticker} — no threads started")
                return

            # Filter to only globally enabled strategies
            valid_strategies = [s for s in selected_strategies if s in self.enabled_strategies]
            
            # Always store the ticker (for All Tickers and Candle Conditions display)
            strategy_risks = self.get_strategy_risks(ticker)
            self.tickers[ticker] = {
                'rsi_1m': rsi_1m,
                'rsi_5m': rsi_5m,
                'strategy_risks': strategy_risks,
                'was_above_sma': False
            }
            self.logger.info(f"Added ticker {ticker} with selected strategies: {selected_strategies} (valid: {valid_strategies}) and risks: {strategy_risks}")

            if not valid_strategies:
                self.logger.info(f"No globally enabled strategies for {ticker} — no threads started")
                return


            # Start threads for each valid selected strategy
            for strategy in valid_strategies:
                key = f"{ticker}_{strategy}"
                ticker_thread = threading.Thread(target=self.run_ticker, args=(ticker, strategy), daemon=True)
                self.ticker_threads[key] = ticker_thread
                ticker_thread.start()
                self.logger.info(f"Started thread for {ticker} with strategy {strategy}")

    def remove_ticker(self, ticker):
        # No lock here — caller (add_ticker) already holds it
        if ticker in self.tickers:
            del self.tickers[ticker]
            self.logger.info(f"Removed ticker {ticker} from self.tickers")
        strategies = ['1Min-2g2r', '1Min-below_sma', '5Min-2g2r', '5Min-below_sma', '1Min-below_pmh', '5Min-below_pmh', '1Min-vwap_crossover', '5Min-vwap_crossover', '1Min-vwap_dev']
        for strategy in strategies:
            key = f"{ticker}_{strategy}"
            if key in self.ticker_threads:
                if self.ticker_threads[key].is_alive():
                    self.logger.info(f"Stopping thread for {ticker} with strategy {strategy}")
                del self.ticker_threads[key]
            if key in self.last_processed:
                del self.last_processed[key]
                self.logger.debug(f"Removed {key} from last_processed")

    def get_data_from_db(self, ticker, table_name, last_timestamp=None, delay=2):
        self.logger.info(f"Fetching data for {ticker} from {table_name}" + 
                         (f" since {last_timestamp}" if last_timestamp else " from market open"))
        
        # Wait until 9:30 AM EST if needed
        now = datetime.now()
        target_time = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now < target_time:
            wait_seconds = (target_time - now).total_seconds()
            self.logger.info(f"Waiting until 9:30 AM to start polling, {wait_seconds:.0f} seconds remaining")
            tm.sleep(wait_seconds)

        conn = None
        cursor = None
        start_time = tm.time()
        timeout = 60  # Max wait 60s before giving up
        poll_interval = 1  # Poll every 1s

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            """, (table_name.lower(),))
            if not cursor.fetchone():
                self.logger.error(f"Table {table_name} does not exist in database")
                return pd.DataFrame()
        except psycopg2.Error as e:
            self.logger.error(f"Database error checking table existence: {e}")
            return pd.DataFrame()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

        while tm.time() - start_time < timeout:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                
                # Default to 9:30 AM today if no last_timestamp
                if last_timestamp is None:
                    last_timestamp = datetime.combine(now.date(), time(9, 30))
                    op = '>='
                else:
                    op = '>'

                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE ticker = %s AND to_timestamp(timestamp, 'YYYY/MM/DD-HH24:MI') {op} %s
                    ORDER BY timestamp ASC
                """
                params = [ticker.upper(), last_timestamp]
                
                self.logger.debug(f"Executing query: {query} with params: {params}")
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                if rows:
                    columns = [desc[0] for desc in cursor.description]
                    data = pd.DataFrame(rows, columns=columns)
                    data['timestamp'] = pd.to_datetime(data['timestamp'], format='%Y/%m/%d-%H:%M', errors='coerce')
                    if data['timestamp'].isna().any():
                        self.logger.warning(f"Failed to parse some timestamps in {table_name} for {ticker}: {data[data['timestamp'].isna()]['timestamp'].tolist()}")
                    self.logger.info(f"New data fetched successfully for {ticker} from {table_name}: {len(data)} rows")
                    return data
                else:
                    self.logger.debug(f"No new data yet for {ticker} in {table_name}, polling again in {poll_interval} seconds")
                    tm.sleep(poll_interval)
            except psycopg2.Error as e:
                self.logger.error(f"Database error during poll: {e}")
                tm.sleep(poll_interval)
            except Exception as e:
                self.logger.error(f"Error retrieving data from {table_name}: {e}")
                tm.sleep(poll_interval)
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

        self.logger.debug(f"No new data found within timeout for {ticker} after {timeout} seconds, skipping this poll")
        return pd.DataFrame()
    
    def calculate_sma10(self, ticker, timeframe, current_timestamp):
        """
        Calculate 10-period SMA based on closes from OHLC tables.
        Returns None if fewer than 10 closes are available.
        """
        table = 'ohlc_1min' if timeframe == '1Min' else 'ohlc_5min'
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            ts_str = current_timestamp.strftime('%Y/%m/%d-%H:%M')
            query = f"""
                SELECT close FROM {table} 
                WHERE ticker = %s AND timestamp <= %s 
                AND DATE(TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI')) = CURRENT_DATE
                ORDER BY timestamp DESC LIMIT 10
            """
            self.logger.debug(f"SMA Query: {query} | ticker={ticker} | ts={ts_str}")
            cursor.execute(query, (ticker.upper(), ts_str))
            rows = cursor.fetchall()
            closes = [float(row[0]) for row in rows if row[0] is not None]
            if len(closes) < 10:
                self.logger.debug(f"Insufficient data for SMA10 on {ticker} ({timeframe}): only {len(closes)} closes available.")
                return None
            sma = sum(closes) / 10.0
            sma_rounded = round(sma, 2)
            self.logger.debug(f"Calculated SMA10 for {ticker} ({timeframe}) at {current_timestamp}: {sma_rounded} (from {len(closes)} candles)")
            return sma_rounded
        except psycopg2.Error as e:
            self.logger.error(f"Error calculating SMA10 for {ticker} ({timeframe}): {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
        
    
    def get_vwap_from_trade_parameters(self, ticker):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            query = """
                SELECT vwap FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """
            cursor.execute(query, (ticker.upper(), today))
            vwap = cursor.fetchone()
            if vwap:
                return vwap[0]
            else:
                self.logger.warning(f"No vwap value found for {ticker} in tradeparameters")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching vwap from tradeparameters: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
                
    def get_open_from_trade_parameters(self, ticker):
        """
        Fetch today's regular market open price (9:30 AM candle open) from tradeparameters table.
        Returns float or None if not found.
        """
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            query = """
                SELECT open FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """
            cursor.execute(query, (ticker.upper(), today))
            result = cursor.fetchone()
            if result and result[0] is not None:
                return float(result[0])
            else:
                self.logger.warning(f"No open price found for {ticker} in tradeparameters")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching open price from tradeparameters: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)            

    def get_pmh_from_trade_parameters(self, ticker):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            query = """
                SELECT pmh FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """
            cursor.execute(query, (ticker.upper(), today))
            pmh = cursor.fetchone()
            if pmh and pmh[0] is not None:
                return float(pmh[0])
            else:
                self.logger.warning(f"No pmh value found for {ticker} in tradeparameters")
                return None
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching pmh from tradeparameters: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def process_market_data(self, ticker, strategy_type):
        if strategy_type.startswith('1Min'):
            table_name = 'ohlc_1min'
            candle_table = 'candleconditions_1min'
        elif strategy_type.startswith('5Min'):
            table_name = 'ohlc_5min'
            candle_table = 'candleconditions_5min'
        else:
            self.logger.error(f"Unsupported strategy type: {strategy_type}")
            return
        if '-2g2r' in strategy_type:
            state_column = 'state_2g2r'
        elif '-below_sma' in strategy_type:
            state_column = 'state_sma'
        elif '-below_pmh' in strategy_type:
            state_column = 'state_below_pmh'
        elif '-vwap_crossover' in strategy_type:
            state_column = 'state_vwap_crossover'
        elif '-vwap_dev' in strategy_type:
            state_column = 'state_vwap_dev'    
        self.logger.info(f"Processing {strategy_type} market data for {ticker}")
        vwap = self.get_vwap_from_trade_parameters(ticker)
        if vwap is None:
            self.logger.warning(f"Skipping processing for {ticker} due to missing VWAP in tradeparameters")
            return
        
        key = f"{ticker}_{strategy_type}"
        last_timestamp = self.last_processed.get(key, None)
        with self.processing_lock:
            data = self.get_data_from_db(ticker, table_name, last_timestamp=last_timestamp)
            if data is not None and not data.empty:
                try:
                    self.logger.debug(f"Processing {len(data)} new rows for {ticker}")
                    self.update_candle_conditions(data, ticker, strategy_type, vwap)
                    
                    # Update last_processed to the latest timestamp processed
                    self.last_processed[key] = data['timestamp'].max()
                    self.logger.debug(f"Updated last_processed for {key} to {self.last_processed[key]}")
                    
                    conn = self.db_pool.getconn()
                    cursor = None
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f"""
                            SELECT {state_column}, timestamp
                            FROM {candle_table}
                            WHERE ticker = %s AND timestamp::time BETWEEN %s AND %s
                            ORDER BY timestamp DESC LIMIT 1
                        """, (ticker, time(9, 30), self.trading_end))
                        latest_row = cursor.fetchone()
                        if latest_row and latest_row[0] == 'SIGNAL_READY':
                            self.logger.info(f"Latest candle for {ticker} at {latest_row[1]} is SIGNAL_READY, firing signals")
                            signals = self.fire_auto_signals(ticker, strategy_type)
                            if signals:
                                self.logger.info(f"Generated {len(signals)} signals for {ticker}")
                        else:
                            self.logger.debug(f"No SIGNAL_READY in latest candle for {ticker}: state={latest_row[0] if latest_row else 'None'}")
                        conn.commit()
                    except psycopg2.Error as e:
                        self.logger.error(f"Error checking latest {candle_table} row for {ticker}: {e}")
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            self.db_pool.putconn(conn)
                except Exception as e:
                    self.logger.error(f"Error processing market data for {ticker}: {str(e)}", exc_info=True)
            else:
                self.logger.debug(f"No new data for {ticker} in {table_name}")
            
    def update_candle_conditions(self, data, ticker, strategy_type, vwap=None):
        table_name = 'candleconditions_1min' if strategy_type.startswith('1Min') else 'candleconditions_5min'
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            pmh = self.get_pmh_from_trade_parameters(ticker)
            for i in range(len(data)):
                row = data.iloc[i]
                timestamp = row['timestamp']
                
                timestamp_db = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                open_price  = float(row['open'])  if row['open']  is not None and str(row['open'])  != 'nan' else None
                close_price = float(row['close']) if row['close'] is not None and str(row['close']) != 'nan' else None
                high_price  = float(row['high'])  if row.get('high') is not None and str(row.get('high', 'nan')) != 'nan' else None
                if open_price is None or close_price is None:
                    self.logger.warning(f"Skipping {ticker} at {timestamp}: open or close is NULL in DB")
                    continue
                if high_price is None:
                    self.logger.warning(f"No high price for {ticker} at {timestamp}")
                    continue
                green_candle = close_price > open_price
                is_green = green_candle and vwap is not None and close_price > vwap
                
                # Get previous states
                cursor.execute(f"""
                    SELECT state_2g2r, state_sma, state_below_pmh, state_vwap_crossover, state_vwap_dev FROM {table_name}
                    WHERE ticker = %s AND timestamp < %s AND DATE(timestamp) = DATE(%s)
                    ORDER BY timestamp DESC LIMIT 1
                """, (ticker, timestamp, timestamp))
                prev_states = cursor.fetchone()
                prev_state_2g2r = prev_states[0] if prev_states else 'WAITING_FOR_FIRST_GREEN'
                prev_state_sma = prev_states[1] if prev_states else 'WAITING_FOR_SMA'
                prev_state_below_pmh = prev_states[2] if prev_states else 'WAITING'
                prev_state_vwap_crossover = prev_states[3] if prev_states else 'WAITING'
                prev_state_vwap_dev = prev_states[4] if prev_states else 'WAITING'

                self.logger.debug(f"Processing {ticker} at {timestamp}: prev_state_2g2r={prev_state_2g2r}, prev_state_sma={prev_state_sma}, is_green={is_green}, open={open_price}, close={close_price}, vwap={vwap}")

                # Determine new states
                new_state_2g2r = None
                new_state_sma = None
                new_state_below_pmh = None
                new_state_vwap_crossover = None
                new_state_vwap_dev = None
                sma_10 = None
                state = None  # Combined or primary state

                if self.strategy_mode in ['both', '2g2r']:
                    new_state_2g2r = 'WAITING_FOR_FIRST_GREEN'
                    if prev_state_2g2r == 'WAITING_FOR_FIRST_GREEN':
                        new_state_2g2r = 'WAITING_FOR_SECOND_GREEN' if is_green else 'WAITING_FOR_FIRST_GREEN'
                    elif prev_state_2g2r == 'WAITING_FOR_SECOND_GREEN':
                        new_state_2g2r = 'WAITING_FOR_FIRST_RED' if is_green else 'WAITING_FOR_FIRST_GREEN'
                    elif prev_state_2g2r == 'WAITING_FOR_FIRST_RED':
                        new_state_2g2r = 'WAITING_FOR_FIRST_RED' if is_green else 'WAITING_FOR_SECOND_RED'
                    elif prev_state_2g2r == 'WAITING_FOR_SECOND_RED':
                        new_state_2g2r = 'WAITING_FOR_FIRST_RED' if is_green else 'SIGNAL_READY'    
                    elif prev_state_2g2r == 'SIGNAL_READY':
                        new_state_2g2r = 'WAITING_FOR_SECOND_GREEN' if is_green else 'WAITING_FOR_FIRST_GREEN'

                if self.strategy_mode in ['both', 'below_sma']:
                    # CORRECT — Use the dedicated SMA function that reads from ohlc_* tables
                    timeframe = '1Min' if strategy_type.startswith('1Min') else '5Min'
                    sma_10 = self.calculate_sma10(ticker, timeframe, timestamp)
                    self.logger.info(f"SMA10 DEBUG: {ticker} @ {timestamp} -> sma_10 = {sma_10}")
                    if sma_10 is None:
                        new_state_sma = 'WAITING_FOR_SMA'
                    elif close_price <= sma_10 and prev_state_sma == 'ABOVE_10SMA':
                        new_state_sma = 'SIGNAL_READY'
                    elif close_price <= sma_10:
                        new_state_sma = 'BELOW_SMA'
                    elif close_price > sma_10 and close_price > vwap:
                        new_state_sma = 'ABOVE_10SMA'
                    elif close_price > sma_10 and close_price <= vwap:
                        new_state_sma = 'BELOW_VWAP'    

                # New strategies states (computed always for consistency)
                if pmh is not None:
                    if timestamp.time() == time(9, 30):  # First candle
                        if open_price <= pmh * 0.80:
                            new_state_below_pmh = 'SIGNAL_READY'
                        else:
                            new_state_below_pmh = 'CONDITION_NOT_MET'
                            
                    else:
                        if prev_state_below_pmh == 'SIGNAL_READY':
                            new_state_below_pmh = 'SIGNAL_FIRED'
                        else:  
                            new_state_below_pmh = prev_state_below_pmh    
                else:
                    new_state_below_pmh = 'MISSING_PMH'

                if pmh is not None and vwap is not None:
                    if timestamp.time() == time(9, 30):  # First candle
                        if open_price <= pmh * 0.80 and open_price < vwap:
                            new_state_vwap_crossover = 'WAITING_FOR_ABOVE_VWAP'
                        else:
                            new_state_vwap_crossover = 'CONDITION_NOT_MET'
                    else:
                        if prev_state_vwap_crossover == 'WAITING_FOR_ABOVE_VWAP':
                            if high_price > vwap:
                                new_state_vwap_crossover = 'ABOVE_VWAP'
                            else:
                                new_state_vwap_crossover = 'WAITING_FOR_ABOVE_VWAP'
                        elif prev_state_vwap_crossover == 'ABOVE_VWAP':
                            if close_price < vwap:
                                new_state_vwap_crossover = 'SIGNAL_READY'
                            else: 
                                new_state_vwap_crossover = 'ABOVE_VWAP'  
                        elif prev_state_vwap_crossover == 'SIGNAL_READY':
                            new_state_vwap_crossover = 'SIGNAL_FIRED'          
                        else:  
                            new_state_vwap_crossover = prev_state_vwap_crossover
                   
                        
                else:
                    new_state_vwap_crossover = 'MISSING_DATA'
                    
                    
                if vwap is not None:
                     
                    
                    if high_price >= vwap * 1.25:
                        if prev_state_vwap_dev == 'SIGNAL_READY':
                            new_state_vwap_dev = 'SIGNAL_FIRED'  # already signaled, suppress repeats
                        else:
                            new_state_vwap_dev = 'SIGNAL_READY'  # first time → fire!  
                    elif high_price > vwap:
                        new_state_vwap_dev = 'ABOVE_VWAP' 
                    elif close_price < vwap:
                        new_state_vwap_dev = 'BELOW_VWAP'    
                    # Reset from SIGNAL_READY/SIGNAL_FIRED when price normalizes
                    elif prev_state_vwap_dev in ['SIGNAL_READY', 'SIGNAL_FIRED']:
                        if close_price >= vwap:
                            new_state_vwap_dev = 'ABOVE_VWAP'
                        else:
                            new_state_vwap_dev = 'BELOW_VWAP'    
                    else: 
                        new_state_vwap_dev = prev_state_vwap_dev        
                           
                        
                         
                           

                # Combined state for legacy 'state' column (e.g., for queries)
                if self.strategy_mode == '2g2r':
                    state = new_state_2g2r
                elif self.strategy_mode == 'below_sma':
                    state = new_state_sma
                else:  # both
                    state = f"{new_state_2g2r}|{new_state_sma}"

                # UPSERT to avoid race conditions
                cursor.execute(f"""
                    INSERT INTO {table_name} (ticker, timestamp, open, high, close, vwap, is_green, sma_10, state,
                                              state_2g2r, state_sma, state_below_pmh, state_vwap_crossover, state_vwap_dev)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        close = EXCLUDED.close,
                        vwap = EXCLUDED.vwap,
                        is_green = EXCLUDED.is_green,
                        sma_10 = EXCLUDED.sma_10,
                        state = EXCLUDED.state,
                        state_2g2r = EXCLUDED.state_2g2r,
                        state_sma = EXCLUDED.state_sma,
                        state_below_pmh = EXCLUDED.state_below_pmh,
                        state_vwap_crossover = EXCLUDED.state_vwap_crossover,
                        state_vwap_dev = EXCLUDED.state_vwap_dev
                """, (ticker, timestamp_db, open_price, high_price, close_price, vwap, is_green, sma_10, state,
                      new_state_2g2r, new_state_sma, new_state_below_pmh, new_state_vwap_crossover, new_state_vwap_dev))

                self.logger.info(f"Updated {ticker} at {timestamp}: is_green={is_green}, sma_10={sma_10}, state={state}, state_2g2r={new_state_2g2r}, state_sma={new_state_sma}, state_below_pmh={new_state_below_pmh}, state_vwap_crossover={new_state_vwap_crossover}")

            conn.commit()
            self.logger.info(f"Completed candle conditions update for {ticker} in {table_name}")
            
            # <<< ADD THIS >>>
            if self.socketio:
                self.socketio.emit('request_candle_conditions')  # triggers clients to refresh
                self.logger.info("Emitted 'request_candle_conditions' to refresh Candle Conditions page")
            # <<< END >>>
        except psycopg2.Error as e:
            self.logger.error(f"Error updating {table_name} for {ticker}: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
        

    def close(self):
        """Close SMTP server and database connections."""
        if self.smtp_server:
            try:
                self.smtp_server.quit()
                self.logger.info("SMTP server closed")
            except Exception as e:
                self.logger.error(f"Failed to close SMTP server: {e}")
        try:
            self.db_pool.closeall()
            self.logger.info("Database connection pool closed")
        except Exception as e:
            self.logger.error(f"Failed to close database connection pool: {e}")

    def notify(self, message):
        if self.smtp_server:
            retries = 2
            for attempt in range(retries):
                try:
                    msg = MIMEText(message)
                    msg['Subject'] = 'Trade Signal Notification'
                    msg['From'] = f'"Algo Trade" <{self.sender_email}>'
                    msg['To'] = self.recipient_email
                    self.smtp_server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
                    self.logger.info(f"Notification sent: {message}")
                    return
                except Exception as e:
                    self.logger.error(f"Attempt {attempt + 1}/{retries} - Failed to send notification: {e}")
                    if attempt < retries - 1:
                        try:
                            smtp_server = os.getenv('SMTP_SERVER')
                            smtp_port = os.getenv('SMTP_PORT')
                            email_user = os.getenv('EMAIL_USER')
                            email_pass = os.getenv('EMAIL_PASS')
                            self.smtp_server = smtplib.SMTP(smtp_server, int(smtp_port))
                            self.smtp_server.starttls()
                            self.smtp_server.login(email_user, email_pass)
                            self.logger.info("Reconnected SMTP server")
                        except Exception as e:
                            self.logger.error(f"Failed to reconnect SMTP server: {e}")
                            return
        else:
            self.logger.error("SMTP server is not initialized. Cannot send notification.")

    def check_active_trade_status(self, ticker, strategy):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            query = """
                SELECT active_trade
                FROM tradestatus
                WHERE ticker = %s AND strategy = %s AND DATE(date) = %s
                ORDER BY id DESC
                LIMIT 1
            """
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(query, (ticker, strategy, today_date))
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        except psycopg2.Error as e:
            self.logger.error(f"Error checking active trade status for {ticker} ({strategy}): {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def get_risk_parameters(self, ticker, date):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            query = """
                SELECT high, account_equity FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """
            cursor.execute(query, (ticker, date))
            row = cursor.fetchone()
            if row:
                hod, account_equity = row
                hod = float(hod) if hod is not None else None
                account_equity = float(account_equity) if account_equity is not None else 50000.0
        
            else:
                self.logger.warning(f"No record found for ticker: {ticker} on date: {date}")
                return None
            if account_equity is None:
                cursor.execute("""
                    SELECT account_equity FROM tradeparameters
                    WHERE account_equity IS NOT NULL AND date < %s
                    ORDER BY date DESC LIMIT 1
                """, (date,))
                equity_row = cursor.fetchone()
                if equity_row and equity_row[0] is not None:
                    account_equity = equity_row[0]
                    self.logger.info(f"Found previous account_equity {account_equity} from an earlier row")
                else:
                    self.logger.error(f"Could not find a valid account_equity before {date}")
                    return None
            return {'hod': hod, 'account_equity': account_equity}
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching risk parameters: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def find_last_swing_high(self, ticker, signal_timestamp):
        """
        Find the most recent swing high before or at the signal_timestamp.
        A swing high is the highest high in a sequence until a lower high appears afterward.
        If the signal candle has the highest high of the day so far, return its high.
        
        Returns: float - the swing high price to use as stop loss
        """
        table_name = 'candleconditions_5min'
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            
            # Define market open as a full timestamp (today at 09:30:00)
            today = datetime.now().date()
            market_open = datetime.combine(today, time(9, 30))
            
            # Fetch all 5min candles from market open up to and including the signal timestamp, ordered chronologically
            query = f"""
                SELECT timestamp, high
                FROM {table_name}
                WHERE ticker = %s
                  AND timestamp::date = %s                  -- Same day
                  AND timestamp >= %s                       -- From market open (timestamp comparison)
                  AND timestamp <= %s                       -- Up to signal timestamp
                ORDER BY timestamp ASC
            """
            
            cursor.execute(query, (ticker.upper(), market_open, signal_timestamp))
            rows = cursor.fetchall()
            
            if not rows:
                self.logger.warning(f"No 5min candles found for {ticker} up to {signal_timestamp}")
                return None
                
            # Convert to list of (timestamp, high)
            candles = [(row[0], float(row[1])) for row in rows]
            
            # The signal candle is the last one
            signal_high = candles[-1][1]
            
            # Find the highest high of the day so far (including signal candle)
            hod_so_far = max(high for _, high in candles)
            
            # If the signal candle is the HOD, use its high
            if signal_high == hod_so_far:
                return signal_high
            
            # Otherwise, scan backward to find the last swing high
            max_high_seen = 0.0
            last_swing_high = None
            
            # Go backward from the candle just before the signal
            for i in range(len(candles) - 2, -1, -1):  # exclude the signal candle itself
                current_high = candles[i][1]
                
                if current_high > max_high_seen:
                    max_high_seen = current_high
                    last_swing_high = current_high
                else:
                    # As soon as we see a high lower than the current max, the previous max is the swing high
                    break
            
            return last_swing_high if last_swing_high is not None else hod_so_far
                
        except psycopg2.Error as e:
            self.logger.error(f"Error finding last swing high for {ticker}: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def fire_auto_signals(self, ticker, strategy_type):
        table_name = 'candleconditions_1min' if strategy_type.startswith('1Min') else 'candleconditions_5min'
        try:
            signals = []
            state_column = (
                'state_2g2r' if strategy_type.endswith('2g2r') else
                'state_sma' if strategy_type.endswith('below_sma') else
                'state_below_pmh' if strategy_type.endswith('below_pmh') else
                'state_vwap_crossover' if strategy_type.endswith('vwap_crossover') else
                'state_vwap_dev' if strategy_type.endswith('vwap_dev') else
                None
                
            )
            
            if state_column is None:
                self.logger.error(f"Unknown strategy_type '{strategy_type}' — no state column defined")
                return []
            max_age = self.max_signal_age_1min if strategy_type.startswith('1Min') else self.max_signal_age_5min
            recent_cutoff = datetime.now() - max_age
            
            conn = self.db_pool.getconn()
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT timestamp, open, high, close, vwap, state_2g2r, state_sma, state_below_pmh, state_vwap_crossover, state_vwap_dev,
                           (SELECT MAX(high) FROM {table_name}
                           WHERE ticker = %s AND timestamp::date = CURRENT_DATE
                           AND timestamp >= %s  -- From 9:30
                           AND timestamp <= c.timestamp) as hod
                    FROM {table_name} c
                    WHERE ticker = %s AND {state_column} = 'SIGNAL_READY'
                    AND timestamp >= %s  -- Recent only
                    AND timestamp::time BETWEEN %s AND %s
                """, (ticker, datetime.combine(datetime.now().date(), time(9, 30)), ticker, recent_cutoff, time(9, 30), self.signal_end))
                rows = cursor.fetchall()
                if not rows:
                    self.logger.debug(f"No recent {state_column}='SIGNAL_READY' rows found for {ticker} within {max_age} after {recent_cutoff}")
                for row in rows:
                    timestamp, open_price, high_price, close_price, vwap, state_2g2r, state_sma, state_below_pmh, state_vwap_crossover, state_vwap_dev, hod = row
                    signal_time = pd.to_datetime(timestamp).time()
                    if not (time(9, 30) <= signal_time <= self.signal_end):
                        self.logger.debug(f"Signal time {signal_time} for {ticker} outside trading hours (9:30-{self.signal_end})")
                        continue
                    # In fire_auto_signals, before signals.append(signal)
                    if not self.bot_enabled:
                        self.logger.info(f"Bot is disabled — skipping signal fire for {ticker} ({strategy_type})")
                        continue
                    signal_row = pd.Series({
                        'timestamp': pd.to_datetime(timestamp),
                        'open': open_price,
                        'high': high_price,
                        'close': close_price,
                        'vwap': vwap,
                        'hod': hod
                    })
                    signal = self.generate_signal(signal_row, ticker, strategy_type)
                    if signal:
                        signals.append(signal)
                        self.logger.info(f"{strategy_type} Signal generated for {ticker} at {timestamp}: {signal}")
                conn.commit()
                self.logger.info(f"Completed signal evaluation for {ticker}. Total signals fired: {len(signals)}")
            except psycopg2.Error as e:
                self.logger.error(f"Error querying {table_name} for {ticker}: {e}")
                conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)
            return signals
        except Exception as e:
            self.logger.error(f"Error firing signals for {ticker} ({strategy_type}): {e}")
            return []

    def generate_signal(self, row, ticker, strategy_type):
        try:
            current_time = datetime.now()
            signal_time = row['timestamp']

            max_signal_age = self.max_signal_age_1min if strategy_type.startswith('1Min') else self.max_signal_age_5min
            if (current_time - signal_time) > max_signal_age:
                self.logger.warning(f"Signal for {ticker} at {signal_time} is too old. Skipping.")
                return None

            # Log raw row data for debugging
            self.logger.debug(f"Raw signal row for {ticker} {strategy_type}: {dict(row)}")

            # Safely convert close price
            try:
                price_current = float(row['close'])
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Invalid or missing 'close' in row for {ticker}: {row.get('close')} | Error: {e}")
                return None

            # Safely convert high from candle (used for validation/logging)
            try:
                high_current = float(row['high'])
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Invalid or missing 'high' in row for {ticker}: {row.get('high')} | Error: {e}")
                return None

            # Use dynamic HOD from the query subselect — always reliable
            try:
                dynamic_hod = float(row['hod'])
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Invalid or missing 'hod' (dynamic high) in row for {ticker}: {row.get('hod')} | Error: {e}")
                return None

            date = datetime.now().strftime('%Y-%m-%d')

            # Fetch risk parameters — only need account_equity now
            try:
                risk_params = self.get_risk_parameters(ticker, date)
                if not risk_params:
                    self.logger.warning(f"No risk parameters found for {ticker} on {date}")
                    return None
            except Exception as e:
                self.logger.error(f"Exception fetching risk parameters for {ticker}: {e}")
                return None
            
            # Retrieve hod from risk parameters
            try:
                hod = float(risk_params['hod'])
                self.logger.debug(f"Using HOD from risk parameters for {ticker}: {hod}")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Invalid 'hod' value for {ticker}: {risk_params['hod']} (type: {type(risk_params['hod'])}). Error: {e}")
                return None

            # Safely get account equity
            try:
                account_equity_raw = risk_params.get('account_equity')
                if account_equity_raw is None:
                    account_equity = 50000.0
                    self.logger.warning(f"account_equity missing for {ticker}, using default 50000.0")
                else:
                    account_equity = float(account_equity_raw)
            except (ValueError, TypeError) as e:
                self.logger.error(f"Failed to convert account_equity for {ticker}: {account_equity_raw} | Error: {e}")
                return None

            if account_equity <= 0:
                self.logger.error(f"Invalid account equity (<=0) for {ticker}: {account_equity}")
                return None

            # Get per-strategy risk from JSON
            try:
                strategy_risks = self.tickers.get(ticker, {}).get('strategy_risks', {})
                if strategy_type not in strategy_risks:
                    self.logger.info(f"No risk defined for strategy {strategy_type} on {ticker}")
                    return None

                risk_percentage_value = strategy_risks[strategy_type]
                if risk_percentage_value is None or risk_percentage_value <= 0:
                    self.logger.info(f"Invalid risk percentage for {ticker} {strategy_type}: {risk_percentage_value}")
                    return None

                risk_percentage = float(risk_percentage_value) / 100.0
            except (ValueError, TypeError, KeyError) as e:
                self.logger.error(f"Error processing strategy_risks for {ticker} {strategy_type}: {e}")
                return None

            # Calculate risk amount
            try:
                risk_amount = account_equity * risk_percentage
                if risk_amount <= 0:
                    self.logger.warning(f"Calculated risk_amount <= 0 for {ticker}: {risk_amount}")
                    return None
            except Exception as e:
                self.logger.error(f"Error calculating risk_amount for {ticker}: {e}")
                return None

            # Determine stop loss
            try:
                if strategy_type.endswith('vwap_dev'):
                    # Special rule: stop loss = current price * 1.2 (i.e., 20% below entry)
                    stop_loss = round(price_current * 1.2, 2)
                    self.logger.info(f"VWAP_DEV strategy: using wide stop loss = entry * 1.2 = {stop_loss}")
                elif strategy_type.endswith('below_pmh') or strategy_type.endswith('vwap_crossover'):
                    pmh = self.get_pmh_from_trade_parameters(ticker)
                    if pmh is None:
                        self.logger.error(f"Missing PMH for {ticker} in {strategy_type}")
                        return None
                    stop_loss = round(float(pmh), 2)
                else:
                    stop_loss = round(hod, 2)
            except (ValueError, TypeError) as e:
                self.logger.error(f"Error converting stop loss value for {ticker}: {e}")
                return None
            except Exception as e:
                self.logger.error(f"Unexpected error setting stop loss for {ticker}: {e}")
                return None

            # Validate stop loss vs entry
            try:
                price_difference = abs(stop_loss - price_current)
                if price_difference == 0:
                    self.logger.error(f"Stop loss equals entry price for {ticker}: SL={stop_loss}, Entry={price_current}")
                    return None

                risk_distance = stop_loss - price_current
                if risk_distance <= 0:
                    self.logger.error(f"Invalid risk distance (<=0) for {ticker}: {risk_distance}")
                    return None
            except Exception as e:
                self.logger.error(f"Error in stop loss validation for {ticker}: {e}")
                return None

            # Calculate shares
            try:
                shares = int(risk_amount / max(price_difference, 0.01))
                if shares <= 0:
                    self.logger.warning(f"Calculated shares <= 0 for {ticker}, skipping signal")
                    return None
            except Exception as e:
                self.logger.error(f"Error calculating shares for {ticker}: {e}")
                return None

            # Calculate target price
            try:
                risk_per_share = stop_loss - price_current
                target_price = price_current - (2 * risk_per_share)
            except Exception as e:
                self.logger.error(f"Error calculating target price for {ticker}: {e}")
                return None

            # Generate trade ID
            try:
                trade_id = self.generate_auto_id()
            except Exception as e:
                self.logger.error(f"Failed to generate trade ID for {ticker}: {e}")
                return None

            # Strategy mode permission check
            try:
                if strategy_type.endswith('2g2r') and self.strategy_mode not in ['both', '2g2r']:
                    self.logger.info(f"Strategy {strategy_type} disabled by mode {self.strategy_mode}")
                    return None
                if strategy_type.endswith('below_sma') and self.strategy_mode not in ['both', 'below_sma']:
                    self.logger.info(f"Strategy {strategy_type} disabled by mode {self.strategy_mode}")
                    return None
            except Exception as e:
                self.logger.error(f"Error in strategy_mode check: {e}")
                return None

            # Check for active trade
            try:
                active_trade_status = self.check_active_trade_status(ticker, strategy_type)
                if active_trade_status in ["open", "pending"]:
                    self.logger.info(f"Skipping signal for {ticker} ({strategy_type}) — active trade: {active_trade_status}")
                    return None
            except Exception as e:
                self.logger.error(f"Error checking active trade status for {ticker}: {e}")
                return None

            # Build final signal dictionary
            try:
                signal = {
                    'tradeID': trade_id,
                    'strategy': strategy_type,
                    'ticker': ticker,
                    'entry_price': round(price_current, 2),
                    's_loss': round(stop_loss, 2),
                    'time': row['timestamp'],
                    'hod': round(hod, 2),
                    'shares': shares,
                    'target_price': round(target_price, 2),
                    'risk': round(risk_amount, 2),
                    'status': 'Fired',
                }
            except Exception as e:
                self.logger.error(f"Error building signal dict for {ticker}: {e}")
                return None

            # Insert signal and notify
            try:
                if self.insert_trade_signal(signal):
                    self.logger.info(f"Signal successfully fired and saved: {signal}")
                    self.risk_management.receive_signal(signal)
                    return signal
                else:
                    self.logger.error(f"Failed to insert signal into DB: {signal}")
                    return None
            except Exception as e:
                self.logger.error(f"Exception during signal insertion for {ticker}: {e}")
                return None

        except Exception as e:
            self.logger.error(f"Unexpected error in generate_signal for {ticker} {strategy_type}: {e}", exc_info=True)
            return None
    

    def insert_trade_signal(self, signal):
        with self.db_lock:
            conn = None
            cursor = None
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                signal_time = signal['time']
                if isinstance(signal_time, pd.Timestamp):
                    signal_time = signal_time.strftime('%Y-%m-%d %H:%M:%S')
                query_insert = """
                    INSERT INTO tradesignal (tradeid, strategy, ticker, price, time, hi, shares, status, target, risk)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query_insert, (
                    signal['tradeID'],
                    signal['strategy'],
                    signal['ticker'],
                    signal['entry_price'],
                    signal_time,
                    signal['hod'],
                    signal['shares'],
                    signal['status'],
                    signal['target_price'],
                    signal['risk']
                ))
                conn.commit()
                self.logger.info(f"Trade signal inserted successfully: {signal}")
                
                # Send email notification
                message = (f"Trade Signal Fired: {signal['ticker']} ({signal['strategy']})\n"
                           f"Time: {signal_time}\n"
                           f"Entry: ${signal['entry_price']}, SL: ${signal['s_loss']}, "
                           f"Target: ${signal['target_price']}, Shares: {signal['shares']}")
                self.notify(message)
                
                # Emit signal to frontend
                if self.socketio:
                    serializable_signal = signal.copy()  # Create a copy to avoid modifying original
                    serializable_signal['time'] = signal_time  # Use string time
                    self.socketio.emit('trade_signal_update', [serializable_signal])
                    self.logger.info(f"Emitted trade_signal_update: {serializable_signal}")
                return True
            except psycopg2.IntegrityError as e:
                self.logger.error(f"IntegrityError inserting trade signal (possible duplicate): {e}")
                return False
            except psycopg2.Error as e:
                self.logger.error(f"Error inserting trade signal: {e}")
                return False
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.db_pool.putconn(conn)

    def evaluate_trade(self, signal):
        self.logger.info(f"Evaluating trade: {signal}")

    def run_ticker(self, ticker, strategy_type):
        key = f"{ticker}_{strategy_type}"
        self.logger.info(f"Starting run_ticker for {ticker} with strategy {strategy_type}")
        
        if strategy_type not in self.enabled_strategies:
            self.logger.info(
                f"Strategy {strategy_type} is disabled – thread will idle until removed."
            )
            while datetime.now().time() <= self.trading_end:
                tm.sleep(5)                     # low-CPU idle loop
            return
        
        try:
            while datetime.now().time() <= self.trading_end:
                current_time = datetime.now()
                self.logger.debug(f"Polling at {current_time} for {ticker} {strategy_type}, second={current_time.second}, minute={current_time.minute}")
                if strategy_type.startswith('1Min') and current_time.second == 2:
                    self.logger.info(f"Processing 1-minute market data for {ticker}")
                    self.process_market_data(ticker, strategy_type)
                if strategy_type.startswith('5Min') and current_time.minute % 5 == 0 and current_time.second == 2:
                    self.logger.info(f"Processing 5-minute market data for {ticker}")
                    self.process_market_data(ticker, strategy_type)
                tm.sleep(1)
        except Exception as e:
            self.logger.error(f"Error running ticker {ticker} with strategy {strategy_type}: {e}")
        finally:
            with self.lock:
                if key in self.ticker_threads:
                    del self.ticker_threads[key]
                    self.logger.info(f"Finished processing {ticker} with strategy {strategy_type}")

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.info("Initializing TradeMonitor in app.py")
    trade_monitor = TradeMonitor()
    sl_monitor = SLMonitor()
    end_of_day = EndOfDay()
    logger.info("Initializing OrderExecution with TradeMonitor in app.py")
    order_execution = OrderExecution(trade_monitor, sl_monitor, end_of_day)
    
    risk_management = RiskManagement(order_execution)
    strategy_logic = StrategyLogic(risk_management=risk_management)
    strategy_logic.fetch_tickers_from_db()
    for thread in threading.enumerate():
        print(thread.name)
    try:
        while True:
            tm.sleep(1)
    except KeyboardInterrupt:
        print("Terminating script...")
        strategy_logic.close()  # Ensure cleanup