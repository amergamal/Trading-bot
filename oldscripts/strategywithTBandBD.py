import time as tm
import pandas as pd
import logging
import sqlite3
from datetime import datetime, time
import threading
from flask_socketio import SocketIO, emit
from scipy.signal import find_peaks
import numpy as np  # Add this

class StrategyLogic:
    def __init__(self, trading_end=time(13, 0)):  # risk_management parameter is optional
        self.logger = logging.getLogger('StrategyLogic')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.tickers = {}  # Store ticker information
        
        self.trade_id_counter = 0  # Counter for generating unique trade IDs
        self.trading_end = trading_end
        
        # Initialize WebSocket client
        self.socketio = SocketIO(message_queue='redis://')
        self.socketio.on_event('ticker_update', self.handle_ticker_update)

        
        
        # Add a lock for synchronizing access to the database
        self.lock = threading.Lock()
        
        # Initialize the dictionary to track ticker threads
        self.ticker_threads = {}  # <-- This line initializes the ticker_threads attribute

    def fetch_tickers_from_db(self):
        """Fetch tickers from the TradeParameters table for the current day."""
        today = datetime.now().strftime('%Y-%m-%d')
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()
            query = """
                SELECT TICKER, RSI_1MIN, RSI_5MIN FROM TradeParameters
                WHERE DATE = ?
            """
            c.execute(query, (today,))
            rows = c.fetchall()
            
            if not rows:
                self.logger.warning(f"No tickers found in the database for date {today}.")
            else:
                self.logger.info(f"Fetched tickers: {rows}")
                
        except Exception as e:    
            self.logger.error(f"Error fetching tickers from db: {e}")
        finally:
            if conn:
                conn.close() 

        current_tickers = set(self.tickers.keys())          
        for row in rows:
            ticker, rsi_1m, rsi_5m = row
            if ticker not in current_tickers:
                self.add_ticker(ticker, rsi_1m, rsi_5m)
                

    def handle_ticker_update(self, message):
        """Handle ticker updates via WebSocket."""
        ticker = message['ticker']
        action = message['action']
        if action == 'add':
            rsi_1m = message['rsi_1m']
            rsi_5m = message['rsi_5m']
            self.add_ticker(ticker, rsi_1m, rsi_5m)

    def generate_trade_id(self):
        self.trade_id_counter += 1
        return f"SL_{self.trade_id_counter}"  # SL for StrategyLogic
    
    def generate_auto_id(self):
        self.trade_id_counter += 1
        return f"AS_{self.trade_id_counter}"  # SL for Auto signals

    def add_ticker(self, ticker, rsi_1m, rsi_5m):
        with self.lock:
            # Define strategy types you are using
            strategies = ['1Min', '1Minco', '1Minde', '5Min', '5Minde']
            for strategy in strategies:
                key = f"{ticker}_{strategy}"
                if key in self.ticker_threads and self.ticker_threads[key].is_alive():
                    self.logger.warning(f"Ticker {ticker} with strategy {strategy} is already being processed.")
                else:
                    self.tickers[ticker] = {
                        'rsi_1m': rsi_1m,
                        'rsi_5m': rsi_5m,
                        'active_trades': {},  # Track active trades by trade ID
                    }
                    self.logger.info(f"Added ticker {ticker} with data: {self.tickers[ticker]}")
                    ticker_thread = threading.Thread(target=self.run_ticker, args=(ticker, strategy), daemon=True)
                    self.ticker_threads[key] = ticker_thread
                    ticker_thread.start()

    def get_data_from_db(self, ticker, table_name, retries=3, delay=5):
        """Fetch data from the database with retry mechanism."""
        self.logger.info(f"Fetching data for {ticker} from {table_name}")
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')  # Correct database name

            today = datetime.now().strftime('%Y/%m/%d')  # Get today's date in the required format

            query = f"""
                SELECT * FROM {table_name}
                WHERE ticker = ? AND substr(timestamp, 1, 10) = ?
            """
            params = (ticker.upper(), today)

            for attempt in range(retries):
                data = pd.read_sql(query, conn, params=params)
                if not data.empty:
                    data['timestamp'] = pd.to_datetime(data['timestamp'], format='%Y/%m/%d-%H:%M')
                    data.sort_values('timestamp', inplace=True)
                    conn.close()
                    self.logger.info(f"Data fetched successfully for {ticker} from {table_name}")
                    return data
                else:
                    self.logger.warning(f"Attempt {attempt+1}: No data found for {ticker} in {table_name}")
                    tm.sleep(delay)
        
            self.logger.error(f"No data found for {ticker} in {table_name} after {retries} attempts.")
        except sqlite3.OperationalError as e:
            self.logger.error(f"OperationalError: {e}")
        except Exception as e:
            self.logger.error(f"Error retrieving data from {table_name}: {e}")
        finally:
            if conn:
                conn.close()    
        return None
    
    

    
    def get_high_from_trade_parameters(self, ticker):
        """Fetch the high value from TradeParameters table for the current day."""
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            today = datetime.now().strftime('%Y-%m-%d')  # Today's date

            query = """
               SELECT HIGH FROM TradeParameters
               WHERE TICKER = ? AND DATE = ?
            """
            c.execute(query, (ticker.upper(), today))
            high = c.fetchone()

            if high:
                return high[0]
            else:
                self.logger.warning(f"No high value found for {ticker} in TradeParameters.")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching high from TradeParameters: {e}")
            return None
        finally:
            if conn:
                conn.close()
                
                
    
            


    def process_market_data(self, ticker, strategy_type):
        """Fetch and process market data based on the strategy type."""
        # Determine the table name based on the strategy type
        if strategy_type in ['1Min', '1Minco', '1Minde', '1Minco_consolidation', '1Minco_trendbreak']:
            table_name = 'ohlc_1min'
        elif strategy_type in ['5Min', '5Minde', '5Min_consolidation', '5Min_trendbreak']:
            table_name = 'ohlc_5min'
        else:
            self.logger.error(f"Unsupported strategy type: {strategy_type}")
            return
        self.logger.info(f"Fetching data for {ticker} from {table_name}")

        data = self.get_data_from_db(ticker, table_name)

        if data is not None and not data.empty:
            self.logger.info(f"Processing {strategy_type} market data for {ticker}")
            try:
                # Calculate indicators
                indicators = self.calculate_indicators(data, strategy_type)
                if indicators is None:
                    self.logger.warning(f"Failed to calculate indicators for {ticker} - {strategy_type}")
                    return

                # Fire auto signals after indicators are calculated
                
                self.fire_auto_signals(indicators, ticker, strategy_type)

            except Exception as e:
                self.logger.error(f"Error processing market data for {ticker}: {str(e)}")
        else:
            self.logger.error(f"No data found for {ticker} in {table_name}")

    

    def calculate_indicators(self, data, strategy_type):
        try:
            if strategy_type in ['1Min', '1Minco', '1Minde', '1Minco_consolidation', '1Minco_trendbreak']:
                return self.calculate_1min_indicators(data)
            elif strategy_type in ['5Min', '5Minde', '5Min_consolidation', '5Min_trendbreak']:
                return self.calculate_5min_indicators(data)
            else:
                raise ValueError(f"Invalid strategy type: {strategy_type}. Use '1Min', '1Minco', '1Minde', '5Min', or '5Minde' (with _consolidation/_trendbreak).")
        except Exception as e:
            self.logger.error(f"Error calculating indicators for {strategy_type}: {e}")
            return None

    def calculate_1min_indicators(self, data):
        try:
            
            data = self.calculate_ema(data, 10)
            data = self.calculate_ema(data, 20)
            data = self.calculate_sma(data, 10)  # Add SMA calculation
            data = self.calculate_sma(data, 20)  # Add SMA calculation
            
           
            
            return data
        except Exception as e:
            self.logger.error(f"Error calculating 1-minute indicators: {e}")
            return None

    def calculate_5min_indicators(self, data):
        try:
            
            data = self.calculate_ema(data, 10)
            data = self.calculate_ema(data, 20)
            
            
            return data
        except Exception as e:
            self.logger.error(f"Error calculating 5-minute indicators: {e}")
            
            return None
        
        
    def calculate_vwap(self, data):
        """
        Calculate the Volume Weighted Average Price (VWAP) for the given data.
        """
        try:
             # Ensure necessary columns are present
            if 'close' not in data.columns or 'volume' not in data.columns:
                self.logger.error("Missing 'close' or 'volume' columns for VWAP calculation.")
                return None
        
            # Calculate VWAP
            data['vwap_cumulative'] = (data['close'] * data['volume']).cumsum()
            data['volume_cumulative'] = data['volume'].cumsum()
            data['vwap'] = data['vwap_cumulative'] / data['volume_cumulative']
            return data
        except Exception as e:
            self.logger.error(f"Error calculating VWAP: {e}")
            return None   
        
        
   

    def calculate_sma(self, data, window):
        try:
            data[f'sma_{window}'] = data['close'].rolling(window=window).mean()
            return data
        except Exception as e:
            self.logger.error(f"Error calculating SMA: {e}")
            return data      
        
        
    def calculate_ema(self, data, window):
        """
        Calculate the Exponential Moving Average (EMA) for a given window.
        """
        try: 
            data[f'ema_{window}'] = data['close'].ewm(span=window, adjust=False).mean()
            return data
        except Exception as e:
            self.logger.error(f"Error calculating SMA: {e}")
            return data     

        
    
                
    def condition_ema_decline(self, row, previous_row, vwap):
        """Condition 1: EMA decline and price above VWAP."""
        ema_current = float(row['ema_10'])
        ema_previous = float(previous_row['ema_10'])
        price_current = float(row['close'])
        return ema_previous > ema_current and price_current > vwap
                
    
        
    
    def condition_sma_crossover(self, row, previous_row, vwap):
        """Condition 2: SMA crossover and price above VWAP."""
        sma10_current = float(row['sma_10'])
        sma10_previous = float(previous_row['sma_10'])
        sma20_current = float(row['sma_20'])
        sma20_previous = float(previous_row['sma_20'])
        price_current = float(row['close'])
        
       
        
        return (
            sma10_previous >= sma20_previous
            and sma10_current < sma20_current
            and price_current > vwap
        )
    
    def detect_consolidation_breakdown(self, data, ticker, strategy_type):
        """
        Detect a consolidation breakdown (price breaking below a range with volume confirmation).
        Returns True if a breakdown signal is detected, False otherwise.
        """
        try:
            # Ensure we have enough data (minimum 10 bars for consolidation detection)
            min_bars = 5 if '1Min' in strategy_type else 3  # Fewer bars for volatility
            if len(data) < min_bars:
                self.logger.warning(f"Not enough data for {ticker} to detect consolidation.")
                return False, None

            # Define consolidation range (look for low volatility and tight range over last 5–10 bars)
            recent_data = data.tail(10)  # Use last 10 bars for consolidation detection
            high_range = recent_data['high'].max()
            low_range = recent_data['low'].min()
            range_size = high_range - low_range

            # Check if range is tight (e.g., < 5% of price or a fixed amount like $0.10 for small caps)
            current_price = float(data.iloc[-1]['close'])
            if range_size / current_price < 0.10:  # 5% range as a threshold
                # Check if price breaks below the consolidation low with volume confirmation
                current_low = float(data.iloc[-1]['low'])
                prev_low = float(data.iloc[-2]['low'])
                current_volume = float(data.iloc[-1]['volume'])
                avg_volume = data['volume'].tail(10).mean()

                if current_low < low_range and current_volume > avg_volume * 1.2:  # Volume spike > 1.5x average
                    # Confirm with a bearish candle (close < open)
                    current_close = float(data.iloc[-1]['close'])
                    current_open = float(data.iloc[-1]['open'])
                    if current_close < current_open:  # Bearish candle
                        self.logger.info(f"Consolidation breakdown detected for {ticker} at price {current_close} with volume {current_volume}")
                        return True, current_price
            return False, None
        except Exception as e:
            self.logger.error(f"Error detecting consolidation breakdown for {ticker}: {e}")
            return False, None
        
    def detect_significant_highs(self, data, height_threshold=0.05, distance=5):
        """
        Detect significant highs in the data using peak detection.
        """
        if 'high' not in data.columns or data['high'].isnull().any() or len(data['high'].dropna()) < 2:
            self.logger.warning(f"Invalid or insufficient 'high' data for peak detection in {data}")
            return np.array([])  # Return empty array to handle gracefully

        highs = data['high'].values
        try:
            peaks, _ = find_peaks(highs, height=highs.mean() * height_threshold, distance=distance)
            return peaks
        except Exception as e:
            self.logger.error(f"Error in find_peaks for {data}: {str(e)}")
            return np.array([])  # Fallback to empty array    

    def detect_trend_break(self, data, ticker, strategy_type):
        """
        Detect a trend break (price breaking below an uptrend line with volume confirmation).
        Returns True if a trend break signal is detected, False otherwise.
        """
        try:
            # Ensure we have enough data (dynamic based on time)
            current_time = data.iloc[-1]['timestamp'].time() if not data.empty else datetime.now().time()
            min_bars = 10 if current_time < time(10, 0) else 20  # Fewer bars pre-10:00 AM
            if len(data) < min_bars:
                self.logger.warning(f"Not enough data for {ticker} to detect trend break (need {min_bars} bars).")
                return False, None

            # Check required columns
            required_columns = ['high', 'low', 'volume', 'close', 'open']
            for col in required_columns:
                if col not in data.columns or data[col].isnull().any():
                    self.logger.error(f"Missing or invalid {col} data for {ticker}.")
                    return False, None

            # Calculate trendline (get two significant highs for uptrend using peak detection)
            recent_data = data.iloc[-20:]  # Use iloc for efficiency
            peak_indices = self.detect_significant_highs(recent_data, height_threshold=0.05, distance=5)
            if not peak_indices.size >= 2:  # Check if peak_indices is a numpy array with at least 2 elements
                self.logger.warning(f"Not enough significant highs to calculate trendline for {ticker}.")
                return False, None

            # Use the two most recent significant highs to draw a trendline
            high1_idx = peak_indices[-2]  # Second-most recent peak
            high2_idx = peak_indices[-1]  # Most recent peak
            high1 = float(recent_data.iloc[high1_idx]['high'])
            high2 = float(recent_data.iloc[high2_idx]['high'])
            time1 = recent_data.index.get_loc(high1_idx)
            time2 = recent_data.index.get_loc(high2_idx)
            slope = (high2 - high1) / (time2 - time1) if time2 != time1 else 0  # Avoid division by zero
            intercept = high1 - slope * time1

            # Current timestamp index
            current_idx = len(recent_data) - 1
            trendline_value = slope * current_idx + intercept

            # Check if price breaks below trendline with volume confirmation
            current_low = float(data.iloc[-1]['low']) if pd.notna(data.iloc[-1]['low']) else None
            if current_low is None:
                self.logger.warning(f"Invalid low value for {ticker}.")
                return False, None

            current_volume = float(data.iloc[-1]['volume']) if pd.notna(data.iloc[-1]['volume']) else 0
            avg_volume = data['volume'].iloc[-10:].mean() if pd.notna(data['volume']).all() else 0
            if avg_volume == 0:
                self.logger.warning(f"No valid volume data for {ticker}.")
                return False, None

            # Adjust volume multiplier based on time and float (assumed nano float for TRNR)
            volume_multiplier = 1.2 if current_time < time(9, 45) or self.get_float_size(ticker) < 5e6 else 1.5
            if current_volume > avg_volume * volume_multiplier:
                # Confirm with a bearish candle (close < open, with body check)
                current_close = float(data.iloc[-1]['close']) if pd.notna(data.iloc[-1]['close']) else None
                current_open = float(data.iloc[-1]['open']) if pd.notna(data.iloc[-1]['open']) else None
                current_high = float(data.iloc[-1]['high']) if pd.notna(data.iloc[-1]['high']) else None
                if current_close is None or current_open is None or current_high is None:
                    self.logger.warning(f"Invalid close, open, or high value for {ticker}.")
                    return False, None

                body_size = abs(current_close - current_open)
                total_range = current_high - current_low if current_low is not None else 0
                if current_close < current_open and body_size > total_range * 0.3:  # Significant bearish body
                    self.logger.info(f"Trend break detected for {ticker} at price {current_close} below trendline {trendline_value}")
                    return True, current_close
            return False, None
        except Exception as e:
            self.logger.error(f"Error detecting trend break for {ticker}: {str(e)}")  # Log full exception for clarity
            return False, None
        
    def condition_ema_deviation(self, row, previous_row, ema_deviation_flag):
        """
        Condition: EMA deviation > 10% and trigger on the next red candle.

        Args:
            row: Current candle data.
            previous_row: Previous candle data.
            ema_deviation_flag: Boolean flag indicating if EMA deviation condition was met.

        Returns:
            tuple: (trigger_signal, updated_ema_deviation_flag)
        """
        current_price = float(row['close'])
        current_open = float(row['open'])
        ema10_current = float(row['ema_10'])

        # Calculate EMA deviation
        ema_deviation = ((current_price - ema10_current) / ema10_current) * 100

        if ema_deviation > 10:
            # Set the flag if EMA deviation > 10 is observed
            return False, True

        if ema_deviation_flag and current_price < current_open:
            # Trigger signal if a red candle is observed after EMA deviation
            return True, False

        # Default: No signal triggered, retain the current flag status
        return False, ema_deviation_flag    
    
    
    def check_active_trade_status(self, ticker, strategy):
        """
        Check if there is an active trade ('open' or 'pending') for the given ticker and strategy.
        """
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            query = """
                SELECT active_trade
                FROM TradeStatus
                WHERE ticker = ? AND strategy = ? AND date = ?
                ORDER BY id DESC
                LIMIT 1
            """
            date = datetime.now().strftime('%Y-%m-%d')  # Current date
            c = conn.cursor()
            c.execute(query, (ticker, strategy, date))
            result = c.fetchone()
            if result:
                return result[0]  # Return the active_trade status (e.g., 'open', 'pending', 'closed')
            return None  # No record found
        except Exception as e:
            self.logger.error(f"Error checking active trade status for {ticker} ({strategy}): {e}")
            return None
        finally:
            if conn:
                conn.close()  
                
    def get_risk_parameters(self, ticker, date):
        try:
            conn = sqlite3.connect('EOD_data.db')
            query = """
                SELECT RISK_1MIN, RISK_5MIN, ACCOUNT_EQUITY FROM TradeParameters
                WHERE TICKER = ? AND DATE = ?
            """
            c = conn.cursor()
            c.execute(query, (ticker, date))
            row = c.fetchone()
            
            if row:
                risk_1min, risk_5min, account_equity = row
            else:
                self.logger.warning(f"No record found for ticker: {ticker} on date: {date}")
                conn.close()
                return None  # No data found, return None


            
            # If account equity is missing, fetch the most recent non-null ACCOUNT_EQUITY before the given date
            if account_equity is None:
                c.execute("""
                    SELECT ACCOUNT_EQUITY FROM TradeParameters
                    WHERE ACCOUNT_EQUITY IS NOT NULL AND DATE < ?
                    ORDER BY DATE DESC LIMIT 1
                """, (date,))
                equity_row = c.fetchone()
                if equity_row and equity_row[0] is not None:
                    account_equity = equity_row[0]
                    self.logger.info(f"Found previous ACCOUNT_EQUITY {account_equity} from an earlier row")
                else:
                    self.logger.error(f"Could not find a valid ACCOUNT_EQUITY before {date}")
                    conn.close()
                    return None  # Still no valid account equity found

            conn.close()
            return {'risk_1min': risk_1min, 'risk_5min': risk_5min, 'account_equity': account_equity}

        except Exception as e:
            self.logger.error(f"Error fetching risk parameters: {e}")
            return None                                    
             
                
    def fire_auto_signals(self, data, ticker, strategy_type, trade_date):
        try:
            self.logger.info(f"Starting fire_auto_signals for {ticker} with strategy {strategy_type}")
            signals = []
            start_time = time(9, 30)
            

            # Calculate VWAP from data
            data = self.calculate_vwap(data)
            if data is None:
                self.logger.error(f"Failed to calculate VWAP for {ticker}. Skipping signal generation.")
                return signals

            # Check active trade status
            active_trade_status = self.check_active_trade_status(ticker, strategy_type)
            
            if active_trade_status in ["open", "pending"]:  # Skip if a trade is active
                
                return signals

            ema_deviation_flag = False  # Flag to track EMA deviation for `5Minde`
  
            # Loop through data rows
            for i in range(1, len(data)):
                

                # Fetch current and previous row
                try:
                    row = data.iloc[i]
                    previous_row = data.iloc[i - 1]
                    
                except Exception as e:
                    self.logger.error(f"Error accessing rows {i} and {i-1} for {ticker}: {e}")
                    continue

                # Signal time
                try:
                    signal_time = row['timestamp'].time()
                    
                except KeyError as e:
                    self.logger.error(f"Missing 'timestamp' in row {i} for {ticker}: {e}")
                    continue

                # High of Day (HOD)
                try:
                    hod = round(float(data['high'][:i + 1].max()), 2)
                    
                    
                    
                except KeyError as e:
                    self.logger.error(f"Missing 'high' column in data for {ticker}: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error calculating HOD for {ticker} at row {i}: {e}")
                    continue
                
                # VWAP for the current row
                vwap = row['vwap']


                # Ensure signal timestamp is within the trading window
                if not (start_time <= signal_time <= self.trading_end):
                    
                    continue

                # Evaluate conditions
                try:
                    consolidation_break, entry_price = self.detect_consolidation_breakdown(data, ticker, strategy_type)
                    if consolidation_break and entry_price:
                        signal = self.generate_signal(row, ticker, f"{strategy_type}_consolidation", vwap, hod, trade_date)
                        if signal:
                            signals.append(signal)
                            self.logger.info(f"Consolidation breakdown signal generated for {ticker} at {entry_price}")

                    # Evaluate trend break (continue even if consolidation signal fires)
                    trend_break, entry_price = self.detect_trend_break(data, ticker, strategy_type)
                    if trend_break and entry_price:
                        signal = self.generate_signal(row, ticker, f"{strategy_type}_trendbreak", vwap, hod, trade_date)
                        if signal:
                            signals.append(signal)
                            self.logger.info(f"Trend break signal generated for {ticker} at {entry_price}")
                    
                    
                    if strategy_type == "5Min" and self.condition_ema_decline(row, previous_row, vwap):
                        self.logger.info(f"Evaluating 5Min EMA Decline condition for {ticker} at row {i}. Signal generated.")
                        signal = self.generate_signal(row, ticker, strategy_type, vwap, hod, trade_date)
                        signals.append(signal)
                        self.logger.info(f"5Min EMA Decline condition met for {ticker} at row {i}. Signal generated.")
                    
                    

                    if strategy_type == "1Minco" and self.condition_sma_crossover(row, previous_row, vwap):
                        self.logger.info(f"Evaluating 1Minco SMA CO condition for {ticker} at row {i}. Signal generated.")
                        signal = self.generate_signal(row, ticker, strategy_type, vwap, hod, trade_date)
                        signals.append(signal)
                        self.logger.info(f"1Min SMA Crossover condition met for {ticker} at row {i}. Signal generated.")

                    if strategy_type == "5Minde":
                        self.logger.info(f"Evaluating 5Min Price deviation condition for {ticker} at row {i}. Signal generated.")
                        trigger_signal, ema_deviation_flag = self.condition_ema_deviation(row, previous_row, ema_deviation_flag)
                        if trigger_signal:
                            signal = self.generate_signal(row, ticker, strategy_type, vwap, hod, trade_date)
                            signals.append(signal)
                            self.logger.info(f"5Minde EMA Deviation condition met for {ticker} at row {i}. Signal generated.")

                except Exception as e:
                    self.logger.error(f"Error evaluating conditions for {ticker} at row {i}: {e}")

            self.logger.info(f"Completed signal evaluation for {ticker}. Total signals fired: {len(signals)}")
            return signals
    
        except Exception as e:
            self.logger.error(f"Error firing signals for {ticker} ({strategy_type}): {e}")
            return []

        
        
    def generate_signal(self, row, ticker, strategy_type, vwap, hod, trade_date):
        """Generate and insert a signal."""
    
        signal = None  # Initialize signal variable

        try:
            # Get the current price
            price_current = float(row['close'])

            # Fetch risk parameters
            risk_params = self.get_risk_parameters(ticker, trade_date)
            if not risk_params:
                self.logger.error(f"No risk parameters found for {ticker} on {trade_date}")
                return None

            # Map strategies to their corresponding risk field
            strategy_risk_map = {
                '1Min': 'risk_1min',
                '1Minco': 'risk_1min',
                '1Minco_consolidation': 'risk_1min',
                '1Minco_trendbreak': 'risk_1min',
                '1Minde': 'risk_1min',
                '5Min': 'risk_5min',
                '5Minde': 'risk_5min',
                '5Min_consolidation': 'risk_5min',
                '5Min_trendbreak': 'risk_5min'
            }

            # Get the correct risk key based on strategy type
            risk_key = strategy_risk_map.get(strategy_type, None)

            if not risk_key:
                self.logger.error(f"Unsupported strategy type: {strategy_type}")
                return None

            # Check if risk parameters are present and valid
            if risk_key not in risk_params or risk_params[risk_key] is None:
                self.logger.error(f"Missing or invalid risk parameter '{risk_key}' for {ticker} on {trade_date}: {risk_params}")
                return None

            if 'account_equity' not in risk_params or risk_params['account_equity'] is None:
                self.logger.error(f"Missing or invalid account equity for {ticker} on {trade_date}")
                return None

            # Calculate risk percentage and amount
            risk_percentage = risk_params[risk_key] / 100
            risk_amount = float(risk_params['account_equity']) * risk_percentage

            # Calculate stop loss and price difference
            stop_loss = hod
            price_difference = abs(stop_loss - price_current)

            if price_difference == 0:
                self.logger.error(f"Stop loss equals current price for {ticker}. Cannot calculate shares.")
                return None

            # Calculate the number of shares to trade
            shares = int(risk_amount / price_difference)
            if shares <= 0:
                self.logger.error(f"Calculated shares for {ticker} is zero or negative. Risk amount: {risk_amount}, Price difference: {price_difference}")
                return None

            # Generate the signal
            trade_id = self.generate_auto_id()
            signal = {
                'trade_id': trade_id,
                'strategy': strategy_type,
                'ticker': ticker,
                'entry_price': round(price_current, 2),
                'vwap': vwap,
                'time': row['timestamp'],
                'hod': round(hod, 2),
                'shares': shares,
                'status': 'Fired'
            }

            # Insert the signal into the database
            if self.insert_trade_signal(signal):
                self.logger.info(f"Signal fired and inserted into DB: {signal}")
            else:
                self.logger.error(f"Failed to insert signal into DB: {signal}")

            return signal

        except Exception as e:
            self.logger.error(f"Error generating signal for {ticker}: {e}")
            return None

    
    
    
    
    def insert_trade_signal(self, signal):
        """Insert a new trade signal into the TradeSignal table without checking for duplicates."""
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            # Insert the new signal without checking for duplicates
            query_insert = """
                INSERT INTO TradeSignal (tradeID, time, strategy, ticker, price, vwap, shares, hi, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            signal_time = signal['time'].strftime('%Y-%m-%d %H:%M:%S')  # Convert Timestamp to string
            c.execute(query_insert, (
                signal['trade_id'],
                signal_time,  # Use the converted string
                signal['strategy'],
                signal['ticker'],
                signal['entry_price'],
                signal['vwap'],
                signal['shares'],
                signal['hod'],
                signal['status']  # Insert status directly from the signal
            ))

            conn.commit()
            self.logger.info(f"Trade signal inserted successfully: {signal}")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting trade signal: {e}")
            return False
        finally:
            if conn:
                conn.close()

        
    def evaluate_trade(self, signal):
        self.logger.info(f"Evaluating trade: {signal}")
        
        
        

    def run_ticker(self, ticker, strategy_type):
        with self.lock:
            # Check if the ticker is in the ticker_threads dictionary
            if ticker not in self.ticker_threads:
                self.ticker_threads[ticker] = {}

            # Check if the strategy is already being processed
            if strategy_type in self.ticker_threads[ticker]:
                if self.ticker_threads[ticker][strategy_type].is_alive():
                    self.logger.warning(f"Ticker {ticker} with strategy {strategy_type} is already being processed.")
                    return
            else:
                self.ticker_threads[ticker][strategy_type] = threading.current_thread()

        self.logger.info(f"Starting run_ticker for {ticker} with strategy {strategy_type}")
        
        # Define strategy groups
        strategies_1min = ['1Minco', '1Min', '1Minde', '1Minco_consolidation', '1Minco_trendbreak']
        strategies_5min = ['5Minde', '5Min', '5Min_consolidation', '5Min_trendbreak']

        try:
            while datetime.now().time() <= self.trading_end:
                current_time = datetime.now()

                # Process 1-minute data every minute and 4 seconds
                if current_time.second == 4:
                    for strategy in strategies_1min:
                        self.logger.info(f"Processing 1-minute market data for {ticker} with strategy {strategy}")
                        self.process_market_data(ticker, strategy)
                    

                # Process 5-minute strategies every 5 minutes and 4 seconds
                if current_time.minute % 5 == 0 and current_time.second == 4:
                    for strategy in strategies_5min:
                        self.logger.info(f"Processing 5-minute market data for {ticker} with strategy {strategy}")
                        self.process_market_data(ticker, strategy)
                    

                tm.sleep(1)
        except Exception as e:
            self.logger.error(f"Error running ticker {ticker} with strategy {strategy_type}: {e}")
        finally:
            with self.lock:
                # Mark the ticker-strategy as inactive
                if ticker in self.ticker_threads and strategy_type in self.ticker_threads[ticker]:
                    del self.ticker_threads[ticker][strategy_type]
                    self.logger.info(f"Finished processing {ticker} with strategy {strategy_type}")





if __name__ == "__main__":
    # Initialize StrategyLogic
    strategy_logic = StrategyLogic()

    # Prompt user for input
    while True:
        date = input("Enter the date (YYYY-MM-DD): ").strip()
        ticker = input("Enter the ticker: ").strip().upper()

        # Validate the date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
            break  # Exit the loop if valid inputs are provided
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")

    # Fetch and process data for the given ticker and date
    print(f"Fetching data for {ticker} on {date}...")
    conn = sqlite3.connect('EOD_data.db')
    try:
        
        # Define strategy groups, including new strategies
        strategies_1min = ['1Min', '1Minco', '1Minde', '1Minco_consolidation', '1Minco_trendbreak']
        strategies_5min = ['5Min', '5Minde', '5Min_consolidation', '5Min_trendbreak']

        # Process 1-minute data
        print("Processing 1-minute data...")
        query_1min = """
            SELECT * FROM ohlc_1min
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ?
        """
        formatted_date = date.replace('-', '/')
        data_1min = pd.read_sql(query_1min, conn, params=(ticker, formatted_date))

        if not data_1min.empty:
            data_1min['timestamp'] = pd.to_datetime(data_1min['timestamp'], format='%Y/%m/%d-%H:%M')
            data_1min.sort_values('timestamp', inplace=True)
            data_1min = strategy_logic.calculate_1min_indicators(data_1min)  # Calculate 1-min indicators

            # Fire signals for each 1-minute strategy
            for strategy in strategies_1min:
                signals = strategy_logic.fire_auto_signals(data_1min, ticker, strategy, date)
                if signals:
                    print(f"{strategy} Signals generated:")
                    for signal in signals:
                        print(signal)
                else:
                    print(f"No signals generated for {ticker} on {date} with strategy {strategy}.")
        else:
            print(f"No 1-minute data found for {ticker} on {date}. Skipping 1-minute processing.")

        # Process 5-minute data
        print("Processing 5-minute data...")
        query_5min = """
            SELECT * FROM ohlc_5min
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ?
        """
        data_5min = pd.read_sql(query_5min, conn, params=(ticker, formatted_date))

        if not data_5min.empty:
            data_5min['timestamp'] = pd.to_datetime(data_5min['timestamp'], format='%Y/%m/%d-%H:%M')
            data_5min.sort_values('timestamp', inplace=True)
            data_5min = strategy_logic.calculate_5min_indicators(data_5min)  # Calculate 5-min indicators

            # Fire signals for each 5-minute strategy
            for strategy in strategies_5min:
                signals = strategy_logic.fire_auto_signals(data_5min, ticker, strategy, date)
                if signals:
                    print(f"{strategy} Signals generated:")
                    for signal in signals:
                        print(signal)
                else:
                    print(f"No signals generated for {ticker} on {date} with strategy {strategy}.")
        else:
            print(f"No 5-minute data found for {ticker} on {date}. Skipping 5-minute processing.")
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        conn.close()

    # Exit after processing
    print("Script execution completed.")






