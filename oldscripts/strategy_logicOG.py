import time as tm
import pandas as pd
import logging
import sqlite3
from datetime import datetime, time
import threading
from flask_socketio import SocketIO, emit
from risk_management import RiskManagement
from order_execution import OrderExecution
from trade_monitor import TradeMonitor
from position_add import PositionAdd

class StrategyLogic:
    def __init__(self, position_add, risk_management, trading_end=time(13, 0)):  # risk_management parameter is optional
        self.logger = logging.getLogger('StrategyLogic')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.tickers = {}  # Store ticker information
        self.position_add = position_add
        self.risk_management = risk_management  # Reference to risk management module
        self.trade_id_counter = 0  # Counter for generating unique trade IDs
        self.trading_end = trading_end
        self.pause_1min_strategy = False  # Control flag to pause 1-minute strategy

        # Initialize WebSocket client
        self.socketio = SocketIO(message_queue='redis://')
        self.socketio.on_event('ticker_update', self.handle_ticker_update)

        self.last_processed_timestamps = {}  # Track last processed timestamp for each ticker
        
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
        return self.trade_id_counter

    def add_ticker(self, ticker, rsi_1m, rsi_5m):
        with self.lock:
            # Define strategy types you are using
            strategies = ['1Min', '5Min']
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
    
    def pause_1min(self):
        """Pause the 1-minute strategy."""
        self.pause_1min_strategy = True
        self.logger.info("Paused 1-minute strategy.")

    def resume_1min(self):
        """Resume the 1-minute strategy."""
        self.pause_1min_strategy = False
        self.logger.info("Resumed 1-minute strategy.")

    
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
                
                
    def get_vwap_from_trade_parameters(self, ticker):
        """Fetch the VWAP value from TradeParameters table for the current day."""
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            today = datetime.now().strftime('%Y-%m-%d')  # Today's date

            query = """
               SELECT VWAP FROM TradeParameters
               WHERE TICKER = ? AND DATE = ?
            """
            c.execute(query, (ticker.upper(), today))
            vwap = c.fetchone()

            if vwap:
                return vwap[0]
            else:
                self.logger.warning(f"No VWAP value found for {ticker} in TradeParameters.")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching VWAP from TradeParameters: {e}")
            return None
        finally:
            if conn:
                conn.close()
            


    def process_market_data(self, ticker, strategy_type):
        """Fetch and process market data based on the strategy type."""
        table_name = 'ohlc_1min' if strategy_type == '1Min' else 'ohlc_5min'
        self.logger.info(f"Fetching data for {ticker} from {table_name}")

        data = self.get_data_from_db(ticker, table_name)

        if data is not None and not data.empty:
            self.logger.info(f"Processing {strategy_type} market data for {ticker}")
            self._process_market_data(ticker, data, strategy_type)
            self.last_processed_timestamps[ticker] = data['timestamp'].max()
        else:
            self.logger.error(f"No data found for {ticker} in {table_name}")

    def _process_market_data(self, ticker, market_data, strategy_type):
        try:
            indicators = self.calculate_indicators(market_data, strategy_type)
            if indicators is not None:
                rsi_key = 'rsi_1m' if strategy_type == '1Min' else 'rsi_5m'
                strategy_params = {'rsi': self.tickers[ticker][rsi_key], 'atr_percentile': 50}
                
                # Calculate highest ATR
                highest_atr = self.calculate_highest_atr([market_data])

                
            else:
                self.logger.warning(f"Indicators calculation failed for {ticker} - {strategy_type}")
        except KeyError as e:
            self.logger.error(f"KeyError processing market data for {ticker}: {e}")
        except Exception as e:
            error_message = str(e)  # Capture the error message
            self.logger.error(f"Error processing market data for {ticker}: {error_message}")

    def calculate_indicators(self, data, strategy_type):
        try:
            if strategy_type == '1Min':
                return self.calculate_1min_indicators(data)
            elif strategy_type == '5Min':
                return self.calculate_5min_indicators(data)
            else:
                raise ValueError("Invalid strategy type. Use '1Min' or '5Min'.")
        except Exception as e:
            self.logger.error(f"Error calculating indicators for {strategy_type}: {e}")
            return None

    def calculate_1min_indicators(self, data):
        try:
            data = self.calculate_rsi(data, 14)  # Smaller window size for mock data
            data = self.calculate_macd(data, 3, 8, 16)
            data = self.calculate_atr(data, 7) 
            highest_atr_value = self.calculate_highest_atr([data])
            data = self.calculate_atr_percentile(data, highest_atr_value)
            
            return data
        except Exception as e:
            self.logger.error(f"Error calculating 1-minute indicators: {e}")
            return None

    def calculate_5min_indicators(self, data):
        try:
            data = self.calculate_rsi(data, 14)  # Smaller window size for mock data
            data = self.calculate_macd(data, 3, 8, 5)
            data = self.calculate_atr(data, 7)
            highest_atr_value = self.calculate_highest_atr([data])
            data = self.calculate_atr_percentile(data, highest_atr_value)
            
            return data
        except Exception as e:
            self.logger.error(f"Error calculating 5-minute indicators: {e}")
            return None

    def calculate_rsi(self, data, length):
        try:
            delta = data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
            rs = gain / loss
            data['rsi'] = 100 - (100 / (1 + rs))
            return data
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {e}")
            return data

    def calculate_macd(self, df, fast_length, slow_length, signal_length):
        try:
            df['fast_ema'] = df['close'].ewm(span=fast_length, adjust=False).mean()
            df['slow_ema'] = df['close'].ewm(span=slow_length, adjust=False).mean()
            df['macd'] = df['fast_ema'] - df['slow_ema']
            df['signal_line'] = df['macd'].ewm(span=signal_length, adjust=False).mean()
            df['macd_diff'] = df['macd'] - df['signal_line']
            return df
        except Exception as e:
            self.logger.error(f"Error calculating MACD: {e}")
            return df

    def calculate_atr(self, df, window=7):
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = abs(df['high'] - df['close'].shift())
        df['low_close'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['tr'].ewm(span=window, adjust=False).mean()
        return df

    def calculate_highest_atr(self, prices):
        highest_atr = 0
        for price_data in prices:
            price_data = self.calculate_atr(price_data)  # Ensure ATR is calculated
            atr = price_data['atr'].max()  # Get the highest ATR value in the DataFrame
            highest_atr = max(highest_atr, atr)
        return highest_atr

    def calculate_atr_percentile(self, data, highest_atr):
        try:
            data['atr_percentile'] = (data['atr'] / highest_atr) * 100
            return data
        except Exception as e:
            self.logger.error(f"Error calculating ATR percentile: {e}")
            return data

    
    def fire_signals(self, ticker, entry_price, hod, shares, strategy_type, risk, s_loss):
        try:
            
            trade_id = self.generate_trade_id()

            # Static values for atr_percentile and rsi
            atr_percentile = 1
            rsi = 1
        
            # Signal creation
            signal = {
                'trade_id': trade_id,
                'strategy': strategy_type,  # Now using the passed strategy type (1Min or 5Min)
                'ticker': ticker,
                'entry_price': round(float(entry_price), 2),
                'atr_percentile': atr_percentile,
                'rsi': rsi,
                'time': datetime.now(),
                'hod': round(float(hod), 2),
                'shares': round(float(shares), 2),
                'risk': (float(risk)),
                's_loss': (float(s_loss)),
                'status': 'Fired'
            }
            
            self.logger.info(f'Generated signal: {signal}')
            

            # Insert the signal into the TradeSignal table with 'Fired' status
            if self.insert_trade_signal(signal):
                self.logger.info(f"Signal {signal['trade_id']} successfully inserted into the TradeSignal table.")
                
            else:
                self.logger.error(f"Failed to insert signal {signal['trade_id']} into the TradeSignal table.")        
                
            # Send to position_add if strategy is 'Add', otherwise send to Risk Management
            if signal['strategy'] == 'Add':
                try:
                    self.position_add.receive_signal(signal)  # Sending to position_add.receive_signal
                    self.logger.info(f'Signal successfully sent to PositionAdd: {signal}')
                except Exception as e:
                    self.logger.error(f"Error sending signal to PositionAdd: {e}")
      
            elif signal['strategy'] in ['1Min', 'limit']:
                try:
                    self.risk_management.receive_signal(signal)
                    self.logger.info(f'Signal successfully sent to RiskManagement: {signal}')
                except Exception as e:
                    self.logger.error(f"Error sending signal to RiskManagement: {e}")
            

            return signal
        except Exception as e:
            self.logger.error(f"Error firing signals: {e}")
            raise e

    
    
    def insert_trade_signal(self, signal):
        """Insert a new trade signal into the TradeSignal table without checking for duplicates."""
        conn = None
        try:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            # Insert the new signal without checking for duplicates
            query_insert = """
                INSERT INTO TradeSignal (tradeID, time, strategy, ticker, price, rsi, shares, atr_percentile, hi, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            signal_time = signal['time'].strftime('%Y-%m-%d %H:%M:%S')  # Convert Timestamp to string
            c.execute(query_insert, (
                signal['trade_id'],
                signal_time,  # Use the converted string
                signal['strategy'],
                signal['ticker'],
                signal['entry_price'],
                signal['rsi'],
                signal['shares'],
                signal['atr_percentile'],
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

        try:
            while datetime.now().time() <= self.trading_end:
                current_time = datetime.now()

                # Process 1-minute data every minute and 4 seconds
                if strategy_type == '1Min' and current_time.second == 4 and not self.pause_1min_strategy:
                    self.logger.info(f"Processing 1-minute market data for {ticker}")
                    self.process_market_data(ticker, '1Min')

                # Process 5-minute data every 5 minutes and 4 seconds
                if strategy_type == '5Min' and current_time.minute % 5 == 0 and current_time.second == 4:
                    self.logger.info(f"Processing 5-minute market data for {ticker}")
                    self.process_market_data(ticker, '5Min')

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
    # Initialize the TradeMonitor instance
    logging.info("Initializing TradeMonitor in app.py")
    trade_monitor = TradeMonitor()

    # Initialize OrderExecution with TradeMonitor
    logging.info("Initializing OrderExecution with TradeMonitor in app.py")
    order_execution = OrderExecution(trade_monitor)

    # Initialize PositionAdd with OrderExecution
    position_add = PositionAdd(order_execution)

    # Initialize RiskManagement with OrderExecution
    risk_management = RiskManagement(order_execution)

    # Initialize StrategyLogic with both PositionAdd and RiskManagement
    strategy_logic = StrategyLogic(position_add=position_add, risk_management=risk_management)

    # Fetch tickers and start processing them
    strategy_logic.fetch_tickers_from_db()
        
    # Pause the 1-minute strategy
    #strategy_logic.pause_1min()

                
    # Check and list all active threads
    for thread in threading.enumerate():
        print(thread.name)    

    # Keep the main thread alive
    try:
        while True:
            tm.sleep(1)  # Keep the main thread running to prevent the script from exiting
    except KeyboardInterrupt:
        print("Terminating script...")

