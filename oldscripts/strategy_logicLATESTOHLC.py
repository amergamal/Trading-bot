import time as tm
import pandas as pd
import logging
import sqlite3
from datetime import datetime, time
import threading
from flask_socketio import SocketIO, emit
from risk_management import RiskManagement
from order_execution import OrderExecution

class StrategyLogic:
    def __init__(self, risk_management=None, trading_end=time(23, 0)):  # risk_management parameter is optional
        self.logger = logging.getLogger('StrategyLogic')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.tickers = {}  # Store ticker information
        self.risk_management = risk_management  # Reference to risk management module
        self.trade_id_counter = 0  # Counter for generating unique trade IDs
        self.trading_end = trading_end  # End of trading time

        # Initialize WebSocket client
        self.socketio = SocketIO(message_queue='redis://')
        self.socketio.on_event('ticker_update', self.handle_ticker_update)

        self.last_processed_timestamps = {}  # Track last processed timestamp for each ticker
        
        # Add a lock for synchronizing access to the database
        self.lock = threading.Lock()

    def fetch_tickers_from_db(self):
        """Fetch tickers from the TradeParameters table for the current day."""
        today = datetime.now().strftime('%Y-%m-%d')
        conn = None
        try:
            conn = sqlite3.connect('test_data.db')
            c = conn.cursor()
            query = """
                SELECT TICKER, RSI_1MIN, RSI_5MIN FROM TradeParameters
                WHERE DATE = ?
            """
            c.execute(query, (today,))
            rows = c.fetchall()
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
                ticker_thread = threading.Thread(target=self.run_ticker, args=(ticker,))
                ticker_thread.start()

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
        self.tickers[ticker] = {
            'rsi_1m': rsi_1m,
            'rsi_5m': rsi_5m,
            'active_trades': {},  # Track active trades by trade ID
        }
        self.logger.info(f"Added ticker {ticker} to self.tickers.")
        threading.Thread(target=self.run_ticker, args=(ticker,), daemon=True).start()

    def get_data_from_db(self, ticker, table_name, retries=3, delay=5):
        """Fetch data from the database with retry mechanism."""
        self.logger.info(f"Fetching data for {ticker} from {table_name}")
        conn = None
        try:
            conn = sqlite3.connect('test_data.db')  # Correct database name

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
            conn = sqlite3.connect('test_data.db')
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
            conn = sqlite3.connect('test_data.db')
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
        table_name = 'latest_ohlc_1min' if strategy_type == '1Min' else 'latest_ohlc_5min'
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

                signals = self.fire_signals(indicators, strategy_params, ticker, strategy_type, highest_atr)
                for signal in signals:
                    self.evaluate_trade(signal)

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

    
    def fire_signals(self, data, strategy_params, ticker, strategy_type, highest_atr):
        try:
            signals = []
            start_time = time(9, 30)

            rsi_threshold = float(strategy_params['rsi'])  # Ensure RSI threshold is a float
            atr_percentile_threshold = float(strategy_params['atr_percentile'])  # Ensure ATR percentile is a float
            
            # Fetch HIGH and VWAP value from TradeParameters
            hod = self.get_high_from_trade_parameters(ticker)
            vwap = self.get_vwap_from_trade_parameters(ticker)  # Fetch VWAP from TradeParameters

            # Ensure hod and vwap are floats, if they are not None
            if hod is not None:
                hod = float(hod)
            if vwap is not None:
                vwap = float(vwap)
            
            for i in range(1, len(data)):
                row = data.iloc[i]
                current_time = row['timestamp'].time()
                previous_row = data.iloc[i - 1]

                # Ensure the comparison values are numeric
                macd_diff_previous = float(previous_row['macd_diff'])
                macd_diff_current = float(row['macd_diff'])
                rsi_current = float(row['rsi'])
                price_current = float(row['close'])
                atr_percentile_current = float(row['atr_percentile'])
                
                

                if hod is not None and vwap is not None and (
                
                    macd_diff_previous > 0 and macd_diff_current < 0 and
                    rsi_current > rsi_threshold and
                    price_current > vwap and
                    atr_percentile_current > atr_percentile_threshold and
                    start_time <= current_time <= self.trading_end):
                    
                    trade_id = self.generate_trade_id()
                    signal = {
                        'trade_id': trade_id,
                        'strategy': strategy_type,
                        'ticker': ticker,
                        'entry_price': round(price_current, 2),
                        'atr_percentile': row['atr_percentile'],
                        'rsi': round(rsi_current),
                        'time': row['timestamp'],
                        'hod': hod,
			            'highest_atr': round(highest_atr, 2),
                        'status': 'Fired'  # Add status directly to the signal
                    }
                    self.logger.info(f'Generated signal: {signal}')
                    signals.append(signal)

                    # Insert the signal into the TradeSignal table with 'Fired' status
                    if self.insert_trade_signal(signal):
                        # Send the signal to the Risk Management module
                        try:
                            self.risk_management.receive_signal(signal)
                            self.logger.info(f'Signal successfully sent to RiskManagement: {signal}')
                        except Exception as e:
                            self.logger.error(f"Error sending signal to RiskManagement: {e}")
                    else:
                        self.logger.error(f"Signal {signal['trade_id']} not sent to RiskManagement due to DB insertion failure")        

            return signals
        except Exception as e:
            self.logger.error(f"Error firing signals: {e}")
            return []

    
    def insert_trade_signal(self, signal):
        """Insert a new trade signal into the TradeSignal table."""
        conn = None
        try:
            conn = sqlite3.connect('test_data.db')
            c = conn.cursor()
            query_check = """
                SELECT COUNT(*) FROM TradeSignal
                WHERE ticker = ? AND strategy = ? AND time = ? AND price = ? AND hi = ? AND rsi = ?
            """
            c.execute(query_check, (signal['ticker'], signal['strategy'], signal['time'].strftime('%Y-%m-%d %H:%M:%S'), 
                                    signal['entry_price'], signal['hod'], signal['rsi']))
            count = c.fetchone()[0]

            if count == 0:
                query_insert = """
                    INSERT INTO TradeSignal (tradeID, time, strategy, ticker, price, rsi, atr_highest, atr_percentile, hi, status)
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
	                signal['highest_atr'],	
                    signal['atr_percentile'],
                    signal['hod'],
                    signal['status']  # Insert status directly from the signal
                ))
                conn.commit()
                return True
            else:
                self.logger.error("Duplicate signal found, not inserted.")
                return False
        except Exception as e:
            self.logger.error(f"Error inserting trade signal: {e}")
            return False
        finally:
            if conn:
                conn.close()
        
    def evaluate_trade(self, signal):
        self.logger.info(f"Evaluating trade: {signal}")

    def run_ticker(self, ticker):
        self.logger.info(f"Starting run_ticker for {ticker}")
        tm.sleep(10)  # Wait for 10 seconds before starting data processing

        while datetime.now().time() <= self.trading_end:
            current_time = datetime.now()

            # Process 1-minute data every minute and 4 seconds
            if current_time.second == 4:
                self.logger.info(f"Processing 1-minute market data for {ticker}")
                self.process_market_data(ticker, '1Min')

            # Process 5-minute data every 5 minutes and 4 seconds
            if current_time.minute % 5 == 0 and current_time.second == 4:
                self.logger.info(f"Processing 5-minute market data for {ticker}")
                self.process_market_data(ticker, '5Min')

            # Sleep for 1 second to reduce CPU usage
            tm.sleep(1)

        self.logger.info(f"Ending run_ticker for {ticker}")

#Initialize the OrderExecution instance 
order_execution = OrderExecution()  # Pass appropriate arguments if needed
risk_management = RiskManagement(order_execution) 

if __name__ == "__main__":
    #Initialize StrategyLogic with the risk_management instance
    strategy_logic = StrategyLogic(risk_management)

    # Fetch tickers and start processing them in separate threads
    strategy_logic.fetch_tickers_from_db()
    for ticker in strategy_logic.tickers.keys():
        ticker_thread = threading.Thread(target=strategy_logic.run_ticker, args=(ticker,))
        ticker_thread.start()
