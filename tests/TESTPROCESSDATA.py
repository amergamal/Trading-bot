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
            strategies = ['1Min', '1Minco', '1Minde' '5Min', '5Minde']
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
        # Determine the table name based on the strategy type
        if strategy_type in ['1Min', '1Minco', '1Minde']:
            table_name = 'ohlc_1min'
        elif strategy_type in ['5Min', '5Minde']:
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
            if strategy_type in ['1Min', '1Minco', '1Minde']:
                return self.calculate_1min_indicators(data)
            elif strategy_type in ['5Min', '5Minde']:
                return self.calculate_5min_indicators(data)
            else:
                raise ValueError("Invalid strategy type. Use '1Min' or '5Min'.")
        except Exception as e:
            self.logger.error(f"Error calculating indicators for {strategy_type}: {e}")
            return None

    def calculate_1min_indicators(self, data):
        try:
            
            data = self.calculate_ema(data, 10)
            data = self.calculate_ema(data, 20)
            
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
                
                
    def condition_ema_decline(self, row, previous_row, vwap):
        """Condition 1: EMA decline and price above VWAP."""
        ema_current = float(row['ema_10'])
        ema_previous = float(previous_row['ema_10'])
        price_current = float(row['close'])
        
        ema_decline = ema_previous > ema_current
        price_above_vwap = price_current > vwap
    
        # Log the evaluation of each condition
        self.logger.info(
            f"Evaluating EMA Decline condition for row {row['id']}."
            f"\nEMA Previous: {ema_previous}, EMA Current: {ema_current}, Decline: {ema_decline}."
            f"\nPrice Current: {price_current}, VWAP: {vwap}, Price > VWAP: {price_above_vwap}."
        )
    
        # Log if specific conditions are met or not
        if ema_decline and not price_above_vwap:
            self.logger.info("EMA is declining, but the price is not above VWAP.")
        elif price_above_vwap and not ema_decline:
            self.logger.info("Price is above VWAP, but EMA is not declining.")
        elif ema_decline and price_above_vwap:
            self.logger.info("Both EMA is declining and price is above VWAP.")
    
        # Return the combined condition
        return ema_decline and price_above_vwap
    
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
            
            # If account equity is missing, fetch the most recent non-null ACCOUNT_EQUITY before the given date
            if row and row[2] is None:
                c.execute("""
                    SELECT ACCOUNT_EQUITY FROM TradeParameters
                    WHERE ACCOUNT_EQUITY IS NOT NULL AND DATE < ?
                    ORDER BY DATE DESC LIMIT 1
                """, (date,))
                equity_row = c.fetchone()
                account_equity = equity_row[0] if equity_row else None
            else:
                account_equity = row[2] if row else None
            conn.close()
            if row:
                return {'risk_1min': row[0], 'risk_5min': row[1], 'account_equity': row[2]}
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error fetching risk parameters: {e}")
            return None             
        
        
    def fire_auto_signals(self, data, ticker, strategy_type):
        try:
            signals = []
            start_time = time(9, 30)
            self.logger.info(f"Starting signal evaluation for {ticker} using {strategy_type} strategy.")

            # Fetch VWAP
            vwap = self.get_vwap_from_trade_parameters(ticker)
            
            vwap = float(vwap) if vwap else None

            # Check active trade status
            active_trade_status = self.check_active_trade_status(ticker, strategy_type)
            self.logger.info(f"Active trade status for {ticker} ({strategy_type}): {active_trade_status}")
            if active_trade_status in ["open", "pending"]:  # Skip if a trade is active
                self.logger.info(f"Skipping signal for {ticker} ({strategy_type}) due to active trade status.")
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

                # Ensure signal timestamp is within the trading window
                if not (start_time <= signal_time <= self.trading_end):
                    self.logger.info(f"Signal time {signal_time} for {ticker} is outside trading window. Skipping row {i}.")
                    continue

                # Evaluate conditions
                try:
                    self.logger.info(
                    f"Evaluating 5Min EMA Decline condition for {ticker} at row {i}."
                    f"\nCurrent Row: {row.to_dict()}."
                    f"\nPrevious Row: {previous_row.to_dict()}."
                    f"\nCurrent Price (Close): {row['close']}."
                    f"\nHOD: {hod}, VWAP: {vwap}."
                    )
                    if strategy_type == "5Min" and self.condition_ema_decline(row, previous_row, vwap):
                        
                        signal = self.generate_signal(row, ticker, strategy_type, vwap, hod)
                        signals.append(signal)
                        self.logger.info(f"5Min EMA Decline condition met for {ticker} at row {i}. Signal generated.")
                        
                    self.logger.info(f"Evaluating 1Minco SMA CO condition for {ticker} at row {i}. HOD: {hod} and VWAP: {vwap}.")
                    if strategy_type == "1Minco" and self.condition_sma_crossover(row, previous_row, vwap):
                        
                        signal = self.generate_signal(row, ticker, strategy_type, vwap, hod)
                        signals.append(signal)
                        self.logger.info(f"1Min SMA Crossover condition met for {ticker} at row {i}. Signal generated.")


                    self.logger.info(f"Evaluating 5Min Price deviation condition for {ticker} at row {i}. HOD: {hod} and VWAP: {vwap}.")
                    if strategy_type == "5Minde":
                        
                        trigger_signal, ema_deviation_flag = self.condition_ema_deviation(row, previous_row, ema_deviation_flag)
                        if trigger_signal:
                            signal = self.generate_signal(row, ticker, strategy_type, vwap, hod)
                            signals.append(signal)
                            self.logger.info(f"5Minde EMA Deviation condition met for {ticker} at row {i}. Signal generated.")

                except Exception as e:
                    self.logger.error(f"Error evaluating conditions for {ticker} at row {i}: {e}")

            self.logger.info(f"Completed signal evaluation for {ticker}. Total signals fired: {len(signals)}")
            return signals
    
        except Exception as e:
            self.logger.error(f"Error firing signals for {ticker} ({strategy_type}): {e}")
            return []
                
             
                
    
        
    def generate_signal(self, row, ticker, strategy_type, vwap, hod):
        """Generate and insert a signal."""
        try:
            
            price_current = float(row['close'])
            
            # Fetch risk parameters
            date = datetime.now().strftime('%Y-%m-%d')
            risk_params = self.get_risk_parameters(ticker, date)
        
            if not risk_params:
                self.logger.error(f"No risk parameters found for {signal['ticker']} on {date}")
                return

            account_equity = risk_params['account_equity']
            risk_percentage = risk_params[f'risk_{strategy_type.lower()}'] / 100
            risk_amount = account_equity * risk_percentage
            
            stop_loss = hod
            price_difference = abs(stop_loss - price_current)
            
            if price_difference == 0:
                self.logger.error(f"Stop loss equals current price for {ticker}. Cannot calculate shares.")
                return None
            
            shares = int(risk_amount / price_difference)
            
            
            trade_id = self.generate_auto_id()

            signal = {
                'trade_id': trade_id,
                'strategy': strategy_type,  # Use the strategy type explicitly
                'ticker': ticker,
                'entry_price': round(price_current, 2),
                
                'vwap': vwap,
                'time': row['timestamp'],
                'hod': round(hod, 2),
                'shares': shares,
                'status': 'Fired',
            }

            if self.insert_trade_signal(signal):
                self.logger.info(f"Signal fired and inserted into DB: {signal}")
                
            else:
                self.logger.error(f"Failed to insert signal into DB: {signal}")
            return signal
        except Exception as e:
            self.logger.error(f"Error generating signal: {e}")
            return None                

    
    def fire_signals(self, ticker, entry_price, hod, shares, strategy_type, risk, s_loss):
        try:
            
            trade_id = self.generate_trade_id()

            
            
            vwap = self.get_vwap_from_trade_parameters(ticker) 
            
            if vwap is not None:
                vwap = float(vwap)
        
            # Signal creation
            signal = {
                'trade_id': trade_id,
                'strategy': strategy_type,  # Now using the passed strategy type (1Min or 5Min)
                'ticker': ticker,
                'entry_price': round(float(entry_price), 2),
                
                'vwap': vwap,
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
                # Send the signal to the Risk Management module
                try:
                    self.risk_management.receive_signal(signal)
                    self.logger.info(f'Signal successfully sent to RiskManagement: {signal}')
                except Exception as e:
                    self.logger.error(f"Error sending signal to RiskManagement: {e}")
                
            else:
                self.logger.error(f"Failed to insert signal {signal['trade_id']} into the TradeSignal table.")        
                
            

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

        try:
            while datetime.now().time() <= self.trading_end:
                current_time = datetime.now()

                # Process 1-minute data every minute and 4 seconds
                if strategy_type in ['1Min', '1Minco', '1Minde'] and current_time.second == 4 and not self.pause_1min_strategy:
                    self.logger.info(f"Processing 1-minute market data for {ticker}")
                    self.process_market_data(ticker, '1Min')

                # Process 5-minute data every 5 minutes and 4 seconds
                if strategy_type in ['5Min', '5Minde'] and current_time.minute % 5 == 0 and current_time.second == 4:
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
    position_add = PositionAdd(order_execution, trade_monitor)

    # Initialize RiskManagement with OrderExecution
    risk_management = RiskManagement(order_execution)

    # Initialize StrategyLogic with both PositionAdd and RiskManagement
    strategy_logic = StrategyLogic(position_add=position_add, risk_management=risk_management)

    # Fetch tickers and start processing them
    strategy_logic.fetch_tickers_from_db()
        
    
                
    # Check and list all active threads
    for thread in threading.enumerate():
        print(thread.name)    

    # Keep the main thread alive
    try:
        while True:
            tm.sleep(1)  # Keep the main thread running to prevent the script from exiting
    except KeyboardInterrupt:
        print("Terminating script...")

