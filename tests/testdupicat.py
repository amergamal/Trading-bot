import sqlite3
from datetime import datetime, time
import pandas as pd
import logging

class TradeSignalTester:
    def __init__(self):
        # Setup the database connection and cursor
        self.conn = sqlite3.connect('tms_data.db')
        self.c = self.conn.cursor()
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def check_existing_trade(self, ticker, strategy, time, price, hi, rsi):
        """Check if a trade signal with the given parameters already exists in the TradeSignal table."""
        query = """
            SELECT COUNT(*) FROM TradeSignal
            WHERE ticker = ? AND strategy = ? AND time = ? AND price = ? AND hi = ? AND rsi = ?
        """
        self.c.execute(query, (ticker, strategy, time.strftime('%Y-%m-%d %H:%M:%S'), price, hi, rsi))
        count = self.c.fetchone()[0]
        return count > 0

    def insert_trade_signal(self, signal):
        """Insert a new trade signal into the TradeSignal table."""
        self.c.execute("""
            INSERT INTO TradeSignal (tradeID, strategy, ticker, price, atr_percentile, rsi, time, hi, atr_highest, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal['trade_id'], signal['strategy'], signal['ticker'], signal['entry_price'],
            signal['atr_percentile'], signal['rsi'], signal['time'], signal['hod'],
            signal['highest_atr'], signal['status']
        ))
        self.conn.commit()

    def fire_signals(self, data, strategy_params, ticker, strategy_type, highest_atr):
        try:
            signals = []
            start_time = time(9, 30)

            rsi_threshold = float(strategy_params['rsi'])  # Ensure RSI threshold is a float
            atr_percentile_threshold = float(strategy_params['atr_percentile'])  # Ensure ATR percentile is a float

            for i in range(1, len(data)):
                row = data.iloc[i]
                current_time = row['timestamp'].time()
                previous_row = data.iloc[i - 1]

                # Ensure the comparison values are numeric
                macd_diff_previous = float(previous_row['macd_diff'])
                macd_diff_current = float(row['macd_diff'])
                rsi_current = float(row['rsi'])
                price_current = float(row['close'])
                vwap_current = float(row['vwap'])
                atr_percentile_current = float(row['atr_percentile'])

                # Calculate HOD (High of Day) up to the current row
                hod = data['high'][:i+1].max()

                # Check the TradeSignal table for existing active trades
                if self.check_existing_trade(ticker, strategy_type, row['timestamp'], price_current, hod, rsi_current):
                    self.logger.error("Duplicate signal found, not sent to Risk Management")
                    continue

                if (macd_diff_previous > 0 and macd_diff_current < 0 and
                    rsi_current > rsi_threshold and
                    price_current > vwap_current and
                    atr_percentile_current > atr_percentile_threshold and
                    start_time <= current_time <= time(13, 0)):  # Assuming trading_end is 13:00
                    
                    trade_id = self.generate_trade_id()
                    signal = {
                        'trade_id': trade_id,
                        'strategy': strategy_type,
                        'ticker': ticker,
                        'entry_price': round(price_current, 2),
                        'atr_percentile': row['atr_percentile'],
                        'rsi': round(rsi_current),
                        'time': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                        'hod': hod,
                        'highest_atr': round(highest_atr, 2),
                        'status': 'Fired'  # Add status directly to the signal
                    }
                    self.logger.info(f'Generated signal: {signal}')
                    signals.append(signal)

                    # Insert the signal into the TradeSignal table with 'Fired' status
                    self.insert_trade_signal(signal)

            return signals
        except ValueError as ve:
            self.logger.error(f"Duplicate trade signal: {ve}")
            return []
        except Exception as e:
            self.logger.error(f"Error firing signals: {e}")
            return []

    def generate_trade_id(self):
        """Generate a unique trade ID (this is just a simple example)."""
        return f"TRADE-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    def cleanup(self):
        """Close the database connection."""
        self.conn.close()

# Mock data
data = pd.DataFrame({
    'timestamp': [datetime(2024, 7, 28, 9, 30), datetime(2024, 7, 28, 9, 31), datetime(2024, 7, 28, 9, 32)],
    'macd_diff': [0.5, -0.2, -0.1],
    'rsi': [55, 65, 70],
    'close': [100, 102, 104],
    'vwap': [99, 100, 101],
    'atr_percentile': [0.7, 0.8, 0.85],
    'high': [101, 103, 105]
})

strategy_params = {
    'rsi': '60',
    'atr_percentile': '0.75'
}

ticker = 'AAPL'
strategy_type = 'MACD'
highest_atr = 2.0

# Create an instance of the tester and run the test
tester = TradeSignalTester()

# First run
print("First run:")
signals1 = tester.fire_signals(data, strategy_params, ticker, strategy_type, highest_atr)
for signal in signals1:
    print(signal)

# Second run (to detect duplicates)
print("\nSecond run (to detect duplicates):")
signals2 = tester.fire_signals(data, strategy_params, ticker, strategy_type, highest_atr)
for signal in signals2:
    print(signal)

# Clean up
tester.cleanup()
