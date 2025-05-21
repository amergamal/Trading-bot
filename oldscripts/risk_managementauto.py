from datetime import datetime
import logging
import sqlite3
import time
from order_execution import OrderExecution  # Adjust import statement as per your actual module structure

class RiskManagementauto:
    def __init__(self, order_execution):
        self.order_execution = order_execution
        self.logger = logging.getLogger('RiskManagementauto')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def receive_signal(self, signal):
        self.logger.info(f"Received signal: {signal}")
        self.process_signal(signal)

    def process_signal(self, signal):
        ticker = signal['ticker']
        strategy = signal['strategy']
        date = datetime.now().strftime('%Y-%m-%d')

        trade_status = self.check_trade_status(ticker, strategy)
        
        if not trade_status:
            self.insert_trade_status(ticker, strategy, 'pending', 0)
            self.calculate_and_send(signal)
        else:
            active_trade, loss_count = trade_status
            if active_trade == 'open':
                self.logger.info("Trade is active")
                return
            elif active_trade == 'pending':
                self.logger.info("Order in progress")
                return
            elif loss_count >= 3:
                self.logger.info("Max loss reached")
                return
            elif active_trade == 'closed' and loss_count < 3:
                self.update_trade_status(ticker, strategy, 'pending')
                self.calculate_and_send(signal)

    def check_trade_status(self, ticker, strategy):
        try:
            conn = sqlite3.connect('EOD_data.db')
            query = """
                SELECT active_trade, loss_count FROM TradeStatus
                WHERE strategy = ? AND ticker = ? AND date = ?
            """
            current_date = datetime.now().strftime('%Y-%m-%d')  # Get the current date
            c = conn.cursor()
            c.execute(query, (strategy, ticker, current_date))
            row = c.fetchone()
            conn.close()
            return row if row else None
        except Exception as e:
            self.logger.error(f"Error checking trade status: {e}")
            return None

    def insert_trade_status(self, ticker, strategy, active_trade, loss_count):
        try:
            conn = sqlite3.connect('EOD_data.db')
            query = """
                INSERT INTO TradeStatus (strategy, ticker, active_trade, loss_count, date)
                VALUES (?, ?, ?, ?, ?)
            """
            c = conn.cursor()
            c.execute(query, (strategy, ticker, active_trade, loss_count, datetime.now().strftime('%Y-%m-%d')))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error inserting trade status: {e}")

    def update_trade_status(self, ticker, strategy, active_trade):
        try:
            conn = sqlite3.connect('EOD_data.db')
            query = """
                UPDATE TradeStatus
                SET active_trade = ?
                WHERE strategy = ? AND ticker = ?
            """
            c = conn.cursor()
            c.execute(query, (active_trade, strategy, ticker))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error updating trade status: {e}")

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

    def calculate_stop_loss(self, hod, strategy):
        try:
            stop_loss = None
            if strategy in ['1Min', '1Minco', '1Minde']:
                stop_loss = hod 
            elif strategy in ['5Min', '5Minco', '5Minde']:
                stop_loss = hod
            if stop_loss is not None:
                # Round the stop loss to 2 decimal places
                stop_loss = round(stop_loss, 2)
            return stop_loss
        except Exception as e:
            self.logger.error(f"Error in calculate_stop_loss: {e}")
            return None

    def calculate_shares(self, risk_amount, stop_loss, signal_price):
        try:
            difference = abs(signal_price - stop_loss)
            if difference == 0:
                raise ValueError("Difference between signal price and stop loss is zero, cannot divide by zero")
            return int(risk_amount / difference)
        except Exception as e:
            self.logger.error(f"Error calculating shares: {e}")
            return 0
        
    def update_trade_signal_shares(self, trade_id, shares, retries=5, initial_delay=1):
        """
        Update the shares column in the TradeSignal table with retry mechanism for database locks.
        """
        attempt = 0
        while attempt < retries:
            try:
                conn = sqlite3.connect('EOD_data.db')
                query = """
                    UPDATE TradeSignal
                    SET shares = ?
                    WHERE tradeID = ?
                """
                c = conn.cursor()
                c.execute(query, (shares, trade_id))
                conn.commit()
                self.logger.info(f"Updated shares to {shares} for trade ID: {trade_id} in TradeSignal table.")
                return  # Exit the function once successful
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    attempt += 1
                    self.logger.warning(f"Database is locked. Retrying {attempt}/{retries} after {initial_delay} seconds...")
                    time.sleep(initial_delay)
                    initial_delay *= 2  # Exponential backoff
                else:
                    self.logger.error(f"OperationalError updating shares in TradeSignal table for trade ID {trade_id}: {e}")
                    break
            except Exception as e:
                self.logger.error(f"Error updating shares in TradeSignal table for trade ID {trade_id}: {e}")
                break
            finally:
                if conn:
                    conn.close()
        else:
            self.logger.error(f"Failed to update shares for trade ID {trade_id} after {retries} retries.")
    

    def calculate_and_send(self, signal):
        # Fetch risk parameters
        date = datetime.now().strftime('%Y-%m-%d')
        risk_params = self.get_risk_parameters(signal['ticker'], date)
        
        if not risk_params:
            self.logger.error(f"No risk parameters found for {signal['ticker']} on {date}")
            return

        account_equity = risk_params['account_equity']
        risk_percentage = risk_params[f'risk_{signal["strategy"].lower()}'] / 100
        risk_amount = account_equity * risk_percentage

        # Calculate stop loss
        stop_loss = self.calculate_stop_loss(signal['hod'], signal['strategy'])
        signal['stop_loss'] = stop_loss

        # Calculate shares
        shares = self.calculate_shares(risk_amount, stop_loss, signal['entry_price'])
        signal['shares'] = shares
        
        
        self.send_to_order_execution(signal)
        
        # Update the shares column in the TradeSignal table
        self.update_trade_signal_shares(signal['trade_id'], shares)


    def send_to_order_execution(self, signal):
        trade_id = signal['trade_id']
        ticker = signal['ticker']
        shares = signal['shares']
        stop_loss = signal['stop_loss']
        strategy = signal['strategy']  # Add strategy to the signal
        date = datetime.now().strftime('%Y-%m-%d')

        # Send market sell order
        command_sell = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'order_type': 'market',
            'stop_price': stop_loss,  # Include stop price for follow-up stop market order
            'date': date,  # Add date to the order
            'strategy': strategy  # Include strategy in the command
        }
        self.logger.info(f"Sending sell market order: {command_sell}")
        self.order_execution.execute_command(command_sell)

        self.logger.info(f"Order details sent to OrderExecution for trade ID: {trade_id}")

        

    

    

if __name__ == "__main__":
    # Initialize OrderExecution module instance (replace with actual implementation)
    order_execution = OrderExecution()  # Create a placeholder order_execution instance
    risk_managementauto = RiskManagementauto(order_execution)  # Create risk_management instance with order_execution

    # The script now runs and waits for signals from StrategyLogic
    while True:
        pass  # Keep the script running indefinitely to wait for signals
