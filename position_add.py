from datetime import datetime
import logging
import sqlite3

from order_execution import OrderExecution  # Adjust import statement as per your actual module structure
from trade_monitor import TradeMonitor


DB_PATH = 'EOD_data.db'

class PositionAdd:
    def __init__(self, order_execution, trade_monitor, db_path=DB_PATH):
        self.order_execution = order_execution
        self.trade_monitor = trade_monitor
        self.logger = logging.getLogger('PositionAdd')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.db_path = db_path

    def receive_signal(self, signal):
        self.logger.info(f"Received signal: {signal}")
        self.process_signal(signal)

    def process_signal(self, signal):
        ticker = signal['ticker']
        strategy = '1Min'
        date = datetime.now().strftime('%Y-%m-%d')

        trade_status = self.check_trade_status(ticker, strategy)
        
        if not trade_status:
            self.insert_trade_status(ticker, strategy, 'pending', 0)
            
            # Call trade_exists to check if the trade exists and get shares, stopOrderID
            trade_exists, shares, stopOrderID = self.trade_exists(signal)
            
            if trade_exists:
                signal['shares'] = shares
                signal['stopOrderID'] = stopOrderID
                self.logger.info(f"Found existing trade for {ticker} with {shares} shares.")
            else:
                self.logger.warning(f"No active trade found for {ticker} in ActiveTrades.")
        
            self.calculate_and_send(signal)
        else:
            active_trade, loss_count = trade_status
            if loss_count >= 3:
                self.logger.info("Max loss reached")
                return
            
            self.update_trade_status(ticker, strategy, 'pending')
            
            # Call trade_exists to check if the trade exists and get shares, stopOrderID
            trade_exists, shares, stopOrderID = self.trade_exists(signal)
        
            if trade_exists:
                signal['shares'] = shares
                signal['stopOrderID'] = stopOrderID
                self.logger.info(f"Found existing trade for {ticker} with {shares} shares.")
            else:
                self.logger.warning(f"No active trade found for {ticker} in ActiveTrades.")
        
            self.calculate_and_send(signal)
            
    def get_db_connection(self):
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None        
            
    def trade_exists(self, signal):
        """Check if the trade exists in the ActiveTrades table and return shares and stopOrderID."""
        conn = self.get_db_connection()
        if not conn:
            return False, None, None  # No connection, return False with None for shares and stopOrderID
    
        try:
            cursor = conn.cursor()
            # Check for the trade matching the ticker in the signal
            cursor.execute("""
                SELECT shares, stopOrderID FROM ActiveTrades
                WHERE ticker = ? AND date = ?
            """, (signal['ticker'], datetime.now().strftime('%Y-%m-%d')))
        
            # Fetch the result
            result = cursor.fetchone()
        
            # If a match is found, return the shares and stopOrderID
            if result:
                shares, stopOrderID = result
                return True, shares, stopOrderID
            else:
                return False, None, None  # No match found
    
        except sqlite3.Error as e:
            self.logger.error(f"Error checking trade existence in ActiveTrades: {e}")
            return False, None, None  # Handle the error and return False
    
        finally:
            conn.close()
   

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

        
    def calculate_new_shares(self, risk_amount, stop_loss, entry_price, current_shares):
        try:
            # Calculate the difference between stop loss and entry price
            difference = abs(entry_price - stop_loss)
            if difference == 0:
                raise ValueError("Difference between signal price and stop loss is zero, cannot divide by zero")
        
            # Calculate new shares: (risk_amount / difference) - current shares
            new_shares = (risk_amount / difference) - current_shares
            return int(new_shares)
        except Exception as e:
            self.logger.error(f"Error calculating new shares: {e}")
            return 0


    def calculate_and_send(self, signal):
        # Fetch risk parameters
        date = datetime.now().strftime('%Y-%m-%d')
        
        # Assign risk_amount directly from the signal
        risk_amount = signal['risk']  # 'risk' is now the actual risk amount
        stop_loss = signal['s_loss']
        current_shares = signal['shares']
        stoporder_id = signal['stopOrderID']
        entry_price = signal['entry_price']
        
        # Get existing trade data (existing shares and entry price)
        existing_shares, existing_entry_price = self.get_existing_trade_data(signal['ticker'], date)
    
             
        
        # Calculate new shares
        new_shares = self.calculate_new_shares(risk_amount, stop_loss, entry_price, current_shares)
        signal['new_shares'] = new_shares
        
        total_shares = current_shares + new_shares  # Sum of existing shares and new shares
        
        # Calculate the new average entry price
        if existing_shares > 0:
            total_value = (existing_shares * existing_entry_price) + (new_shares * entry_price)
            average_entry_price = round(total_value / total_shares, 2)
        else:
            average_entry_price = entry_price  # If no existing shares, the entry price is the new entry price
        
        if self.update_stopmarket_shares(stoporder_id, total_shares, stop_loss):
            logging.info(f"StopMarket updated for stopOrderID {stoporder_id} with {total_shares} shares and stop loss {stop_loss}.")
        
            self.send_to_order_execution(signal)

        
            
            # Send replace stop order to TradeMonitor
            self.trade_monitor.send_replace_order(
                stoporder_id,  # The StopOrderID we got from trade_exists
                signal['ticker'],  # The ticker from the signal
                total_shares,  # The new number of shares
                signal['s_loss'],  # The new stop price (stop loss)
                signal['trade_id']  # The trade ID from the signal
            )
            
            # Update the ActiveTrades table with total_shares and stop_loss
            if self.update_active_trades(signal['ticker'], total_shares, stop_loss, average_entry_price):
                logging.info(f"ActiveTrades updated for {signal['ticker']} with {total_shares} shares and stop loss {stop_loss}, and entry price {average_entry_price}.")

                # Update TradeDetails table with total_shares based on tradeID and ticker
                if self.update_trade_details(signal['ticker'], total_shares):
                    logging.info(f"TradeDetails updated for {signal['ticker']} with {total_shares} shares.")
                    
                
                else:
                    logging.error(f"Failed to update TradeDetails for {signal['ticker']}.")
            else:
                logging.error(f"Failed to update ActiveTrades for {signal['ticker']}.")
                
                
    def get_existing_trade_data(self, ticker, date):
        conn = self.get_db_connection()
        if not conn:
            return 0, 0.0  # Return 0 if no existing shares or error

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT shares, entry_price FROM ActiveTrades
                WHERE ticker = ? AND date = ?
            """, (ticker, date))
            result = cursor.fetchone()
            if result:
                return result[0], result[1]  # Return existing shares and entry price
            else:
                return 0, 0.0  # No existing trade found, return 0 shares and 0.0 price
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching existing trade data for {ticker}: {e}")
            return 0, 0.0
        finally:
            conn.close()
            
        

    def send_to_order_execution(self, signal):
        trade_id = signal['trade_id']
        ticker = signal['ticker']
        shares = signal['new_shares']
        
        strategy = signal['strategy']  # Add strategy to the signal
        date = datetime.now().strftime('%Y-%m-%d')

        # Send market sell order
        command_add = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'order_type': 'add',
            
            'date': date,  # Add date to the order
            'strategy': strategy  # Include strategy in the command
        }
        self.logger.info(f"Sending sell market order: {command_add}")
        self.order_execution.execute_command(command_add)

        self.logger.info(f"Order details sent to OrderExecution for trade ID: {trade_id}")

    def update_active_trades(self, ticker, total_shares, stop_loss, average_entry_price):
        conn = self.get_db_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ActiveTrades
                SET shares = ?, stop_Loss = ?, entry_price = ?
                WHERE ticker = ?
            """, (total_shares, stop_loss, average_entry_price, ticker))
            conn.commit()
            logging.info(f"ActiveTrades updated for {ticker} with {total_shares} shares and stop loss {stop_loss}, and entry price {average_entry_price}.")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"Error updating ActiveTrades for {ticker}: {e}")
            return False
        finally:
            conn.close()
            
    def update_trade_details(self, ticker, total_shares):
        conn = self.get_db_connection()
        if not conn:
            return False
        try:
            # Fetch tradeID from ActiveTrades
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID FROM ActiveTrades
                WHERE ticker = ?
            """, (ticker,))
            trade_id = cursor.fetchone()

            if trade_id:
                trade_id = trade_id[0]
                # Update TradeDetails with the total shares for the given tradeID
                cursor.execute("""
                    UPDATE TradeDetails
                    SET shares = ?
                    WHERE tradeID = ? AND ticker = ?
                """, (total_shares, trade_id, ticker))
                conn.commit()
                logging.info(f"TradeDetails updated for {ticker} with {total_shares} shares and tradeID {trade_id}.")
                return True
            else:
                self.logger.warning(f"No tradeID found for {ticker}.")
                return False
        except sqlite3.Error as e:
            self.logger.error(f"Error updating TradeDetails for {ticker}: {e}")
            return False
        finally:
            conn.close()
        
        
    def update_stopmarket_shares(self, stop_order_id, total_shares, stop_loss):
        conn = self.get_db_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE StopMarket
                SET shares = ?, price = ?
                WHERE orderID = ?
            """, (total_shares, stop_loss, stop_order_id))
            conn.commit()
            logging.info(f"StopMarket updated with {total_shares} shares and stop loss {stop_loss} for stopOrderID {stop_order_id}.")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"Error updating StopMarket for stopOrderID {stop_order_id}: {e}")
            return False
        finally:
            conn.close()
    
    

    

    

if __name__ == "__main__":
    # Initialize TradeMonitor instance
    trade_monitor = TradeMonitor()  # Create an instance of TradeMonitor

    # Initialize OrderExecution module instance with trade_monitor
    order_execution = OrderExecution(trade_monitor=trade_monitor)  # Pass trade_monitor to OrderExecution
    
    # Initialize PositionAdd with both order_execution and trade_monitor
    position_add = PositionAdd(order_execution=order_execution, trade_monitor=trade_monitor)  # Pass both arguments

    # The script now runs and waits for signals from StrategyLogic
    while True:
        pass  # Keep the script running indefinitely to wait for signals
