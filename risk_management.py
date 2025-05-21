from datetime import datetime
import logging
import sqlite3
import threading
from order_execution import OrderExecution  # Adjust import statement as per your actual module structure


class RiskManagement:
    def __init__(self, order_execution):
        self.order_execution = order_execution
        self.logger = logging.getLogger('RiskManagement')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def receive_signal(self, signal):
        self.logger.info(f"Received signal: {signal}")
        # Process each signal in a new thread
        signal_thread = threading.Thread(target=self.process_signal, args=(signal,))
        signal_thread.start()
        
    def process_signal(self, signal):
        ticker = signal['ticker']
        strategy = signal['strategy']
        date = datetime.now().strftime('%Y-%m-%d')

        trade_status = self.check_trade_status(ticker, strategy)

        if strategy in ['1Min', 'limit']:
            # Process signal as is for 1Min or limit strategies
            if not trade_status:
                self.insert_trade_status(ticker, strategy, 'pending', 0)
                self.send_to_order_execution(signal)
            else:
                active_trade, loss_count = trade_status
                if loss_count >= 3:
                    self.logger.info("Max loss reached for strategy 1Min or limit")
                    return
                self.update_trade_status(ticker, strategy, 'pending')
                self.send_to_order_execution(signal)
        else:
            # Process signal using the method for other strategies
            if not trade_status:
                self.insert_trade_status(ticker, strategy, 'pending', 0)
                self.send_to_order_execution(signal)
                
            else:
                active_trade, loss_count = trade_status
                if active_trade == 'open':
                    self.logger.info(f"Trade is active for {strategy}")
                    return
                elif active_trade == 'pending':
                    self.logger.info(f"Order in progress for {strategy}")
                    return
                elif loss_count >= 3:
                    self.logger.info(f"Max loss reached for {strategy}")
                    return
                elif active_trade == 'closed' and loss_count < 3:
                    self.update_trade_status(ticker, strategy, 'pending')
                    self.send_to_order_execution(signal)
                    

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
            
         
            
       
   
            

    def send_to_order_execution(self, signal):
        trade_id = signal['trade_id']
        ticker = signal['ticker']
        shares = signal['shares']
        hod = signal['hod']
        stop_loss = signal['s_loss']
        entry_price = signal['entry_price']
        strategy = signal['strategy']  # Add strategy to the signal
        date = datetime.now().strftime('%Y-%m-%d')
        
        # Determine order type and include entry_price if strategy is limit
        if strategy in ['1Min', '1MinBS', '1Minco', '5Min', '5MinBS', '5Minde']:
            order_type = 'market'
        elif strategy == 'limit':
            order_type = 'limit'
        elif strategy == 'long':
            order_type = 'marketl'    
        else:
            self.logger.error("Unknown strategy type.")
            return
        
        # Set stop_price based on strategy
        if strategy in ['1Min', 'limit', 'marketl']:
            stop_price = stop_loss
        else:
            stop_price = hod

        # Send market sell order
        command_sell = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'order_type': order_type,
            'stop_price': stop_price,  # Include stop price for follow-up stop market order
            'date': date,  # Add date to the order
            'strategy': strategy  # Include strategy in the command
        }
        
        if order_type == 'limit':
            command_sell['price'] = entry_price  # Add entry price for limit orders
            
        self.logger.info(f"Sending {order_type} order: {command_sell}")
        self.order_execution.execute_command(command_sell)

        self.logger.info(f"Order details sent to OrderExecution for trade ID: {trade_id}")

        

    

    

if __name__ == "__main__":
    # Initialize OrderExecution module instance (replace with actual implementation)
    order_execution = OrderExecution()  # Create a placeholder order_execution instance
    risk_management = RiskManagement(order_execution)  # Create risk_management instance with order_execution

    # The script now runs and waits for signals from StrategyLogic
    while True:
        pass  # Keep the script running indefinitely to wait for signals
