from datetime import datetime
import logging
import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
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
        try:
            self.db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                **config.DB_CONFIG
            )
            self.logger.info("PostgreSQL connection pool initialized")
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
            raise

    def receive_signal(self, signal):
        self.logger.info(f"Received signal: {signal}")
        # Process each signal in a new thread
        signal_thread = threading.Thread(target=self.process_signal, args=(signal,))
        signal_thread.start()
        
    def process_signal(self, signal):
        ticker = signal['ticker']
        strategy = signal['strategy']
        trade_id = signal['tradeID']
        date = datetime.now().strftime('%Y-%m-%d')

        trade_status = self.check_trade_status(ticker, strategy)

        
        
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
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            query = """
                SELECT active_trade, loss_count FROM tradestatus
                WHERE strategy = %s AND ticker = %s AND date = %s
            """
            
            cursor.execute(query, (strategy, ticker, today_date))
            row = cursor.fetchone()
            return row if row else None
        except psycopg2.Error as e:
            self.logger.error(f"Error checking trade status: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def insert_trade_status(self, ticker, strategy, active_trade, loss_count):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            query = """
                INSERT INTO tradestatus (strategy, ticker, active_trade, loss_count, date)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (strategy, ticker, active_trade, loss_count, today_date))
            conn.commit()
            self.logger.info(f"Inserted trade status for ticker: {ticker}, strategy: {strategy}, active_trade: {active_trade}")
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting trade status: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def update_trade_status(self, ticker, strategy, active_trade):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            query = """
                UPDATE tradestatus
                SET active_trade = %s
                WHERE strategy = %s AND ticker = %s AND date = %s
            """
            
            cursor.execute(query, (active_trade, strategy, ticker, today_date))
            conn.commit()
            self.logger.info(f"Updated trade status to {active_trade} for ticker={ticker}, strategy={strategy}, date={today_date}")
    
        except psycopg2.Error as e:
            self.logger.error(f"Error updating trade status: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
            
         
            
       
   
            

    def send_to_order_execution(self, signal):
        trade_id = signal['tradeID']
        ticker = signal['ticker']
        shares = signal['shares']
        
        stop_price = signal['s_loss']
        entry_price = signal['entry_price']
        strategy = signal['strategy']  # Add strategy to the signal
        target_price = signal['target_price']
        risk_amount = signal['risk']
        
        
        
        
        

        # Send market sell order
        command_sell = {
            'order_type': 'target',
            'ticker': ticker,
            'shares': shares,
            'trade_id': trade_id,
            'stop_price': stop_price,
            'strategy': strategy,
            'price': entry_price,  # Maps to entry_price in execute_command
            'target_price': target_price,
            'risk': risk_amount
            
        }
        
        
            
        self.logger.info(f"Sending order: {command_sell}")
        self.order_execution.execute_command(command_sell)

        self.logger.info(f"Order details sent to OrderExecution for trade ID: {trade_id}")

        

    

    

if __name__ == "__main__":
    # Initialize OrderExecution module instance (replace with actual implementation)
    order_execution = OrderExecution()  # Create a placeholder order_execution instance
    risk_management = RiskManagement(order_execution)  # Create risk_management instance with order_execution

    # The script now runs and waits for signals from StrategyLogic
    while True:
        pass  # Keep the script running indefinitely to wait for signals
