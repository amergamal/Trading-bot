import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
import threading
import logging
import socket
import time
import traceback
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os
 


LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015

class SSMonitor:
    def __init__(self, order_execution=None, socketio=None):
        self.logger = logging.getLogger('SSMonitor')
        self.logger.debug("SSMonitor logger initialized")       
        self.order_execution = order_execution  
        self.stopsellOrderIDs = set()
        self.trade_details_map = {}
        self.execution_price = {}  # order_id -> fill price
        self.order_to_trade_id = {}  # Map stopshortOrderID to tradeID
        self.missed_entries = set()  # trade_ids already handled as missed entry
        self.lock = threading.Lock()
        self.socketio = socketio
        self.client_socket = None
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
        if socketio:
            print("\n" + "!"*60)
            print("ABOUT TO EMIT canceled_sell_update FROM SSMONITOR")
            print(f"self.socketio: {self.socketio}")
            print(f"self.socketio id: {id(self.socketio)}")
            print(f"self.socketio.server: {self.socketio.server}")
            print(f"self.socketio.server id: {id(self.socketio.server) if self.socketio.server else None}")
            print("!"*60 + "\n")
        
        if not socketio:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        
        self.logger.info("SSMonitor instance created and initialized.")
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(current_dir, '.env')
        if not os.path.exists(env_path):
            self.logger.error(f".env file not found at path: {env_path}")
            self.smtp_server = None
            self.smtp_server_name = None
            self.smtp_port = None
            self.email_user = None
            self.email_pass = None
        else:
            load_dotenv(env_path)
            self.smtp_server_name = os.getenv('SMTP_SERVER')
            self.smtp_port = os.getenv('SMTP_PORT')
            self.email_user = os.getenv('EMAIL_USER')
            self.email_pass = os.getenv('EMAIL_PASS')
            self.sender_email = os.getenv('SENDER_EMAIL')
            self.recipient_email = os.getenv('RECIPIENT_EMAIL')
            if not all([self.smtp_server_name, self.smtp_port, self.email_user, self.email_pass, self.sender_email, self.recipient_email]):
                self.logger.error("One or more email environment variables are missing.")
                self.smtp_server = None
            else:
                self.logger.debug("Attempting initial SMTP server initialization")
                self._initialize_smtp()
                if self.smtp_server:
                    self.logger.info("SMTP server initialized successfully during __init__")
                else:
                    self.logger.warning("SMTP server initialization failed during __init__")
    
    def _initialize_smtp(self):
        try:
            self.smtp_server = smtplib.SMTP(self.smtp_server_name, int(self.smtp_port))
            self.smtp_server.starttls()
            self.smtp_server.login(self.email_user, self.email_pass)
            self.logger.debug("SMTP server (re)initialized successfully")
        except Exception as e:
            self.smtp_server = None
            self.logger.error(f"Failed to (re)initialize SMTP server: {e}")

    def notify(self, message):
        if not all([self.smtp_server_name, self.smtp_port, self.email_user, self.email_pass, self.sender_email, self.recipient_email]):
            self.logger.warning("Email notification not sent: SMTP server or email details not configured.")
            return
        
        self.logger.debug(f"Attempting to send email notification. Message: {message}")
        
        def send_email():
            try:
                # Check if connected with noop; if fails, it raises
                self.smtp_server.noop()
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPException) as e:
                self.logger.warning(f"SMTP connection lost ({e}); reinitializing...")
                self._initialize_smtp()
            
            if self.smtp_server:
                try:
                    msg = MIMEText(message)
                    msg['Subject'] = 'Trade open'
                    msg['From'] = f'"Trade Bot" <{self.sender_email}>'  # Set sender name to "Trade Bot"
                    msg['To'] = self.recipient_email
                    self.smtp_server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
                    self.logger.info(f"Email notification sent successfully: {message}")
                except Exception as e:
                    self.logger.error(f"Failed to send email notification: {e}")
                    # One retry after reinitialize
                    self.logger.debug("Retrying email send after reinitialization...")
                    self._initialize_smtp()
                    if self.smtp_server:
                        try:
                            self.smtp_server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
                            self.logger.info(f"Email notification sent on retry: {message}")
                        except Exception as retry_e:
                            self.logger.error(f"Retry failed to send email notification: {retry_e}")
            else:
                self.logger.warning("Cannot send email: SMTP server not initialized after attempt.")

        send_email()
    
    def connect_to_server(self):
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.settimeout(5.0)
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                self.logger.info("Retrying connection in 5 seconds...")
                time.sleep(5)
                
    def disconnect_from_server(self):
        if self.client_socket:
            try:
                self.client_socket.close()
            except socket.error:
                pass
            self.client_socket = None
            self.logger.info("Socket connection closed.")            
            
    def listen_to_server(self):
        """Start a thread to listen for server responses."""
        threading.Thread(target=self._listen_to_server, daemon=True).start()

    def _listen_to_server(self):
        while True:
            if not self.client_socket:
                self.logger.debug("Client socket is not connected, attempting to reconnect...")
                self.connect_to_server()
                time.sleep(1)
                continue
            try:
                response = self.client_socket.recv(4096).decode()
                if response:
                    #self.logger.debug(f"Received response: {response}")
                    self.process_response(response)
                else:
                    self.logger.debug("No response received, waiting...")
                    time.sleep(0.5)
            except socket.timeout:
                continue
            except socket.error as e:
                self.logger.error(f"Socket error while listening: {e}")
                self.disconnect_from_server()
                time.sleep(1)
                continue
            except Exception as e:
                self.logger.error(f"Error listening to server: {e}")


    def wait_for_server(self):
        """Keep checking if the server is up before connecting."""
        retry_delay = 5
        while True:
            try:
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                break
            except socket.error:
                self.logger.info(f"Server not available, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    

    def receive_order_details(self, trade_details):
        """Receive trade details and track stopshortOrderID."""
        self.logger.info(f"Received trade details: {trade_details}")
        with self.lock:
            stop_sell_id = trade_details.get('stopSellID')
            
            trade_id = trade_details.get('tradeID')
            if stop_sell_id and trade_id:
                self.stopsellOrderIDs.add(stop_sell_id)
                self.order_to_trade_id[stop_sell_id] = trade_id
                # Store full trade details for later use
                self.trade_details_map[trade_id] = trade_details.copy()
                self.logger.info(f"Stored trade_details for trade_id={trade_id}")
            

    def process_response(self, response):
        """Process server response lines for stop and buy orders."""
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%ORDER'):
                parts = line.split()
                try:
                    order_id_1 = parts[1]  # Order ID in part 1
                    order_id_13 = parts[13]  # Order ID in part 13
                    ticker = parts[3]
                    price = parts[9]
                    status = parts[11]
                    update_status_time = parts[12]

                    # Check if order ID matches stopshortOrderID
                    if order_id_1 in self.stopsellOrderIDs or order_id_13 in self.stopsellOrderIDs:
                        order_id = order_id_1 if order_id_1 in self.stopsellOrderIDs else order_id_13
                        
                        self.logger.info(f"Processing stop sell - Order ID: {order_id}, Status: {status}")
                        if status =="Executed":
                            self.process_executed_order(order_id, price, update_status_time, status)
                        else:
                            self.update_sell_market_status(order_id, status, ticker, update_status_time)
                            self.logger.info(f"Updating stop sell - Order ID: {order_id}, Status: {status}")
                                   
                    
                    else:
                        self.logger.debug(f"No matching stopshortOrderID for {order_id_1} or {order_id_13}")
                except IndexError as e:
                    self.logger.error(f"Error parsing %ORDER response: {e}")
            elif line.startswith('%OrderAct'):
                parts = line.split()
                try:
                    order_id = parts[1]
                    status = parts[2]
                    ticker = parts[4]
                    price = float(parts[6])
                    update_status_time = parts[8]
                    if order_id in self.stopsellOrderIDs:
                        if status == "Execute":
                            exec_price = float(parts[6])
                            self.execution_price[order_id] = exec_price
                            self.logger.info(f"Captured execution price for order_id={order_id}: {exec_price}")
                        if status == "Canceled":
                            self.update_sell_market_status(order_id, status, ticker, update_status_time)
                    
                except IndexError as e:
                    self.logger.error(f"Error parsing %OrderAct response: {e}")

    
    

    def process_executed_order(self, order_id, price, update_status_time, status):
        """Stop-sell executed trigger stop_only with original stop price, ticker, target."""
        self.logger.info(f"Stop-sell EXECUTED: order_id={order_id}, exec_price={price}")
        
        exec_price = self.execution_price.pop(order_id, None)
        if exec_price is None:
            self.logger.warning(
                f"No %OrderAct price found for order_id={order_id}, "
                f"falling back to line price {price}"
            )
            exec_price = float(price)

        self.logger.info(f"Using execution price: {exec_price}")

        # 1. Update sellmarket
        conn = cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE sellmarket
                SET status='Executed', act_status='Executed',
                    time=%s, price=%s, date=%s
                WHERE orderid=%s
            """, (update_status_time, exec_price,
                  datetime.now().strftime('%Y-%m-%d'), order_id))
            self.logger.info(f"sellmarket updated – order_id={order_id}, price={exec_price}")
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"DB error updating sellmarket: {e}")
            if conn: conn.rollback()
        finally:
            if cursor: cursor.close()
            if conn: self.db_pool.putconn(conn)

        # 2. Get trade_id and original data
        trade_id = self.order_to_trade_id.get(order_id)
        if not trade_id:
            self.logger.error(f"No trade_id for stopSellID {order_id}")
            return

        # Get original stop price, ticker, target from stored trade_details
        # (We stored the full dict in receive_order_details via self.trade_details_map)
        trade_details = self.trade_details_map.get(trade_id)
        if not trade_details:
            self.logger.error(f"No trade_details stored for trade_id={trade_id}")
            return

        stop_price = trade_details.get('stop_loss')
        ticker     = trade_details.get('ticker')
        target     = trade_details.get('target_price')
        risk       = trade_details.get('risk')

        if not all([stop_price, ticker]):
            self.logger.error(f"Missing stop_price or ticker in trade_details: {trade_details}")
            return

        # 3. Trigger stop_only
        if not self.order_execution:
            self.logger.error("OrderExecution instance missing")
            return

        try:
            cmd = {
                'order_type'   : 'stop_only',
                'trade_id'     : trade_id,
                'ticker'       : ticker,
                'shares'       : trade_details.get('shares', 0),
                'stop_price'   : float(stop_price),     # original stop price
                'price'        : exec_price,     # entry_price = stop price
                'target_price' : float(target) if target else None,
                'strategy'     : 'stop',
                'sellOrderID'  : order_id,
                'risk'         : float(risk) 
            }
            if not self.order_execution:
                self.logger.error("order_execution is None; cannot execute command.")
                return  # Or raise an exception
            self.order_execution.execute_command(cmd)
            self.logger.info(f"stop_only fired: {ticker} @ {stop_price}, target={target}")
        except Exception as e:
            self.logger.error(f"Failed to fire stop_only: {e}")

        # 4. Email
        msg = (
            "Stop-Sell Executed -> Stop-Only Placed\n"
            f"Ticker: {ticker}\n"
            f"Trade ID: {trade_id}\n"
            f"Execution Price: ${exec_price}\n"
            f"Stop-Only Price: ${stop_price}\n"
            f"Target: ${target}\n"
            f"Time: {update_status_time}"
        )
        self.notify(msg)

    def update_sell_market_status(self, order_id, status, ticker, update_status_time):
        """Update SellMarket status and act_status in the database."""
        self.logger.info(f"Updating SellMarket status for order_id {order_id} to {status}")
        
        trade_id = self.order_to_trade_id.get(order_id)
        if not trade_id:
            self.logger.error(f"Cannot find trade_id for stopSellID {order_id}")
            # fall back to the old behaviour (no trade_id) – keep the function working
            trade_id = None
        else:
            self.logger.debug(f"Found trade_id={trade_id} for order_id={order_id}")
        
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE sellmarket
                SET status = %s, act_status = %s, time = %s, date = %s
                WHERE orderid = %s
            """, (status, status, update_status_time, datetime.now().strftime('%Y-%m-%d'), order_id))
            
            if cursor.rowcount == 0:
                self.logger.warning(f"No SellMarket record found for order_id {order_id} to update")
                
            else:
                self.logger.info(f"SellMarket status updated for order_id {order_id}")
            if status == "Canceled" and trade_id and self.order_execution:
                # This replaces ALL the old manual SQL + emit code
                self.order_execution._update_tradesignal_status(
                    ticker=ticker,
                    trade_id=trade_id,
                    status="Canceled"
                )
                # Archive + delete (unchanged)
                cursor.execute("SELECT * FROM sellmarket WHERE orderid = %s", (order_id,))
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    sell_data = dict(zip(columns, row))
                    placeholders = ", ".join(["%s"] * len(columns))
                    insert_query = f"INSERT INTO canceledstop ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(insert_query, row)
                    self.logger.info(f"Canceled order {order_id} archived to canceledstop")
                    # EMIT TO FRONTEND: remove from Orders table, add to CanceledStop
                    
                    
                    if self.socketio:
                        payload = {
                            'tradeID': sell_data.get('tradeid'),
                            'strategy': sell_data.get('strategy'),
                            'time': sell_data.get('time'),
                            'ticker': sell_data.get('ticker'),
                            'shares': sell_data.get('shares'),
                            'price': float(sell_data.get('price')) if sell_data.get('price') else 0.0,
                            'action': sell_data.get('action', 'S'),
                            'status': 'Canceled',
                            'act_status': sell_data.get('act_status', 'Canceled'),
                            'notes': sell_data.get('notes') or '',
                            'orderID': sell_data.get('orderid'),
                        }
                        
                        self.socketio.emit('canceled_sell_update', payload)
                        self.logger.info(f"Emitted canceled_sell_update for order_id={order_id}, trade_id={trade_id}")
                    
                cursor.execute("DELETE FROM sellmarket WHERE orderid = %s", (order_id,))
                self.logger.info(f"Deleted canceled order {order_id} from sellmarket")

            conn.commit()

        except psycopg2.Error as e:
            self.logger.error(f"Error updating StopMarket status: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    


    

    

    def receive_latest_price(self, ticker, current_price):
        """Called by VwapFetch on every price tick. Detects stop-sell entries that price has skipped past."""
        # Limit fill = entry_stop_price - 0.02; if price is 2+ cents below that, entry is missed
        MISSED_THRESHOLD_BUFFER = 0.02

        with self.lock:
            candidates = [
                (order_id, trade_id, self.trade_details_map.get(trade_id))
                for order_id, trade_id in list(self.order_to_trade_id.items())
                if trade_id not in self.missed_entries
            ]

        for order_id, trade_id, details in candidates:
            if not details:
                continue
            if details.get('ticker', '').upper() != ticker.upper():
                continue

            entry_stop_price = float(details.get('entry_price') or 0)
            if entry_stop_price <= 0:
                continue

            # stop-sell-limit: triggers at entry_stop_price, limit fill at entry_stop_price - 0.02
            # Missed when price has fallen well below the limit
            limit_price = entry_stop_price - 0.02
            if current_price >= limit_price - MISSED_THRESHOLD_BUFFER:
                continue

            self.logger.warning(
                f"Missed entry: {ticker} price={current_price:.2f} < limit={limit_price:.2f} "
                f"— canceling entry order {order_id} for trade {trade_id}"
            )

            with self.lock:
                self.missed_entries.add(trade_id)
                self.stopsellOrderIDs.discard(order_id)

            self.send_command(f'CANCEL {order_id}')

            if self.order_execution:
                try:
                    self.order_execution._update_tradesignal_status(
                        ticker=ticker, trade_id=trade_id, status='Missed'
                    )
                except Exception as e:
                    self.logger.error(f"Failed to update tradesignal for missed entry {trade_id}: {e}")

            if self.socketio:
                self.socketio.emit('missed_entry', {
                    'ticker': ticker, 'tradeID': trade_id,
                    'entry_price': entry_stop_price, 'current_price': current_price,
                    'message': f'{ticker} entry missed — price skipped past ${entry_stop_price:.2f}'
                })

    def send_command(self, command):
        with self.lock:
            if not self.client_socket:
                self.logger.info("Socket is not connected, attempting to reconnect...")
                self.connect_to_server()
            try:
                self.client_socket.sendall((command + '\n').encode())  # Add newline
                self.logger.debug(f"Command sent: {command}")
                return True
            except socket.error as e:
                self.logger.error(f"Socket error while sending command: {e}")
                self.disconnect_from_server()
                return False
             

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    stopsell_monitor = SSMonitor()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")