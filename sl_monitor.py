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

class SLMonitor:
    def __init__(self, socketio=None):
        self.logger = logging.getLogger('SLMonitor')
        self.multi_log = logging.getLogger('MultiStrategy')
        self.logger.debug("SLMonitor logger initialized")

        self.stopOrderIDs = set()

        self.order_to_trade_id = {}  # Map stopOrderID to tradeID
        self.lock = threading.Lock()
        self.socketio = socketio
        self.trade_monitor = None  # wired in app.py after both modules exist
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
        
        if not socketio:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        
        self.logger.info("SLMonitor instance created and initialized.")
        
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

    def notify(self, message, subject="Trade Closed Notification"):
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
                    msg['Subject'] = subject 
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
        """Receive trade details and track stopOrderID."""
        self.logger.info(f"Received trade details: {trade_details}")
        with self.lock:
            stop_order_id = trade_details.get('stopOrderID')
            
            trade_id = trade_details.get('tradeID')
            if stop_order_id:
                self.stopOrderIDs.add(stop_order_id)
                self.order_to_trade_id[stop_order_id] = trade_id
                self.logger.info(f"Tracking stopOrderID: {stop_order_id}, tradeID: {trade_id}")
            

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

                    # Check if order ID matches stopOrderID
                    if order_id_1 in self.stopOrderIDs or order_id_13 in self.stopOrderIDs:
                        order_id = order_id_1 if order_id_1 in self.stopOrderIDs else order_id_13
                        if self.is_stop_order_in_trade_details(order_id):
                            self.logger.info(f"Processing stop order - Order ID: {order_id}, Status: {status}")
                            if status =="Executed":
                                self.process_executed_order(order_id, price, update_status_time, status)
                            else:
                                self.update_stop_market_status(order_id, status, update_status_time)
                                self.logger.info(f"Updating stop order - Order ID: {order_id}, Status: {status}")
                                   
                    
                    else:
                        self.logger.debug(f"No matching stopOrderID for {order_id_1} or {order_id_13}")
                except IndexError as e:
                    self.logger.error(f"Error parsing %ORDER response: {e}")
            elif line.startswith('%OrderAct'):
                parts = line.split()
                try:
                    order_id = parts[1]
                    status = parts[2]
                    price = parts[6]
                    update_status_time = parts[8]
                    if order_id in self.stopOrderIDs:
                        self.logger.info(f"Processing stop OrderAct - Order ID: {order_id}, Status: {status}")
                        if status == "Canceled":
                            self.update_stop_market_status(order_id, status, update_status_time)
                    
                except IndexError as e:
                    self.logger.error(f"Error parsing %OrderAct response: {e}")

    def is_stop_order_in_trade_details(self, stop_order_id):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM tradedetails WHERE stoporderid = %s", (stop_order_id,))
            result = cursor.fetchone()
            return result is not None
        except psycopg2.Error as e:
            self.logger.error(f"Database error checking stopOrderID: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
    def _update_tradesignal_status(self, trade_id: str, ticker: str, new_status: str):
        """Update tradesignal.status for a given trade_id / ticker."""
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE tradesignal
                SET status = %s
                WHERE ticker = %s AND tradeid = %s
                RETURNING tradeid, time, strategy, ticker, price, shares, target, hi, risk, status
            """, (new_status, ticker, trade_id))

            row = cursor.fetchone()
            if row:
                signal = {
                    'tradeID': row[0],
                    'time': str(row[1]) if row[1] else '',
                    'strategy': row[2],
                    'ticker': row[3],
                    'price': float(row[4]) if row[4] else 0.0,
                    'shares': int(row[5]) if row[5] else 0,
                    'target': float(row[6]) if row[6] else 0.0,
                    'hi': float(row[7]) if row[7] else 0.0,
                    'risk': float(row[8]) if row[8] else 0.0,
                    'status': row[9],
                }

                self.logger.info(f"Updated tradesignal  {new_status} for {ticker}, trade_id={trade_id}")

                # ONE PLACE — EMIT HERE
                if self.socketio:
                    self.socketio.emit('trade_signal_update', [signal])
                    self.logger.info(f"Emitted trade_signal_update {new_status} for trade_id={trade_id}")

                conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"DB error updating tradesignal: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
    

    def process_executed_order(self, order_id, price, update_status_time, status):
        """Process executed or canceled stop order."""
        self.logger.info(f"Processing executed/canceled order for order_id {order_id}")
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT a.tradeid, a.strategy, a.ticker, a.shares, a.entry_price, a.time, a.stoporderid, a.target, a.risk
                FROM activetrades a
                LEFT JOIN tradedetails t ON a.tradeid = t.tradeid AND a.stoporderid = t.stoporderid
                WHERE a.stoporderid = %s
            """, (order_id,))
            trade = cursor.fetchone()
            if not trade:
                self.logger.warning(f"No active trade found for stopOrderID {order_id}")
                conn.commit()
                return
            trade_id, strategy, ticker, shares, entry_price, entry_time, stop_order_id, target, risk = trade
            cursor.execute("""
                SELECT price
                FROM stopmarket
                WHERE orderid = %s AND tradeid = %s::text
            """, (stop_order_id, str(trade_id)))
            stop_market_row = cursor.fetchone()
            original_stop_loss = float(stop_market_row[0]) if stop_market_row and stop_market_row[0] is not None else None
            exit_price = float(price) if price.replace('.', '', 1).isdigit() else 0.0
            entry_price = float(entry_price) if entry_price is not None else 0.0
            risk = float(risk) if risk is not None else None
            r_gain_loss = 0.0
            if original_stop_loss and exit_price:
                risk_per_share = entry_price - original_stop_loss
                if risk_per_share != 0:
                    r_gain_loss = (entry_price - exit_price) / abs(risk_per_share)
            # === NEW: Fetch accumulated realized from partial closes ===
            cursor.execute("SELECT realized FROM activetrades WHERE stoporderid = %s", (order_id,))
            accumulated_row = cursor.fetchone()
            partial_realized = float(accumulated_row[0]) if accumulated_row and accumulated_row[0] else 0.0

            # Calculate realized for remaining shares
            remaining_realized = round((entry_price - exit_price) * shares, 2)

            # Total realized = partials + final
            realized = partial_realized + remaining_realized
            closed_trade_data = {
                'tradeID': trade_id,
                'strategy': strategy,
                'ticker': ticker,
                'shares': shares,
                'entry_price': entry_price,
                'entry_time': entry_time,
                'original_stop_loss': original_stop_loss,
                'stop_loss': original_stop_loss,
                'sl_time': None,
                'exit_price': exit_price,
                'exit_time': update_status_time,
                'reason': 'StopLoss',
                'date': datetime.now().strftime('%Y-%m-%d'),
                'realized': realized,
                'r_gain_loss': r_gain_loss,
                'target': str(target) if target is not None else None,  # Convert to text
                'risk': risk
            }
            
            

            # ←←← ADD THIS BLOCK HERE ←←←
            cursor.execute("""
                INSERT INTO executedstop 
                (tradeid, ticker, shares, stoporderid, strategy, time, 
                 entry_price, stop_loss, executed_time, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tradeid) DO NOTHING
            """, (
                str(trade_id), ticker, shares, order_id, strategy,
                entry_time, entry_price, exit_price, update_status_time,
                datetime.now().strftime('%Y-%m-%d')
            ))
            self.logger.info(f"Inserted executed stop: {trade_id} @ {exit_price}")
            if self.socketio:
                executed_data = {
                    'tradeID': trade_id,
                    'orderID': order_id,
                    'strategy': strategy,
                    'time': update_status_time,
                    'ticker': ticker,
                    'shares': shares,
                    'price': exit_price
                }
                self.socketio.emit('executed_stop_update', executed_data)
            # ←←← END BLOCK ←←←
            cursor.execute("DELETE FROM stopmarket WHERE orderid = %s", (order_id,))
            # Then continue with closedtrades insert...
            
            cursor.execute("""
                INSERT INTO closedtrades 
                (tradeid, strategy, ticker, shares, entry_price, entry_time,
                 original_stop_loss, stop_loss, sl_time, exit_price, exit_time,
                 reason, date, realized, r_gain_loss, target, risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (trade_id, strategy, ticker, shares, entry_price, entry_time,
                  original_stop_loss, original_stop_loss, None, exit_price,
                  update_status_time, 'StopLoss',
                  datetime.now().strftime('%Y-%m-%d'), realized, r_gain_loss, str(target) if target is not None else None, risk if risk is not None else None))
            cursor.execute("DELETE FROM activetrades WHERE stoporderid = %s", (order_id,))
            cursor.execute("DELETE FROM tradedetails WHERE tradeid = %s", (trade_id,))
            
            # ←←← UPDATE TRADESIGNAL TO Closed-SL ←←←
            self._update_tradesignal_status(trade_id, ticker, "Closed-SL")
            #cursor.execute("""
                #UPDATE stopmarket
                #SET status = %s, act_status = %s, time = %s, price = %s, date = %s
                #WHERE orderid = %s
            #""", (status, status, update_status_time, exit_price,
                  #datetime.now().strftime('%Y-%m-%d'), order_id))
            #if cursor.rowcount == 0:
                #self.logger.warning(f"No StopMarket record found for stopOrderID {order_id} to update")
                
                
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE tradestatus
                SET active_trade = %s
                WHERE ticker = %s AND strategy = %s AND date = %s
            """, ('closed', ticker, strategy, today_date))
            cursor.execute("""
                UPDATE borrowedshares
                SET available_shares = available_shares + %s, last_updated = %s
                WHERE ticker = %s AND DATE(last_updated) = %s
            """, (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date))
            if cursor.rowcount > 0:
                self.logger.info(f"Updated borrowedshares for ticker {ticker}, increased available_shares by {shares}")
            else:
                self.logger.warning(f"No BorrowedShares record updated for ticker {ticker}, date={today_date}")
            conn.commit()
            self.logger.info(f"Moved trade {trade_id} to closedtrades and updated tradestatus for stoporderid {order_id}")
            self.stopOrderIDs.discard(order_id)

            # For Multi trades the stop executing IS the breakout — notify trade_monitor
            # so it starts waiting for the red candle to place the add-on.
            if strategy == 'Multi' and self.trade_monitor:
                self.multi_log.info(
                    f"[STOP-EXECUTED] {ticker} | TradeID={trade_id} | StopOrderID={order_id} | "
                    f"Exit=${exit_price:.2f} | Realized=${realized:.2f} | Notifying TradeMonitor of breakout"
                )
                self.trade_monitor.on_multi_breakout_confirmed(ticker, exit_price)

            if self.socketio:
                self.socketio.emit('closed_trade_update', closed_trade_data)
                self.logger.info(f"Emitted closed_trade_update: {closed_trade_data}")
                active_trade_removal = {'tradeID': trade_id}
                self.socketio.emit('active_trade_remove', active_trade_removal)
                self.logger.info(f"Emitted active_trade_remove: {active_trade_removal}")
                stop_market_data = {
                    'tradeID': trade_id,
                    'strategy': strategy,
                    'time': update_status_time,
                    'ticker': ticker,
                    'shares': shares,
                    'price': exit_price,
                    'orderID': order_id,
                    'action': 'StopMarket',
                    'status': status,
                    'act_status': status,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
                self.socketio.emit('stop_market_update', stop_market_data)
                self.logger.info(f"Emitted stop_market_update: {stop_market_data}")
            
            # Send email notification
            reason = closed_trade_data['reason']                 # <-- already set above
            subject = f"Trade closed-{reason}"  
            message = (f"Trade Closed: {ticker} ({strategy})\n"
                       f"Trade ID: {trade_id}\n"
                       f"Shares Closed: {shares}\n"
                       f"Entry Price: ${entry_price}, Exit Price: ${exit_price}\n"
                       f"Realized: ${realized}\n"
                       f"Reason: StopLoss\n"
                       f"Time: {update_status_time}")
            self.notify(message, subject=subject)
        except psycopg2.Error as e:
            self.logger.error(f"Error processing stop order: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def update_stop_market_status(self, order_id, status, update_status_time):
        """Update StopMarket status and act_status in the database."""
        self.logger.info(f"Updating StopMarket status for order_id {order_id} to {status}")
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE stopmarket
                SET status = %s, act_status = %s, time = %s, date = %s
                WHERE orderid = %s
            """, (status, status, update_status_time, datetime.now().strftime('%Y-%m-%d'), order_id))
            
            if cursor.rowcount == 0:
                self.logger.warning(f"No StopMarket record found for order_id {order_id} to update")
                
            else:
                self.logger.info(f"StopMarket status updated for order_id {order_id}")
            cursor.execute("SELECT * FROM stopmarket WHERE orderid = %s", (order_id,))
            row = cursor.fetchone()
            if not row:
                return
            cols = [desc[0] for desc in cursor.description]
            stop_data = dict(zip(cols, row))
    
            if status == "Canceled":
                cursor.execute("INSERT INTO canceledstop SELECT * FROM stopmarket WHERE orderid = %s", (order_id,))
                cursor.execute("DELETE FROM stopmarket WHERE orderid = %s", (order_id,))
                self.logger.info(f"Canceled order {order_id} archived and removed")
                
                if self.socketio:
                    canceled_data = {
                        'tradeID': stop_data['tradeid'],
                        'orderID': stop_data['orderid'],
                        'strategy': stop_data['strategy'],
                        'time': stop_data['time'],
                        'ticker': stop_data['ticker'],
                        'shares': stop_data['shares'],
                        'price': float(stop_data['price']) if stop_data['price'] else None,
                        'status': stop_data['status'],
                        'act_status': stop_data['act_status']
                    }
                    self.socketio.emit('canceled_stop_update', canceled_data)
                    self.logger.info(f"Emitted canceled_stop_update for {order_id}")

            
             
            else:
                if self.socketio:
                    emit_data = {
                        'tradeID': stop_data['tradeid'],
                        'strategy': stop_data['strategy'],
                        'time': stop_data['time'],
                        'ticker': stop_data['ticker'],
                        'shares': stop_data['shares'],
                        'price': float(stop_data['price']) if stop_data['price'] else None,
                        'action': stop_data['action'],
                        'status': stop_data['status'],
                        'act_status': stop_data['act_status'],
                        'notes': stop_data['notes'],
                        'orderID': stop_data['orderid']
                    }
                    self.socketio.emit('stop_market_update', emit_data)
                    self.logger.info(f"Emitted stop_market_update for {order_id}")

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

    

    

    def cancel_stop_order(self, stop_order_id):
        """Send cancel command for stop order and track status."""
        try:
            self.logger.debug(f"Entering cancel_stop_order for stop_order_id={stop_order_id}")
            self.send_command(f'CANCEL {stop_order_id}')
            self.logger.info(f"Successfully sent CANCEL command for buy_order_id={stop_order_id}")
        except Exception as e:
            self.logger.error(f"Failed to send CANCEL command for stop_order_id={stop_order_id}: {e}, Traceback: {traceback.format_exc()}")
        finally:
            self.logger.debug(f"Exiting cancel_stop_order for stop_order_id={stop_order_id}")

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
    sl_monitor = SLMonitor()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")