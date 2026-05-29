import socket
import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
import logging
import random
from datetime import datetime
import time
import schedule
import threading
import smtplib
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer
import urllib.parse

LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015

def generate_token():
    return str(random.randint(100000, 999999))

class EndOfDay:
    def __init__(self, socketio=None):
        self.logger = logging.getLogger('EndOfDay')
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.lock = threading.Lock()
        self.client_socket = None
        self.keep_listening = True
        self.order_reasons = {}
        self.socketio = socketio
        if not socketio:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        # Initialize PostgreSQL connection pool
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
        self.logger.info("EOD instance created and initialized.")
        
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
                    
            # Secure serializer for email close buttons
            secret_key = os.getenv('SECRET_KEY')
            if not secret_key:
                self.logger.error("SECRET_KEY not found in .env – email close buttons disabled")
                self.serializer = None
            else:
                self.serializer = URLSafeTimedSerializer(secret_key)
                self.logger.info("Email close button serializer initialized")                

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
        
        self.logger.debug(f"Attempting to send email notification")
        
        def send_email():
            try:
                # Check if connected with noop; if fails, it raises
                self.smtp_server.noop()
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPException) as e:
                self.logger.warning(f"SMTP connection lost ({e}); reinitializing...")
                self._initialize_smtp()
            
            if self.smtp_server:
                try:
                    msg = MIMEText(message, 'html')  # <-- ADD ', 'html''
                    msg['Subject'] = subject
                    msg['From'] = f'"Trade Bot" <{self.sender_email}>'  # Set sender name to "Trade Bot"
                    msg['To'] = self.recipient_email
                    self.smtp_server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
                    self.logger.info(f"Email notification sent successfully")
                except Exception as e:
                    self.logger.error(f"Failed to send email notification: {e}")
                    # One retry after reinitialize
                    self.logger.debug("Retrying email send after reinitialization...")
                    self._initialize_smtp()
                    if self.smtp_server:
                        try:
                            self.smtp_server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
                            self.logger.info(f"Email notification sent on retry")
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
                self.logger.info("Connected to server 5015.")
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                self.logger.info("Retrying connection in 5 seconds...")
                time.sleep(5)
                
                
    def listen_to_server(self):
        """Continuously listen for server responses."""
        while self.keep_listening:
            if not self.client_socket:
                self.logger.debug("Client socket is not connected, attempting to reconnect...")
                self.connect_to_server()
                time.sleep(1)
                continue
          
            try:
                response = self.client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    self.handle_response(response)
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
                self.logger.error(f"Error listening to server: {e}, response: {repr(response)}")
                continue  # Continue listening instead of stopping
                      

    def disconnect_from_server(self):
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
            self.logger.info("Socket connection closed.")

    def receive_order_details(self, trade_details):
        """Receive trade details and insert into the database."""
        trade_id = trade_details['tradeID']
        stop_order_id = trade_details['stopOrderID']
        strategy = trade_details.get('strategy')

        if not stop_order_id:
            self.logger.error(f"Missing stopOrderID in trade details for trade_id {trade_id}: {trade_details}")
            return
        if not strategy:
            self.logger.error(f"Missing strategy in trade details for trade_id {trade_id}: {trade_details}")
            return

        with self.lock:
            self.insert_trade_details(trade_details)
            self.logger.info(f"Received and stored trade details for {trade_id}: {trade_details}")

    
    def insert_trade_details(self, trade_details):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            # Add date to trade_details
            trade_details['date'] = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('''
                INSERT INTO tradedetails (tradeid, time, ticker, strategy, shares, entry_price, stop_loss, sellorderid, stoporderid, date)
                VALUES (%(tradeID)s, %(time)s, %(ticker)s, %(strategy)s, %(shares)s, %(entry_price)s, %(stop_loss)s, %(sellOrderID)s, %(stopOrderID)s, %(date)s)
                ON CONFLICT (tradeid) DO NOTHING
            ''', trade_details)
            conn.commit()
            self.logger.info(f"Inserted trade details: {trade_details}")
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting trade details: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)    

        

    def close_position(self, ticker, trade_id, reason=None):
        trade_id = str(trade_id)  # Convert to string for TEXT column
        self.logger.debug(f"Starting to close position for ticker={ticker}, trade_id={trade_id}")
        current_date = datetime.now().strftime('%Y-%m-%d')
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, strategy, ticker, shares, entry_price, stop_loss, time, stoporderid, last_price
                FROM activetrades
                WHERE ticker = %s AND tradeid = %s
            """, (ticker, trade_id))
            open_trade = cursor.fetchone()
            if open_trade:
                tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time, stopOrderID, last_price = open_trade
                self.logger.info(f"Closing position for tradeID {tradeID}, ticker={ticker}, stopOrderID={stopOrderID}")
                # Match tradedetails by stoporderid and ticker
                cursor.execute("""
                    SELECT tradeid, strategy, ticker, shares, time, stoporderid
                    FROM tradedetails
                    WHERE stoporderid = %s AND ticker = %s AND date = %s
                """, (stopOrderID, ticker, current_date))
                trade_detail = cursor.fetchone()
                if trade_detail:
                    td_tradeID, td_strategy, td_ticker, td_shares, td_time, td_stopOrderID = trade_detail
                    self.logger.info(f"Matched tradedetails: tradeID={td_tradeID}, strategy={td_strategy}, stopOrderID={td_stopOrderID}")
                    if strategy != td_strategy:
                        self.logger.warning(f"Strategy mismatch for tradeID {tradeID}: activetrades='{strategy}', tradedetails='{td_strategy}'. Using tradedetails strategy.")
                    strategy = td_strategy  # Use strategy from tradedetails
                    if strategy.lower() in ['limit', 'target']:
                        self.send_buy_limit_order(tradeID, strategy, ticker, shares, stopOrderID, last_price, reason)
                    else:
                        self.send_buy_market_order(tradeID, strategy, ticker, shares, stopOrderID, reason)
                        
                else:
                    self.logger.error(f"No matching tradedetails for tradeID={tradeID}, stopOrderID={stopOrderID}, ticker={ticker}")
            else:
                self.logger.info(f"No open position found for ticker {ticker}")
        except psycopg2.Error as e:
            self.logger.error(f"Database error closing position for ticker {ticker}, tradeID {trade_id}: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
        # Update tradesignal to Closed-PT
        if trade_id and ticker:
            self._update_tradesignal_status(trade_id, ticker, "Closed-PT") 
            
    def partial_close_trade(self, trade_id, shares_to_close, ticker, leftover_shares, last_price=None):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            # Fetch entry price, current shares, strategy, and last_price
            cursor.execute("""
                SELECT a.entry_price, a.shares, a.strategy, tp.last
                FROM activetrades a
                LEFT JOIN tradeparameters tp ON a.ticker = tp.ticker
                WHERE a.tradeid = %s AND a.ticker = %s
            """, (trade_id, ticker))
            row = cursor.fetchone()
            if not row:
                self.logger.error(f"partial_close_trade: No active trade found for trade_id {trade_id}, ticker {ticker}")
                return {'status': 'error', 'error': 'Trade not found'}

            entry_price, current_shares, strategy, db_last_price = row
            entry_price = float(entry_price)
            db_last_price = float(db_last_price) if db_last_price else None
            # Use DB price if available, fall back to caller-supplied price (e.g. from trade_monitor)
            last_price = db_last_price if db_last_price is not None else last_price

            # Validate shares
            if shares_to_close > current_shares:
                return {'status': 'error', 'error': 'Cannot close more shares than held'}

            if last_price is None:
                self.logger.error(f"partial_close_trade: No last_price available for {ticker}")
                return {'status': 'error', 'error': 'No current price available'}

            # Send buy limit order using existing EOD method
            # It adds +0.05 to last_price internally and uses TIF=DAY+
            self.send_buy_limit_order(
                trade_id=trade_id,
                strategy=strategy,
                ticker=ticker,
                shares=shares_to_close,
                stopOrderID=None,  # Not needed for partial close
                last_price=last_price,
                reason='PartialClose'
            )

            # Placeholder: Use last_price + 0.05 as estimated fill (since limit is aggressive)
            # Real fill will come via handle_response insert_buy_market later
            estimated_fill_price = last_price + 0.02

            # Calculate realized from this partial (using estimated fill)
            realized_this = round((entry_price - estimated_fill_price) * shares_to_close, 2)

            # Update activetrades: reduce shares, add to realized
            cursor.execute("""
                UPDATE activetrades
                SET shares = %s,
                    realized = COALESCE(realized, 0) + %s
                WHERE tradeid = %s AND ticker = %s
            """, (leftover_shares, realized_this, trade_id, ticker))

            # Update tradedetails for consistency
            cursor.execute("""
                UPDATE tradedetails
                SET shares = %s
                WHERE tradeid = %s AND ticker = %s
            """, (leftover_shares, trade_id, ticker))

            conn.commit()
            
            self.logger.info(f"Partial close limit order sent: trade_id={trade_id}, closed={shares_to_close}, "
                             f"leftover={leftover_shares}, estimated_realized={realized_this}")
            
            # === NEW: Send Partial Close Email Notification ===
            pl_this_color = "#d32f2f" if realized_this < 0 else "#2e7d32"
            html_parts = [
                "<html><body style='font-family:Arial,sans-serif; background:#f4f4f4; padding:20px;'>",
                "<div style='max-width:640px; margin:0 auto; background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.1);'>",
                "<div style='background:#fd7e14; color:white; padding:20px; text-align:center;'>",  # Orange header for partial
                "<h2 style='margin:0;'>Partial Close Executed</h2>",
                "</div>",
                "<div style='padding:20px;'>",
                "<div style='text-align:center; margin-bottom:20px;'>",
                f"<strong style='font-size:24px; color:#333;'>{ticker}</strong>",
                f"<span style='color:#777; font-size:14px;'> | ID: {trade_id} | {strategy.title()}</span><br><br>",
                f"<strong>Shares Closed:</strong> {shares_to_close} (of {current_shares})<br><br>",
                f"<strong>Remaining Shares:</strong> {leftover_shares}<br><br>",
                f"<strong>Entry Price:</strong> ${entry_price:.2f} &nbsp;&nbsp; ",
                f"<strong>Est. Exit Price:</strong> ${estimated_fill_price:.2f}<br><br>",
                f"<strong>Realized from this partial:</strong> ",
                f"<span style='font-weight:bold; font-size:18px; color:{pl_this_color};'>${realized_this:+.2f}</span><br><br>",
                f"<strong>Reason:</strong> Manual Partial Close<br>",
                "</div>",
                "<p style='text-align:center; color:#999; font-size:12px; margin-top:30px;'>",
                "Remaining position is still active with updated stop order.",
                "</p>",
                "</div></div></body></html>"
            ]

            partial_message = "".join(html_parts)
            partial_subject = f"Partial Close: {ticker} ({shares_to_close} shares)"

            self.notify(partial_message, subject=partial_subject)
            self.logger.info(f"Sent partial close email for {ticker}, trade_id={trade_id}")
            # === END NEW ===
            
            # === NEW: Mark this trade as having been partially closed (manual or auto) ===
            try:
                cursor.execute("""
                    UPDATE activetrades
                    SET partial_closed = TRUE
                    WHERE tradeid = %s AND ticker = %s
                """, (trade_id, ticker))
                conn.commit()
                self.logger.info(f"Marked trade {trade_id} ({ticker}) as partially closed in DB")
            except Exception as e:
                self.logger.error(f"Failed to set partial_closed flag for {trade_id}: {e}")
                conn.rollback()
                # Don't fail the whole partial close just because of this flag

            # Emit full trade data so frontend can update all cells without a page refresh
            if self.socketio:
                try:
                    cur2 = conn.cursor()
                    cur2.execute("""
                        SELECT tradeid, time, strategy, ticker, shares, entry_price, target,
                               stop_loss, lu_price, unrealized, realized, date,
                               sellorderid, stoporderid, last_price, risk
                        FROM activetrades WHERE tradeid = %s AND ticker = %s
                    """, (trade_id, ticker))
                    r = cur2.fetchone()
                    cur2.close()
                    if r:
                        self.socketio.emit('active_trade_update', {
                            'tradeID': r[0], 'time': str(r[1]) if r[1] else '',
                            'strategy': r[2], 'ticker': r[3], 'shares': r[4],
                            'entry_price': float(r[5]) if r[5] is not None else None,
                            'target': float(r[6]) if r[6] is not None else None,
                            'stop_loss': float(r[7]) if r[7] is not None else None,
                            'lu_price': float(r[8]) if r[8] is not None else None,
                            'unrealized': float(r[9]) if r[9] is not None else None,
                            'realized': float(r[10]) if r[10] is not None else 0.0,
                            'date': str(r[11]) if r[11] else '',
                            'sellOrderID': r[12], 'stopOrderID': r[13],
                            'last_price': float(r[14]) if r[14] is not None else None,
                            'risk': float(r[15]) if r[15] is not None else 0.0,
                        })
                    else:
                        self.socketio.emit('active_trade_update', {'tradeID': trade_id})
                except Exception as emit_err:
                    self.logger.error(f"partial_close_trade emit error: {emit_err}")
                    self.socketio.emit('active_trade_update', {'tradeID': trade_id})

            return {'status': 'success', 'realized_this': realized_this}

        except Exception as e:
            self.logger.error(f"partial_close_trade error: {e}")
            if conn:
                conn.rollback()
            return {'status': 'error', 'error': str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)               

    def close_positions(self):
        self.logger.info("EOD close_positions fired.")
        current_date = datetime.now().strftime('%Y-%m-%d')
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, strategy, ticker, shares, entry_price, stop_loss, time, stoporderid
                FROM activetrades
                WHERE date = %s
            """, (current_date,))
            open_trades = cursor.fetchall()
            self.logger.debug(f"Open trades fetched: {open_trades}")
            for trade in open_trades:
                tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time, stopOrderID = trade
                self.logger.debug(f"Evaluating trade: {tradeID}, {strategy}, {ticker}")
                cursor.execute("""
                    SELECT tradeid, strategy, ticker, shares, time, stoporderid
                    FROM tradedetails
                    WHERE tradeid = %s AND strategy = %s AND ticker = %s AND shares = %s AND stoporderid = %s AND date = %s
                """, (str(tradeID), strategy, ticker, str(shares), stopOrderID, current_date))
                trade_detail = cursor.fetchone()
                if trade_detail:
                    self.logger.info(f"Closing position for tradeID {tradeID}")
                    self.send_buy_market_order(tradeID, strategy, ticker, shares, stopOrderID, reason='EOD')
                else:
                    self.logger.warning(f"No matching tradedetails for tradeID {tradeID}, proceeding with activetrades data")
                    # Fall back to activetrades data for closure
                    self.send_buy_market_order(tradeID, strategy, ticker, shares, stopOrderID)
                # Update tradesignal to Closed-EOD
                self._update_tradesignal_status(tradeID, ticker, "Closed-EOD")    
        except psycopg2.Error as e:
            self.logger.error(f"Database error closing positions: {e}")
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

                # EMIT HERE — ONE PLACE, ALWAYS
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

    def send_buy_market_order(self, trade_id, strategy, ticker, shares, stopOrderID, reason=None):
        """Send a command to buy back the shares."""
        token = generate_token()
        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares} MKT TIF=DAY'
        
        # Store the reason in the dictionary
        with self.lock:
            self.order_reasons[token] = reason
            self.logger.debug(f"Stored reason {reason} for token {token}")

        # Ensure the socket is connected before sending
        if not self.client_socket:
            self.connect_to_server()

        if self.client_socket:
            try:
                # Send command
                self.logger.debug(f"Sending buy market order for trade_id {trade_id} with command: {buy_command}")
                self.send_command(buy_command)
                
                # Update the token in the tradedetails table
                conn = None
                cursor = None
                try:
                    conn = self.db_pool.getconn()
                    cursor = conn.cursor()
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    update_query = """
                        UPDATE tradedetails
                        SET token = %s
                        WHERE tradeid = %s AND ticker = %s AND date = %s
                    """
                    cursor.execute(update_query, (token, str(trade_id), ticker, current_date))
                    if cursor.rowcount > 0:
                        self.logger.debug(f"Updated token {token} for trade_id {trade_id}, ticker {ticker}, date {current_date} in tradedetails table")
                    else:
                        self.logger.warning(f"No matching record found in tradedetails for trade_id {trade_id}, ticker {ticker}, date {current_date}")
                    conn.commit()
                except psycopg2.Error as e:
                    self.logger.error(f"Database error updating token for trade_id {trade_id}: {e}")
                    if conn:
                        conn.rollback()
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        self.db_pool.putconn(conn)
            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.disconnect_from_server()
                
    def send_buy_limit_order(self, trade_id, strategy, ticker, shares, stopOrderID, last_price, reason=None):
        """Send a command to buy back the shares."""
        token = generate_token()
        limit_price = float(last_price) + 0.02  # Add 0.05 to last_price
        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares} {limit_price} TIF=DAY+ {trade_id}'
        
        # Store the reason in the dictionary
        with self.lock:
            self.order_reasons[token] = reason
            self.logger.debug(f"Stored reason {reason} for token {token}")

        # Ensure the socket is connected before sending
        if not self.client_socket:
            self.connect_to_server()

        if self.client_socket:
            try:
                # Send command
                self.logger.debug(f"Sending buy LIMIT order for trade_id {trade_id} with command: {buy_command}")
                self.send_command(buy_command)
                
                # Update the token in the tradedetails table
                conn = None
                cursor = None
                try:
                    conn = self.db_pool.getconn()
                    cursor = conn.cursor()
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    update_query = """
                        UPDATE tradedetails
                        SET token = %s
                        WHERE tradeid = %s AND ticker = %s AND date = %s
                    """
                    cursor.execute(update_query, (token, str(trade_id), ticker, current_date))
                    if cursor.rowcount > 0:
                        self.logger.debug(f"Updated token {token} for trade_id {trade_id}, ticker {ticker}, date {current_date} in tradedetails table")
                    else:
                        self.logger.warning(f"No matching record found in tradedetails for trade_id {trade_id}, ticker {ticker}, date {current_date}")
                    conn.commit()
                except psycopg2.Error as e:
                    self.logger.error(f"Database error updating token for trade_id {trade_id}: {e}")
                    if conn:
                        conn.rollback()
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        self.db_pool.putconn(conn)
            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.disconnect_from_server()            

    
              
    def run_combined_tasks(self):
        """Run end-of-day tasks at 15:55 and check active trades every 15 minutes at :00, :15, :30, and :45."""
        self.logger.info("Starting EndOfDay tasks with scheduled active trade checks")
        
        # Check active trades every minute (for testing)
        #schedule.every(1).minutes.do(self.check_and_notify_active_trades)
        
        # Schedule active trade checks every 15 minutes
        schedule.every().hour.at(":00").do(self.check_and_notify_active_trades)
        schedule.every().hour.at(":15").do(self.check_and_notify_active_trades)
        schedule.every().hour.at(":30").do(self.check_and_notify_active_trades)
        schedule.every().hour.at(":45").do(self.check_and_notify_active_trades)
        
        # Schedule the close positions task at 15:55
        schedule.every().day.at("15:55").do(self.close_positions)
        
        while True:
            try:
                current_time = datetime.now()
                if current_time.hour == 16 and current_time.minute >= 1:
                    self.logger.info("EndOfDay tasks completed")
                    schedule.clear()
                    break
                schedule.run_pending()
            except Exception as e:
                self.logger.error(f"EOD scheduler error: {e}", exc_info=True)
            time.sleep(1)

    def handle_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            parts = line.split()
            token = None
            status = None
            if len(parts) > 1:
                if parts[0].upper() == "%ORDER":
                    token = parts[2]
                    status = parts[11]
                    self.logger.debug(f"Processing %ORDER: token={token}, status={status}")
                elif parts[0].upper() == "%ORDERACT":
                    token = parts[-1]
                    status = parts[2]
                    self.logger.debug(f"Processing %ORDERACT: token={token}, status={status}")
                else:
                    continue
                if token and status:
                    try:
                        conn = self.db_pool.getconn()
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT tradeid, strategy, ticker, shares, stoporderid
                            FROM tradedetails
                            WHERE token = %s AND date = %s
                        """, (token, datetime.now().strftime('%Y-%m-%d')))
                        trade_detail = cursor.fetchone()
                        if trade_detail:
                            trade_id, strategy, ticker, shares, stop_order_id = trade_detail
                            self.logger.debug(f"Found tradedetails for token {token}: trade_id={trade_id}, ticker={ticker}, stopOrderID={stop_order_id}")
                            # Retrieve reason from dictionary
                            with self.lock:
                                reason = self.order_reasons.get(token, None)
                                self.logger.debug(f"Retrieved reason {reason} for token {token}")
                            if stop_order_id and status.upper() in ["EXECUTED", "EXECUTE"]:
                                self.logger.debug(f"Processing buy response for trade_id {trade_id}, token {token}")
                                self.process_buy_response(line, trade_id, ticker, shares, token, stop_order_id, reason)
                                if parts[0].upper() == "%ORDER" and status.upper() == "EXECUTED":
                                # Clean up the dictionary
                                    with self.lock:
                                        if token in self.order_reasons:
                                            del self.order_reasons[token]
                                            self.logger.debug(f"Removed reason for token {token} from order_reasons")    
                            elif not stop_order_id:
                                self.logger.error(f"Missing stop_order_id for trade_id {trade_id}")
                            
                                
                        else:
                            self.logger.warning(f"No tradedetails record found for token {token}")
                        conn.commit()
                    except psycopg2.Error as e:
                        self.logger.error(f"Error querying tradedetails for token {token}: {e}")
                        if conn:
                            conn.rollback()
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            self.db_pool.putconn(conn)

    def process_buy_response(self, line, trade_id, ticker, shares, token, stop_order_id, reason=None):
        self.logger.debug(f"Processing buy response for trade_id: {trade_id}, ticker: {ticker}")
        try:
            act_status = ""
            notes = ""
            executed = False
            time_executed = None
            price = None
            order_id = None
            status = None
            action = None
            parts = line.split()
            if parts[0].upper() == "%ORDER":
                order_id = parts[1]
                status = parts[11]
                shares = parts[6]
                ticker = parts[3]
                time_executed = parts[12]
                try:
                    dt = datetime.strptime(time_executed, '%H:%M:%S')
                    time_executed = dt.strftime('%H:%M:%S')
                except ValueError:
                    self.logger.warning(f"Invalid time format: {time_executed}, using as-is")
                price = float(parts[9])
                action = parts[4]
                if status == 'Executed':
                    executed = True
            elif parts[0].upper() == "%ORDERACT":
                act_status = parts[2]
                action = parts[3]
                ticker = parts[4]
                shares = parts[5]
                price = float(parts[6]) if parts[6].replace('.', '', 1).isdigit() else 0.0
                time_executed = parts[8]
                try:
                    dt = datetime.strptime(time_executed, '%H:%M:%S')
                    time_executed = dt.strftime('%H:%M:%S')
                except ValueError:
                    self.logger.warning(f"Invalid time format: {time_executed}, using as-is")
                notes = ' '.join(parts[9:-1])
                if act_status == 'Execute':
                    executed = True
            if order_id:
                self.logger.debug(f"Order line found: {line}")
                if executed or status == "Executed":
                    self.logger.debug(f"Buy order executed for trade_id: {trade_id}, ticker: {ticker}")
                    try:
                        # Verify trade details in TradeDetails
                        conn = self.db_pool.getconn()
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT strategy, stoporderid, token
                            FROM tradedetails
                            WHERE tradeid = %s AND ticker = %s AND date = %s
                        """, (str(trade_id), ticker, datetime.now().strftime('%Y-%m-%d')))
                        result = cursor.fetchone()
                        if result:
                            strategy, stored_stop_order_id, stored_token = result
                            self.logger.debug(f"Found TradeDetails: trade_id={trade_id}, strategy={strategy}, stopOrderID={stored_stop_order_id}, token={stored_token}")
                            if strategy is None:
                                self.logger.error(f"No strategy found in trade_details for trade_id {trade_id}, token {token}")
                                return
                            if stored_stop_order_id != stop_order_id:
                                self.logger.warning(f"stopOrderID mismatch for trade_id {trade_id}: expected {stop_order_id}, found {stored_stop_order_id}")
                            self.logger.debug(f"Retrieved strategy {strategy} for trade_id {trade_id}")
                        else:
                            self.logger.error(f"Trade ID {trade_id} not found in trade_details for token {token}")
                            return
                            
                        self.logger.info(f"Order executed for trade_id {trade_id}")
                        self.logger.debug(f"Starting insert_buy_market for trade_id: {trade_id}")
                        self.insert_buy_market(trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes)
                        self.logger.debug(f"Completed insert_buy_market for trade_id: {trade_id}")
                        
                        if parts[0].upper() == "%ORDER" and status.upper() == "EXECUTED":    
                            if reason == 'PartialClose':
                                self.logger.debug(f"Processing partial close for trade_id: {trade_id}")
                                # Update realized with real exit price
                                self.update_realized_after_partial(trade_id, ticker, price, shares)
                                # Replace stop with leftover shares
                                self.replace_stop_after_partial(trade_id, ticker, stop_order_id, shares)
                            else:
                                self.logger.debug(f"Starting cancel_stop_order for trade_id: {trade_id}")
                                self.cancel_stop_order(trade_id, stop_order_id)
                                self.logger.debug(f"Completed cancel_stop_order for trade_id: {trade_id}")
                                self.logger.debug(f"Starting move_trade_to_closed for trade_id: {trade_id}")
                                self.move_trade_to_closed(trade_id, ticker, price, time_executed, reason)
                                self.logger.debug(f"Completed move_trade_to_closed for trade_id: {trade_id}")
                    except Exception as e:
                        self.logger.error(f"Error processing trade_id {trade_id} after execution: {e}", exc_info=True)
                        if conn:
                            conn.rollback()
                        return
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            self.db_pool.putconn(conn)
                elif act_status == "Send_rej":
                    self.logger.warning(f"Buy order rejected for trade_id: {trade_id}, ticker: {ticker}, notes: {notes}")
                    try:
                        conn = self.db_pool.getconn()
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT strategy, stoporderid, token
                            FROM tradedetails
                            WHERE tradeid = %s AND ticker = %s AND date = %s
                        """, (str(trade_id), ticker, datetime.now().strftime('%Y-%m-%d')))
                        result = cursor.fetchone()
                        if result:
                            strategy, stored_stop_order_id, stored_token = result
                            self.logger.debug(f"Found TradeDetails for rejected order: trade_id={trade_id}, strategy={strategy}, stopOrderID={stored_stop_order_id}, token={stored_token}")
                            if strategy is None:
                                self.logger.error(f"No strategy found in trade_details for trade_id {trade_id}, token {token}")
                                return
                            self.insert_buy_market(trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes)
                            self.logger.info(f"Inserted rejected buy order into buymarket for trade_id: {trade_id}")
                        else:
                            self.logger.error(f"Trade ID {trade_id} not found in trade_details for token {token}")
                        conn.commit()
                    except psycopg2.Error as e:
                        self.logger.error(f"Error inserting rejected buy order for trade_id {trade_id}: {e}")
                        if conn:
                            conn.rollback()
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            self.db_pool.putconn(conn)
                else:
                    self.logger.debug(f"Order not executed or rejected. Status: {status}, Act_status: {act_status}")
        except Exception as e:
            self.logger.error(f"Error processing buy response for trade_id {trade_id}, token {token}: {e}", exc_info=True)
                

    def insert_buy_market(self, trade_id, strategy, time_executed, ticker, shares, price, token, order_id, action, status, act_status, notes):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            buy_market_data = {
                'tradeID': trade_id,
                'strategy': strategy,
                'time': time_executed,
                'ticker': ticker,
                'shares': int(shares),
                'price': float(price),
                'token': token,
                'orderID': order_id,
                'action': action,
                'status': status,
                'act_status': act_status,
                'notes': notes,
                'date': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Insert new record
            
            cursor.execute("""
                INSERT INTO buymarket (tradeid, strategy, ticker, shares, price, token, orderid, action, status, act_status, notes, time, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (trade_id, strategy, ticker, str(shares), str(price), token, order_id, action, status, act_status, notes,
                    time_executed, datetime.now().strftime('%Y-%m-%d')))
            self.logger.info(f"Inserted buy_market for trade_id: {trade_id}")
            if self.socketio:
                self.socketio.emit('buy_market_update', buy_market_data)
                self.logger.info(f"Emitted buy_market_update: {buy_market_data}")
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting/updating buy_market: {e}")
            if conn:
                conn.rollback()
        except Exception as e:
            self.logger.error(f"Error in insert_buy_market: {e}, buy_market_data: {buy_market_data}")        
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def cancel_stop_order(self, trade_id, stop_order_id):
        self.logger.debug(f"Attempting to cancel stop order for trade_id: {trade_id}")
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT stoporderid FROM tradedetails WHERE tradeid = %s AND stoporderid = %s
            """, (str(trade_id), stop_order_id))
            result = cursor.fetchone()
            if result:
                cancel_command = f'CANCEL {stop_order_id}'
                self.logger.debug(f"Sending cancel command: {cancel_command}")
                self.send_command(cancel_command)
            else:
                self.logger.warning(f"No matching stop order found for trade_id: {trade_id}, stopOrderID: {stop_order_id}")
        except psycopg2.Error as e:
            self.logger.error(f"Error canceling stop order: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def send_command(self, command):
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

    def move_trade_to_closed(self, trade_id, ticker, exit_price, exit_time, reason=None):
        self.logger.debug(f"Moving trade to closed for trade_id: {trade_id}, ticker: {ticker}")
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            query = """
                SELECT a.strategy, a.shares, a.entry_price, a.time, td.stop_loss AS original_stop_loss, a.target, a.risk
                FROM activetrades a
                JOIN tradedetails td ON a.tradeid = td.tradeid AND a.ticker = td.ticker AND td.date = %s
                WHERE a.tradeid = %s AND a.ticker = %s
            """
            params = (datetime.now().strftime('%Y-%m-%d'), str(trade_id), ticker)
            self.logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            trade = cursor.fetchone()
            if not trade:
                self.logger.warning(f"No active trade found for trade_id {trade_id}, ticker {ticker}")
                return

            strategy, shares, entry_price, entry_time, original_stop_loss, target, risk = trade
            if target is None:
                self.logger.warning(f"No target price found for trade_id {trade_id}, ticker {ticker}")
            if original_stop_loss is None:
                self.logger.warning(f"No stopmarket record for trade_id {trade_id}, ticker {ticker}, setting r_gain_loss to 0")
                r_gain_loss = 0.0
            else:
                risk_per_share = float(entry_price) - float(original_stop_loss)
                if risk_per_share != 0:
                    r_gain_loss = (float(entry_price) - float(exit_price)) / abs(risk_per_share)
                else:
                    self.logger.warning(f"Risk per share is zero for trade_id {trade_id}, setting r_gain_loss to 0")
                    r_gain_loss = 0.0
                    
                    
            cursor.execute("SELECT realized FROM activetrades WHERE tradeid = %s AND ticker = %s", (trade_id, ticker))
            accumulated = cursor.fetchone()
            partial_realized = float(accumulated[0]) if accumulated and accumulated[0] else 0.0

            remaining_realized = round((float(entry_price) - float(exit_price)) * float(shares), 2)
            total_realized = partial_realized + remaining_realized
            
            pl_color = "#d32f2f" if total_realized < 0 else "#2e7d32"
            reason_display = reason.title() if reason else "Take Profit"

            # === HTML Email Notification (matching active trades style) ===
            html_parts = [
                "<html><body style='font-family:Arial,sans-serif; background:#f4f4f4; padding:20px;'>",
                "<div style='max-width:640px; margin:0 auto; background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.1);'>",
                "<div style='background:#28a745; color:white; padding:20px; text-align:center;'>",  # Green header for closed trade
                "<h2 style='margin:0;'>Trade Closed Successfully</h2>",
                "</div>",
                "<div style='padding:20px;'>",
                "<div style='text-align:center; margin-bottom:20px;'>",
                f"<strong style='font-size:24px; color:#333;'>{ticker}</strong>",
                f"<span style='color:#777; font-size:14px;'> | ID: {trade_id} | {strategy.title()}</span><br><br>",
                f"<strong>Shares Closed:</strong> {shares}<br><br>",
                f"<strong>Entry Price:</strong> ${float(entry_price):.2f} &nbsp;&nbsp; <strong>Exit Price:</strong> ${float(exit_price):.2f}<br><br>",
                f"<strong>Realized P/L:</strong> ",
                f"<span style='font-weight:bold; font-size:18px; color:{pl_color};'>${total_realized:+.2f}</span><br><br>",
                f"<strong>Reason:</strong> {reason_display}<br>",
                f"<strong>Exit Time:</strong> {exit_time}",
                "</div>",
                "<p style='text-align:center; color:#999; font-size:12px; margin-top:30px;'>",
                "This trade has been moved to closed trades.",
                "</p>",
                "</div></div></body></html>"
            ]

            message = "".join(html_parts)
            subject = f"Trade Closed: {ticker} – {reason_display}"

            # === End of HTML Email ===

            # Convert Decimal to float for JSON serialization
            closed_trade_data = {
                'tradeID': trade_id,
                'strategy': strategy,
                'ticker': ticker,
                'shares': int(shares),  # Ensure integer
                'entry_price': float(entry_price) if entry_price is not None else None,
                'entry_time': entry_time,
                'original_stop_loss': float(original_stop_loss) if original_stop_loss is not None else None,
                'stop_loss': float(original_stop_loss) if original_stop_loss is not None else None,
                'sl_time': None,
                'exit_price': float(exit_price) if exit_price is not None else None,
                'exit_time': exit_time,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'reason': reason or 'TakeProfit',
                'realized': float(total_realized) if total_realized is not None else None,
                'r_gain_loss': float(r_gain_loss) if r_gain_loss is not None else None,
                'target': float(target) if target is not None else None,
                'risk': float(risk) if risk is not None else None
            }
            cursor.execute("""
                INSERT INTO closedtrades (
                    tradeid, strategy, ticker, shares, entry_price, entry_time,
                    original_stop_loss, stop_loss, sl_time, exit_price, exit_time,
                    date, reason, realized, r_gain_loss, target, risk
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                trade_id, strategy, ticker, shares, entry_price, entry_time,
                original_stop_loss, original_stop_loss, None, exit_price, exit_time,
                datetime.now().strftime('%Y-%m-%d'), reason or 'TakeProfit', total_realized, r_gain_loss, target, risk
            ))
            cursor.execute("""
                DELETE FROM activetrades WHERE tradeid = %s AND ticker = %s
            """, (trade_id, ticker))
            # Update tradestatus using ticker, strategy, and date
            current_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE tradestatus
                SET active_trade = 'closed'
                WHERE ticker = %s AND strategy = %s AND date = %s
            """, (ticker, strategy, current_date))
            if cursor.rowcount == 0:
                self.logger.warning(f"No tradestatus record updated for ticker {ticker}, strategy {strategy}, date {current_date}")
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                UPDATE borrowedshares
                SET available_shares = available_shares + %s, last_updated = %s
                WHERE ticker = %s AND DATE(last_updated) = %s
            """, (shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, today_date))
            if cursor.rowcount > 0:
                self.logger.info(f"Updated borrowedshares for ticker {ticker}, increased available_shares by {shares}")
            conn.commit()
            self.logger.info(f"Moved trade {trade_id} to closedtrades and updated tradestatus")
            # Send styled HTML email
            self.notify(message, subject=subject)
            self.logger.info(f"Sent styled trade closed email for {ticker} (Realized: ${total_realized:+.2f})")

            
            if self.socketio:
                self.socketio.emit('closed_trade_update', closed_trade_data)
                self.logger.info(f"Emitted closed_trade_update: {closed_trade_data}")
                active_trade_removal = {'tradeID': trade_id}
                self.socketio.emit('active_trade_remove', active_trade_removal)
                self.logger.info(f"Emitted active_trade_remove: {active_trade_removal}")
            
        except psycopg2.Error as e:
            self.logger.error(f"Database error moving trade to closed for trade_id {trade_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
        except Exception as e:
            self.logger.error(f"Unexpected error moving trade to closed for trade_id {trade_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
                
    def update_realized_after_partial(self, trade_id, ticker, exit_price, closed_shares):
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT entry_price, realized FROM activetrades
                WHERE tradeid = %s AND ticker = %s
            """, (trade_id, ticker))
            row = cursor.fetchone()
            if row:
                entry_price, current_realized = row
                entry_price = float(entry_price)
                current_realized = float(current_realized) if current_realized else 0.0
                realized_this = round((entry_price - float(exit_price)) * int(closed_shares), 2)
                new_realized = current_realized + realized_this
                cursor.execute("""
                    UPDATE activetrades
                    SET realized = %s
                    WHERE tradeid = %s AND ticker = %s
                """, (new_realized, trade_id, ticker))
                conn.commit()
                self.logger.info(f"Updated realized for partial close: trade_id={trade_id}, new_realized={new_realized}")
                if self.socketio:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute("""
                            SELECT tradeid, time, strategy, ticker, shares, entry_price, target,
                                   stop_loss, lu_price, unrealized, realized, date,
                                   sellorderid, stoporderid, last_price, risk
                            FROM activetrades WHERE tradeid = %s AND ticker = %s
                        """, (trade_id, ticker))
                        r = cur2.fetchone()
                        cur2.close()
                        if r:
                            self.socketio.emit('active_trade_update', {
                                'tradeID': r[0], 'time': str(r[1]) if r[1] else '',
                                'strategy': r[2], 'ticker': r[3], 'shares': r[4],
                                'entry_price': float(r[5]) if r[5] is not None else None,
                                'target': float(r[6]) if r[6] is not None else None,
                                'stop_loss': float(r[7]) if r[7] is not None else None,
                                'lu_price': float(r[8]) if r[8] is not None else None,
                                'unrealized': float(r[9]) if r[9] is not None else None,
                                'realized': float(r[10]) if r[10] is not None else 0.0,
                                'date': str(r[11]) if r[11] else '',
                                'sellOrderID': r[12], 'stopOrderID': r[13],
                                'last_price': float(r[14]) if r[14] is not None else None,
                                'risk': float(r[15]) if r[15] is not None else 0.0,
                            })
                        else:
                            self.socketio.emit('active_trade_update', {'tradeID': trade_id})
                    except Exception as emit_err:
                        self.logger.error(f"update_realized emit error: {emit_err}")
                        self.socketio.emit('active_trade_update', {'tradeID': trade_id})
        except Exception as e:
            self.logger.error(f"Error updating realized after partial: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def replace_stop_after_partial(self, trade_id, ticker, stop_order_id, closed_shares):
        conn = None
        cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT shares, price, strategy FROM stopmarket
                WHERE orderid = %s
            """, (stop_order_id,))
            row = cursor.fetchone()
            
            if not row:
                self.logger.error(f"replace_stop_after_partial: No stopmarket record found for orderid {stop_order_id}")
                return
            total_shares, current_stop_price, strategy = row
            total_shares = int(total_shares)
            leftover_shares = total_shares - int(closed_shares)
                
            if leftover_shares <= 0:
                self.logger.info(f"Leftover shares <= 0 ({leftover_shares}), canceling stop order instead of replacing")
                self.cancel_stop_order(trade_id, stop_order_id)
                return    
                
            if strategy == 'market':
                command = f"REPLACE {stop_order_id} {leftover_shares} STOPMKT {current_stop_price:.2f}"
                self.logger.info(f"[{ticker}] REPLACE STOPMKT -> {current_stop_price:.2f} (shares: {leftover_shares})")    
            else:    
                trigger_price = round(current_stop_price - 0.02, 2)
                command = f"REPLACE {stop_order_id} {leftover_shares} STOPLMT {trigger_price:.2f} {current_stop_price:.2f}"
                self.logger.info(f"[{ticker}] REPLACE STOPLMT -> trigger {trigger_price:.2f} | limit {current_stop_price:.2f} (shares: {leftover_shares})")
            # Send the replace command
            self.send_command(command + '\n')  # Ensure newline
            # Update DB
            cursor.execute("""
                UPDATE stopmarket
                SET shares = %s
                WHERE orderid = %s
            """, (leftover_shares, stop_order_id))
            cursor.execute("""
                UPDATE activetrades
                SET shares = %s
                WHERE tradeid = %s AND ticker = %s
            """, (leftover_shares, trade_id, ticker))
            cursor.execute("""
                UPDATE tradedetails
                SET shares = %s
                WHERE tradeid = %s AND ticker = %s
            """, (leftover_shares, trade_id, ticker))
            conn.commit()
            if self.socketio:
                self.socketio.emit('stop_market_update', {
                    'tradeID': trade_id,
                    'ticker': ticker,
                    'shares': leftover_shares,
                    'price': current_stop_price,
                    'orderID': stop_order_id
                })

            
        except Exception as e:
            self.logger.error(f"Error replacing stop after partial: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.db_pool.putconn(conn)            
    
    def check_and_notify_active_trades(self):
        """Send email with active trades + secure bulletproof Close button for each."""
        conn = cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeid, strategy, ticker, shares, entry_price, stop_loss, last_price, unrealized
                FROM activetrades
                ORDER BY ticker
            """)
            active_trades = cursor.fetchall()
            
            if not active_trades:
                self.logger.info("No active trades – skipping email.")
                return

            num_trades = len(active_trades)
            public_url = os.getenv('PUBLIC_URL')

            # Case 1: No serializer → plain text only
            if not hasattr(self, 'serializer') or self.serializer is None:
                self.logger.warning("Serializer not available – sending plain text email without close buttons")
                lines = ["Current Active Trades:\n"]
                for trade in active_trades:
                    trade_id, strategy, ticker, shares, entry_price, stop_loss, last_price, unrealized = trade
                    lines.append(
                        f"{ticker} | ID: {trade_id} | {strategy.title()}\n"
                        f"Shares: {shares} | Entry: ${entry_price:.2f} | Stop: ${stop_loss:.2f}\n"
                        f"Last: ${last_price:.2f} | Unrealized: ${unrealized:+.2f}\n"
                    )
                message = "\n".join(lines)
                subject = f"Active Trades ({num_trades})"

            # Case 2: Serializer exists
            else:
                # Subcase 2a: No PUBLIC_URL → fallback plain text
                if not public_url:
                    self.logger.error("PUBLIC_URL not found in .env – falling back to plain text email (no close buttons)")
                    lines = ["Current Active Trades (Close buttons disabled – PUBLIC_URL missing):\n"]
                    for trade in active_trades:
                        trade_id, strategy, ticker, shares, entry_price, stop_loss, last_price, unrealized = trade
                        lines.append(
                            f"{ticker} | ID: {trade_id} | {strategy.title()}\n"
                            f"Shares: {shares} | Entry: ${entry_price:.2f} | Stop: ${stop_loss:.2f}\n"
                            f"Last: ${last_price:.2f} | Unrealized: ${unrealized:+.2f}\n"
                        )
                    message = "\n".join(lines)
                    subject = f"Active Trades ({num_trades}) – Close Disabled"

                # Subcase 2b: Everything ready → full HTML email with button BELOW info
                else:
                    # Ensure trailing slash
                    if not public_url.endswith('/'):
                        public_url += '/'

                    html_parts = [
                        "<html><body style='font-family:Arial,sans-serif; background:#f4f4f4; padding:20px;'>",
                        "<div style='max-width:640px; margin:0 auto; background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.1);'>",
                        "<div style='background:#dc3545; color:white; padding:20px; text-align:center;'>",
                        f"<h2 style='margin:0;'>Active Trades ({num_trades})</h2>",
                        "</div>",
                        "<div style='padding:20px;'>",
                        "<p style='color:#555;'>Click the red button below each trade to close it immediately.</p>",
                        "<ul style='list-style:none; padding:0; margin:0;'>"
                    ]

                    for trade in active_trades:
                        trade_id, strategy, ticker, shares, entry_price, stop_loss, last_price, unrealized = trade
                        
                        # Token for full close
                        close_token_data = {'trade_id': trade_id, 'ticker': ticker}
                        close_signed = self.serializer.dumps(close_token_data, salt='close-trade')
                        close_url = f"{public_url}close_trade?token={urllib.parse.quote(close_signed)}"

                        # Token for partial close – different salt!
                        partial_token_data = {'trade_id': trade_id, 'ticker': ticker}
                        partial_signed = self.serializer.dumps(partial_token_data, salt='close-partial')
                        partial_url = f"{public_url}close_partial?token={urllib.parse.quote(partial_signed)}"

                        pl_color = "#d32f2f" if unrealized < 0 else "#2e7d32"

                        html_parts.append(f"""
                            <li style="border:1px solid #eee; border-radius:8px; margin:15px 0; padding:20px; background:#fafafa;">
                                <!-- Trade Information -->
                                <div style="text-align:center; margin-bottom:20px;">
                                    <strong style="font-size:20px; color:#333;">{ticker}</strong>
                                    <span style="color:#777; font-size:14px;"> | ID: {trade_id} | {strategy.title()}</span><br><br>
                                    <strong>Shares:</strong> {shares} &nbsp;&nbsp;
                                    <strong>Entry:</strong> ${entry_price:.2f} &nbsp;&nbsp;
                                    <strong>Stop:</strong> ${stop_loss:.2f}<br><br>
                                    <strong>Last Price:</strong> ${last_price:.2f} &nbsp;&nbsp;
                                    <strong>Unrealized P/L:</strong>
                                    <span style="font-weight:bold; color:{pl_color};"> ${unrealized:+.2f}</span>
                                </div>

                                <!-- Two Buttons Side-by-Side -->
                                <div style="text-align:center; margin-top:20px;">
                                    <table role="presentation" border="0" cellpadding="0" cellspacing="10" align="center">
                                        <tr>
                                            <!-- Full Close Button (Red) -->
                                            <td align="center">
                                                <!--[if mso]>
                                                <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word"
                                                    href="{close_url}" style="height:50px;v-text-anchor:middle;width:180px;" arcsize="15%" fillcolor="#dc3545">
                                                    <w:anchorlock/>
                                                    <center style="color:#ffffff;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;">
                                                        Close Trade
                                                    </center>
                                                </v:roundrect>
                                                <![endif]-->

                                                <!--[if !mso]><!-- -->
                                                <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                    <tr>
                                                        <td align="center" bgcolor="#dc3545" style="border-radius:8px; padding:15px 20px;">
                                                            <a href="{close_url}" target="_blank"
                                                               style="font-family:Arial,sans-serif; font-size:16px; color:#ffffff; text-decoration:none; font-weight:bold;">
                                                                Close Trade
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </table>
                                                <!--<![endif]-->
                                            </td>

                                            <!-- Partial Close Button (Orange) -->
                                            <td align="center">
                                                <!--[if mso]>
                                                <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word"
                                                    href="{partial_url}" style="height:50px;v-text-anchor:middle;width:180px;" arcsize="15%" fillcolor="#fd7e14">
                                                    <w:anchorlock/>
                                                    <center style="color:#ffffff;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;">
                                                        Close Partial
                                                    </center>
                                                </v:roundrect>
                                                <![endif]-->

                                                <!--[if !mso]><!-- -->
                                                <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                    <tr>
                                                        <td align="center" bgcolor="#fd7e14" style="border-radius:8px; padding:15px 20px;">
                                                            <a href="{partial_url}" target="_blank"
                                                               style="font-family:Arial,sans-serif; font-size:16px; color:#ffffff; text-decoration:none; font-weight:bold;">
                                                                Close Partial
                                                            </a>
                                                        </td>
                                                    </tr>
                                                </table>
                                                <!--<![endif]-->
                                            </td>
                                        </tr>
                                    </table>
                                </div>
                            </li>
                        """)

                    html_parts.extend([
                        "</ul>",
                        "<p style='text-align:center; color:#999; font-size:12px; margin-top:30px;'>",
                        "Secure links expire in 1 hour.<br>",
                        "Only click if you intend to close the trade.",
                        "</p>",
                        "</div></div></body></html>"
                    ])

                    message = "".join(html_parts)
                    subject = f"Active Trades ({num_trades}) – Close from Email"

            # Send the email
            self.notify(message, subject=subject)
            self.logger.info(f"Active trades email sent ({num_trades} trades)")

        except Exception as e:
            self.logger.error(f"Error in check_and_notify_active_trades: {e}", exc_info=True)
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)        
                

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    end_of_day = EndOfDay()
    end_of_day.connect_to_server()
    
    # Start listening to server in a separate thread
    listener_thread = threading.Thread(target=end_of_day.listen_to_server, daemon=True)
    listener_thread.start()
    combined_thread = threading.Thread(target=end_of_day.run_combined_tasks, daemon=True)
    combined_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        end_of_day.keep_listening = False
        end_of_day.disconnect_from_server()