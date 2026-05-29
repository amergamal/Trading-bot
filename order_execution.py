import psycopg2
from psycopg2 import pool
import config
import socket
import random
import logging
import time
import threading
from datetime import datetime
from trade_monitor import TradeMonitor
from sl_monitor import SLMonitor
from end_of_day import EndOfDay
from short_locate import Slocate





logger = logging.getLogger('OrderExecution')
multi_log = logging.getLogger('MultiStrategy')


# Initialize a global lock for managing access to the token_map
token_map_lock = threading.Lock()

LOCAL_SERVER_PORT = 5015

def generate_token():
    return str(random.randint(100000, 999999))

def send_command(command):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', LOCAL_SERVER_PORT))
    client_socket.sendall(command.encode())
    logger.debug(f'Sent command: {command}')
    return client_socket

def process_response(response, token_map, available_shares_map=None):
    lines = response.strip().split('\n')
    order_received = False
    for line in lines:
        logger.debug(f'Processing response line: {line}')
        if line.startswith('%ORDER'):
            order_received = True
            parts = line.split()
            try:
                response_token = parts[2]
                order_id = parts[1]
                time_str = parts[12]
                ticker = parts[3]
                shares = parts[6]
                price = parts[9]
                action = parts[4]
                status = parts[11]
                with token_map_lock:
                    if response_token in token_map:
                        token_map[response_token].update({
                            'order_status': status,
                            'order_time': time_str,
                            'ticker': ticker,
                            'shares': shares,
                            'price': price,
                            'action': action,
                            'order_id': order_id
                        })
                        logger.debug(f"Updated token_map for token {response_token}: {token_map[response_token]}")
                logger.info(f'Order - Order ID: {order_id}, Token: {response_token}, Status: {status}, Time: {time_str}, Ticker: {ticker}, Shares: {shares}, Price: {price}, Action: {action}')
            except IndexError as e:
                logger.error(f'Error parsing %ORDER response: {e}')
        elif line.startswith('%OrderAct'):
            parts = line.split()
            try:
                order_id = parts[1]
                action_type = parts[2]
                b_s = parts[3]
                ticker = parts[4]
                shares = parts[5]
                price = parts[6]
                route = parts[7]
                time_str = parts[8]
                note = ' '.join(parts[9:-1])
                token = parts[-1]
                with token_map_lock:
                    if token in token_map:
                        token_map[token].update({
                            'order_act_action': action_type,
                            'note': note,
                            'order_time': time_str,
                            'ticker': ticker,
                            'shares': shares,
                            'price': price,
                            'action': b_s,
                            'order_id': order_id
                        })
                        logger.debug(f"Updated token_map for token {token}: {token_map[token]}")
                logger.info(f'Order Act - Order ID: {order_id}, Token: {token}, Action: {action_type}, Note: {note}')
            except IndexError as e:
                logger.error(f'Error parsing %OrderAct response: {e}')
        elif line.startswith('$SLAvailQueryRet') and available_shares_map is not None:
            parts = line.split()
            try:
                account, ticker, available_shares = parts[1], parts[2], int(parts[3])
                with token_map_lock:
                    available_shares_map[ticker] = available_shares
                logger.info(f"Available shares for {ticker}: {available_shares}")
            except (IndexError, ValueError) as e:
                logger.error(f'Error parsing $SLAvailQueryRet response: {e}')
    return order_received

def listen_for_responses(client_socket, token_map, available_shares_map=None):
    buffer_size = 4096
    while True:
        response = client_socket.recv(buffer_size).decode()
        if response:
            logger.debug(f'Received response: {response}')
            process_response(response, token_map, available_shares_map)
        else:
            logger.debug('No response received, waiting...')
            time.sleep(0.5)

def send_sell_limit_order(ticker, shares, trade_id, strategy, price, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_sell = f'NEWORDER {token} S {ticker} SMAT {shares} {price} TIF=DAY+ {trade_id}'
    client_socket = send_command(command_sell)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token 

def send_buy_limit_order(ticker, shares, target_price, trade_id, strategy, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_buy_limit = f'NEWORDER {token} B {ticker} SMAT {shares} {target_price} ARCA TIF=DAY+ {trade_id}'
    client_socket = send_command(command_buy_limit)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token                       

def send_sell_market_order(ticker, shares, trade_id, strategy, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_sell = f'NEWORDER {token} S {ticker} SMAT {shares} MKT TIF=DAY {trade_id}'
    client_socket = send_command(command_sell)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token

def send_stop_market_order(ticker, shares, stop_price, trade_id, strategy, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_stop_market = f'NEWORDER {token} B {ticker} SMAT {shares} STOPMKT {stop_price} {trade_id}'
    client_socket = send_command(command_stop_market)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token
def send_stop_sell_limit_order(ticker, shares, stop_price, trade_id, strategy, token_map):
    token = generate_token()
    limit_price = round(stop_price - 0.02, 2)  # 2 pennies below stop price

    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_stop_market = f'NEWORDER {token} S {ticker} SMAT {shares} STOPLMT {stop_price} {limit_price} {trade_id}'
    client_socket = send_command(command_stop_market)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token


def send_stop_sell_order(ticker, shares, stop_price, trade_id, strategy, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_stop_market = f'NEWORDER {token} S {ticker} SMAT {shares} STOPMKT {stop_price} {trade_id}'
    client_socket = send_command(command_stop_market)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token

def send_stop_limit_order(ticker, shares, stop_price, trade_id, strategy, token_map):
    token = generate_token()
    # Critical fix: limit price must be >= stop price for buy-to-cover
    limit_price = round(stop_price + 0.03, 2)   # +3 cents buffer
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id, 'strategy': strategy}
    command_stop_market = f'NEWORDER {token} B {ticker} SMAT {shares} STOPLMT {stop_price} {limit_price} {trade_id}'
    client_socket = send_command(command_stop_market)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token

class OrderExecution:
    def __init__(self, trade_monitor, sl_monitor, end_of_day, stopsell_monitor=None, risk_management=None, risk_managementauto=None, slocate=None, socketio=None):
        self.risk_management = risk_management
        self.risk_managementauto = risk_managementauto
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.token_map = {}
        self.available_shares_map = {}  # Store $SLAvailQueryRet results
        self.trade_monitor = trade_monitor
        self.sl_monitor = sl_monitor
        self.stopsell_monitor = stopsell_monitor
        self.end_of_day = end_of_day
        self.slocate = slocate if slocate else Slocate(socketio=socketio)
        
        self.socketio = socketio
        self.logger = logging.getLogger('OrderExecution')
        self.logger.debug(f"Received socketio instance: {socketio}, type: {type(socketio)}")
        if socketio is None:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        else:
            self.logger.info("SocketIO instance provided; real-time updates enabled.")
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
        self.logger.info("OrderExecution initialized with TradeMonitor, SLMonitor, SSMonitor, and EndOfDay")
        
        if self.stopsell_monitor is None:
            self.logger.warning("stopsell_monitor not provided during init; it must be set manually before use.")

    def check_available_shares(self, ticker, required_shares, trade_id):
        self.logger.info(f"Querying available shares to short for {ticker}...")
        # Clear existing entry for ticker to force waiting for new response
        with token_map_lock:
            if ticker in self.available_shares_map:
                del self.available_shares_map[ticker]
                self.logger.debug(f"Cleared stale available_shares_map entry for {ticker}")
                
                
        command = f"SLAvailQuery {config.get_active_account()} {ticker}"
        client_socket = send_command(command)
        self.logger.info(f"Sending command: {command}")

        # Start listener thread to capture $SLAvailQueryRet
        response_thread = threading.Thread(
            target=listen_for_responses,
            args=(client_socket, self.token_map, self.available_shares_map),
            daemon=True
        )
        response_thread.start()

        # Wait for response (up to 10 seconds)
        start_time = time.time()
        while ticker not in self.available_shares_map and time.time() - start_time < 10:
            time.sleep(0.5)
        
        if ticker in self.available_shares_map:
            available_shares = self.available_shares_map[ticker]
            self.logger.info(f"Available shares for {ticker} from SLAvailQuery: {available_shares}")
        else:
            self.logger.warning(f"No response from SLAvailQuery for {ticker}, assuming 0 shares available")
            available_shares = 0  # Assume no shares if SLAvailQuery fails

        if available_shares >= required_shares:
            self.logger.info(f"Sufficient shares available ({available_shares} >= {required_shares}) for {ticker}")
            return available_shares, True
        else:
            needed_shares = required_shares - available_shares
            self.logger.info(f"Need to borrow {needed_shares} additional shares for {ticker}")
            borrow_result = self.slocate.receive_order_details(ticker, needed_shares, trade_id)
        
            if borrow_result.get('already_shortable', False):
                self.logger.info(f"Ticker {ticker} is already shortable, no borrowing needed")
                return available_shares, True  # Treat as sufficient shares since already shortable
            elif borrow_result['status'] == 'success':
                self.logger.info(f"Successfully borrowed {needed_shares} shares for {ticker}")
                return available_shares + needed_shares, True
            else:
                self.logger.error(f"Failed to borrow shares for {ticker}: {borrow_result['message']}")
                return available_shares, False

    def insert_into_database(self, table, details):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            columns = ', '.join(details.keys())
            placeholders = ', '.join(['%s'] * len(details))
            sql = f'INSERT INTO {table.lower()} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
            cursor.execute(sql, list(details.values()))
            conn.commit()
            self.logger.info(f'Successfully inserted into {table}: {details}')
            if self.socketio:
                event_map = {
                    'sellmarket': 'sell_market_update',
                    'stopmarket': 'stop_market_update',
                    'buymarket': 'buy_market_update'
                }
                event = event_map.get(table.lower())
                if event:
                    self.socketio.emit(event, details)
                    self.logger.info(f"Emitted {event}: {details}")
        except psycopg2.Error as e:
            self.logger.error(f'Error inserting into {table}: {e}')
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)
            
    def store_order_details(self, token, table):
        details = self.token_map.get(token)
        if details:
            data = {
                'tradeID': details['trade_id'],
                'strategy': details['strategy'],
                'time': details.get('order_time', 'N/A'),
                'ticker': details.get('ticker', 'N/A'),
                'shares': details.get('shares', 'N/A'),
                'price': details.get('price', 'N/A'),
                'token': token,
                'orderID': details.get('order_id', 'N/A'),
                'action': details.get('action', 'N/A'),
                'status': details.get('order_status', 'N/A'),
                'act_status': details.get('order_act_action', 'N/A'),
                'notes': details.get('note', 'N/A'),
                'date': details.get('date', datetime.now().strftime('%Y-%m-%d'))
            }
            logging.debug(f'Inserting into {table}: {data}')
            self.insert_into_database(table, data)        

    def execute_command(self, command):
        order_type = command['order_type']
        ticker = command['ticker']
        shares = command['shares']
        trade_id = command['trade_id']
        stop_price = command.get('stop_price')
        strategy = command.get('strategy')
        entry_price = command.get('price')
        target_price = command.get('target_price')
        risk = command['risk']
        required_shares = int(shares)
        self.logger.info(f"Received command: order_type={order_type}, ticker={ticker}, shares={shares}, required_shares={required_shares}, trade_id={trade_id}")

        if order_type in ['limit', 'market', 'target', 'stop']:
            if entry_price is not None:
                entry_price = round(float(entry_price), 2)
            if stop_price is not None:
                stop_price = round(float(stop_price), 2)
            if target_price is not None:
                target_price = round(float(target_price), 2)
            self.logger.info(f"Processing {order_type} order for {ticker} with {shares} shares")

            # Paper accounts don't support share location via DAS API — skip and proceed directly
            if config.get_account_mode() == 'paper':
                self.logger.info(f"Paper mode: skipping share locate for {ticker}, proceeding with order")
                available_shares, has_sufficient_shares = required_shares, True
            else:
                available_shares, has_sufficient_shares = self.check_available_shares(ticker, required_shares, trade_id)

            if has_sufficient_shares:
                self.logger.info(f"Sufficient shares available for {ticker}: available_shares={available_shares}, required_shares={required_shares}")
            else:
                self.logger.error(f"Insufficient shares for {ticker}: available_shares={available_shares}, required_shares={required_shares}")
                self._update_tradesignal_status(ticker, trade_id, "Failed")
                return  # Stop if borrowing failed
            
            # Proceed with sell market order for target orders
            if order_type == 'target':
                sell_token = send_sell_limit_order(ticker, shares, trade_id, strategy, entry_price, self.token_map)
                self.logger.info(f"Sent sell target order for {ticker} with token {sell_token}")

                order_confirmed = self.wait_for_order_confirmation(sell_token)

                with token_map_lock:
                    self.store_order_details(sell_token, 'SellMarket')

                if order_confirmed:
                    self.logger.info(f'Sell limit order executed for {ticker}')
                    self._update_tradesignal_status(ticker, trade_id, "Executed")

                    # Initialize Multi pyramid state as soon as the short entry fills,
                    # before the stop order is placed, so price monitoring starts immediately.
                    if strategy == 'Multi':
                        self.trade_monitor.handle_first_multi_entry({
                            'ticker': ticker,
                            'risk': risk,
                            'stop_loss': stop_price,
                        })
                        self.logger.info(f"Multi state initialized for {ticker} with stop=${stop_price}")
                        multi_log.info(f"[ENTRY-FILLED] {ticker} | TradeID={trade_id} | Sell limit filled | Stop=${stop_price} | Risk=${risk}")

                    if stop_price:
                        stop_token = send_stop_limit_order(ticker, shares, stop_price, trade_id, strategy, self.token_map)
                        self.logger.info(f'Sent stop limit order for {ticker} at stop price {stop_price}')
                        stop_order_confirmed = self.bstop_and_blimit_order_confirmation(stop_token)
                        with token_map_lock:
                            self.store_order_details(stop_token, 'StopMarket')
                        if stop_order_confirmed:
                            trade_details = {
                                'tradeID': trade_id,
                                'time': datetime.now().strftime('%H:%M:%S'),
                                'strategy': strategy,
                                'ticker': ticker,
                                'shares': shares,
                                'entry_price': self.token_map[sell_token].get('price', 0),
                                'stop_loss': stop_price,
                                'target_price': target_price,
                                'risk': risk,
                                'sellOrderID': self.token_map[sell_token].get('order_id', 'N/A'),
                                'stopOrderID': self.token_map[stop_token].get('order_id', 'N/A'),
                                'stopSellID': 'N/A'
                            }
                            if strategy == 'Multi':
                                multi_log.info(
                                    f"[STOP-PLACED] {ticker} | TradeID={trade_id} | "
                                    f"StopOrderID={trade_details['stopOrderID']} | Stop=${stop_price} | "
                                    f"Entry=${trade_details['entry_price']} | Shares={shares}"
                                )
                            self.logger.info(f"Sending trade details to TradeMonitor: {trade_details}")
                            self.trade_monitor.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to TradeMonitor successfully")
                            self.logger.info(f"Sending trade details to SLMonitor: {trade_details}")
                            self.sl_monitor.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to SLMonitor successfully")
                            self.logger.info(f"Sending trade details to EndOfDay: {trade_details}")
                            self.end_of_day.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to EndOfDay successfully")

                        else:
                            self.logger.error(f"Stop market order for {ticker} failed.")
                            if strategy == 'Multi':
                                multi_log.error(f"[STOP-FAILED] {ticker} | TradeID={trade_id} | Initial stop order NOT confirmed — position unprotected!")
                    # Clean up token_map after successful execution
                    self.cleanup_token_map(sell_token)
                    if stop_price:
                        self.cleanup_token_map(stop_token)
                else:
                    self.logger.error(f"Sell market order for {ticker} failed.")
                    self._update_tradesignal_status(ticker, trade_id, "Canceled")
                    self.cleanup_token_map(sell_token)
                    


            # Proceed with sell market order for market orders
            if order_type == 'market':
                sell_token = send_sell_market_order(ticker, shares, trade_id, strategy, self.token_map)
                self.logger.info(f"Sent sell market order for {ticker} with token {sell_token}")

                order_confirmed = self.wait_for_order_confirmation(sell_token)

                with token_map_lock:
                    self.store_order_details(sell_token, 'SellMarket')

                if order_confirmed:
                    self.logger.info(f'Sell market order executed for {ticker}')
                    self._update_tradesignal_status(ticker, trade_id, "Executed")
                    if stop_price:
                        stop_token = send_stop_market_order(ticker, shares, stop_price, trade_id, strategy, self.token_map)
                        self.logger.info(f'Sent stop market order for {ticker} at stop price {stop_price}')
                        stop_order_confirmed = self.bstop_and_blimit_order_confirmation(stop_token)
                        with token_map_lock:
                            self.store_order_details(stop_token, 'StopMarket')
                        if stop_order_confirmed:
                            trade_details = {
                                'tradeID': trade_id,
                                'time': datetime.now().strftime('%H:%M:%S'),
                                'strategy': strategy,
                                'ticker': ticker,
                                'shares': shares,
                                'entry_price': self.token_map[sell_token].get('price', 0),
                                'stop_loss': stop_price,
                                'target_price': target_price,
                                'risk': risk,
                                'sellOrderID': self.token_map[sell_token].get('order_id', 'N/A'),
                                'stopOrderID': self.token_map[stop_token].get('order_id', 'N/A'),
                                'stopSellID': 'N/A'
                            }
                            self.logger.info(f"Sending trade details to TradeMonitor: {trade_details}")
                            self.trade_monitor.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to TradeMonitor successfully")
                            self.logger.info(f"Sending trade details to SLMonitor: {trade_details}")
                            self.sl_monitor.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to SLMonitor successfully")
                            self.logger.info(f"Sending trade details to EndOfDay: {trade_details}")
                            self.end_of_day.receive_order_details(trade_details)
                            self.logger.info("Trade details sent to EndOfDay successfully")
                            
                        else:
                            self.logger.error(f"Stop market order for {ticker} failed.")
                    # Clean up token_map after successful execution
                    self.cleanup_token_map(sell_token)
                    if stop_price:
                        self.cleanup_token_map(stop_token)
                else:
                    self.logger.error(f"Sell market order for {ticker} failed.")
                    self._update_tradesignal_status(ticker, trade_id, "Canceled")
                    self.cleanup_token_map(sell_token)
                    # Update TradeStatus to closed on failure
                    try:
                        conn = self.db_pool.getconn()
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT 1 FROM tradestatus
                            WHERE ticker = %s AND strategy = %s AND date = %s
                        """, (ticker, strategy, datetime.now().strftime('%Y-%m-%d')))
                        exists = cursor.fetchone()
                        if exists:
                            cursor.execute("""
                                UPDATE tradestatus
                                SET active_trade = 'closed'
                                WHERE ticker = %s AND strategy = %s AND date = %s
                            """, (ticker, strategy, datetime.now().strftime('%Y-%m-%d')))
                            self.logger.info(f"Updated TradeStatus to closed for {ticker}, {strategy} on {datetime.now().strftime('%Y-%m-%d')} due to sell market order rejection")
                        conn.commit()
                    except psycopg2.Error as e:
                        self.logger.warning(f"Database warning while updating TradeStatus for {ticker}: {e}")
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            self.db_pool.putconn(conn)

            elif order_type == 'limit':
                sell_token = send_sell_limit_order(ticker, shares, trade_id, strategy, entry_price, self.token_map)
                self.logger.info(f"Sent sell limit order for {ticker} with token {sell_token}")
                order_confirmed = self.bstop_and_blimit_order_confirmation(sell_token)
                with token_map_lock:
                    self.store_order_details(sell_token, 'SellMarket')
                if order_confirmed:
                    self.logger.info(f'Sell limit order executed for {ticker}')
                    self._update_tradesignal_status(ticker, trade_id, "L-confirmed")
                    trade_details = {
                        'tradeID': trade_id,
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'strategy': strategy,
                        'ticker': ticker,
                        'shares': shares,
                        'entry_price': self.token_map[sell_token].get('price', 0),
                        'stop_loss': stop_price,
                        'target_price': target_price,
                        'risk': risk,
                        'sellOrderID': self.token_map[sell_token].get('order_id', 'N/A'),
                        'stopOrderID': 'N/A',
                        'stopSellID': self.token_map[sell_token].get('order_id', 'N/A'),
                    }
                            
                    self.logger.info(f"Sending trade details to SSMonitor: {trade_details}")
                    if self.stopsell_monitor is None:
                        self.logger.error("stopsell_monitor is None; cannot send order details.")
                        return  # Or raise an exception
                    self.stopsell_monitor.receive_order_details(trade_details)
                else:
                    self.logger.error(f"Sell stop order for {ticker} failed.")
                    self._update_tradesignal_status(ticker, trade_id, "Canceled")
                            
                self.cleanup_token_map(sell_token)           
                    
                            
            elif order_type == 'stop':
                stop_token = send_stop_sell_limit_order(ticker, shares, entry_price, trade_id, strategy, self.token_map)
                self.logger.info(f"Sent sell stop order for {ticker} with token {stop_token}")
                stop_order_confirmed = self.bstop_and_blimit_order_confirmation(stop_token)
                with token_map_lock:
                    self.store_order_details(stop_token, 'SellMarket')
                if stop_order_confirmed:
                    self.logger.info(f'Sell stop order accepted for {ticker}')
                    self._update_tradesignal_status(ticker, trade_id, "Stop-confirmed")
                    trade_details = {
                        'tradeID': trade_id,
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'strategy': strategy,
                        'ticker': ticker,
                        'shares': shares,
                        'entry_price': entry_price,
                        'stop_loss': stop_price,
                        'target_price': target_price,
                        'risk': risk,
                        'sellOrderID': 'N/A',
                        'stopOrderID': 'N/A',
                        'stopSellID': self.token_map[stop_token].get('order_id', 'N/A')
                    }
                    self.logger.info(f"Sending trade details to SSMonitor: {trade_details}")
                    if self.stopsell_monitor is None:
                        self.logger.error("stopsell_monitor is None; cannot send order details.")
                        return  # Or raise an exception
                    self.stopsell_monitor.receive_order_details(trade_details)
                else:
                    self.logger.error(f"Sell stop order for {ticker} failed.")
                    self._update_tradesignal_status(ticker, trade_id, "Canceled")
                  
                self.cleanup_token_map(stop_token)
                
                   
        elif order_type == 'stop_only':
            # ---- bypass borrow completely ----
            self.logger.info(f"STOP_ONLY:  skip borrow for trade_id={trade_id}")

            # Use values directly from command (passed by SSMonitor)
            ticker       = command.get('ticker')
            shares       = int(command.get('shares', 0))
            stop_price   = command.get('stop_price')
            target_price = command.get('target_price')
            risk = command['risk']
            trade_id = command['trade_id']
            sell_order_id = command.get('sellOrderID', 'N/A')  # from stop-sell

            if not all([ticker, shares, stop_price]):
                self.logger.error(f"STOP_ONLY: missing required fields: {command}")
                return
 
            # ---- send stop-market order ----
            stop_token = send_stop_limit_order(
                ticker, shares, stop_price, trade_id, strategy, self.token_map)

            self.logger.info(f"STOP_ONLY: bypassed borrow, sent STOPMKT token={stop_token}")
    
            # ---- wait for Accepted ----
            accepted = self.bstop_and_blimit_order_confirmation(stop_token)

            with token_map_lock:
                self.store_order_details(stop_token, 'StopMarket')

            if accepted:
                stop_order_id = self.token_map[stop_token].get('order_id', 'N/A')
                self._update_tradesignal_status(ticker, trade_id, "Executed")

                trade_details = {
                    'tradeID'     : trade_id,
                    'time'        : datetime.now().strftime('%H:%M:%S'),
                    'strategy'    : strategy,
                    'ticker'      : ticker,
                    'shares'      : shares,
                    'entry_price' : entry_price,
                    'stop_loss'   : stop_price,
                    'target_price': target_price,
                    'risk': risk,
                    'sellOrderID' : sell_order_id,
                    'stopOrderID' : stop_order_id,
                    'stopSellID'  : 'N/A'
                }

                # Send to monitors
                self.trade_monitor.receive_order_details(trade_details)
                self.logger.info(f"Sending trade details to TradeMonitor: {trade_details}")
                self.sl_monitor.receive_order_details(trade_details)
                self.logger.info("Trade details sent to SLMonitor successfully")
                self.end_of_day.receive_order_details(trade_details)
                self.logger.info("STOP_ONLY -> sent to TradeMonitor & EndOfDay")

                self.cleanup_token_map(stop_token)
                
            else:
                self.logger.error("STOP_ONLY -> stop order rejected")
                self.cleanup_token_map(stop_token)
        else:
            self.logger.error(
                f"UNKNOWN OR UNSUPPORTED order_type='{order_type}' | "
                f"trade_id={trade_id} | ticker={ticker} | full command={command}"
            )
            # Optional: mark the signal as failed so it doesn't stay pending forever
            try:
                self._update_tradesignal_status(ticker, trade_id, "Failed - Unknown order_type")
            except:
                pass  # don't crash if update fails

        # Always return at the end (some branches already return early, that's fine)
        return   
    
    def execute_multi_add_on(self, trade_id, ticker, shares, limit_price, stop_loss, risk, strategy='Multi'):
        self.logger.info(f"Multi add-on: {ticker} tradeID={trade_id} limit={limit_price} stop={stop_loss} risk=${risk} shares={shares}")
        multi_log.info(f"[ADD-ON-SEND] {ticker} | TradeID={trade_id} | Limit=${limit_price:.2f} | Stop=${stop_loss:.2f} | Shares={shares} | Risk=${risk:.2f}")

        # === 1. Place the short-sell limit entry ===
        sell_token = send_sell_limit_order(ticker, shares, trade_id, strategy, limit_price, self.token_map)
        sell_confirmed = self.bstop_and_blimit_order_confirmation(sell_token)

        with token_map_lock:
            self.store_order_details(sell_token, 'SellMarket')

        if not sell_confirmed:
            self.logger.error(f"Multi add-on sell rejected for {ticker}")
            multi_log.error(f"[ADD-ON-REJECTED] {ticker} | TradeID={trade_id} | Sell limit NOT confirmed — add-on aborted")
            self.cleanup_token_map(sell_token)
            return

        sell_order_id = self.token_map[sell_token].get('order_id', 'N/A')
        multi_log.info(f"[ADD-ON-FILLED] {ticker} | TradeID={trade_id} | SellOrderID={sell_order_id} | Filled at ~${limit_price:.2f}")

        # === 2. Place the stop-buy to protect the add-on position ===
        stop_order_id = 'N/A'
        stop_token = send_stop_limit_order(ticker, shares, stop_loss, trade_id, strategy, self.token_map)
        self.logger.info(f"Multi add-on: sent stop-buy for {ticker} at ${stop_loss:.2f}")
        stop_confirmed = self.bstop_and_blimit_order_confirmation(stop_token)

        with token_map_lock:
            self.store_order_details(stop_token, 'StopMarket')

        if stop_confirmed:
            stop_order_id = self.token_map[stop_token].get('order_id', 'N/A')
            self.logger.info(f"Multi add-on: stop-buy confirmed for {ticker} orderID={stop_order_id}")
            multi_log.info(f"[ADD-ON-STOP-OK] {ticker} | TradeID={trade_id} | StopOrderID={stop_order_id} | Stop=${stop_loss:.2f} confirmed")
        else:
            self.logger.error(f"Multi add-on: stop-buy NOT confirmed for {ticker} — position is unprotected!")
            multi_log.error(f"[ADD-ON-STOP-FAIL] {ticker} | TradeID={trade_id} | Stop-buy NOT confirmed at ${stop_loss:.2f} — position unprotected!")

        # === 3. Register the add-on with all monitors ===
        self._update_tradesignal_status(ticker, trade_id, "Executed")

        trade_details = {
            'tradeID': trade_id,
            'time': datetime.now().strftime('%H:%M:%S'),
            'strategy': strategy,
            'ticker': ticker,
            'shares': shares,
            'entry_price': limit_price,
            'stop_loss': stop_loss,
            'target_price': None,
            'risk': risk,
            'sellOrderID': sell_order_id,
            'stopOrderID': stop_order_id,
            'stopSellID': 'N/A'
        }

        self.trade_monitor.receive_order_details(trade_details)
        self.sl_monitor.receive_order_details(trade_details)
        self.end_of_day.receive_order_details(trade_details)   
        
    def _update_tradesignal_status(self, ticker: str, trade_id: str, status: str):
        """Set tradesignal.status to the given value."""
        conn = cursor = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE tradesignal
                SET status = %s
                WHERE ticker = %s AND tradeid = %s
                RETURNING tradeid, time, strategy, ticker, price, shares, target, hi, risk, status
            """, (status, ticker, trade_id))
            
            updated_row = cursor.fetchone()  # Now this works because of RETURNING
            conn.commit()


            if updated_row:
                # Build the full signal dict exactly like frontend expects
                signal = {
                    'tradeID': updated_row[0],
                    'time': str(updated_row[1]) if updated_row[1] else '',
                    'strategy': updated_row[2],
                    'ticker': updated_row[3],
                    'price': float(updated_row[4]) if updated_row[4] else 0.0,
                    'shares': int(updated_row[5]) if updated_row[5] else 0,
                    'target': float(updated_row[6]) if updated_row[6] else 0.0,
                    'hi': float(updated_row[7]) if updated_row[7] else 0.0,
                    'risk': float(updated_row[8]) if updated_row[8] else 0.0,
                    'status': updated_row[9],
                }

                self.logger.info(f"tradesignal {status} for {ticker}, trade_id={trade_id}")

                # THIS IS THE KEY LINE — EMIT TO FRONTEND
                if self.socketio:
                    self.socketio.emit('trade_signal_update', signal)
                    self.logger.info(f"Emitted via global_socketio: {signal}")
                else:
                    self.logger.warning("global_socketio not available")

            else:
                self.logger.warning(f"No tradesignal row updated for ticker={ticker}, trade_id={trade_id}")
        except psycopg2.Error as e:
            self.logger.error(f"DB error updating tradesignal: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)                              

    def get_available_shares(self, ticker):
        self.logger.debug(f"Checking available shares for ticker: {ticker}")
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT available_shares 
                FROM borrowedshares 
                WHERE ticker = %s 
                AND DATE(last_updated) = %s
            """, (ticker, today_date))
            row = cursor.fetchone()
            if row:
                available_shares = row[0]
                self.logger.info(f"{available_shares} shares available for {ticker} in borrowedshares table")
                return available_shares
            else:
                self.logger.warning(f"No shares found for {ticker} in borrowedshares table")
                return 0
        except psycopg2.Error as e:
            self.logger.error(f"Database error while checking available shares: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def _cancel_das_order(self, token):
        """Send CANCEL to DAS for the order linked to token.
        Returns 'filled' if it executed during the cancel window, True if canceled, False if no order_id."""
        with token_map_lock:
            order_id = self.token_map.get(token, {}).get('order_id')

        if not order_id or order_id == 'N/A':
            self.logger.critical(
                f"Order timeout — token={token} has no order_id. "
                f"Order may still be live in DAS. Manual cleanup required."
            )
            return False

        self.logger.warning(f"Order timeout — sending CANCEL {order_id} to DAS")
        try:
            sock = send_command(f'CANCEL {order_id}')
            sock.close()
        except Exception as e:
            self.logger.error(f"Failed to send CANCEL {order_id}: {e}")
            return False

        # Grace window: watch for a fill that beats the cancel
        deadline = time.time() + 4
        while time.time() < deadline:
            time.sleep(0.5)
            with token_map_lock:
                status = self.token_map.get(token, {}).get('order_status', '')
                act    = self.token_map.get(token, {}).get('order_act_action', '')
            if status == 'Executed':
                self.logger.warning(f"Order {order_id} filled during cancel window — treating as executed")
                return 'filled'
            if status in ('Canceled', 'Cancelled') or act in ('Canceled', 'Cancelled'):
                self.logger.info(f"Order {order_id} confirmed canceled in DAS")
                return True

        self.logger.warning(f"No DAS cancel confirmation for {order_id} — assuming canceled")
        return True

    def wait_for_order_confirmation(self, token, timeout=10):
        start_time = time.time()
        while True:
            time.sleep(1)
            with token_map_lock:
                order_status = self.token_map.get(token, {}).get('order_status')
                order_act_status = self.token_map.get(token, {}).get('order_act_action')
                if order_status == 'Executed':
                    self.logger.info(f"Order {token} status: Executed")
                    return True
                if order_act_status == 'Send_Rej':
                    self.logger.info(f"Order {token} status: Send_Rej")
                    return False
            if time.time() - start_time > timeout:
                self.logger.warning(f"Order {token} timed out — attempting DAS cancel")
                result = self._cancel_das_order(token)
                return result == 'filled'

    def bstop_and_blimit_order_confirmation(self, token, timeout=5):
        start_time = time.time()
        while True:
            time.sleep(1)
            with token_map_lock:
                order_status = self.token_map.get(token, {}).get('order_status')
                order_act_status = self.token_map.get(token, {}).get('order_act_action')
                if order_status == 'Accepted':
                    self.logger.info(f"Stop order {token} status: Accepted")
                    return True
                if order_act_status == 'Send_Rej':
                    self.logger.info(f"Stop order {token} status: Send_Rej")
                    return False
            if time.time() - start_time > timeout:
                self.logger.warning(f"Stop order {token} confirmation timed out.")
                return False

    def cleanup_token_map(self, token):
        with token_map_lock:
            if token in self.token_map:
                del self.token_map[token]
                self.logger.info(f"Cleaned up token {token} from token_map")

if __name__ == "__main__":
    while True:
        time.sleep(1)