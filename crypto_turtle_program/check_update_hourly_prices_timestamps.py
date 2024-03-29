# IMPORTS
import datetime as dt
import requests as req
import psycopg as pg

import crypto_turtle_timings as ctt
import crypto_turtle_logger as log
import crypto_turtle_database as db

function_name = "TEST TEST TEST"
log_check_one_symbol = log.log_duration_start(function_name)
new_connection = db.db_connect_open()
cursor = new_connection.cursor()

symbols = ["BTC-USD"]

def get_daily_prices_for_symbols(symbols, new_connection):
    api_call_count = 0
    api_call_count_successful = 0
    
    # TIME references
    current_unix_time = ctt.now_basic_unix
    end_time = ctt.now_previous_day
    start_time = ctt.ninety_days_ago
    granularity = 86400

    now = dt.datetime.now(dt.timezone.utc)
    yesterday = now - dt.timedelta(days=1)
    start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0)
    start_of_yesterday_unix = int(start_of_yesterday.timestamp())

    log.log_message(f"START OF YESTERDAY TIMESTAMP IS: {start_of_yesterday_unix}")
    
    try:
        cursor = new_connection.cursor()    
        cursor.execute(db.dailyprices_select_count())
        initial_row_count = cursor.fetchone()[0]
        log.log_message(f"{function_name} initial row count is: {initial_row_count}")    
    except pg.OperationalError as e:
        log.log_message(f"{function_name} db cursor not open: {e}")
        return
    
    for symbol in symbols:
        cursor.execute(db.symbols_select_symbol_on_name(), (symbol,))
        symbol_data = cursor.fetchone()
        symbol_id = symbol_data[0] if symbol_data else None

        # When was the last DAILY row entered for a symbol compared to unix now?
        cursor.execute(db.dailyprices_select_max_unix_on_symbol_id(), (symbol_id,))
        latest_daily_timestamp = cursor.fetchone()[0]
        log.log_message(f"BEFORE——{symbol}: UNIX: {current_unix_time} DB: {latest_daily_timestamp}")

        if latest_daily_timestamp is not None and latest_daily_timestamp < start_of_yesterday_unix:
            # Coinbase API endpoint for fetching candles data
            url = f"https://api.pro.coinbase.com/products/{symbol}/candles"
            params = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'granularity': granularity
            }
            response = req.get(url, params=params)
            api_call_count += 1
            
            if response.status_code == 200:
                api_call_count_successful += 1
                
                data = response.json()
                sorted_data = sorted(data, key=lambda x: x[0])
            
                for daily_data in sorted_data:
                    unix_timestamp = daily_data[0]
                    high_price = daily_data[2]
                    low_price = daily_data[1]
                    open_price = daily_data[3]
                    close_price = daily_data[4]

                    cursor.execute(db.dailyprices_insert_symbol_prices(), (symbol_id, unix_timestamp, high_price, low_price, open_price, close_price))
                    log.log_message(f"{symbol} UPDATING: {unix_timestamp}, {high_price}, {low_price}, {open_price}, {close_price}")
                try:
                    new_connection.commit()
                except Exception as e:
                    log.log_message("ERROR:", e)
                    new_connection.rollback()
        else:
            log.log_message(f"NO DAILY API CALL. {symbol} already up-to-date")
        
        cursor.execute(db.dailyprices_select_max_unix_on_symbol_id(), (symbol_id,))
        latest_timestamp_2 = cursor.fetchone()[0]
        time_diff_2 = current_unix_time - latest_timestamp_2 if latest_timestamp_2 is not None else None
        log.log_message(f"AFTER——{symbol}: UNIX: {current_unix_time} DB: {latest_timestamp_2}, time_diff: {time_diff_2}")
    
    log.log_message(f"{function_name}: api calls: {api_call_count}")
    log.log_message(f"{function_name}: api calls successful: {api_call_count_successful}")
    cursor.execute(db.dailyprices_select_count())
    final_row_count = cursor.fetchone()[0]    
    log.log_message(f"{function_name} final row count is: {final_row_count}")
    log.log_message(f"{function_name} rows added is: {final_row_count - initial_row_count}")    



###############################################################################################

def get_hourly_prices_for_symbols(symbols, new_connection):
    api_call_count = 0
    api_call_count_successful = 0
    
    # TIME references
    current_unix_time = ctt.now_basic_unix
    end_time = ctt.now_previous_hour
    start_time = ctt.twenty_four_hours_ago
    granularity = 3600
    start_of_previous_hour_unix = int(ctt.now_previous_hour.timestamp())
    log.log_message(f"CURRENT UNIX IS: {current_unix_time}")
    log.log_message(f"START OF PREVIOUS HOUR UNIX IS: {start_of_previous_hour_unix}")
    
    try:
        cursor = new_connection.cursor()    
        cursor.execute(db.dailyprices_select_count())
        initial_row_count = cursor.fetchone()[0]
        log.log_message(f"{function_name} initial row count is: {initial_row_count}")    
    except pg.OperationalError as e:
        log.log_message(f"{function_name} db cursor not open: {e}")
        return
    
    
    for symbol in symbols:                
        cursor.execute(db.symbols_select_symbol_on_name(), (symbol,))
        symbol_data = cursor.fetchone()
        symbol_id = symbol_data[0] if symbol_data else None
        
        # When was the last HOURLY row entered for a symbol compared to unix now?
        cursor.execute(db.hourlyprices_select_max_unix_on_symbol_id(), (symbol_id,))
        latest_hourly_timestamp = cursor.fetchone()[0]        
        log.log_message(f"BEFORE——{symbol}: LATEST DB UNIX: {latest_hourly_timestamp}")
        
        if latest_hourly_timestamp is not None and latest_hourly_timestamp < start_of_previous_hour_unix:
            # Coinbase API candles endpoint
            url = f"https://api.pro.coinbase.com/products/{symbol}/candles"
            params = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'granularity': granularity
            }
            response = req.get(url, params=params)
            api_call_count += 1
            
            if response.status_code == 200:
                api_call_count_successful += 1
                
                data = response.json()
                sorted_data = sorted(data, key=lambda x: x[0])
                
                for hourly_data in sorted_data:
                    unix_timestamp = hourly_data[0]
                    high_price = hourly_data[2]
                    low_price = hourly_data[1]
                    open_price = hourly_data[3]
                    close_price = hourly_data[4]
                    cursor.execute(db.hourlyprices_insert_symbol_prices(), (symbol_id, unix_timestamp, high_price, low_price, open_price, close_price))                    
                    log.log_message(f"API UNIX: {unix_timestamp}, High: {high_price}")
                                

                try:
                    new_connection.commit()
                except Exception as e:
                    log.log_message("ERROR:", e)
                    new_connection.rollback()
        else:
            log.log_message(f"NO API CALL. {symbol} already up-to-date")
            
        cursor.execute(db.hourlyprices_select_max_unix_on_symbol_id(), (symbol_id,))
        latest_timestamp_2 = cursor.fetchone()[0]
        time_diff_2 = current_unix_time - latest_timestamp_2 if latest_timestamp_2 is not None else None
        log.log_message(f"AFTER——{symbol}: UNIX: {current_unix_time} DB: {latest_timestamp_2}, time_diff: {time_diff_2}")

##############################################################################################################

get_hourly_prices_for_symbols(symbols, new_connection)
# get_daily_prices_for_symbols(symbols, new_connection)

cursor.close()
db.db_connect_close(new_connection)
log.log_duration_end(log_check_one_symbol)