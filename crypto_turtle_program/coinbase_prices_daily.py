# IMPORTS
# 1. get_daily_prices_for_symbols
# 2. update_daily_highs_lows
# 3. 
# 4.
# 5.

#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################

import requests as req
import datetime as dt
import psycopg as pg
import contextlib as cl

import crypto_turtle_logger as log
import crypto_turtle_timings as ctt
import crypto_turtle_database as db

#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################

def get_daily_prices_for_symbols(symbols, new_connection):
    function_name = "GET DAILY PRICES FOR SYMBOLS"
    log_daily_price_check = log.log_duration_start(function_name)
    
    # Ready to receive
    api_call_count = 0
    api_call_count_successful = 0
    
    # TIMES: 1) period to check + 2) time of latest row vs. unix start of yesterday, which is what Coinbase API returns
    current_unix_time = ctt.now_basic_unix
    end_time = ctt.now_previous_day
    start_time = ctt.three_days_ago
    granularity = 86400
    start_of_yesterday_unix = int(ctt.now_previous_day.timestamp())
    
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
        
        if latest_daily_timestamp is None or latest_daily_timestamp < start_of_yesterday_unix:
            # Coinbase API calls, endpoint for fetching candles data
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
                    log.log_message(f"UPDATING {symbol}: unix: {unix_timestamp}, high_price: {high_price}, low_price: {low_price}, open_price: {open_price}, close_price: {close_price}")
                try:
                    new_connection.commit()
                except Exception as e:
                    log.log_message("ERROR:", e)
                    new_connection.rollback()

        else:
            log.log_message(f"No DAILY data for {symbol} is already up to date")
        
    log.log_message(f"{function_name}: api calls: {api_call_count}")
    log.log_message(f"{function_name}: api calls successful: {api_call_count_successful}")
    cursor.execute(db.dailyprices_select_count())
    final_row_count = cursor.fetchone()[0]    
    log.log_message(f"{function_name} final row count is: {final_row_count}")
    log.log_message(f"{function_name} rows added is: {final_row_count - initial_row_count}")    
        
    cursor.close()
    log.log_duration_end(log_daily_price_check)
    return

#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################
#################################################################################################################################

time_periods = [10, 20, 55]  # Time periods remain the same, but calculations will change

def update_daily_highs_lows(symbols, new_connection):
    function_name = "UPDATE DAILY HIGHS LOWS"
    log_update_daily_highs_lows = log.log_duration_start(function_name)
    
    try:
        # Use a context manager to ensure the cursor is closed automatically
        with cl.closing(new_connection.cursor()) as cursor:
            # Create a temporary table for batch update operations
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS batch_updates (
                    SymbolID INT,
                    UnixTimestamp BIGINT,
                    Period VARCHAR(255),
                    HighValue NUMERIC,
                    LowValue NUMERIC
                );
            """)
            
            # Assuming 'symbols' is a list of symbol names and 'connection' is your database connection
            for symbol in symbols:
                log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} STARTS")
                cursor.execute("SELECT SymbolID FROM Symbols WHERE SymbolName = %s;", (symbol,))
                symbol_id_row = cursor.fetchone()
                
                if not symbol_id_row:
                    print(f"Symbol {symbol} not found.")
                    continue
                
                symbol_id = symbol_id_row[0]
                
                # Fetch the UnixTimestamps needing updates for high/low values
                cursor.execute("""
                    SELECT UnixTimestamp 
                    FROM DailyPrices 
                    WHERE SymbolID = %s AND (
                        "10dayhigh" IS NULL OR "10dayhigh" = 0 OR
                        "20dayhigh" IS NULL OR "20dayhigh" = 0 OR
                        "55dayhigh" IS NULL OR "55dayhigh" = 0 OR
                        UnixTimestamp >= (SELECT MAX(UnixTimestamp) FROM DailyPrices WHERE SymbolID = %s) - (2 * 86400)
                    )
                    ORDER BY UnixTimestamp;
                """, (symbol_id, symbol_id))
                
                timestamps = cursor.fetchall()
                
                for unixtimestamp in (row[0] for row in timestamps):
                    # Example time_periods: [10, 20, 55]
                    for period in [10, 20, 55]:
                        start_unixtimestamp = unixtimestamp - (period * 86400)
                        
                        cursor.execute("""
                            SELECT MAX(HighPrice), MIN(LowPrice) 
                            FROM DailyPrices 
                            WHERE SymbolID = %s AND UnixTimestamp BETWEEN %s AND %s;
                        """, (symbol_id, start_unixtimestamp, unixtimestamp))
                        
                        high, low = cursor.fetchone()
                        
                        if high is not None and low is not None:
                            cursor.execute("""
                                INSERT INTO batch_updates (SymbolID, UnixTimestamp, Period, HighValue, LowValue)
                                VALUES (%s, %s, %s, %s, %s);
                            """, (symbol_id, unixtimestamp, f"{period}day", high, low))
            
            # Apply batch updates from the temporary table to the DailyPrices table
            cursor.execute("""
                UPDATE DailyPrices AS dp
                SET
                    "10dayhigh" = bu.HighValue,
                    "10daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '10day';
                
                UPDATE DailyPrices AS dp
                SET
                    "20dayhigh" = bu.HighValue,
                    "20daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '20day';
                
                UPDATE DailyPrices AS dp
                SET
                    "55dayhigh" = bu.HighValue,
                    "55daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '55day';
            """)
            
            # Commit the transaction
            new_connection.commit()
            log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} ENDS")          
    except Exception as e:
        print(f"Error updating high and low for symbols: {e}")
        # Rollback in case of error
        new_connection.rollback()

    log.log_duration_end(log_update_daily_highs_lows)