# 1. Imports
# 2. get_hourly_prices_for_symbols
# 3. compare_prices_percentages
# 4. xxxxxxxxxxx

########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################

import requests as req
import datetime as dt
import psycopg as pg
from contextlib import closing

import crypto_turtle_logger as log
import crypto_turtle_timings as ctt
import crypto_turtle_database as db

########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################

def get_hourly_prices_for_symbols(symbols, new_connection):
    function_name = "UPDATE HOURLY PRICES"
    log_hourly_price_check = log.log_duration_start(function_name)
    
    api_call_count = 0
    api_call_count_successful = 0
    
    # TIME references
    current_unix_time = ctt.now_basic_unix
    end_time = ctt.now_previous_hour
    start_time = ctt.three_hours_ago
    granularity = 3600
    start_of_previous_hour_unix = int(ctt.now_previous_hour.timestamp())
    log.log_message(f"CURRENT UNIX IS: {current_unix_time}")
    log.log_message(f"START OF PREVIOUS HOUR UNIX IS: {start_of_previous_hour_unix}")
    
    try:
        cursor = new_connection.cursor()        
        cursor.execute(db.hourlyprices_select_count())
        initial_row_count = cursor.fetchone()[0]
        log.log_message(f"{function_name}: initial row count is: {initial_row_count}")
    except pg.OperationalError as e:
        log.log_message(f"{function_name}: connection NOT OPEN. Not even the first cursor: {e}")
        return 
    
    with new_connection.cursor() as cursor:
        # Create a temporary table for hourly price updates
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_hourly_prices (
                symbolid INTEGER,
                unixtimestamp BIGINT,
                highprice NUMERIC,
                lowprice NUMERIC,
                openprice NUMERIC,
                closeprice NUMERIC
            );
        """)

        for symbol in symbols:
            cursor.execute("SELECT symbolid FROM symbols WHERE symbolname = %s;", (symbol,))
            symbol_data = cursor.fetchone()
            symbol_id = symbol_data[0] if symbol_data else None

            # When was the last HOURLY row entered for a symbol compared to unix now?
            cursor.execute("SELECT MAX(unixtimestamp) FROM hourlyprices WHERE symbolid = %s;", (symbol_id,))
            latest_hourly_timestamp = cursor.fetchone()[0]

            if latest_hourly_timestamp is None or latest_hourly_timestamp < start_of_previous_hour_unix:
                # Coinbase API endpoint for fetching candles data
                url = f"https://api.pro.coinbase.com/products/{symbol}/candles"
                params = {'start': start_time.isoformat(), 'end': end_time.isoformat(), 'granularity': granularity}
                response = req.get(url, params=params)
                api_call_count += 1

                if response.status_code == 200:
                    api_call_count_successful += 1
                    log.log_message(f"{function_name}: UPDATING: {symbol}")
                    
                    data = response.json()
                    sorted_data = sorted(data, key=lambda x: x[0])

                    # Instead of inserting each row individually, store them in the temporary table
                    for hourly_data in sorted_data:
                        cursor.execute("""
                            INSERT INTO temp_hourly_prices (symbolid, unixtimestamp, highprice, lowprice, openprice, closeprice)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """, (symbol_id, hourly_data[0], hourly_data[2], hourly_data[1], hourly_data[3], hourly_data[4]))

                else:
                    log.log_message(f"{function_name}: Failed to retrieve data for {symbol}")

        # After collecting all data, insert it into the actual hourlyprices table from the temporary table
        cursor.execute("""
            INSERT INTO hourlyprices (symbolid, unixtimestamp, highprice, lowprice, openprice, closeprice)
            SELECT symbolid, unixtimestamp, highprice, lowprice, openprice, closeprice
            FROM temp_hourly_prices
            ON CONFLICT (symbolid, unixtimestamp)
            DO UPDATE SET
                highprice = EXCLUDED.highprice,
                lowprice = EXCLUDED.lowprice,
                openprice = EXCLUDED.openprice,
                closeprice = EXCLUDED.closeprice;
        """)

        # Commit the transaction to finalize the bulk insert
        new_connection.commit()

        # Optionally, drop the temporary table if you won't use it further
        cursor.execute("DROP TABLE IF EXISTS temp_hourly_prices;")

    log.log_message(f"{function_name}: total api calls is: {api_call_count}")
    log.log_message(f"{function_name}: api calls successful: {api_call_count_successful}")
    cursor = new_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM HourlyPrices")
    final_row_count = cursor.fetchone()[0]
    log.log_message(f"{function_name}: final row count is: {final_row_count}")
    log.log_message(f"{function_name}: rows added is: {final_row_count - initial_row_count}")    
        
    cursor.close()
    log.log_duration_end(log_hourly_price_check)
    return


########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


def compare_prices_percentages(new_connection):
    print("MESSAGE 1")
    try:
        with new_connection.cursor() as cursor:
            print("MESSAGE 2")
            current_unix_timestamp = int(ctt.now_basic_unix)
            print("MESSAGE 3")
            now = int((dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)).timestamp())
            two_hours_ago = now - 2 * 60 * 60
            four_hours_ago = now - 4 * 60 * 60
            twelve_hours_ago = now - 12 * 60 * 60
            twenty_four_hours_ago = now - 24 * 60 * 60
            forty_eight_hours_ago = now - 48 * 60 * 60
            seventy_two_hours_ago = now - 72 * 60 * 60
            one_hundred_twenty_hours_ago = now - 120 * 60 * 60
            print("MESSAGE 4")

            query = """
                SELECT s.SymbolID,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS LatestHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwoHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS FourHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwelveHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwentyFourHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS FortyEightHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS SeventyTwoHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS OneHundredTwentyHoursAgoHigh
                FROM Symbols s
                WHERE s.Status = 'online'
            """            
            cursor.execute(query, (now, two_hours_ago, four_hours_ago, twelve_hours_ago, twenty_four_hours_ago, forty_eight_hours_ago, seventy_two_hours_ago, one_hundred_twenty_hours_ago))
            results = cursor.fetchall()
            print("MESSAGE 5")
            print(results)
            # Create temporary table for batch update operations 
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_hourly_percentage_increases AS
                SELECT * FROM hourly_percentage_increases WITH NO DATA;
            """)
            print("MESSAGE 6")
            # Go through each result and calculate the percentage differences
            for result in results:
                symbol_id, latest_high, two_hours_high, four_hours_high, twelve_hours_high, twenty_four_hours_high, forty_eight_hours_high, seventy_two_hours_high, one_hundred_twenty_hours_high = result
                print(result)
                
                # Calculate all the percentage differences for all active symbols vs. time periods
                two_hours_difference = ((latest_high - two_hours_high) / two_hours_high) * 100 if two_hours_high is not None else None
                four_hours_difference = ((latest_high - four_hours_high) / four_hours_high) * 100 if four_hours_high is not None else None
                twelve_hours_difference = ((latest_high - twelve_hours_high) / twelve_hours_high) * 100 if twelve_hours_high is not None else None
                twenty_four_hours_difference = ((latest_high - twenty_four_hours_high) / twenty_four_hours_high) * 100 if twenty_four_hours_high is not None else None
                forty_eight_hours_difference = ((latest_high - forty_eight_hours_high) / forty_eight_hours_high) * 100 if forty_eight_hours_high is not None else None
                seventy_two_hours_difference = ((latest_high - seventy_two_hours_high) / seventy_two_hours_high) * 100 if seventy_two_hours_high is not None else None
                one_hundred_twenty_hours_difference = ((latest_high - one_hundred_twenty_hours_high) / one_hundred_twenty_hours_high) * 100 if one_hundred_twenty_hours_high is not None else None
                print("MESSAGE 7")
                

                # Check which of those percentage results meet your selection critiera
                short_term_increases = [two_hours_difference, four_hours_difference, twelve_hours_difference, twenty_four_hours_difference]
                long_term_increases = [forty_eight_hours_difference, seventy_two_hours_difference, one_hundred_twenty_hours_difference]
                short_term_check = any(x > 5 for x in short_term_increases if x is not None)
                long_term_check = any(x > 10 for x in long_term_increases if x is not None)
                if short_term_check or long_term_check:
                    print("MESSAGE 8")
                    insert_data = (symbol_id, two_hours_difference, four_hours_difference, twelve_hours_difference, twenty_four_hours_difference, forty_eight_hours_difference, seventy_two_hours_difference, one_hundred_twenty_hours_difference, current_unix_timestamp)
                    print(insert_data)    
                    print("MESSAGE 9")
                    
                    cursor.execute("""
                        INSERT INTO temp_hourly_percentage_increases 
                        (symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                        twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                        one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, insert_data)
                    

            # Now do the buik insert into the actual hourly_percentage_increases table
            cursor.execute("""
                INSERT INTO hourly_percentage_increases 
                (symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat)
                SELECT symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat
                FROM temp_hourly_percentage_increases
                ON CONFLICT (symbolid, report_generated_at)
                DO UPDATE
                SET two_hours_increase = EXCLUDED.two_hours_increase,
                    four_hours_increase = EXCLUDED.four_hours_increase,
                    twelve_hours_increase = EXCLUDED.twelve_hours_increase,
                    twenty_four_hours_increase = EXCLUDED.twenty_four_hours_increase,
                    forty_eight_hours_increase = EXCLUDED.forty_eight_hours_increase,
                    seventy_two_hours_increase = EXCLUDED.seventy_two_hours_increase,
                    one_hundred_twenty_hours_increase = EXCLUDED.one_hundred_twenty_hours_increase,
                    updatedat = EXCLUDED.updatedat;
                """)
                
            # And finally drop the temporary table
            cursor.execute("DROP TABLE IF EXISTS temp_hourly_percentage_increases")
            
            new_connection.commit()
    except Exception as e:
        print(f"HOURLY PERCENTAGES: Error in generating report: {e}")
        log.log_message(f"HOURLY PERCENTAGES: Error in generating report: {e}")
        
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################