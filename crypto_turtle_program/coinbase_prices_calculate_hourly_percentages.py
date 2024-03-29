import datetime as dt
import psycopg  # Assuming psycopg2 for PostgreSQL connection
import flask as fk
import crypto_turtle_logger as log
import crypto_turtle_timings as ctt
import crypto_turtle_database as db

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