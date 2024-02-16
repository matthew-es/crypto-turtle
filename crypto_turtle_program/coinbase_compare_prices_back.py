import decimal as dec
import psycopg as pg
import crypto_turtle.crypto_turtle_database as db
import crypto_turtle.crypto_turtle_logger as log

new_log = log.log_duration_start()

def print_symbols_exceeding_highs():
    # Establish a connection to the database
    new_connection = db.db_connect_open()
    
    print("MESSAGE 0")

    # Create a cursor object
    cursor = new_connection.cursor()
    
    # query = """
    #     SELECT DISTINCT
    #         s.SymbolName,
    #         dp.Date,
    #         dp.HighPrice,
    #         dp."55dayhigh" AS Previous55DayHigh
    #     FROM 
    #         dailyprices dp
    #     JOIN 
    #         symbols s ON dp.SymbolID = s.SymbolID
    #     WHERE 
    #         dp.Date >= CURRENT_DATE - INTERVAL '30 days'
    #         AND dp.HighPrice > dp."55dayhigh"
    #     ORDER BY 
    #         s.SymbolName, dp.Date;
    # """
    
    # interval = 14
    # query = f"""
    #     SELECT 
    #         s.SymbolName,
    #         latest.Date AS LatestDate,
    #         latest.HighPrice AS LatestHigh,
    #         previous."10dayhigh" AS Previous10DayHigh,
    #         previous."20dayhigh" AS Previous20DayHigh,
    #         previous."55dayhigh" AS Previous55DayHigh
    #     FROM 
    #         dailyprices latest
    #     JOIN 
    #         dailyprices previous ON latest.SymbolID = previous.SymbolID AND previous.Date = latest.Date - INTERVAL '1 day'
    #     JOIN 
    #         symbols s ON latest.SymbolID = s.SymbolID
    #     WHERE 
    #         latest.Date >= (SELECT MAX(Date) FROM dailyprices) - INTERVAL '{interval} days'
    #     ORDER BY 
    #         latest.Date DESC, s.SymbolName;
    # """
    
    
    query = """
        SELECT 
            sub.SymbolName,
            sub.LatestDate,
            sub.LatestHigh,
            sub.Previous55DayHigh,
            sub.YesterdayLow,
            MAX(dp.LowPrice) AS HighestLowAfterBreakout
        FROM (
            SELECT 
                s.SymbolName,
                dp.SymbolID,  -- Added SymbolID for join
                dp.Date AS LatestDate,
                dp.HighPrice AS LatestHigh,
                prev_dp."55dayhigh" AS Previous55DayHigh,
                yest_dp.LowPrice AS YesterdayLow,
                ROW_NUMBER() OVER (PARTITION BY s.SymbolName ORDER BY dp.Date) as rn
            FROM 
                dailyprices dp
            JOIN 
                symbols s ON dp.SymbolID = s.SymbolID
            JOIN 
                dailyprices prev_dp ON dp.SymbolID = prev_dp.SymbolID AND prev_dp.Date = dp.Date - INTERVAL '1 day'
            LEFT JOIN
                dailyprices yest_dp ON dp.SymbolID = yest_dp.SymbolID AND yest_dp.Date = CURRENT_DATE - INTERVAL '1 day'
            WHERE 
                dp.Date >= CURRENT_DATE - INTERVAL '50 days'
                AND dp.HighPrice > prev_dp."55dayhigh"
        ) AS sub
        JOIN 
            dailyprices dp ON sub.SymbolID = dp.SymbolID AND dp.Date > sub.LatestDate
        WHERE sub.rn = 1
        GROUP BY 
            sub.SymbolName, sub.LatestDate, sub.LatestHigh, sub.Previous55DayHigh, sub.YesterdayLow
        ORDER BY 
            sub.SymbolName;
    """
    
    try:
        cursor.execute(query)
        print(cursor)
        rows = cursor.fetchall()
        log.log_message(f"Number of rows fetched: {len(rows)}")
        
        # print("Symbol | Date | High Price | Previous 55-Day High")
        # for row in rows:
        #     symbol_name, date, high_price, previous_55_day_high = row
        #     print(f"{symbol_name} | {date} | {high_price} | {previous_55_day_high}")

        
        # print("Symbol Name | 55-Day Breakout Date | Highest Low Date | % Difference")
        # for row in rows:
        #     symbol_name, breakout_date, highest_low_date, percentage_difference = row
        #     print(f"{symbol_name} | {breakout_date} | {highest_low_date} | {percentage_difference:.2f}%")


        # # Print the header
        # print(f"{'Symbol':<15} {'Breakout Date':<15} {'Highest Low Date':<20} {'% Difference':<15}")
        
        # # Iterate through the results and print them
        # for row in rows:
        #     symbol, breakout_date, highest_low_date, percent_difference = row
        #     print(f"{symbol:<15} {breakout_date:<15} {highest_low_date:<20} {percent_difference:.2f}%")

        
        # breakout_10day = []
        # breakout_20day = []
        # breakout_55day = []
        # breakout_counts = {}

        # for row in rows:
        #     symbol, latest_date, latest_high, previous_55day_high = row
            
        #     breakout_count = 0

        #     # if latest_high > previous_10day_high:
        #     #     breakout_10day.append(f"{symbol}: {latest_high} | {previous_10day_high}")
        #     #     breakout_count += 1

        #     # if latest_high > previous_20day_high:
        #     #     breakout_20day.append(f"{symbol}: {latest_high} | {previous_20day_high}")
        #     #     breakout_count += 1

        #     if latest_high > previous_55day_high:
        #         breakout_55day.append(f"{latest_date}—{symbol}: {latest_high} | {previous_55day_high}")
        #         breakout_count += 1
            
        #     if breakout_count > 0:
        #         breakout_counts[symbol] = breakout_count

        # breakout_10day.sort()
        # breakout_20day.sort()
        # breakout_55day.sort()
        
        grouped_results = {}
    
        for row in rows:
            symbol_name, latest_date, latest_high, previous_55_day_high, yesterday_low, highest_low = row
            if symbol_name not in grouped_results:
                grouped_results[symbol_name] = []
            grouped_results[symbol_name].append((latest_date, latest_high, previous_55_day_high, yesterday_low, highest_low))

        capital_invested = 0
        capital_lost = 0
        capital_won = 0
        how_many_losers = 0
        how_many_winners = 0
        
        for symbol in grouped_results:
            for data in grouped_results[symbol]:
                latest_date, latest_high, previous_55_day_high, yesterday_low, highest_low = data
                price_difference = highest_low - previous_55_day_high
                stop_price = previous_55_day_high * dec.Decimal('0.98')
                stop_price_executed = max(stop_price, highest_low)
                profit_loss = stop_price_executed - previous_55_day_high
                percentage = (profit_loss/previous_55_day_high)*100
                
                if_100 = ((profit_loss/previous_55_day_high)*100)
                
                capital_invested += 100
        
                if if_100 < 0:
                    how_many_losers += 1
                    capital_lost += if_100
                else:
                    how_many_winners += 1
                    capital_won += if_100

                log.log_message(f"{symbol}———Date: {latest_date}, Breakout: {round(latest_high, 10)}, 55-Day High: {round(previous_55_day_high, 10)}, Highest Low: {round(highest_low, 10)}, STOP: {round(stop_price_executed, 10)}, DIFF: {round(profit_loss, 10)}, PERCENT: {round(percentage, 2)}%, IF100: {if_100}" )
        
        capital_new_total = capital_invested - capital_lost + capital_won
        return_on_capital = ((capital_new_total-capital_invested)/capital_invested)*100
        
        log.log_message(f"LOSERS: {how_many_losers} WINNERS: {how_many_winners}")
        log.log_message(f"CAPITAL INVESTED: {capital_invested}")
        log.log_message(f"CAPITAL WON: {round(capital_won, 2)}")
        log.log_message(f"CAPITAL LOST: {round(capital_lost, 2)}")
        log.log_message(f"CAPITAL NEW TOTAL: {round(capital_new_total, 2)}")
        log.log_message(f"CAPITAL RETURN: {round(return_on_capital, 2)} %")
        
        # log.log_message(f"CAPITAL AT END: {round(capital_after_position, 2)}")
        # log.log_message(f"RETURN ON CAPITAL: {round(return_on_capital, 2)}%")
        
        # # Log or print the grouped lists
        # log.log_message("10-DAY BREAKOUTS")
        # for symbol_info in breakout_10day:
        #     log.log_message(symbol_info)

        # log.log_message("20-DAY BREAKOUTS")
        # for symbol_info in breakout_20day:
        #     log.log_message(symbol_info)

        # log.log_message("55-DAY BREAKOUTS")
        # for symbol_info in breakout_55day:
        #     log.log_message(symbol_info)
        
        # multi_breakouts = [symbol for symbol, count in breakout_counts.items() if count > 1]
        # multi_breakouts.sort()
        # log.log_message("SYMBOLS BREAKING MULTIPLE HIGHS")
        # for symbol in multi_breakouts:
        #     log.log_message(symbol)
        
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        db.db_connect_close(new_connection)


print_symbols_exceeding_highs()
log.log_duration_end(new_log, "COMPARE PRICES")