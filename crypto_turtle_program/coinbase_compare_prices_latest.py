import decimal as dec
import psycopg as pg
import crypto_turtle.crypto_turtle_database as db
import crypto_turtle.crypto_turtle_logger as log


def print_symbols_exceeding_highs(new_connection):
    
    # Create a cursor object
    cursor = new_connection.cursor()
    
    query20 = """
        SELECT 
            s.SymbolName,
            dp.Date AS LatestDate,
            dp.HighPrice AS LatestHigh,
            prev_dp."20dayhigh" AS Previous20DayHigh
        FROM 
            dailyprices dp
        JOIN 
            symbols s ON dp.SymbolID = s.SymbolID
        JOIN 
            dailyprices prev_dp ON dp.SymbolID = prev_dp.SymbolID AND prev_dp.Date = dp.Date - INTERVAL '1 day'
        WHERE 
            dp.Date >= CURRENT_DATE - INTERVAL '5 days'
            AND dp.HighPrice > prev_dp."20dayhigh"
        ORDER BY 
            s.SymbolName, dp.Date;
    """
    
    query55 = """
        SELECT 
            s.SymbolName,
            dp.Date AS LatestDate,
            dp.HighPrice AS LatestHigh,
            prev_dp."55dayhigh" AS Previous55DayHigh
        FROM 
            dailyprices dp
        JOIN 
            symbols s ON dp.SymbolID = s.SymbolID
        JOIN 
            dailyprices prev_dp ON dp.SymbolID = prev_dp.SymbolID AND prev_dp.Date = dp.Date - INTERVAL '1 day'
        WHERE 
            dp.Date >= CURRENT_DATE - INTERVAL '5 days'
            AND dp.HighPrice > prev_dp."55dayhigh"
        ORDER BY 
            s.SymbolName, dp.Date;
    """
        
    try:
        cursor.execute(query20)
        rows = cursor.fetchall()
        log.log_message(f"Number of rows fetched 20-DAY: {len(rows)}")
        
        grouped_results_20 = {}
        for row in rows:
            symbol_name, latest_date, latest_high, previous_20_day_high = row
            if symbol_name not in grouped_results_20:
                grouped_results_20[symbol_name] = []
            grouped_results_20[symbol_name].append((latest_date, latest_high, previous_20_day_high))

        for symbol in grouped_results_20:
            log.log_message(f"{symbol}:")
            for data in grouped_results_20[symbol]:
                latest_date, latest_high, previous_20_day_high = data
                # log.log_message(f"——Date: {latest_date}, Latest High: {latest_high}, Previous 20-Day High: {previous_20_day_high}")

        
        cursor.execute(query55)
        rows = cursor.fetchall()
        log.log_message(f"Number of rows fetched 55-DAY: {len(rows)}")
        
        grouped_results_55 = {}
        for row in rows:
            symbol_name, latest_date, latest_high, previous_55_day_high = row
            if symbol_name not in grouped_results_55:
                grouped_results_55[symbol_name] = []
            grouped_results_55[symbol_name].append((latest_date, latest_high, previous_55_day_high))

        for symbol in grouped_results_55:
            log.log_message(f"{symbol}:")
            for data in grouped_results_55[symbol]:
                latest_date, latest_high, previous_55_day_high = data
                # log.log_message(f"——Date: {latest_date}, Latest High: {latest_high}, Previous 55-Day High: {previous_55_day_high}")


        cursor.close()
        
        
        html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Crypto Turtle Results</title>
            </head>
            <body>
                <h1>Crypto Turtle Analysis Results</h1>
            """

        # Function to format symbol for TradingView URL
        def format_symbol_for_tradingview(symbol):
            return symbol.replace("-", "")

        # Adding 20-Day Breakout Results
        html_content += "<h2>20-Day Breakout Results</h2>"
        html_content += "<ul>"
        for symbol, data_list in grouped_results_20.items():
            tradingview_url = f"https://www.tradingview.com/chart/?symbol=COINBASE:{format_symbol_for_tradingview(symbol)}"
            html_content += f"<li><a href='{tradingview_url}' target='_blank'>{symbol}</a><ul>"
            for data in data_list:
                latest_date, latest_high, previous_20_day_high = data
                html_content += f"<li>Date: {latest_date}, Latest High: {latest_high}, Previous 20-Day High: {previous_20_day_high}</li>"
            html_content += "</ul></li>"
        html_content += "</ul>"

        # Adding 55-Day Breakout Results
        html_content += "<h2>55-Day Breakout Results</h2>"
        html_content += "<ul>"
        for symbol, data_list in grouped_results_55.items():
            tradingview_url = f"https://www.tradingview.com/chart/?symbol=COINBASE:{format_symbol_for_tradingview(symbol)}"
            html_content += f"<li><a href='{tradingview_url}' target='_blank'>{symbol}</a><ul>"
            for data in data_list:
                latest_date, latest_high, previous_55_day_high = data
                html_content += f"<li>Date: {latest_date}, Latest High: {latest_high}, Previous 55-Day High: {previous_55_day_high}</li>"
            html_content += "</ul></li>"
        html_content += "</ul>"

        html_content += """
        </body>
        </html>
        """

        # Writing the HTML content to a file
        with open("crypto_turtle_results.html", "w") as file:
            file.write(html_content)

        print("HTML file created successfully.")

        
        
        
        
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