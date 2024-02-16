import decimal as dec
import psycopg as pg
from collections import defaultdict
from datetime import datetime
        
import crypto_turtle_database as db
import crypto_turtle_logger as log
import crypto_turtle_timings as ctt

def print_symbols_exceeding_highs(new_connection):
    cursor = new_connection.cursor()

    query = """
        SELECT 
            s.SymbolName,
            dp.UnixTimestamp AS LatestUnixTimestamp,
            dp.HighPrice AS LatestHigh,
            COALESCE(prev_dp."10dayhigh", -1) AS Previous10DayHigh,
            COALESCE(prev_dp."20dayhigh", -1) AS Previous20DayHigh,
            COALESCE(prev_dp."55dayhigh", -1) AS Previous55DayHigh
        FROM 
            dailyprices dp
        JOIN 
            symbols s ON dp.SymbolID = s.SymbolID
        JOIN 
            dailyprices prev_dp ON dp.SymbolID = prev_dp.SymbolID AND prev_dp.UnixTimestamp = dp.UnixTimestamp - 86400
        WHERE 
            dp.UnixTimestamp >= (extract(epoch from CURRENT_DATE) - (30 * 86400))
            AND (dp.HighPrice > COALESCE(prev_dp."10dayhigh", -1) OR dp.HighPrice > COALESCE(prev_dp."20dayhigh", -1) OR dp.HighPrice > COALESCE(prev_dp."55dayhigh", -1))
        ORDER BY 
            s.SymbolName, dp.UnixTimestamp;
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Structure to hold grouped results
        grouped_results = defaultdict(lambda: defaultdict(dict))

        # Iterate over rows and group the data
        for row in rows:
            symbol_name, latest_unixtimestamp, latest_high, previous_10_day_high, previous_20_day_high, previous_55_day_high = row
            latest_date = datetime.utcfromtimestamp(latest_unixtimestamp).strftime('%Y-%m-%d')
            
            date_data = grouped_results[symbol_name][latest_date]
            
            if latest_high > previous_55_day_high:
                date_data["55day"] = f"55 HIGH: {previous_55_day_high} — High: {latest_high}"

            symbols_count = len(grouped_results)
        
        formatted_datetime = ctt.now_formatted()
        html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>10-Day Breakouts On Coinbase</title>
                <meta charset="utf-8">
                <link rel="stylesheet" href="/crypto_turtle/crypto_turtle_styles.css">
            </head>
            <body>
                <div class="container">
                <h1>55-Day Breakouts On Coinbase</h1>
                <p><strong>Last updated:</strong> {formatted_datetime}</p>
                <p><strong>Coinbase symbols with 55-day breakout in last 30 days:</strong> {symbols_count}</p>
        """

        # Function to format symbol for TradingView URL
        def format_symbol_for_tradingview(symbol):
            return symbol.replace("-", "")

    # Adding grouped results to HTML
        for symbol, dates_data in grouped_results.items():
            tradingview_url = f"https://www.tradingview.com/chart/?symbol=COINBASE:{format_symbol_for_tradingview(symbol)}"
            html_content += f"<h3><a href='{tradingview_url}' target='_blank'>{symbol}</a></h3>"
            for date, breakouts in dates_data.items():
                html_content += f"<ul>"
                for breakout_type, breakout_info in breakouts.items():
                    html_content += f"<li>{date}—{breakout_info}</li>"
                html_content += "</ul>"

        html_content += """
            </div>
            </body>
            </html>
        """
        
        cursor.close()
        
        # Writing the HTML content to a file
        with open("crypto_turtle_breakouts_daily_55.html", "w") as file:
            file.write(html_content)

        print("55 DAY HIGH HTML file with grouped TradingView links created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()


# def print_symbols_exceeding_highs(new_connection):
    
#     # Create a cursor object
#     # Create a cursor object
#     cursor = new_connection.cursor()
#     print("MESSAGE 1")
#     query = """
#         SELECT 
#             s.SymbolName,
#             dp.Date AS LatestDate,
#             dp.HighPrice AS LatestHigh,
#             COALESCE(prev_dp."10dayhigh", -1) AS Previous10DayHigh,
#             COALESCE(prev_dp."20dayhigh", -1) AS Previous20DayHigh,
#             COALESCE(prev_dp."55dayhigh", -1) AS Previous55DayHigh
#         FROM 
#             dailyprices dp
#         JOIN 
#             symbols s ON dp.SymbolID = s.SymbolID
#         JOIN 
#             dailyprices prev_dp ON dp.SymbolID = prev_dp.SymbolID AND prev_dp.Date = dp.Date - INTERVAL '1 day'
#         WHERE 
#             dp.Date >= CURRENT_DATE - INTERVAL '60 days'
#             AND (dp.HighPrice > COALESCE(prev_dp."10dayhigh", -1) OR dp.HighPrice > COALESCE(prev_dp."20dayhigh", -1) OR dp.HighPrice > COALESCE(prev_dp."55dayhigh", -1))
#         ORDER BY 
#             s.SymbolName, dp.Date;
#     """
#     print("MESSAGE 2")

#     try:
#         from collections import defaultdict
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         # Structure to hold grouped results
#         grouped_results = defaultdict(lambda: defaultdict(dict))

#         # Iterate over rows and group the data
#         for row in rows:
#             symbol_name, latest_date, latest_high, previous_10_day_high, previous_20_day_high, previous_55_day_high = row
            
#             date_data = grouped_results[symbol_name][latest_date]
            
#             # if latest_high > previous_10_day_high:
#             #     date_data["10day"] = f"10 HIGH: {previous_10_day_high} — High: {latest_high}"
#             # if latest_high > previous_20_day_high:
#             #     date_data["20day"] = f"20 HIGH: {previous_20_day_high} — High: {latest_high}"
#             if latest_high > previous_55_day_high:
#                 date_data["55day"] = f"55 HIGH: {previous_55_day_high} — High: {latest_high}"

#         # Generating HTML content
#         symbols_count = len(grouped_results)
        
#         html_content = f"""
#             <!DOCTYPE html>
#             <html>
#             <head>
#                 <title>Breakouts This Month On Coinbase</title>
#                 <meta charset="utf-8">
#                 <link rel="stylesheet" href="/crypto_turtle/crypto_turtle_styles.css">
#             </head>
#             <body>
#                 <div class="container">
#                 <h1>Breakouts This Month On Coinbase</h1>
#                 <p><strong>Total symbols with breakout (last 60 days):</strong> {symbols_count}</p>
#         """

#         # Function to format symbol for TradingView URL
#         def format_symbol_for_tradingview(symbol):
#             return symbol.replace("-", "")

#        # Adding grouped results to HTML
#         for symbol, dates_data in grouped_results.items():
#             tradingview_url = f"https://www.tradingview.com/chart/?symbol=COINBASE:{format_symbol_for_tradingview(symbol)}"
#             html_content += f"<h3><a href='{tradingview_url}' target='_blank'>{symbol}</a></h3>"
#             for date, breakouts in dates_data.items():
#                 html_content += f"<ul>"
#                 for breakout_type, breakout_info in breakouts.items():
#                     html_content += f"<li>{date}—{breakout_info}</li>"
#                 html_content += "</ul>"

#         html_content += """
#             </div>
#             </body>
#             </html>
#         """

#         # The 'html_content' variable now contains the formatted HTML content

        
#         cursor.close()
        

#         # Writing the HTML content to a file
#         with open("crypto_turtle_breakouts_daily_55.html", "w") as file:
#             file.write(html_content)

#         print("HTML file with grouped TradingView links created successfully.")

#         # breakout_10day = []
#         # breakout_20day = []
#         # breakout_55day = []
#         # breakout_counts = {}

#         # for row in rows:
#         #     symbol, latest_date, latest_high, previous_55day_high = row
            
#         #     breakout_count = 0

#         #     # if latest_high > previous_10day_high:
#         #     #     breakout_10day.append(f"{symbol}: {latest_high} | {previous_10day_high}")
#         #     #     breakout_count += 1

#         #     # if latest_high > previous_20day_high:
#         #     #     breakout_20day.append(f"{symbol}: {latest_high} | {previous_20day_high}")
#         #     #     breakout_count += 1

#         #     if latest_high > previous_55day_high:
#         #         breakout_55day.append(f"{latest_date}—{symbol}: {latest_high} | {previous_55day_high}")
#         #         breakout_count += 1
            
#         #     if breakout_count > 0:
#         #         breakout_counts[symbol] = breakout_count

#         # breakout_10day.sort()
#         # breakout_20day.sort()
#         # breakout_55day.sort()
        
        
        
#         # # Log or print the grouped lists
#         # log.log_message("10-DAY BREAKOUTS")
#         # for symbol_info in breakout_10day:
#         #     log.log_message(symbol_info)

#         # log.log_message("20-DAY BREAKOUTS")
#         # for symbol_info in breakout_20day:
#         #     log.log_message(symbol_info)

#         # log.log_message("55-DAY BREAKOUTS")
#         # for symbol_info in breakout_55day:
#         #     log.log_message(symbol_info)
        
#         # multi_breakouts = [symbol for symbol, count in breakout_counts.items() if count > 1]
#         # multi_breakouts.sort()
#         # log.log_message("SYMBOLS BREAKING MULTIPLE HIGHS")
#         # for symbol in multi_breakouts:
#         #     log.log_message(symbol)
        
#     except Exception as e:
#         print(f"An error occurred: {e}")