import decimal as dec
import psycopg as pg
import crypto_turtle_logger as log
import crypto_turtle_timings as ctt

def print_symbols_exceeding_highs(new_connection):
    
    # Create a cursor object
    cursor = new_connection.cursor()

    query = """
        WITH LatestDailyHighs AS (
            SELECT 
                SymbolID,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "10dayhigh" END) AS Latest10DayHigh,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "20dayhigh" END) AS Latest20DayHigh,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "55dayhigh" END) AS Latest55DayHigh
            FROM DailyPrices
            GROUP BY SymbolID
        )
        SELECT 
            s.SymbolName,
            hp.Timestamp AS HourlyTimestamp,
            hp.HighPrice AS HourlyHigh,
            ldh.Latest10DayHigh,
            ldh.Latest20DayHigh,
            ldh.Latest55DayHigh
        FROM HourlyPrices hp
        JOIN Symbols s ON hp.SymbolID = s.SymbolID
        JOIN LatestDailyHighs ldh ON s.SymbolID = ldh.SymbolID
        WHERE hp.Timestamp >= CURRENT_DATE - INTERVAL '24 hours'
        AND (
            hp.HighPrice > ldh.Latest10DayHigh
            OR hp.HighPrice > ldh.Latest20DayHigh
            OR hp.HighPrice > ldh.Latest55DayHigh
        )
        ORDER BY s.SymbolName, hp.Timestamp;
    """

    query = """
        WITH LatestDailyHighs AS (
            SELECT 
                SymbolID,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "10dayhigh" END) AS Latest10DayHigh,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "20dayhigh" END) AS Latest20DayHigh,
                MAX(CASE WHEN Date = CURRENT_DATE - INTERVAL '1 day' THEN "55dayhigh" END) AS Latest55DayHigh
            FROM DailyPrices
            GROUP BY SymbolID
        )
        SELECT 
            s.SymbolName,
            hp.unixtimestamp AS HourlyUnixTimestamp,
            hp.HighPrice AS HourlyHigh,
            ldh.Latest10DayHigh,
            ldh.Latest20DayHigh,
            ldh.Latest55DayHigh
        FROM HourlyPrices hp
        JOIN Symbols s ON hp.SymbolID = s.SymbolID
        JOIN LatestDailyHighs ldh ON s.SymbolID = ldh.SymbolID
        WHERE hp.unixtimestamp >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '24 hours'))::bigint
        AND (
            hp.HighPrice > ldh.Latest10DayHigh
            OR hp.HighPrice > ldh.Latest20DayHigh
            OR hp.HighPrice > ldh.Latest55DayHigh
        )
        ORDER BY s.SymbolName, hp.unixtimestamp;
    """

    try:
        from collections import defaultdict
        cursor.execute(query)
        rows = cursor.fetchall()
        log.log_message(f"Number of rows fetched 20-DAY: {len(rows)}")


        # Structure to hold grouped results
        grouped_results = defaultdict(lambda: defaultdict(dict))

        for row in rows:
            symbol_name, hourly_timestamp, hourly_high, daily_10_high, daily_20_high, daily_55_high = row
            
            grouped_results[symbol_name][hourly_timestamp]["hourly_high"] = hourly_high
            grouped_results[symbol_name][hourly_timestamp]["10_day_high"] = daily_10_high
            grouped_results[symbol_name][hourly_timestamp]["20_day_high"] = daily_20_high
            grouped_results[symbol_name][hourly_timestamp]["55_day_high"] = daily_55_high

        # Generating HTML content
        symbols_count = len(grouped_results)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Breakouts Today On Coinbase</title>
            <meta charset="utf-8">
            <link rel="stylesheet" href="/crypto_turtle/crypto_turtle_styles.css">
        </head>
        <body>
            <div class="container">
            <h1>Breakouts Today On Coinbase</h1>
            <p><strong>Total symbols breaking out (last 24 hours):</strong> {symbols_count}</p>
        """

        # Function to format symbol for TradingView URL
        def format_symbol_for_tradingview(symbol):
            return symbol.replace("-", "")

        for symbol, timestamps_data in grouped_results.items():
            tradingview_url = f"https://www.tradingview.com/chart/?symbol=COINBASE:{format_symbol_for_tradingview(symbol)}"
            html_content += f"<div><ul>"

            for timestamp, data in timestamps_data.items():
                html_content += f"""
                <li>
                    <strong><a href='{tradingview_url}' target='_blank'>{symbol}</a></strong>: 
                    {timestamp} — New High: {round(data["hourly_high"], 10)} — 
                    10-day: {round(data.get("10_day_high", "N/A"), 10)} — 
                    20-day: {round(data.get("20_day_high", "N/A"), 10)} — 
                    55-day: {round(data.get("55_day_high", "N/A"), 10)}
                </li>
                """

            html_content += "</ul></div>"

        html_content += """
        </div>
        </body>
        </html>
        """
        
        cursor.close()
        
        # Writing the HTML content to a file
        with open("crypto_turtle_results_hourly.html", "w") as file:
            file.write(html_content)
        
    except Exception as e:
        print(f"An error occurred: {e}")