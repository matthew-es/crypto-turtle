# IMPORTS
import requests as req

import crypto_turtle_timings as ctt
import crypto_turtle_database as db
import crypto_turtle_logger as log

log_check_one_symbol = log.log_duration_start("CHECK ONE SYMBOL")

symbol = "ABT-USD"

twenty_four_hours_ago = ctt.twenty_four_hours_ago
timestamp_utc = ctt.timestamp_utc
timestamp_local = ctt.timestamp_local
number_of_hours = ctt.number_of_hours

end_time = ctt.now_previous_hour
start_time = ctt.twenty_four_hours_ago

log.log_message(f"{symbol} AND {start_time}")

def fetch_api_data_for_symbol(symbol):
    # Set the granularity to 3600 seconds (1 hour)
    granularity = 3600

    # Coinbase API endpoint for fetching candles data
    url = f"https://api.pro.coinbase.com/products/{symbol}/candles"

    # Parameters for the API call
    params = {
        'start': start_time.isoformat(),
        'end': end_time.isoformat(),
        'granularity': granularity
    }

    # Make the API call
    response = req.get(url, params=params)

    if response.status_code == 200:
        data = response.json()

        # Sort the data by timestamp (earliest to latest)
        sorted_data = sorted(data, key=lambda x: x[0])

        # Count the number of rows
        row_count = len(sorted_data)

        # Print the row count
        log.log_message(f"TOTAL API rows: {row_count}")

        # Print the data for each hour
        for hourly_data in sorted_data:
            timestamp = hourly_data[0]
            timestamp1 = ctt.timestamp_local(timestamp)
            timestamp2 = ctt.timestamp_utc(timestamp)
            low_price = hourly_data[1]
            high_price = hourly_data[2]
            open_price = hourly_data[3]
            close_price = hourly_data[4]
            log.log_message(f"API TIMESTAMP: {timestamp}, HIGH: {high_price}")
            # print(f"API TIMESTAMP: {timestamp}, API LOCAL: {timestamp1}, API UTC: {timestamp2}, Low: {low_price}, High: {high_price}, Open: {open_price}, Close: {close_price}")
    else:
        log.log_message(f"Failed to retrieve data for {symbol}. Status code: {response.status_code}")

def check_one_symbol_in_db(symbol):
    new_connection = db.db_connect_open()

    query = """
    SELECT s.SymbolName,
            (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.Timestamp <= %s ORDER BY hp.Timestamp DESC LIMIT 1) AS LatestHigh,
            (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.Timestamp <= %s ORDER BY hp.Timestamp DESC LIMIT 1) AS TwentyFourHoursAgoHigh
        FROM Symbols s
        WHERE s.SymbolName = %s;
    """
    cursor = new_connection.cursor()
    cursor.execute(query, (end_time, start_time, symbol))

    query2 = """
        SELECT hp.UnixTimestamp, hp.HighPrice
        FROM HourlyPrices hp
        JOIN Symbols s ON hp.SymbolID = s.SymbolID
        WHERE s.SymbolName = %s AND hp.Timestamp >= %s
        ORDER BY hp.Timestamp ASC;
    """
    cursor2 = new_connection.cursor()
    cursor2.execute(query2, (symbol, start_time))

    # query2 = """
    #     SELECT hp.Timestamp, hp.HighPrice
    #     FROM HourlyPrices hp
    #     JOIN Symbols s ON hp.SymbolID = s.SymbolID
    #     WHERE s.SymbolName = %s AND hp.Timestamp >= NOW() - make_interval(hours := %s)
    #     ORDER BY hp.Timestamp ASC;
    # """
    # cursor2 = new_connection.cursor()
    # cursor2.execute(query2, (symbol, number_of_hours))

    results = cursor.fetchall()            
    for result in results:
        log.log_message(result)
        
    results = cursor2.fetchall()
    log.log_message(f"TOTAL DB ROWS: {len(results)}")            
    for result in results:
        timestamp = result[0]
        # timesstamp1 = ctt.timestamp_local(timestamp)
        # timesstamp2 = ctt.timestamp_utc(timestamp)
        log.log_message(f"DB TIMESTAMP: {timestamp}, HIGH: {result[1]}")
        # print(f"DB TIMESTAMP: {timestamp}, DB LOCAL: {timesstamp1}, DB UTC: {timesstamp2}, HIGH: {result[1]}")

# Example usage

fetch_api_data_for_symbol(symbol)
check_one_symbol_in_db(symbol)

log.log_duration_end(log_check_one_symbol)