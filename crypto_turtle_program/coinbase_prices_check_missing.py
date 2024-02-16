import datetime as dt
import requests
import psycopg  # Assuming you're using psycopg2 for database connection
import crypto_turtle.crypto_turtle_database as db
import coinbase_symbols as sym
import crypto_turtle.crypto_turtle_logger as log

log.log_message("*************************************")
log.log_message("CHECKING SYMBOL HOURLY PRICE DATA")

new_connection = db.db_connect_open()
symbols = sym.get_online_symbols(new_connection)


def fill_missing_hourly_prices(symbols, new_connection):
    missing_data_log = []
    now = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_time = now - dt.timedelta(hours=24)

    cursor = new_connection.cursor()
    count_equal = 0
    count_not_equal = 0

    for symbol in symbols:
        cursor.execute("SELECT SymbolID FROM Symbols WHERE SymbolName = %s", (symbol,))
        symbol_id_row = cursor.fetchone()
        if not symbol_id_row:
            continue
        
        symbol_id = symbol_id_row[0]
        
        # Fetch API data
        url = f"https://api.pro.coinbase.com/products/{symbol}/candles"
        params = {'start': start_time.isoformat(), 'end': now.isoformat(), 'granularity': 3600}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            count_in_api = len(data)

            # Check existing count in the database
            cursor.execute("""
                SELECT COUNT(*) FROM HourlyPrices 
                WHERE SymbolID = %s AND Timestamp >= %s AND Timestamp < %s
            """, (symbol_id, start_time, now))
            count_in_db = cursor.fetchone()[0]

            if count_in_db < count_in_api:
                missing_data_log.append(symbol)

                # Insert missing data
                for hourly_data in data:
                    timestamp = dt.datetime.fromtimestamp(hourly_data[0], tz=dt.timezone.utc)
                    high_price = hourly_data[2]
                    low_price = hourly_data[1]
                    open_price = hourly_data[3]
                    close_price = hourly_data[4]
                    
                    insert_query = """
                        INSERT INTO HourlyPrices (SymbolID, Timestamp, HighPrice, LowPrice, OpenPrice, ClosePrice)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (SymbolID, Timestamp) 
                        DO UPDATE SET
                            HighPrice = EXCLUDED.HighPrice,
                            LowPrice = EXCLUDED.LowPrice,
                            OpenPrice = EXCLUDED.OpenPrice,
                            ClosePrice = EXCLUDED.ClosePrice
                    """
                    cursor.execute(insert_query, (symbol_id, timestamp, high_price, low_price, open_price, close_price))

                new_connection.commit()
                log.log_message(f"{symbol} — {count_in_db}/{count_in_api} Inserted missing hourly prices")
                count_not_equal += 1
            else:
                log.log_message(f"{symbol} — {count_in_db}/{count_in_api} ALL GOOD")
                count_equal += 1
    cursor.close()
    log.log_message(f"COUNT EQUAL: {count_equal}")
    log.log_message(f"COUNT NOT EQUAL: {count_not_equal}")
    return missing_data_log

fill_missing_hourly_prices(symbols, new_connection)

db.db_connect_close(new_connection)