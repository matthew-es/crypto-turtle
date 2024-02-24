# IMPORTS
import requests as req
import psycopg as pg

import crypto_turtle_logger as log
import crypto_turtle_database as db


# GRAB SYMBOLS FROM COINBASE
def get_coinbase_symbols():
    function_name = "GET COINBASE SYMBOLS"
    log_get_coinbase_symbols = log.log_duration_start(function_name)
    url = "https://api.pro.coinbase.com/products"

    response = req.get(url)
    response.raise_for_status()

    if response.status_code == 200:
        symbols = response.json()
        symbol_details = {pair["id"]: pair for pair in symbols}
        log.log_message(f"{function_name}: API returns {len(symbol_details)} symbols.")
        log.log_duration_end(log_get_coinbase_symbols)
        return symbol_details

    else:
        log.log_duration_end(log_get_coinbase_symbols)
        log.log_message("Failed to connect to Coinbase. Status code:", response.status_code)
        return{}    


# ARE THE SYMBOLS IN THE DATABASE TABLE?
def check_and_insert_symbols(new_connection, symbol_details):
    function_name = "DB SYMBOLS CHECK AND INSERT"
    log_check_and_insert_symbols = log.log_duration_start(function_name)
    
    try:
        cursor = new_connection.cursor()
        cursor.execute(db.symbols_select_count())
        how_many_symbols = cursor.fetchone()[0]
        log.log_message(f"{function_name}: SELECT COUNT returns {how_many_symbols} symbols in database.")
        print("MESSAGE 1")
        for symbol, details in symbol_details.items():
            status = details.get('status')
            trading_disabled = details.get('trading_disabled', False)
            print("MESSAGE 2")
            cursor.execute(db.symbols_select_status_trading_disabled(), (symbol,))
            symbol_data = cursor.fetchone()
            print("MESSAGE 3")
            if symbol_data:
                symbol_id, existing_status, existing_trading_disabled = symbol_data
                print("MESSAGE 4")
                print(symbol_data)
                if existing_status != status or existing_trading_disabled != trading_disabled:                  
                    cursor.execute(db.symbols_update_status_trading_disabled(), (status, trading_disabled, symbol_id))
                    new_connection.commit()
                print("MESSAGE 5")
            else:
                cursor.execute(db.symbols_insert_symbol(), (symbol, status, trading_disabled))
                symbol_id = cursor.fetchone()[0]
                new_connection.commit()
                print("MESSAGE 6")
        print("MESSAGE 7")
        cursor.execute(db.symbols_select_count_group_by_status())
        status_counts = cursor.fetchall()
        for status, count in status_counts:
            log.log_message(f"{function_name}: total symbols in database with status '{status}': {count}")
    except Exception as e:
        log.log_message(f"Error in check_and_insert_symbols: {e}")
    finally:
        log.log_duration_end(log_check_and_insert_symbols)
        cursor.close()


# GET ALL THE SYMBOLS FROM THE DATABASE TABLE
def get_online_symbols(new_connection):
    function_name = "GET ONLINE SYMBOLS"
    log_get_online_symbols = log.log_duration_start(function_name)

    try:
        cursor = new_connection.cursor()
        cursor.execute(db.symbols_select_status_online())
        online_symbols = [row[0] for row in cursor.fetchall()]
        log.log_message(f"{function_name}: there are {len(online_symbols)} symbols online.")
        return online_symbols
    except Exception as e:
        log.log_message(f"{function_name}: Error fetching online symbols:", e)
        return []
    finally:
        log.log_duration_end(log_get_online_symbols)
        cursor.close()