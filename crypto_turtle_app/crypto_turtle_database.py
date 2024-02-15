import psycopg as pg
import config as cf
import crypto_turtle_logger as log

# Connect to postgres database with psycopg
def db_connect_open():        
    log.log_message("DB CONNECTION OPENED")
    
    try:
        config = cf.current_config
        connection = pg.connect(
            dbname = config.database_name,
            user = config.database_user,
            password = config.database_password,
            host = config.database_host,
            port = config.database_port
        )
        return connection
    
    except pg.OperationalError as e:
        print("Unable to connect to the database from db_connect:", e)

# Close the connection to the database
def db_connect_close(connection):
    connection.close()
    log.log_message("DB CONNECTION CLOSED")


##############################
### QUERIES ##################
# Symbols
def symbols_select_count():
    return "SELECT COUNT(*) FROM Symbols;"

def symbols_select_count_group_by_status():
    return "SELECT Status, COUNT(*) FROM Symbols GROUP BY Status;"

def symbols_select_symbol_on_name():
    return "SELECT SymbolID FROM Symbols WHERE SymbolName = %s;"

def symbols_select_status_trading_disabled():
    return "SELECT SymbolName, Status, TradingDisabled FROM Symbols WHERE SymbolName = %s;"

def symbols_select_status_online():
    return "SELECT SymbolName FROM Symbols WHERE Status = 'online';"

def symbols_update_status_trading_disabled():
    return "UPDATE Symbols SET Status = %s, TradingDisabled = %s WHERE SymbolID = %s;"

def symbols_insert_symbol():
    return "INSERT INTO Symbols (SymbolName, Status, TradingDisabled) VALUES (%s, %s, %s) RETURNING SymbolID;"


# Daily Prices
def dailyprices_select_count():
    return "SELECT COUNT(*) FROM DailyPrices;"

def dailyprices_select_max_unix_on_symbol_id():
    return "SELECT MAX(UnixTimestamp) FROM DailyPrices WHERE SymbolID = %s;"

def dailyprices_insert_symbol_prices():
    return """
        INSERT INTO DailyPrices (SymbolID, UnixTimestamp, HighPrice, LowPrice, OpenPrice, ClosePrice)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (SymbolID, UnixTimestamp) 
        DO UPDATE SET
            UnixTimestamp = EXCLUDED.UnixTimestamp,
            HighPrice = EXCLUDED.HighPrice,
            LowPrice = EXCLUDED.LowPrice,
            OpenPrice = EXCLUDED.OpenPrice,
            ClosePrice = EXCLUDED.ClosePrice;
        ;
    """

# Hourly Prices
def hourlyprices_select_count():    
    return "SELECT COUNT(*) FROM HourlyPrices;"

def hourlyprices_select_max_unix_on_symbol_id():
    return "SELECT MAX(UnixTimestamp) FROM HourlyPrices WHERE SymbolID = %s;"

def hourlyprices_insert_symbol_prices():
    return """
        INSERT INTO HourlyPrices (SymbolID, UnixTimestamp, HighPrice, LowPrice, OpenPrice, ClosePrice)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (SymbolID, UnixTimestamp) 
        DO UPDATE SET
            UnixTimestamp = EXCLUDED.UnixTimestamp,
            HighPrice = EXCLUDED.HighPrice,
            LowPrice = EXCLUDED.LowPrice,
            OpenPrice = EXCLUDED.OpenPrice,
            ClosePrice = EXCLUDED.ClosePrice;
        ;
    """
def hourlyprices_select_last_30_for_symbol_on_name():
    return """
    SELECT hourlyprices.* 
        FROM hourlyprices 
        JOIN symbols ON hourlyprices.SymbolId = symbols.SymbolId
        WHERE symbolname = %s 
        ORDER BY hourlyprices.unixtimestamp DESC 
        LIMIT 30;
    """