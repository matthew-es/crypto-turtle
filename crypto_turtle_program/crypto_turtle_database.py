import psycopg as pg
import os
import crypto_turtle_logger as log

# Connect to postgres database with psycopg
def db_connect_open():        
    log.log_message("DB CONNECTION OPENED")
    
    try:
        connection = pg.connect(
            host = os.getenv("DATABASE_HOST"),
            dbname = os.getenv("DATABASE_NAME"),
            user = os.getenv("DATABASE_USER"),
            password = os.getenv("DATABASE_PASSWORD"),
            port = os.getenv("DATABASE_PORT")
            # ,
            # sslmode="require"
        )
        return connection
        
    except pg.OperationalError as e:
        print("Unable to connect to the database from db_connect:", e)

# Close the connection to the database
def db_connect_close(connection):
    connection.close()
    log.log_message("DB CONNECTION CLOSED")

##############################
###### DO the tables exist? Do we need to create them?
def check_or_create_tables(new_connection):
    function_name = "CHECK OR CREATE TABLES"
    log_check_or_create_tables = log.log_duration_start(function_name)
    
    cursor = new_connection.cursor()
    cursor.execute("""
                   SELECT EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_proc p
                        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                        WHERE n.nspname = 'public'  -- or your specific schema
                        AND p.proname = 'update_updatedat_column'  -- your function name
                    );
                    """)
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(create_shared_functions())
        new_connection.commit()
        log.log_message("CREATED FUNCTIONS: updatedat")
    else:
        log.log_message("FUNCTION: updatedat already exists")
    new_connection.commit()
    
    
    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'symbols');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(symbols_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: symbols")
    else:
        log.log_message("TABLE: symbols already exists")
    new_connection.commit()

    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'hourlyprices');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(hourlyprices_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: hourlyprices")
    else:
        log.log_message("TABLE: hourlyprices already exists")
    new_connection.commit()

    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'hourly_percentage_increases');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(hourly_percentage_increases_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: hourly percentage increases")
    else:
        log.log_message("TABLE: hourly percentage increases already exists")
    new_connection.commit()
    
    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'hourly_breakouts');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(hourly_breakouts_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: hourly breakouts")
    else:
        log.log_message("TABLE: hourly breakouts already exists")
    new_connection.commit()

    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dailyprices');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(dailyprices_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: dailyprices")
    else:
        log.log_message("TABLE: dailyprices already exists")
    new_connection.commit()
    
    cursor = new_connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users');")
    exists = cursor.fetchone()[0]
    if not exists:
        cursor.execute(users_create_table())
        new_connection.commit()
        log.log_message("CREATED TABLE: users")
    else:
        log.log_message("TABLE: users already exists")
    new_connection.commit()
    
    log.log_duration_end(log_check_or_create_tables)
    
##############################
###### CREATE TABLES statements

def create_shared_functions():
    return """
        CREATE OR REPLACE FUNCTION update_updatedat_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updatedat = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """

def symbols_create_table():
    return """
        CREATE TABLE symbols (
        symbolid SERIAL PRIMARY KEY,
        symbolname VARCHAR(255) NOT NULL UNIQUE,
        description TEXT,
        status VARCHAR(255),
        tradingdisabled BOOLEAN
    );
    """

def hourlyprices_create_table():
    return """
        CREATE TABLE hourlyprices (
            hourlypriceid SERIAL PRIMARY KEY,
            symbolid INTEGER REFERENCES symbols(symbolid),
            highprice NUMERIC(31,15) NOT NULL,
            lowprice NUMERIC(31,15) NOT NULL,
            openprice NUMERIC(31,15),
            closeprice NUMERIC(31,15),
            createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            unixtimestamp BIGINT,
            UNIQUE(symbolid, unixtimestamp)
        );

        CREATE TRIGGER update_hourlyprices_modtime
            BEFORE UPDATE ON hourlyprices
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    """

def hourly_percentage_increases_create_table():
    return """        
        CREATE TABLE hourly_percentage_increases (
            id SERIAL PRIMARY KEY,
            symbolid INT NOT NULL,
            two_hours_increase NUMERIC(31, 15),
            four_hours_increase NUMERIC(31, 15),
            twelve_hours_increase NUMERIC(31, 15),
            twenty_four_hours_increase NUMERIC(31, 15),
            forty_eight_hours_increase NUMERIC(31, 15),
            seventy_two_hours_increase NUMERIC(31, 15),
            one_hundred_twenty_hours_increase NUMERIC(31, 15),
            report_generated_at BIGINT,
            createdat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (symbolid) REFERENCES symbols(symbolid),
            UNIQUE (symbolid, report_generated_at)
        );

        CREATE INDEX idx_two_hours_increase ON hourly_percentage_increases(two_hours_increase DESC);
        CREATE INDEX idx_four_hours_increase ON hourly_percentage_increases(four_hours_increase DESC);
        CREATE INDEX idx_twelve_hours_increase ON hourly_percentage_increases(twelve_hours_increase DESC);
        CREATE INDEX idx_twenty_four_hours_increase ON hourly_percentage_increases(twenty_four_hours_increase DESC);
        CREATE INDEX idx_forty_eight_hours_increase ON hourly_percentage_increases(forty_eight_hours_increase DESC);
        CREATE INDEX idx_seventy_two_hours_increase ON hourly_percentage_increases(seventy_two_hours_increase DESC);
        CREATE INDEX idx_one_hundred_twenty_hours_increase ON hourly_percentage_increases(one_hundred_twenty_hours_increase DESC);
        
        CREATE TRIGGER update_hourly_percentage_increases_modtime
            BEFORE UPDATE ON hourly_percentage_increases
            FOR EACH ROW
            EXECUTE FUNCTION update_updatedat_column();
    """

def hourly_breakouts_create_table():
    return """        
        CREATE TABLE hourly_breakouts (
            breakout_id SERIAL PRIMARY KEY,
            symbolid INTEGER NOT NULL,
            unixtimestamp BIGINT NOT NULL,
            hourly_high NUMERIC(31, 15) NOT NULL,
            day10_high NUMERIC(31, 15) NOT NULL,
            day20_high NUMERIC(31, 15) NOT NULL,
            day55_high NUMERIC(31, 15) NOT NULL,
            createdat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (symbolid, unixtimestamp),
            FOREIGN KEY (symbolid) REFERENCES symbols(symbolid)
        );

        CREATE INDEX idx_hourly_breakouts_symbolid ON hourly_breakouts(symbolid);
        CREATE INDEX idx_hourly_breakouts_unixtimestamp ON hourly_breakouts(unixtimestamp);
        
        CREATE TRIGGER update_hourly_breakouts_modtime
            BEFORE UPDATE ON hourly_breakouts
            FOR EACH ROW EXECUTE FUNCTION update_updatedat_column();
    """

def dailyprices_create_table():
    return """
        CREATE TABLE dailyprices (
            dailypriceid SERIAL PRIMARY KEY,
            symbolid INTEGER REFERENCES symbols(symbolid),
            highprice NUMERIC(31,15) NOT NULL,
            lowprice NUMERIC(31,15) NOT NULL,
            openprice NUMERIC(31,15) NOT NULL DEFAULT 0,
            closeprice NUMERIC(31,15) NOT NULL DEFAULT 0,
            "20dayhigh" NUMERIC(31,15) NOT NULL DEFAULT 0,
            "10dayhigh" NUMERIC(31,15) NOT NULL DEFAULT 0,
            "55dayhigh" NUMERIC(31,15) NOT NULL DEFAULT 0,
            "10daylow" NUMERIC(31,15) NOT NULL DEFAULT 0,
            "20daylow" NUMERIC(31,15) NOT NULL DEFAULT 0,
            "55daylow" NUMERIC(31,15) NOT NULL DEFAULT 0,
            createdat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            unixtimestamp BIGINT,
            UNIQUE(symbolid, unixtimestamp)
        );

        CREATE TRIGGER update_dailyprices_modtime
            BEFORE UPDATE ON dailyprices
            FOR EACH ROW
            EXECUTE FUNCTION update_updatedat_column();
    """

def users_create_table():
    return """        
        CREATE TABLE users (
            uuid uuid PRIMARY KEY,
            email varchar(255) UNIQUE NOT NULL,
            email_confirmed boolean NOT NULL DEFAULT false,
            email_confirm_token text,
            password_digest text NOT NULL,
            password_reset_token text,
            password_reset_expiry bigint,
            login_is_allowed boolean NOT NULL DEFAULT true,
            login_last_time bigint,
            login_failed_attempts integer NOT NULL DEFAULT 0,
            login_lockout_until bigint,
            can_access_level integer NOT NULL DEFAULT 0,
            can_access_until bigint,
            createdat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TRIGGER update_users_modtime
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION update_updatedat_column();
    """

##############################
### OPERATIONS QUERIES ##################
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