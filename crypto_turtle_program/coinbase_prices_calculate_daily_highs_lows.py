import datetime as dt
import psycopg as pg
import contextlib as cl

import crypto_turtle_logger as log
import crypto_turtle_database as db

#####################################################################################################

time_periods = [10, 20, 55]  # Time periods remain the same, but calculations will change

def update_high_lows(symbols, connection):
    log.log_message("CHECKING HIGH LOWS TEMP TABLE STARTS")
    try:
        # Use a context manager to ensure the cursor is closed automatically
        with cl.closing(connection.cursor()) as cursor:
            # Create a temporary table for batch update operations
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS batch_updates (
                    SymbolID INT,
                    UnixTimestamp BIGINT,
                    Period VARCHAR(255),
                    HighValue NUMERIC,
                    LowValue NUMERIC
                );
            """)
            
            # Assuming 'symbols' is a list of symbol names and 'connection' is your database connection
            for symbol in symbols:
                log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} STARTS")
                cursor.execute("SELECT SymbolID FROM Symbols WHERE SymbolName = %s;", (symbol,))
                symbol_id_row = cursor.fetchone()
                
                if not symbol_id_row:
                    print(f"Symbol {symbol} not found.")
                    continue
                
                symbol_id = symbol_id_row[0]
                
                # Fetch the UnixTimestamps needing updates for high/low values
                cursor.execute("""
                    SELECT UnixTimestamp 
                    FROM DailyPrices 
                    WHERE SymbolID = %s AND (
                        "10dayhigh" IS NULL OR 
                        "20dayhigh" IS NULL OR 
                        "55dayhigh" IS NULL OR 
                        UnixTimestamp >= (SELECT MAX(UnixTimestamp) FROM DailyPrices WHERE SymbolID = %s) - (2 * 86400)
                    )
                    ORDER BY UnixTimestamp;
                """, (symbol_id, symbol_id))
                
                timestamps = cursor.fetchall()
                
                for unixtimestamp in (row[0] for row in timestamps):
                    # Example time_periods: [10, 20, 55]
                    for period in [10, 20, 55]:
                        start_unixtimestamp = unixtimestamp - (period * 86400)
                        
                        cursor.execute("""
                            SELECT MAX(HighPrice), MIN(LowPrice) 
                            FROM DailyPrices 
                            WHERE SymbolID = %s AND UnixTimestamp BETWEEN %s AND %s;
                        """, (symbol_id, start_unixtimestamp, unixtimestamp))
                        
                        high, low = cursor.fetchone()
                        
                        if high is not None and low is not None:
                            cursor.execute("""
                                INSERT INTO batch_updates (SymbolID, UnixTimestamp, Period, HighValue, LowValue)
                                VALUES (%s, %s, %s, %s, %s);
                            """, (symbol_id, unixtimestamp, f"{period}day", high, low))
            
            # Apply batch updates from the temporary table to the DailyPrices table
            cursor.execute("""
                UPDATE DailyPrices AS dp
                SET
                    "10dayhigh" = bu.HighValue,
                    "10daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '10day';
                
                UPDATE DailyPrices AS dp
                SET
                    "20dayhigh" = bu.HighValue,
                    "20daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '20day';
                
                UPDATE DailyPrices AS dp
                SET
                    "55dayhigh" = bu.HighValue,
                    "55daylow" = bu.LowValue
                FROM batch_updates AS bu
                WHERE dp.SymbolID = bu.SymbolID AND dp.UnixTimestamp = bu.UnixTimestamp AND bu.Period = '55day';
            """)
            
            # Commit the transaction
            connection.commit()
            log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} ENDS")          
    except Exception as e:
        print(f"Error updating high and low for symbols: {e}")
        # Rollback in case of error
        connection.rollback()
    log.log_message("CHECKING HIGH LOWS TEMP TABLE ENDS")


# def update_high_lows(symbols, new_connection):
#     for symbol in symbols:
#         try:
#             cursor = new_connection.cursor()
            
#             # First, get the symbol ID
#             cursor.execute(db.symbols_select_symbol_on_name(), (symbol,))
#             symbol_id = cursor.fetchone()
#             if not symbol_id:
#                 print(f"Symbol {symbol} not found in Symbols table.")
#                 continue
#             symbol_id = symbol_id[0]
#             log.log_message(f"CHECKING HIGH LOWS FOR: {symbol}")

#             # Fetch UnixTimestamps that need updating
#             cursor.execute("""
#                 SELECT UnixTimestamp FROM DailyPrices
#                 WHERE SymbolID = %s AND (
#                     "10dayhigh" IS NULL OR 
#                     "20dayhigh" IS NULL OR 
#                     "55dayhigh" IS NULL OR
#                     UnixTimestamp >= (SELECT MAX(UnixTimestamp) FROM DailyPrices) - (2 * 86400)
#                 )
#                 ORDER BY UnixTimestamp
#             """, (symbol_id,))
#             timestamps = [row[0] for row in cursor.fetchall()]

#             for unixtimestamp in timestamps:
#                 for period in time_periods:
#                     start_unixtimestamp = unixtimestamp - (period * 86400)  # Calculate start timestamp
                    
#                     cursor.execute("""
#                         SELECT MAX(HighPrice), MIN(LowPrice) FROM DailyPrices
#                         WHERE SymbolID = %s AND UnixTimestamp BETWEEN %s AND %s
#                     """, (symbol_id, start_unixtimestamp, unixtimestamp))
#                     period_high, period_low = cursor.fetchone()

#                     # Update the corresponding high and low for the current UnixTimestamp
#                     if period_high and period_low:
#                         update_query = f"""
#                             UPDATE DailyPrices
#                             SET "{period}dayhigh" = %s, "{period}daylow" = %s
#                             WHERE SymbolID = %s AND UnixTimestamp = %s
#                         """
#                         cursor.execute(update_query, (period_high, period_low, symbol_id, unixtimestamp))
#                         new_connection.commit()
            
#             log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} DONE")

#         except Exception as e:
#             print(f"Error updating high and low for symbol {symbol}: {e}")
#             new_connection.rollback()
#         finally:
#             cursor.close()