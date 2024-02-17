import datetime as dt
import psycopg as pg

import crypto_turtle_logger as log
import crypto_turtle_database as db

#####################################################################################################

time_periods = [10, 20, 55]  # Time periods remain the same, but calculations will change

def update_high_lows(symbols, new_connection):
    for symbol in symbols:
        try:
            cursor = new_connection.cursor()
            
            # First, get the symbol ID
            cursor.execute(db.symbols_select_symbol_on_name(), (symbol,))
            symbol_id = cursor.fetchone()
            if not symbol_id:
                print(f"Symbol {symbol} not found in Symbols table.")
                continue
            symbol_id = symbol_id[0]
            log.log_message(f"CHECKING HIGH LOWS FOR: {symbol}")

            # Fetch UnixTimestamps that need updating
            cursor.execute("""
                SELECT UnixTimestamp FROM DailyPrices
                WHERE SymbolID = %s AND (
                    "10dayhigh" IS NULL OR 
                    "20dayhigh" IS NULL OR 
                    "55dayhigh" IS NULL OR
                    UnixTimestamp >= (SELECT MAX(UnixTimestamp) FROM DailyPrices) - (2 * 86400)
                )
                ORDER BY UnixTimestamp
            """, (symbol_id,))
            timestamps = [row[0] for row in cursor.fetchall()]

            for unixtimestamp in timestamps:
                for period in time_periods:
                    start_unixtimestamp = unixtimestamp - (period * 86400)  # Calculate start timestamp
                    
                    cursor.execute("""
                        SELECT MAX(HighPrice), MIN(LowPrice) FROM DailyPrices
                        WHERE SymbolID = %s AND UnixTimestamp BETWEEN %s AND %s
                    """, (symbol_id, start_unixtimestamp, unixtimestamp))
                    period_high, period_low = cursor.fetchone()

                    # Update the corresponding high and low for the current UnixTimestamp
                    if period_high and period_low:
                        update_query = f"""
                            UPDATE DailyPrices
                            SET "{period}dayhigh" = %s, "{period}daylow" = %s
                            WHERE SymbolID = %s AND UnixTimestamp = %s
                        """
                        cursor.execute(update_query, (period_high, period_low, symbol_id, unixtimestamp))
                        new_connection.commit()
            
            log.log_message(f"CHECKING HIGH LOWS FOR: {symbol} DONE")

        except Exception as e:
            print(f"Error updating high and low for symbol {symbol}: {e}")
            new_connection.rollback()
        finally:
            cursor.close()



# # The time periods in days to use for the highs and lows
# time_periods = [10, 20, 55]

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
                        
#             # Fetch dates that need updating
#             # Includes dates with missing high/low data and recent dates
#             cursor.execute("""
#                 SELECT Date FROM DailyPrices
#                 WHERE SymbolID = %s AND (
#                     "10dayhigh" IS NULL OR 
#                     "20dayhigh" IS NULL OR 
#                     "55dayhigh" IS NULL OR
#                     Date >= (SELECT MAX(Date) FROM DailyPrices) - INTERVAL '2 days'
#                 )
#                 ORDER BY Date
#             """, (symbol_id,))
#             dates = [row[0] for row in cursor.fetchall()]

#             for date in dates:
#                 for period in time_periods:
#                     start_date = date - dt.timedelta(days=period)
#                     cursor.execute("""
#                         SELECT MAX(HighPrice), MIN(LowPrice) FROM DailyPrices
#                         WHERE SymbolID = %s AND Date BETWEEN %s AND %s
#                     """, (symbol_id, start_date, date))
#                     period_high, period_low = cursor.fetchone()

#                     # Update the corresponding high and low for the current date
#                     if period_high and period_low:
#                         update_query = f"""
#                             UPDATE DailyPrices
#                             SET "{period}dayhigh" = %s, "{period}daylow" = %s
#                             WHERE SymbolID = %s AND Date = %s
#                         """
#                         cursor.execute(update_query, (period_high, period_low, symbol_id, date))
#                         new_connection.commit()

#         except Exception as e:
#             print(f"Error updating high and low for symbol {symbol}: {e}")
#             new_connection.rollback()
#         finally:
#             cursor.close()