# IMPORTS
import logging
import sys

import crypto_turtle_logger as log
import crypto_turtle_database as db

import coinbase_symbols as sym
import coinbase_prices_hourly as hour
# import coinbase_prices_calculate_hourly_percentages as hper

# import coinbase_prices as prices
# import coinbase_prices_daily as day
# import coinbase_prices_calculate_daily_highs_lows as highs

# import coinbase_prices_hourly_breakouts as hbrk
# import coinbase_prices_daily_percentages as dper
# import coinbase_prices_daily_breakouts as dbrk

########################################################################################

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default KeyboardInterrupt handler to avoid capturing interrupt signals.
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

def setup_logging():
    log_file_path = 'crypto_turtle_log.txt'
    logging.basicConfig(filename=log_file_path, level=logging.ERROR,
                        format='%(asctime)s:%(levelname)s:%(message)s')

# Call setup_logging at the start of your main script or application entry point.
setup_logging()

# Set the global exception hook to our custom handler.
sys.excepthook = handle_exception

########################################################################################

# LET'S GO
log.log_message("********************************************************")
log_crypto_turtle = log.log_duration_start("CRYPTO TURTLE WHOLE PROGRAM")

########################################################################################

# OPEN DATABASE CONNECTION
new_connection = db.db_connect_open()
db.check_or_create_tables(new_connection)

########################################################################################

# SYMBOLS CHECK
log_time_symbols = log.log_duration_start("SYMBOLS CHECK")
symbol_details = sym.get_coinbase_symbols()
sym.check_and_insert_symbols(new_connection, symbol_details)
symbols = sym.get_online_symbols(new_connection)
log.log_duration_end(log_time_symbols)


########################################################################################

# HOURLY PRICES FIRST
log_hourly_prices = log.log_duration_start("HOURLY PRICES CHECK")
hour.get_hourly_prices_for_symbols(symbols, new_connection)
hour.compare_prices_percentages(new_connection)
# hour.print_symbols_exceeding_highs(new_connection)
log.log_duration_end(log_hourly_prices)

########################################################################################

# GET THE DAILY PRICES
# log_daily_prices = log.log_duration_start("DAILY PRICES CHECK")
# prices.get_daily_prices_for_symbols(symbols, new_connection)

# log_highs_lows = log.log_duration_start("HIGHS LOWS CHECK")
# highs.update_high_lows(symbols, new_connection)
# log.log_duration_end(log_highs_lows)

# log_breakouts = log.log_duration_start("BREAKOUTS CHECK")
# import coinbase_compare_prices_daily_10 as comp10
# comp10.print_symbols_exceeding_highs(new_connection)
# import coinbase_compare_prices_daily_20 as comp20
# comp20.print_symbols_exceeding_highs(new_connection)
# import coinbase_compare_prices_daily_55 as comp55
# comp55.print_symbols_exceeding_highs(new_connection)
# log.log_duration_end(log_breakouts)

# log.log_duration_end(log_daily_prices)

########################################################################################

db.db_connect_close(new_connection)
log.log_message("Crypto Turtle has FINISHED, STOPPED, THE END.")
log.log_duration_end(log_crypto_turtle)
log.log_message("********************************************************")

########################################################################################