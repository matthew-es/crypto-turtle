# IMPORTS
import logging
import sys

import crypto_turtle_logger as log
import crypto_turtle_database as db

import coinbase_symbols as sym
import coinbase_prices_hourly as hour

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
log_crypto_turtle = log.log_duration_start("CRYPTO TURTLE RUN HOURLY BEGINS")

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

# HOURLY PRICES
log_hourly_prices = log.log_duration_start("HOURLY PRICES CHECK")
hour.get_hourly_prices_for_symbols(symbols, new_connection)
hour.hourly_percentages(new_connection)
hour.hourly_breakouts(symbols, new_connection)
# hour.print_symbols_exceeding_highs(new_connection)
log.log_duration_end(log_hourly_prices)

########################################################################################

db.db_connect_close(new_connection)
log.log_message("Crypto Turtle HOURLY PRICES has FINISHED, STOPPED, THE END.")
log.log_duration_end(log_crypto_turtle)
log.log_message("********************************************************")

########################################################################################