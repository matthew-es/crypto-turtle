# IMPORTS
import crypto_turtle_logger as log
import crypto_turtle_database as db
import coinbase_symbols as sym
import coinbase_prices as prices
import coinbase_prices_calculate_daily_highs_lows as highs

# LET'S GO
log.log_message("********************************************************")
log_crypto_turtle = log.log_duration_start("CRYPTO TURTLE WHOLE PROGRAM")

# OPEN DATABASE CONNECTION
new_connection = db.db_connect_open()

# CHECK DB TABLES EXIST
db.check_or_create_tables(new_connection)

# SYMBOLS CHECK
log_time_symbols = log.log_duration_start("SYMBOLS CHECK")
symbol_details = sym.get_coinbase_symbols()
sym.check_and_insert_symbols(new_connection, symbol_details)
symbols = sym.get_online_symbols(new_connection)
log.log_duration_end(log_time_symbols)

# GET THE DAILY PRICES
log_prices = log.log_duration_start("PRICES CHECK")
prices.get_daily_prices_for_symbols(symbols, new_connection)
prices.get_hourly_prices_for_symbols(symbols, new_connection)
# missing.fill_missing_hourly_prices(symbols, new_connection)
log.log_duration_end(log_prices)

# WORK OUT ALL THE HIGHS AND LOWS
log_highs_lows = log.log_duration_start("HIGHS LOWS CHECK")
highs.update_high_lows(symbols, new_connection)
log.log_duration_end(log_highs_lows)

# BREKAOUTS
log_breakouts = log.log_duration_start("BREAKTOUTS CHECK")

import coinbase_compare_prices_daily_10 as comp10
comp10.print_symbols_exceeding_highs(new_connection)

import coinbase_compare_prices_daily_20 as comp20
comp20.print_symbols_exceeding_highs(new_connection)

import coinbase_compare_prices_daily_55 as comp55
comp55.print_symbols_exceeding_highs(new_connection)

# import coinbase_compare_prices_hourly as comp
# comp.print_symbols_exceeding_highs(new_connection)

import coinbase_compare_prices_percentages as comp
comp.compare_prices_percentages(new_connection)

log.log_duration_end(log_breakouts)


# END. STOP.
db.db_connect_close(new_connection)
log.log_message("Crypto Turtle has FINISHED, STOPPED, THE END.")
log.log_duration_end(log_crypto_turtle)
log.log_message("********************************************************")