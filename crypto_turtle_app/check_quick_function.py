import time as t
import requests as req

import crypto_turtle_logger as log
import crypto_turtle_database as db
import crypto_turtle_timings as ctt

# Open logging
function_name = "CHECK UPDATE HOURLY PRICES TIMESTAMPS"
new_log = log.log_duration_start(function_name)
print(function_name)
# Open database connection
new_connection = db.db_connect_open()

############################################################################

symbol = "BTC-USD"

cursor = new_connection.cursor()
cursor.execute(db.hourlyprices_select_last_30_for_symbol_on_name(), (symbol,))
results = cursor.fetchall()

for row in results:
    log.log_message(row[8]) # prints the unix timestamp


end_time = ctt.now_previous_hour
start_time = ctt.twenty_four_hours_ago

# Coinbase API candles endpoint
url = f"https://api.pro.coinbase.com/products/{symbol}/candles"
params = {
    'start': start_time.isoformat(),
    'end': end_time.isoformat(),
    'granularity': 3600
}
response = req.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    sorted_data = sorted(data, key=lambda x: x[0])
    
    for hourly_data in sorted_data:
        unix_timestamp = hourly_data[0]
        high_price = hourly_data[2]
        low_price = hourly_data[1]
        open_price = hourly_data[3]
        close_price = hourly_data[4]
        log.log_message(f"API UNIX: {unix_timestamp}, High: {high_price}")
                        


############################################################################

# Close database connection
db.db_connect_close(new_connection)

# Close logging
log.log_duration_end(new_log)