import crypto_turtle.crypto_turtle_database as db
import crypto_turtle.crypto_turtle_logger as log

new_log = log.log_duration_start()
new_connection = db.db_connect_open()

# Create a cursor object
cursor = new_connection.cursor()

# SQL query
query = """
    SELECT s.SymbolName, COUNT(dp.*) as RowCount
    FROM dailyprices dp
    JOIN symbols s ON dp.SymbolID = s.SymbolID
    GROUP BY s.SymbolName;
"""

try:
    # Execute the query
    cursor.execute(query)

    # Fetch and print the results
    for row in cursor.fetchall():
        print(f"Symbol: {row[0]}, Row Count: {row[1]}")

except Exception as e:
    print(f"An error occurred: {e}")

cursor.close()

db.db_connect_close(new_connection)    
log.log_duration_end(new_log, "COUNT AND LIST ROWS PER SYMBOL")