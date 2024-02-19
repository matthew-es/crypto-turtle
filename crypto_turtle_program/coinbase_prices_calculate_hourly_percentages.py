import datetime as dt
import psycopg  # Assuming psycopg2 for PostgreSQL connection
import flask as fk
import crypto_turtle_logger as log
import crypto_turtle_timings as ctt
import crypto_turtle_database as db

def compare_prices_percentages(new_connection):
    print("MESSAGE 1")
    try:
        with new_connection.cursor() as cursor:
            print("MESSAGE 2")
            current_unix_timestamp = int(ctt.now_basic_unix)
            print("MESSAGE 3")
            now = int((dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)).timestamp())
            two_hours_ago = now - 2 * 60 * 60
            four_hours_ago = now - 4 * 60 * 60
            twelve_hours_ago = now - 12 * 60 * 60
            twenty_four_hours_ago = now - 24 * 60 * 60
            forty_eight_hours_ago = now - 48 * 60 * 60
            seventy_two_hours_ago = now - 72 * 60 * 60
            one_hundred_twenty_hours_ago = now - 120 * 60 * 60
            print("MESSAGE 4")

            query = """
                SELECT s.SymbolID,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS LatestHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwoHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS FourHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwelveHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS TwentyFourHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS FortyEightHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS SeventyTwoHoursAgoHigh,
                    (SELECT hp.HighPrice FROM HourlyPrices hp WHERE s.SymbolID = hp.SymbolID AND hp.unixtimestamp <= %s ORDER BY hp.unixtimestamp DESC LIMIT 1) AS OneHundredTwentyHoursAgoHigh
                FROM Symbols s
                WHERE s.Status = 'online'
            """            
            cursor.execute(query, (now, two_hours_ago, four_hours_ago, twelve_hours_ago, twenty_four_hours_ago, forty_eight_hours_ago, seventy_two_hours_ago, one_hundred_twenty_hours_ago))
            results = cursor.fetchall()
            print("MESSAGE 5")
            print(results)
            # Create temporary table for batch update operations 
            cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_hourly_percentage_increases AS
                SELECT * FROM hourly_percentage_increases WITH NO DATA;
            """)
            print("MESSAGE 6")
            # Go through each result and calculate the percentage differences
            for result in results:
                symbol_id, latest_high, two_hours_high, four_hours_high, twelve_hours_high, twenty_four_hours_high, forty_eight_hours_high, seventy_two_hours_high, one_hundred_twenty_hours_high = result
                print(result)
                
                # Calculate all the percentage differences for all active symbols vs. time periods
                two_hours_difference = ((latest_high - two_hours_high) / two_hours_high) * 100 if two_hours_high is not None else None
                four_hours_difference = ((latest_high - four_hours_high) / four_hours_high) * 100 if four_hours_high is not None else None
                twelve_hours_difference = ((latest_high - twelve_hours_high) / twelve_hours_high) * 100 if twelve_hours_high is not None else None
                twenty_four_hours_difference = ((latest_high - twenty_four_hours_high) / twenty_four_hours_high) * 100 if twenty_four_hours_high is not None else None
                forty_eight_hours_difference = ((latest_high - forty_eight_hours_high) / forty_eight_hours_high) * 100 if forty_eight_hours_high is not None else None
                seventy_two_hours_difference = ((latest_high - seventy_two_hours_high) / seventy_two_hours_high) * 100 if seventy_two_hours_high is not None else None
                one_hundred_twenty_hours_difference = ((latest_high - one_hundred_twenty_hours_high) / one_hundred_twenty_hours_high) * 100 if one_hundred_twenty_hours_high is not None else None
                print("MESSAGE 7")
                

                # Check which of those percentage results meet your selection critiera
                short_term_increases = [two_hours_difference, four_hours_difference, twelve_hours_difference, twenty_four_hours_difference]
                long_term_increases = [forty_eight_hours_difference, seventy_two_hours_difference, one_hundred_twenty_hours_difference]
                short_term_check = any(x > 5 for x in short_term_increases if x is not None)
                long_term_check = any(x > 10 for x in long_term_increases if x is not None)
                if short_term_check or long_term_check:
                    print("MESSAGE 8")
                    insert_data = (symbol_id, two_hours_difference, four_hours_difference, twelve_hours_difference, twenty_four_hours_difference, forty_eight_hours_difference, seventy_two_hours_difference, one_hundred_twenty_hours_difference, current_unix_timestamp)
                    print(insert_data)    
                    print("MESSAGE 9")
                    
                    cursor.execute("""
                        INSERT INTO temp_hourly_percentage_increases 
                        (symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                        twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                        one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, insert_data)
                    

            # Now do the buik insert into the actual hourly_percentage_increases table
            cursor.execute("""
                INSERT INTO hourly_percentage_increases 
                (symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat)
                SELECT symbolid, two_hours_increase, four_hours_increase, twelve_hours_increase, 
                twenty_four_hours_increase, forty_eight_hours_increase, seventy_two_hours_increase, 
                one_hundred_twenty_hours_increase, report_generated_at, createdat, updatedat
                FROM temp_hourly_percentage_increases
                ON CONFLICT (symbolid, report_generated_at)
                DO UPDATE
                SET two_hours_increase = EXCLUDED.two_hours_increase,
                    four_hours_increase = EXCLUDED.four_hours_increase,
                    twelve_hours_increase = EXCLUDED.twelve_hours_increase,
                    twenty_four_hours_increase = EXCLUDED.twenty_four_hours_increase,
                    forty_eight_hours_increase = EXCLUDED.forty_eight_hours_increase,
                    seventy_two_hours_increase = EXCLUDED.seventy_two_hours_increase,
                    one_hundred_twenty_hours_increase = EXCLUDED.one_hundred_twenty_hours_increase,
                    updatedat = EXCLUDED.updatedat;
                """)
                
            # And finally drop the temporary table
            cursor.execute("DROP TABLE IF EXISTS temp_hourly_percentage_increases")
            
            new_connection.commit()
    except Exception as e:
        print(f"HOURLY PERCENTAGES: Error in generating report: {e}")
        log.log_message(f"HOURLY PERCENTAGES: Error in generating report: {e}")

# Call it with db connection
# new_connection = db.db_connect_open()

# db.db_connect_close(new_connection)
   
                                                        
    #             previous_high_keys_mapping = {
    #                 '2hrs': 'two_hours_high',
    #                 '4hrs': 'four_hours_high',
    #                 '12hrs': 'twelve_hours_high',
    #                 '24hrs': 'twenty_four_hours_high',
    #                 '48hrs': 'forty_eight_hours_high',
    #                 '72hrs': 'seventy_two_hours_high',
    #                 '120hrs': 'one_hundred_twenty_hours_high',
    #             }
                
    #         # Define the order of periods for comparison
    #         period_order = ['2hrs', '4hrs', '12hrs', '24hrs', '48hrs', '72hrs', '120hrs']
            
    #         def custom_sort(item):
    #             symbol, periods_data = item
    #             sort_values = [float('inf')] * len(period_order)
    #             for i, period in enumerate(period_order):
    #                 if period in periods_data and 'increase' in periods_data[period]:
    #                     # Use the 'increase' value for sorting, ensuring it's a numerical value
    #                     sort_values[i] = -periods_data[period]['increase']
    #                 else:
    #                     sort_values[i] = float('inf')  # Maintain high sort value for missing data
    #             return tuple(sort_values)


    #         # Apply the custom sort function
    #         trending_sorted = sorted(trending_results.items(), key=custom_sort)
            
    #         def get_color_style(percentage):
    #             if percentage >= 100:
    #                 return "purple"
    #             elif percentage >= 75:
    #                 return "green-very-dark"
    #             elif percentage >= 50:
    #                 return "green-dark"
    #             elif percentage >= 25:
    #                 return "green"
    #             elif percentage >= 10:
    #                 return "green-light"
    #             elif percentage >= 5:
    #                 return "green-very-light"
    #             elif percentage > 0:
    #                 return "orange"
    #             elif percentage == 0:
    #                 return "orange-light"
    #             elif percentage <= -50:
    #                 return "red-very-dark"
    #             elif percentage <= -25:
    #                 return "red-dark"
    #             elif percentage <= -10:
    #                 return "red"
    #             elif percentage <= -5:
    #                 return "red-light"
    #             elif percentage < 0:
    #                 return "red-very-light"
    #             else:
    #                 return ""
            
    #         current_datetime = dt.datetime.now()
    #         formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
    #         # Start of HTML content
    #         html_content = f"""
    #         <!DOCTYPE html>
    #         <html>
    #         <head>
    #             <title>CryptoTurtle.Pro</title>
    #             <meta charset="utf-8">
                
    #             <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    #             <meta http-equiv="Pragma" content="no-cache" />
    #             <meta http-equiv="Expires" content="0" />
                
    #             <link rel="icon" type="image/png" href="/static/favicon.png">
    #             <link rel="stylesheet" type="text/css" href="/static/css/crypto_turtle_styles.css">
    #         </head>
    #         <body>
    #         """
            
    #         html_content += """
    #             <div class="container">
    #             <h1>CryptoTurtle.Pro</h1>
    #         """
    #         html_content += f"""<p class=\"small\">
    #         <strong>Last updated:</strong> {formatted_datetime}.
    #         <br />Monitors all active symbols returned by the Coinbase API and compares the previous hour's high to the high X hours earlier.
    #         </p>"""           
            
    #         html_content += """
    #         <table>
    #             <thead>
    #                 <tr>
    #                     <th class="table-align-left">Symbol</th>
    #                     <th class=\"table-align-right\">2 Hrs</th>
    #                     <th class=\"table-align-right\">4 Hrs</th>
    #                     <th class=\"table-align-right\">12 Hrs</th>
    #                     <th class=\"table-align-right\">24 Hrs</th>
    #                     <th class=\"table-align-right\">48 Hrs</th>
    #                     <th class=\"table-align-right\">72 Hrs</th>
    #                     <th class=\"table-align-right\">120 Hrs</th>
    #                     <th class=\"table-align-right\">TV</th>
    #                     <th class=\"table-align-right\">CB</th>
    #                 </tr>
    #             </thead>
    #             <tbody>
    #         """
            
    #         for symbol, data in trending_sorted:
    #             tradingview_url = f"https://www.tradingview.com/chart/?symbol={symbol.replace('-', '')}"
    #             coinbase_url = f"https://www.coinbase.com/advanced-trade/spot/{symbol}"
                
    #             html_content += f"<tr><td class=\"table-align-left\">{symbol}</td>"
                
    #             for period in period_order:
    #                 if period in data:
    #                     period_data = data[period]
    #                     increase = period_data.get('increase', '-/-')  # Use '-/-' as default if no increase
    #                     latest_high = period_data.get('latest_high', '-/-')
    #                     previous_high_key = previous_high_keys_mapping[period]  # Derive the key for previous high
    #                     previous_high = period_data.get(previous_high_key, '-/-')
                        
    #                     if latest_high is None or previous_high is None:
    #                         html_content += f"<td class=\"table-right-align orange-light\">-/-</td>"
    #                     else:
    #                         result_1 = ((latest_high - previous_high)/previous_high)*100
    #                         print(f"Result 1: {result_1:.2f}%")
    #                         html_content += f"<td class=\"table-align-right {get_color_style(result_1)}\">{result_1:.2f}%</td>"
                            
                            
    #                         # style = get_color_style(increase) if increase != '-/-' else ""
    #                         # increase_str = f'<span style="{style}">{increase}%</span>' if increase != '-/-' else "-/-"
    #                         # latest_high_str = f"Latest: {latest_high:.10f}" if latest_high != '-/-' else "Latest: -/-"
    #                         # previous_high_str = f"Previous: {previous_high:.10f}" if previous_high != '-/-' else "Previous: -/-"                            
    #                         # html_content += f"""
    #                         #     <td class=\"table-align-right {get_color_style(result_1)}\">
    #                         #         {result_1:.2f}%<br>
    #                         #         {latest_high_str}<br>
    #                         #         {previous_high_str}
    #                         #     </td>
    #                         #     """
                            
    #             html_content += f"<td class=\"table-align-right\"><a href='{tradingview_url}' target='_blank'>TV</a></td>"
    #             html_content += f"<td class=\"table-align-right\"><a href='{coinbase_url}' target='_blank'>CB</a></td>"
    #             html_content += "</tr>"

    #         html_content += """
    #             </tbody>
    #         </table>
    #         """
            
    #         html_content += "</div>" # Close container div    
    #         html_content += """
    #             </body>
    #             </html>
    #         """
            
    #         with open("../crypto_turtle_app/templates/crypto_turtle_pumping_percentages.html", "w") as file:
    #             file.write(html_content)
    #         log.log_message("HOURLY PERCENTAGES HTML report generated successfully.")

    # except Exception as e:
    #     print(f"Error in generating report: {e}")