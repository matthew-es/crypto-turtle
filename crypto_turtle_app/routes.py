import os as os
import flask as fk
import subprocess as sb
import sys as sys
import psycopg as pg
import threading as th

def init_routes(app):
    @app.before_request
    def enforce_www_and_https():    
        url = fk.request.url
        redirect_url = None
            
        if os.getenv('FLASK_ENV') == 'development':
            print("Skipping redirection for development environment")
            return
        
        if url.startswith('http://www.'):
            redirect_url = url.replace('http://', 'https://', 1)
        elif url.startswith('http://'):
            redirect_url = url.replace('http://', 'https://www.', 1)
        elif url.startswith('https://www.'):
            redirect_url = url
        elif url.startswith('https://'):
            redirect_url = url.replace('https://', 'https://www.', 1)
        else:
            redirect_url = 'https://www.' + url

            return fk.redirect(redirect_url, code=301)
    
    @app.route('/')
    def home():
        # return "Hello, World!"
        # return fk.render_template('home.html')
        # return fk.render_template('crypto_turtle_pumping_percentages.html')
        response = fk.make_response(fk.render_template('crypto_turtle_pumping_percentages.html'))
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response

    log_file_path = 'crypto_turtle_program/crypto_turtle_log.txt'  # Update this path
    # base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # log_file_path = os.path.join(base_dir, 'crypto_turtle_program/crypto_turtle_log.txt')
        
    # View logs for debugging
    @app.route('/logs')
    def show_logs():
        try:
            with open(log_file_path, 'r') as file:
                content = file.read()
            return fk.Response(content, mimetype='text/plain')
        except FileNotFoundError:
            return "Log file not found.", 404

    def log_to_file(message):
        """Utility function to log a message to a file."""
        with open(log_file_path, 'a') as file:
            file.write(message + '\n')

    @app.after_request
    def log_response(response):
        """Logs details of the HTTP response after every request."""
        log_message = f"Request: {fk.request.method} {fk.request.path} | Response: {response.status}"
        log_to_file(log_message)
        return response
    

    @app.route('/btc-usd')
    def btc_usd():
        return "BTC-USD"

    @app.route('/eth-usd')
    def eth_usd():
        return "ETH-USD"
    
    # Connect to postgres database with psycopg
    def db_connect_open():         
        try:
            connection = pg.connect(
                host = os.getenv("DATABASE_HOST"),
                dbname = os.getenv("DATABASE_NAME"),
                user = os.getenv("DATABASE_USER"),
                password = os.getenv("DATABASE_PASSWORD"),
                port = os.getenv("DATABASE_PORT")
            )
            return connection
        
        except pg.OperationalError as e:
            print("Unable to connect to the database from db_connect:", e)

    @app.route('/db-test')
    def db_test():
        conn = db_connect_open()
        if conn:
            cursor = conn.cursor()
            # Query to fetch table names from the public schema
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables = cursor.fetchall()
            table_names = ', '.join([table[0] for table in tables])
            cursor.close()
            conn.close()
            if tables:
                message = f"Successfully connected to the database. Tables: {table_names}."
            else:
                message = "Successfully connected to the database. No tables found."
        else:
            message = "Failed to connect to the database."
        return fk.render_template_string('<h1>{{ message }}</h1>', message=message)
    
    # Main trigger update route to ping from somewhere else
    def run_crypto_turtle():
        """Function to run the crypto_turtle.py script in a separate thread."""
        try:
            result = sb.run(
                [sys.executable, 'crypto_turtle.py'],
                cwd='crypto_turtle_program',  # Adjust as necessary
                check=True,
                stdout=sb.PIPE,
                stderr=sb.PIPE,
                text=True
            )
            print(f"Success: {result.stdout}")
        except sb.CalledProcessError as e:
            print(f"Failed to trigger the update process. {e}\nOutput: {e.stderr} {e.stdout}")

    @app.route('/trigger-update', methods=['POST'])
    def trigger_update():
        expected_token = os.getenv('UPDATE_TOKEN')
        token = fk.request.headers.get('Authorization')
        
        if not token or token != expected_token:
            fk.abort(403)
        
        # Start the crypto_turtle.py script in a background thread
        thread = th.Thread(target=run_crypto_turtle)
        thread.start()
        
        # Immediately return a success response
        return fk.jsonify({'status': 'success', 'message': 'Process started.'}), 200
    
    
    # @app.route('/trigger-update', methods=['POST'])
    # def trigger_update():
    #     try:
    #         expected_token = os.getenv('UPDATE_TOKEN')
    #         token = fk.request.headers.get('Authorization')
            
    #         if not token or token != expected_token:
    #             fk.abort(403)
            
    #         sb.run(
    #             [sys.executable, 'crypto_turtle.py'],
    #             cwd='crypto_turtle_program',  # Adjust as necessary
    #             check=True,
    #             stdout=sb.PIPE,
    #             stderr=sb.PIPE,
    #             text=True
    #         )
    #         return "Success", 200
    #     except sb.CalledProcessError as e:
    #         return f"Failed to trigger the update process. {e}\nOutput: {e.stderr} {e.stdout}", 500