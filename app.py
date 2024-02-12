from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return "Hello, World, we're here! Let's go again!"

@app.route('/btc-usd')
def btc_usd():
    return "BTC-USD"

if __name__ == '__main__':
    app.run(debug=True)