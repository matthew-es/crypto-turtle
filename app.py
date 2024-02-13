from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template('home.html')

@app.route('/btc-usd')
def btc_usd():
    return "BTC-USD"

if __name__ == '__main__':
    app.run(debug=True)