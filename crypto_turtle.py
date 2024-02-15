import crypto_turtle_app as ct
import flask as fk

@ct.app.route('/')
def hello_world():
    # return "Hello, World!"
    return fk.render_template('home.html')

@ct.app.route('/btc-usd')
def btc_usd():
    return "BTC-USD"

@ct.app.route('/eth-usd')
def eth_usd():
    return "ETH-USD"

# if os.getenv('FLASK_ENV') == 'development':
#     ct.app.config.from_object('config.DevelopmentConfig')
# else:
#     ct.app.config.from_object('config.ProductionConfig')

if __name__ == '__main__':
    ct.app.run(debug=True)