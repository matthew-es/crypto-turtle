import flask as fk
import dotenv as de
import os as os

de.load_dotenv('.flaskenv')
app = fk.Flask(__name__)

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
def hello_world():
    # return "Hello, World!"
    return fk.render_template('home.html')

@app.route('/btc-usd')
def btc_usd():
    return "BTC-USD"

@app.route('/eth-usd')
def eth_usd():
    return "ETH-USD"

# from flask import Flask
# import config as cf

# # app.config.from_object(cf.Config)

# app.debug = True