import flask as fk
import dotenv as de
import os as os

de.load_dotenv('.flaskenv')
app = fk.Flask(__name__)

print(f"FLASK_ENV 1: {os.getenv('FLASK_ENV')}")

@app.before_request
def enforce_www_and_https():    
    url = fk.request.url
    redirect_url = None
    
    print(f"FLASK_ENV 2: {os.getenv('FLASK_ENV')}")
    
    if os.getenv('FLASK_ENV') == 'development':
        print("Skipping redirection for development environment")
        return
        
    # Check if the URL does not start with https://www.
    if url.startswith('https://www.'):
        return url
    # Redirect http://www.example.com to https://www.example.com

    if url.startswith('http://www.'):
        redirect_url = url.replace('http://www.', 'https://www.', 1)
    # Redirect http://example.com to https://www.example.com
    elif url.startswith('http://'):
        redirect_url = url.replace('http://', 'https://www.', 1)
    # Redirect https://example.com to https://www.example.com
    elif url.startswith('https://') and not url.startswith('https://www.'):
        redirect_url = url.replace('https://', 'https://www.', 1)
    else:
        redirect_url = "https://www" + url
    
    # # Check if the redirect URL is the same as the current URL
    # if redirect_url == url:
    #     return
    
    # # Check if the redirect URL is already redirected
    # if redirect_url.startswith('https://www.') and redirect_url[12:] == url[8:]:
    #     return
    
    

    code = 301
    return fk.redirect(redirect_url, code=code)


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