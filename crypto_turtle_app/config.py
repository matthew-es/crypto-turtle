import dotenv as dot
import os as os

dot.load_dotenv()

class Config(object):
    SECRET_KEY = 'your_secret_key'

class DevelopmentConfig(Config):
    DEBUG = True
    database_name = os.getenv("LOCAL_DATABASE_NAME")
    database_host = os.getenv("LOCAL_DATABASE_HOST")
    database_user = os.getenv("LOCAL_DATABASE_USER")
    database_password = os.getenv("LOCAL_DATABASE_PASSWORD")
    database_port = os.getenv("LOCAL_DATABASE_PORT")

class ProductionConfig(Config):
    DEBUG = False
    database_name = os.getenv("PRODUCTION_DATABASE_NAME")
    database_user = os.getenv("PRODUCTION_DATABASE_USER")
    database_password = os.getenv("PRODUCTION_DATABASE_PASSWORD")
    database_host = os.getenv("PRODUCTION_DATABASE_HOST")
    database_port = os.getenv("PRODUCTION_DATABASE_PORT")
    
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}

current_config = config[os.getenv('FLASK_ENV', 'development')]()