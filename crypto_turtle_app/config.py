import dotenv as dot
import os as os

dot.load_dotenv()

class Config(object):
    SECRET_KEY = 'your_secret_key'
    database_name = os.getenv("DATABASE_NAME")
    database_user = os.getenv("DATABASE_USER")
    database_password = os.getenv("DATABASE_PASSWORD")
    database_host = os.getenv("DATABASE_HOST")
    database_port = os.getenv("DATABASE_PORT")
    
class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False    
    
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}

current_config = config[os.getenv('FLASK_ENV', 'development')]()