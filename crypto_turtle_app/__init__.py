import flask as fk
import dotenv as de

de.load_dotenv('.flaskenv')
app = fk.Flask(__name__)

from .routes import init_routes
init_routes(app)