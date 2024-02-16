import flask as fk
import dotenv as de
import os 

print("Current Working Directory:", os.getcwd())
print("Directory Contents:", os.listdir('../'))  # Adjust path as necessary

de.load_dotenv('.flaskenv')
app = fk.Flask(__name__)

from .routes import init_routes
init_routes(app)