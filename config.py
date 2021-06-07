# -- config section -----------------------------------------------------------
# URL of the CAS service used by the institution
CAS_URL='https://cas.amu.edu.pl/cas/'

# must be 1, 2 or 3; depending on the version of CAS used by the institution
CAS_VERSION=2

# address and port can be changed here; do not append the name with a '/'
APP_URL='http://localhost:5000'

# must be True or False; change to True for detailed logs during app runtime
DEBUG=True

# the code below applies the given configurarion and the user is not ----------
# encouraged to change it in any way ------------------------------------------

from flask import Flask, redirect, url_for, request, session, g
from flask_admin.contrib.sqla import ModelView
from flask_admin import Admin
from flask_cors import CORS
from cas import CASClient
from os.path import dirname, realpath
from os import urandom, path

ABSOLUTE_DIR_PATH = dirname(realpath(__file__))
pabs = lambda p: path.join(ABSOLUTE_DIR_PATH, p)
popen = lambda p, mode: open(pabs(p), mode)

app = Flask(__name__)
app.config.from_mapping(
    SECRET_KEY=urandom(22), #'sTzMzxFX8BcJt3wuvNvDeQ',
    FLASK_ADMIN_SWATCH='cerulean',
    SQLALCHEMY_DATABASE_URI='sqlite:///master.db',
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    DEBUG=DEBUG
)

# under class definitions in database.py ADMIN is also later attached to the db
ADMIN = Admin(app, name='Ankieter+', template_mode='bootstrap3')

CAS_CLIENT = CASClient(
    version=CAS_VERSION,
    service_url=f'{APP_URL}/login',
    server_url=CAS_URL
)

cors = CORS(app, resources={r"*": {"origins": "http://localhost:4200"}})
@app.after_request
def after_request(response):
  response.headers.add(
    'Access-Control-Allow-Origin', 'http://localhost:4200')
  response.headers.add(
    'Access-Control-Allow-Headers',
    'Origin, X-Requested-With, Content-Type, Accept, Authorization')
  response.headers.add(
    'Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  response.headers.add(
    'Access-Control-Allow-Credentials', 'true')
  return response
