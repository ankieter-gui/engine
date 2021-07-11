# -- config section -----------------------------------------------------------
# URL of the CAS service used by the institution
CAS_URL='https://cas.amu.edu.pl/cas/'

# must be 1, 2 or 3; depending on the version of CAS used by the institution
CAS_VERSION=2

# app address can be set here; do not append it with a '/'
APP_URL='https://localhost'

# app port can be set here
APP_PORT=5000

# seconds between subsequent daemon wakeups (for eg. the gatherer daemon)
DINTERVAL=5*60

# guest account username
GUEST_NAME='Goście'

# default admin permission for all surveys and reports
ADMIN_DEFAULT_PERMISSION = 'o'

# must be True or False; change to True to unlock the possibility to LOG IN
# WITHOUT PASSWORD and detailed logs during app runtime
DEBUG=True

#if True, the app is not being hosted on 0.0.0.0
LOCALHOST=True

# the code below applies the given configurarion and the user is not ----------
# encouraged to change it in any way ------------------------------------------

from flask import Flask, redirect, url_for, request, session, g
from flask_admin.contrib.sqla import ModelView
from flask_admin import Admin
from flask_cors import CORS
from cas import CASClient
from os.path import dirname, realpath
from os import urandom, path, chdir

ABSOLUTE_DIR_PATH = dirname(realpath(__file__))
chdir(ABSOLUTE_DIR_PATH)
popen = lambda p, mode: open(pabs(p), mode)
pabs = lambda p: path.join(ABSOLUTE_DIR_PATH, p)

SALT_LENGTH=22

app = Flask(__name__)
app.config.from_mapping(
    SECRET_KEY=urandom(SALT_LENGTH) if not DEBUG else 'sTzMzxFX8BcJt3wuvNvDeQ',
    FLASK_ADMIN_SWATCH='cerulean',
    SQLALCHEMY_DATABASE_URI='sqlite:///master.db',
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    DEBUG=DEBUG
)

# under class definitions in database.py ADMIN is also later attached to the db
ADMIN = Admin(app, name='Ankieter+', template_mode='bootstrap3')

CAS_CLIENT = CASClient(
    version=CAS_VERSION,
    service_url=f'{APP_URL}:{APP_PORT}/api/login',
    server_url=CAS_URL
)

cors = CORS(app, resources={r"*": {"origins": f"http://{APP_URL}:4200"}})
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
