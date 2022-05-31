from flask import Flask, redirect, url_for, request, session, g
from flask_admin.contrib.sqla import ModelView
from flask_admin import Admin
from flask_cors import CORS
from cas import CASClient
from os.path import dirname, realpath
from os import urandom, path, chdir

try:
    # Import the config
    import config
    keys = dir(config)

    # Assign required values
    CAS_URL = config.CAS_URL
    CAS_VERSION = config.CAS_VERSION
    APP_URL = config.APP_URL
    APP_PORT = config.APP_PORT

    # TODO: validate values

    # Assign optional values
    if 'SSL_CONTEXT' in keys:
        SSL_CONTEXT = config.SSL_CONTEXT
    else:
        SSL_CONTEXT = 'adhoc'

    if 'GUEST_NAME' in keys:
        GUEST_NAME = config.GUEST_NAME
    else:
        GUEST_NAME = "Goście"

    if 'ADMIN_DEFAULT_PERMISSION' in keys:
        ADMIN_DEFAULT_PERMISSION = config.ADMIN_DEFAULT_PERMISSION
    else:
        ADMIN_DEFAULT_PERMISSION = 'o'

    if 'DAEMONS_INTERVAL' in keys:
        DAEMONS_INTERVAL = config.DAEMONS_INTERVAL
    else:
        DAEMONS_INTERVAL = 5*60

    if 'LOCALHOST' in keys:
        LOCALHOST = config.LOCALHOST
    else:
        LOCALHOST = False

    if 'DEBUG' in keys:
        DEBUG = config.DEBUG
    else:
        DEBUG = False
except:
    print('config.py incorrect or not present: running default debug config')
    CAS_URL= ''
    CAS_VERSION = 2
    APP_URL = 'https://localhost'
    APP_PORT = 5000
    SSL_CONTEXT = 'adhoc'
    GUEST_NAME = 'Goście'
    ADMIN_DEFAULT_PERMISSION = 'o'
    DAEMONS_INTERVAL = 5*60
    LOCALHOST = True
    DEBUG = True

SALT_LENGTH=22

ABSOLUTE_DIR_PATH = dirname(realpath(__file__))
chdir(ABSOLUTE_DIR_PATH)
popen = lambda p, mode: open(pabs(p), mode)
pabs = lambda p: path.join(ABSOLUTE_DIR_PATH, p)

app = Flask(__name__)
app.config.from_mapping(
    SECRET_KEY=urandom(SALT_LENGTH) if not DEBUG else 'sTzMzxFX8BcJt3wuvNvDeQ',
    FLASK_ADMIN_SWATCH='cerulean',
    SQLALCHEMY_DATABASE_URI='sqlite:///master.db',
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    DEBUG=DEBUG
)
log = app.logger.info

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
