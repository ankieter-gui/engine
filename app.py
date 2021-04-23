from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_sqlalchemy import SQLAlchemy
from cas import CASClient
from setup import *
import sqlite3
import sys

app = Flask(__name__)

app.config.from_mapping(
    SECRET_KEY='sTzMzxFX8BcJt3wuvNvDeQ',
    FLASK_ADMIN_SWATCH='cerulean',
    SQLALCHEMY_DATABASE_URI='sqlite:///master.db',
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    DEBUG=True
)
DATABASE = SQLAlchemy(app)
admin = Admin(app, name='Ankieter', template_mode='bootstrap3')
admin.add_view(ModelView(User, DATABASE.session))
cas_url = 'https://cas.amu.edu.pl/cas'

cas_client = CASClient(
    version=2,
    service_url='http://localhost:5000/login',
    server_url='https://cas.amu.edu.pl/cas/'
)

# DATABASE = './master.db'

@app.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return '''<p>Witaj {}</p>'''.format(username)
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    ticket = request.args.get('ticket')
    if not ticket:
        return redirect(cas_client.get_login_url())

    user, attributes, pgtiou = cas_client.verify_ticket(ticket)

    if not user:
        return '<p>Failed to verify</p>'

    session['username'] = user
    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run()
