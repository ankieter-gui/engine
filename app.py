from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_jsonpify import jsonify
from cas import CASClient
from setup import *
from os import path
import sqlite3
import os
import table
import survey

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "http://localhost:4200"}})
@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', 'http://localhost:4200')
  response.headers.add('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  response.headers.add('Access-Control-Allow-Credentials', 'true')
  return response
app.config.from_mapping(
    SECRET_KEY='sTzMzxFX8BcJt3wuvNvDeQ',
    FLASK_ADMIN_SWATCH='cerulean',
    SQLALCHEMY_DATABASE_URI='sqlite:///master.db',
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    DEBUG=True
)

DATABASE = SQLAlchemy(app)
ADMIN = Admin(app, name='Ankieter', template_mode='bootstrap3')
ADMIN.add_view(ModelView(User, DATABASE.session))

CAS_CLIENT = CASClient(
    version=2,
    service_url='http://localhost:5000/login',
    server_url='https://cas.amu.edu.pl/cas/'
)


def open_database_file(survey_id:int):
    script_absolute_directory_path = os.path.dirname(os.path.realpath(__file__))
    db_absolute_path = path.join(script_absolute_directory_path, "data", str(survey_id) + ".db")
    return sqlite3.connect(db_absolute_path)


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    def get_meta(survey_id):
        conn = open_database_file(survey_id)
        cur = conn.cursor()
        cur.execute("select * from meta")
        data = cur.fetchall()
        conn.close()
        return data
    user = User.query.filter_by(CasLogin=session['username']).first()
    survey_permissions = SurveyPermission.query.filter_by(UserId=user.id).all()
    result = {}
    for sp in survey_permissions:
        survey = Survey.query.filter_by(id=sp.SurveyId).first()
        meta = get_meta(survey.AnkieterId)
        print(meta)
        #na szybko - do poprawy
        result[survey.AnkieterId] = {
            'surveyId': survey.AnkieterId,
            'userId': sp.UserId,
            'startedOn': meta[0][0],
            'endsOn': meta[0][1],
            'isActive': meta[0][2],
            'questionCount': meta[0][3]
        }
    print(result)
    return result


@app.route('/data/<int:survey_id>', methods=['POST'])
def get_data(survey_id):
    if not request.json:
        # TODO: return json with errors
        return
    json_request = request.json

    result = table.create(json_request, conn)
    conn.close()
    return result


@app.route('/data/<int:survey_id>/types', methods=['GET'])
def data_types(survey_id):
    conn = open_database_file(survey_id)
    types = survey.get_data_types(conn)
    conn.close()
    return types


@app.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return '''<p>Witaj {}</p></br><a href="{}">Wyloguj</a>'''.format(username, url_for('logout'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    ticket = request.args.get('ticket')
    if not ticket:
        return redirect(CAS_CLIENT.get_login_url())

    user, attributes, pgtiou = CAS_CLIENT.verify_ticket(ticket)

    if not user:
        return '<p>Failed to verify</p>'

    session['username'] = user
    return redirect(url_for('index'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(CAS_CLIENT.get_logout_url())


if __name__ == '__main__':
    app.run()
