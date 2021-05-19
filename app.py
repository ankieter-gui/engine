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
import database
from errors import *

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

db = SQLAlchemy(app)
ADMIN = Admin(app, name='Ankieter', template_mode='bootstrap3')
ADMIN.add_view(ModelView(User, db.session))

CAS_CLIENT = CASClient(
    version=2,
    service_url='http://localhost:5000/login',
    server_url='https://cas.amu.edu.pl/cas/'
)


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    user = User.query.filter_by(CasLogin=session['username']).first()
    survey_permissions = SurveyPermission.query.filter_by(UserId=user.id).all()
    result = []
    for sp in survey_permissions:
        survey = Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'name': "placeholder name",
            'type': "survey",
            'id': survey.AnkieterId,
            'userId': sp.UserId,
            'startedOn': survey.StartedOn.timestamp(),
            'endsOn': survey.EndsOn.timestamp(),
            'isActive': survey.IsActive,
            'questionCount': survey.QuestionCount
        })
    return {"objects": result}


# ścieżki na następny przyrost
#@app.route('/survey/<int:survey_id>', methods=['GET'])
#@app.route('/survey/<int:survey_id>', methods=['POST'])
#@app.route('/report/new', methods=['POST'])


@app.route('/report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    file = open(f'/report/{report_id}.json', mode='r')
    data = file.read()
    file.close()
    return data


@app.route('/report/<int:report_id>', methods=['POST'])
def set_report(report_id):
    file = open(f'/report/{report_id}.json', mode='w')
    file.write(request.json)
    file.close()


@app.route('/report/new', methods=['POST'])
def create_report():
    # można pomyśleć o maksymalnej, dużej liczbie raportów dla każdego użytkownika
    # ze względu na bezpieczeństwo.
    try:
        if not request.json:
            raise APIError('empty json request')
        if 'userId' not in request.json or not isinstance(request.json['userId'], int):
            raise APIError('wrong user id')
        if 'surveyId' not in request.json or not isinstance(request.json['surveyId'], int):
            raise APIError('wrong survey id')
        if 'title' not in request.json or not isinstance(request.json['title'], str):
            raise APIError('wrong title type')

        report_id = database.create_report(json.userId, json.surveyId, json.title)

        file = open(f'/report/{report_id}.json', mode='w')
        file.write(request.json)
        file.close()
    except APIError as err:
        return err.add_details('could not create report').as_dict()
    return report_id


@app.route('/data/<int:survey_id>', methods=['POST'])
def get_data(survey_id):
    try:
        conn = database.open_survey(survey_id)
        result = table.create(request.json, conn)
    except APIError as err:
        result = err.as_dict()
    conn.close()
    return result


@app.route('/data/<int:survey_id>/types', methods=['GET'])
def data_types(survey_id):
    conn = database.open_survey(survey_id)
    types = database.get_types(conn)
    conn.close()
    return types


@app.route('/data/<int:survey_id>/questions', methods=['GET'])
def get_questions(survey_id):
    conn = database.open_survey(survey_id)
    questions = database.get_columns(conn)
    conn.close()
    return {'questions': questions}


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
