from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from cas import CASClient
from setup import *
import json
import sqlite3
import os
import table
import database
import grammar
import error

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


@app.route('/report/new', methods=['POST'])
def create_report():
    # można pomyśleć o maksymalnej, dużej liczbie raportów dla każdego użytkownika
    # ze względu na bezpieczeństwo.
    try:
        grammar.check(grammar.REQUEST_CREATE_SURVEY, request.json)

        report = request.json
        report_id = database.create_report(report["userId"], report["surveyId"], report["title"])

        file = open(f'report/{report_id}.json', 'w')
        json.dump(report, file)
        file.close()
    except error.API as err:
        return err.add_details('could not create report').as_dict()
    return {"reportId": report_id}


@app.route('/report/<int:report_id>', methods=['POST'])
def set_report(report_id):
    file = open(f'report/{report_id}.json', mode='w')
    json.dump(request.json, file)
    file.close()


@app.route('/report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    file = open(f'report/{report_id}.json', mode='r')
    data = json.load(file)
    file.close()
    return data


@app.route('/data/<int:survey_id>', methods=['POST'])
def get_data(survey_id):
    try:
        conn = database.open_survey(survey_id)
        result = table.create(request.json, conn)
    except error.API as err:
        result = err.as_dict()
    conn.close()
    return result


@app.route('/report/<int:report_id>/survey', methods=['GET'])
def get_report_survey(report_id):
    try:
        survey_id = database.get_report_survey(report_id)
    except error.API as err:
        return err.add_details('could not find the source survey').as_dict()
    return {"surveyId": survey_id}


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
