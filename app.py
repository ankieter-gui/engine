from flask import redirect, url_for, request, session, g
from config import *
import json
import sqlite3
import os
import table
import database
import grammar
import error


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    user = database.User.query.filter_by(CasLogin=session['username']).first()
    survey_permissions = database.SurveyPermission.query.filter_by(UserId=user.id).all()
    result = []
    for sp in survey_permissions:
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'name': survey.Name,
            'type': "survey",
            'id': survey.AnkieterId,
            'userId': sp.UserId,
            'startedOn': survey.StartedOn.timestamp(),
            'endsOn': survey.EndsOn.timestamp(),
            'isActive': survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg
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
        user = database.get_user(session['username'])
        report_id = database.create_report(user.id, report["surveyId"], report["title"])
        with popen(f'report/{report_id}.json', 'w') as file:
            json.dump(report, file)
    except error.API as err:
        return err.add_details('could not create report').as_dict()
    return {"reportId": report_id}


@app.route('/report/<int:report_id>', methods=['POST'])
def set_report(report_id):
    with popen(f'report/{report_id}.json', 'w') as file:
        json.dump(request.json, file)


@app.route('/report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    with popen(f'report/{report_id}.json', 'r') as file:
        data = json.load(file)
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
def get_data_types(survey_id):
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
