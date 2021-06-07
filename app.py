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
    user = database.get_user()
    survey_permissions = database.SurveyPermission.query.filter_by(UserId=user.id).all()
    result = []
    for sp in survey_permissions:
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'type': 'survey',
            'endsOn':survey.EndsOn.timestamp() if survey.EndsOn is not None  else None,
            'startedOn': survey.StartedOn.timestamp() if survey.StartedOn is not None  else None,
            'id':            survey.id,
            'name':          survey.Name,
            'ankieterId':    survey.AnkieterId,
            'isActive':      survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg,
            'userId':        sp.UserId
        })
    report_permissions = database.ReportPermission.query.filter_by(UserId=user.id).all()
    for rp in report_permissions:
        report = database.Report.query.filter_by(id=rp.ReportId).first()
        survey = database.get_report_survey(report.id)
        result.append({
            'type': 'report',
            'id':              report.id,
            'name':            report.Name,
            'connectedSurvey': {"id":report.SurveyId, "name":survey.Name },
            'backgroundImg':   report.BackgroundImg,
            'userId':          rp.UserId
        })
    return {"objects": result}


# ścieżki na następny przyrost
#@app.route('/survey/<int:survey_id>', methods=['GET'])
#@app.route('/survey/<int:survey_id>', methods=['POST'])
#@app.route('/report/new', methods=['POST'])

@app.route('/data/new', methods=['POST'])
def upload_results():
    if not request.files['file']:
        return error.API("could not create survey file").as_dict()

    file = request.files['file']
    name, ext = file.filename.rsplit('.', 1)

    if 'name' in request.form:
        name = request.form['name']
    if ext.lower() != 'csv':
        return error.API("expected a CSV file").as_dict()

    survey = database.create_survey(database.get_user(), name)

    file.save(os.path.join(ABSOLUTE_DIR_PATH, "raw/", f"{survey.id}.csv"))

    database.csv_to_db(survey.id)
    conn = database.open_survey(survey.id)
    survey.QuestionCount = len(database.get_columns(conn))
    conn.close()

    return {
        "id": survey.id,
        "name": name
    }


@app.route('/report/new', methods=['POST'])
def create_report():
    # można pomyśleć o maksymalnej, dużej liczbie raportów dla każdego użytkownika
    # ze względu na bezpieczeństwo.
    try:
        grammar.check(grammar.REQUEST_CREATE_SURVEY, request.json)

        report = request.json
        user = database.get_user()
        report = database.create_report(user.id, report["surveyId"], report["title"])
        with popen(f'report/{report.id}.json', 'w') as file:
            json.dump(report, file)
    except error.API as err:
        return err.add_details('could not create report').as_dict()
    return {"reportId": report.id}


@app.route('/report/<int:report_id>/copy', methods=['GET'])
def copy_report(report_id):
    report = get_report(report_id)
    if 'error' in report:
        return report
    try:
        user = database.get_user()
        survey = database.get_report_survey(report_id)
        report = database.create_report(user.id, survey.id, report["title"])
        with popen(f'report/{report.id}.json', 'w') as file:
            json.dump(report, file)
    except error.API as err:
        return err.add_details('could not copy the report').as_dict()
    return {"reportId": report.id}


@app.route('/report/<int:report_id>', methods=['POST'])
def set_report(report_id):
    user_perm = database.ReportPermission.query.filter_by(ReportId=report_id,UserId=database.get_user().id).first().Type
    if user_perm not in ['o', 'w']:
        return error.API("You have no permission to edit this report.")
    with popen(f'report/{report_id}.json', 'w') as file:
        json.dump(request.json, file)
    return {"reportId": report_id}


@app.route('/report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    with popen(f'report/{report_id}.json', 'r') as file:
        data = json.load(file)
    return data


@app.route('/survey/<int:survey_id>', methods=['DELETE'])
def delete_survey(survey_id):
    user_perm = database.SurveyPermission.query.filter_by(SurveyId=survey_id,UserId=database.get_user().id).first().Type
    if user_perm != 'o':
        return error.API("You have no permission to delete this survey.")
    return database.delete_survey(survey_id)


@app.route('/report/<int:report_id>', methods=['DELETE'])
def delete_report(report_id):
    user_perm = database.ReportPermission.query.filter_by(ReportId=report_id,UserId=database.get_user().id).first().Type
    if user_perm != 'o':
        return error.API("You have no permission to delete this report")
    return database.delete_report(report_id)


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
        survey = database.get_report_survey(report_id)
    except error.API as err:
        return err.add_details('could not find the source survey').as_dict()
    return {"surveyId": survey.id}


@app.route('/data/<int:survey_id>/types', methods=['GET'])
def get_data_types(survey_id):
    db = database.Survey.query.filter_by(id=survey_id).first()
    if db:
        conn = database.open_survey(survey_id)
        types = database.get_types(conn)
        conn.close()
    return types


@app.route('/data/<int:survey_id>/questions', methods=['GET'])
def get_questions(survey_id):
    db = database.Survey.query.filter_by(id=survey_id).first()
    if db:
        conn = database.open_survey(survey_id)
        questions = database.get_columns(conn)
        conn.close()
    return {'questions': questions}


@app.route('/report/<int:report_id>/rename', methods=['POST'])
def rename_report(report_id):
    try:
        result = database.rename_report(report_id, request.json)
    except error.API as err:
        result = err.as_dict()
    return result


@app.route('/survey/<int:survey_id>/rename', methods=['POST'])
def rename_survey(survey_id):
    try:
        result = database.rename_survey(survey_id, request.json)
    except error.API as err:
        result = err.as_dict()
    return result


@app.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return '''<p>Witaj {}</p></br><a href="{}">Wyloguj</a>'''.format('123456789', url_for('logout'))
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


@app.route("/user",  methods=['GET'])
def user():
    try:
        user = database.get_user()
        return {"id":user.id, "logged":True, "username":session['username']}
    except:
        return {"logged":False}


if __name__ == '__main__':
    app.run()
