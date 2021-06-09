from flask import send_from_directory, redirect, url_for, request, session, g
from config import *
import json
import sqlite3
import os
import threading
import database
import grammar
import daemon
import table
import error


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    user = database.get_user()
    survey_permissions = database.SurveyPermission.query.filter_by(UserId=user.id).all()
    result = []
    for sp in survey_permissions:
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'type':          'survey',
            'endsOn':        survey.EndsOn.timestamp() if survey.EndsOn is not None else None,
            'startedOn':     survey.StartedOn.timestamp() if survey.StartedOn is not None else None,
            'id':            survey.id,
            'name':          survey.Name,
            'ankieterId':    survey.AnkieterId,
            'isActive':      survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg,
            'userId':        sp.UserId,
            'answersCount':  database.get_answers_count(survey)
        })
    report_permissions = database.ReportPermission.query.filter_by(UserId=user.id).all()
    for rp in report_permissions:
        report = database.Report.query.filter_by(id=rp.ReportId).first()
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'type': 'report',
            'id':              report.id,
            'name':            report.Name,
            'connectedSurvey': {"id": report.SurveyId, "name":survey.Name },
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

    database.csv_to_db(survey, f"{survey.id}.csv")
    conn = database.open_survey(survey)
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

        data = request.json
        user = database.get_user()
        # czy użytkownik widzi tę ankietę?
        report = database.create_report(user, data["surveyId"], data["title"])
        with open(f'report/{report.id}.json', 'w') as file:
            json.dump(data, file)
    except error.API as err:
        return err.add_details('could not create report').as_dict()
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>/copy', methods=['GET'])
def copy_report(report_id):
    data = get_report(report_id)
    if 'error' in data:
        return data
    try:
        user = database.get_user()
        report = database.get_report(report_id)
        survey = database.get_report_survey(report)
        report = database.create_report(user, survey, report["title"])
        with open(f'report/{report.id}.json', 'w') as file:
            json.dump(data, file)
    except error.API as err:
        return err.add_details('could not copy the report').as_dict()
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>', methods=['POST'])
def set_report(report_id):
    report = database.get_report(report_id)
    perm = database.get_report_permission(report, database.get_user())
    if perm not in ['o', 'w']:
        return error.API("you have no permission to edit this report")
    with open(f'report/{report_id}.json', 'w') as file:
        json.dump(request.json, file)
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    with open(f'report/{report_id}.json', 'r') as file:
        data = json.load(file)
    return data


@app.route('/survey/<int:survey_id>', methods=['DELETE'])
def delete_survey(survey_id):
    try:
        survey = database.get_survey(survey_id)
        perm = database.get_survey_permission(survey, database.get_user())
        if perm != 'o':
            raise error.API("you have no permission to delete this survey")
        database.delete_survey(survey)
    except error.API as err:
        return err.add_details('could not delete survey').as_dict()
    return {
        'message': 'report has been deleted',
        'surveyId': survey_id
    }


@app.route('/report/<int:report_id>', methods=['DELETE'])
def delete_report(report_id):
    try:
        report = database.get_report(report_id)
        perm = database.get_report_permission(report, database.get_user())
        if perm != 'o':
            raise error.API("you have no permission to delete this report")
        database.delete_report(report)
    except error.API as err:
        return err.add_details('could not delete report').as_dict()
    return {
        'message': 'report has been deleted',
        'reportId': report_id
    }


@app.route('/data/<int:survey_id>', methods=['POST'])
def get_data(survey_id):
    try:
        survey = database.get_survey(survey_id)
        conn = database.open_survey(survey)
        result = table.create(request.json, conn)
    except error.API as err:
        result = err.as_dict()
    conn.close()
    return result


@app.route('/report/<int:report_id>/survey', methods=['GET'])
def get_report_survey(report_id):
    try:
        report = database.get_report(report_id)
        survey = database.get_report_survey(report)
    except error.API as err:
        return err.add_details('could not find the source survey').as_dict()
    return {
        "surveyId": survey.id
    }


@app.route('/data/<int:survey_id>/types', methods=['GET'])
def get_data_types(survey_id):
    try:
        survey = database.get_survey(survey_id)
        conn = database.open_survey(survey)
        types = database.get_types(conn)
        conn.close()
    except error.API as err:
        return err.add_details('could not get question types').as_dict()
    return types


@app.route('/data/<int:survey_id>/questions', methods=['GET'])
def get_questions(survey_id):
    try:
        survey = database.get_survey(survey_id)
        conn = database.open_survey(survey)
        questions = database.get_columns(conn)
        conn.close()
    except error.API as err:
        return err.add_details('could not get question order').as_dict()
    return {
        'questions': questions
    }


@app.route('/report/<int:report_id>/rename', methods=['POST'])
def rename_report(report_id):
    try:
        # uprawnienia
        if 'title' not in request:
            raise error.API('no parameter title')
        report = database.get_report(report_id)
        result.Name = request.json['title']
        db.session.commit()
    except error.API as err:
        result = err.add_info('could not rename report').as_dict()
    return {
        'message': 'report name has been changed',
        'reportId': report.id,
        'title': request.json['title']
    }


@app.route('/survey/<int:survey_id>/rename', methods=['POST'])
def rename_survey(survey_id):
    try:
        # uprawnienia
        if 'title' not in request:
            raise error.API('no parameter title')
        survey = database.get_survey(survey_id)
        survey.Name = request.json['title']
        db.session.commit()
    except error.API as err:
        result = err.add_info('could not rename survey').as_dict()
    return {
        'message': 'survey name has been changed',
        'surveyId': survey.id,
        'title': request.json['title']
    }


@app.route('/survey/<int:survey_id>/share', methods=['POST'])
def share_survey(survey_id):
    json = request.json
    survey = database.get_survey(survey_id)
    perm = database.get_survey_permission(survey, database.get_user())
    if perm != "o":
        return error.API("you must be the owner to share this survey")
    for CasLogin in json.values():
        user = database.get_user(CasLogin)
        database.set_survey_permission(survey, user, 'r')
    return {
        "message": "permissions added"
    }


@app.route('/report/<int:report_id>/share', methods=['POST'])
def share_report(report_id):
    json = request.json
    report = database.get_survey(report_id)
    perm = database.get_report_permission(report, database.get_user())
    if perm != "o":
        return error.API("you must be the owner to share this report")
    for CasLogin in json.values():
        user = database.get_user(CasLogin)
        database.set_survey_permission(report, user, 'r')
    return {
        "message": "permissions added"
    }


# {'group': 'nazwa grupy'}
@app.route('/group/all', methods=['DELETE'])
def delete_group():
    try:
        if database.get_user().Role != 's':
            raise error.API('insufficient permissions')
        grammar.check(grammar.REQUEST_GROUP, request.json)
        database.delete_group(request.json['group'])
    except error.API as err:
        return err.add_details('failed deleting group').as_dict()
    return {
        'message': 'group deleted'
    }


@app.route('/group/all', methods=['GET', 'POST'])
def get_groups():
    try:
        if database.get_user().Role not in ['s', 'u']:
            raise error.API('insufficient permissions')

        result = {}
        for group in database.get_groups():
            result[group] = []
            for user in database.get_group_users(group):
                result[group].append(user.id)
    except error.API as err:
        return err.add_details('failed getting list of groups').as_dict()
    return result


# {'nazwa grupy': [user_id_1, user_id_2, ...], 'nazwa grupy': ...}
@app.route('/group/change', methods=['POST'])
def set_group():
    try:
        if database.get_user().Role != 's':
            raise error.API('insufficient permissions')
        for group, *ids in request.json:
            grammar.check([int], ids)
            for id in ids:
                user = database.get_user(id)
                database.set_user_group(user.id, group)
    except error.API as err:
        return err.add_details('failed adding users to groups').as_dict()
    return {
        'message': 'users added to groups'
    }


# {'nazwa grupy': [user_id_1, user_id_2, ...], 'nazwa grupy': ...}
@app.route('/group/change', methods=['DELETE'])
def unset_group():
    try:
        if database.get_user().Role != 's':
            raise error.API('insufficient permissions')
        for group, *ids in request.json:
            grammar.check([int], ids)
            for id in ids:
                user = database.get_user(id)
                database.unset_user_group(user.id, group)
    except error.API as err:
        return err.add_details('failed removing users from groups').as_dict()
    return {
        'message': 'users removed from groups'
    }


# {'group': 'nazwa grupy'}
@app.route('/group/users', methods=['POST'])
def get_group_users():
    try:
        if database.get_user().Role not in ['s', 'u']:
            raise error.API('insufficient permissions')
        grammar.check(grammar.REQUEST_GROUP, request.json)
        users = database.get_group_users(request.json['group'])
    except error.API as err:
        return err.add_details('failed finding group users').as_dict()
    return {
        request.json['group']: [user.id for user in users]
    }


@app.route('/user/<int:user_id>/group', methods=['GET', 'POST'])
def get_user_groups(user_id):
    try:
        if database.get_user().Role not in ['s', 'u']:
            raise error.API('insufficient permissions')

        result = {}
        user = database.get_user(user_id)
        for group in database.get_user_groups(user):
            result[group] = []
            for user in database.get_group_users(group):
                result[group].append(user.id)
    except error.API as err:
        return err.add_details('failed finding user groups').as_dict()
    return result


@app.route('/user/all', methods['get'])
def get_user_list():
    return get_users()


@app.route('/users', methods=['GET'])
def get_users():
    return database.get_users()


@app.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return redirect("http://localhost:4200")
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


@app.route('/user',  methods=['GET'])
def user():
    try:
        user = database.get_user()
        return {"id":user.id, "logged":True, "username":session['username']}
    except:
        return {"logged":False}


@app.route('/bkg/<path:path>', methods=['GET'])
def get_bkg(path):
    return send_from_directory('bkg', path)


if __name__ == '__main__':
    for d in daemon.LIST:
        threading.Thread(target=d, daemon=True).start()
    app.run()
