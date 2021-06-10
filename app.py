from functools import wraps
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

def on_errors(details):
    def on_error_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except error.API as err:
                return err.add_details(details).as_dict()
        return decorated_function
    return on_error_decorator


def for_roles(*roles):
    def for_roles_decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if database.get_user().Role not in roles:
                raise error.API('insufficient privileges')
            return f(*args, **kwargs)
        return decorated_function
    return for_roles_decorator


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    user = database.get_user()
    survey_permissions = database.SurveyPermission.query.filter_by(UserId=user.id).all()
    result = []
    for sp in survey_permissions:
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'type': 'survey',
            'endsOn': survey.EndsOn.timestamp() if survey.EndsOn is not None else None,
            'startedOn': survey.StartedOn.timestamp() if survey.StartedOn is not None else None,
            'id': survey.id,
            'name': survey.Name,
            'ankieterId': survey.AnkieterId,
            'isActive': survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg,
            'userId': sp.UserId,
            'answersCount': database.get_answers_count(survey)
        })
    report_permissions = database.ReportPermission.query.filter_by(UserId=user.id).all()
    for rp in report_permissions:
        report = database.Report.query.filter_by(id=rp.ReportId).first()
        survey = database.Survey.query.filter_by(id=sp.SurveyId).first()
        result.append({
            'type': 'report',
            'id': report.id,
            'name': report.Name,
            "sharedTo":database.get_report_users(report),
            'connectedSurvey': {"id": report.SurveyId, "name": survey.Name},
            'backgroundImg': report.BackgroundImg,
            'userId': rp.UserId
        })
    return {"objects": result}


@app.route('/data/new', methods=['POST'])
@on_errors('could not save survey data')
def upload_results():
    if not request.files['file']:
        raise error.API("empty survey data")

    file = request.files['file']
    name, ext = file.filename.rsplit('.', 1)

    if 'name' in request.form:
        name = request.form['name']
    if ext.lower() != 'csv':
        raise error.API("expected a CSV file")

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
@on_errors('could not create report')
def create_report():
    # można pomyśleć o maksymalnej, dużej liczbie raportów dla każdego użytkownika
    # ze względu na bezpieczeństwo.
    grammar.check(grammar.REQUEST_CREATE_SURVEY, request.json)

    data = request.json
    user = database.get_user()
    # czy użytkownik widzi tę ankietę?
    survey = database.get_survey(data["surveyId"])
    report = database.create_report(user, survey, data["title"])
    with open(f'report/{report.id}.json', 'w') as file:
        json.dump(data, file)
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>/copy', methods=['GET'])
@on_errors('could not copy the report')
def copy_report(report_id):
    data = get_report(report_id)
    if 'error' in data:
        return data
    user = database.get_user()
    report = database.get_report(report_id)
    survey = database.get_report_survey(report)
    report = database.create_report(user, survey, report["title"])
    with open(f'report/{report.id}.json', 'w') as file:
        json.dump(data, file)
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>', methods=['POST'])
@on_errors('could not save the report')
def set_report(report_id):
    report = database.get_report(report_id)
    perm = database.get_report_permission(report, database.get_user())
    if perm not in ['o', 'w']:
        raise error.API("you have no permission to edit this report")
    with open(f'report/{report_id}.json', 'w') as file:
        json.dump(request.json, file)
    return {
        "reportId": report.id
    }


@app.route('/report/<int:report_id>/users', methods=['GET'])
@on_errors('could not get the report users')
@for_roles('s', 'u')
def get_report_users(report_id):
    report = database.get_report(report_id)
    return database.get_report_users(report)



@app.route('/report/<int:report_id>', methods=['GET'])
@on_errors('could not open the report')
def get_report(report_id):
    with open(f'report/{report_id}.json', 'r') as file:
        data = json.load(file)
    return data


@app.route('/survey/<int:survey_id>', methods=['DELETE'])
@on_errors('could not delete survey')
def delete_survey(survey_id):
    survey = database.get_survey(survey_id)
    perm = database.get_survey_permission(survey, database.get_user())
    if perm != 'o':
        raise error.API("you have no permission to delete this survey")
    database.delete_survey(survey)
    return {
        'message': 'report has been deleted',
        'surveyId': survey_id
    }


@app.route('/report/<int:report_id>', methods=['DELETE'])
@on_errors('could not delete report')
def delete_report(report_id):
    report = database.get_report(report_id)
    perm = database.get_report_permission(report, database.get_user())
    if perm != 'o':
        raise error.API("you have no permission to delete this report")
    database.delete_report(report)
    return {
        'message': 'report has been deleted',
        'reportId': report_id
    }


@app.route('/data/<int:survey_id>', methods=['POST'])
@on_errors('could not obtain survey data')
def get_data(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    result = table.create(request.json, conn)
    conn.close()
    return result


@app.route('/report/<int:report_id>/survey', methods=['GET'])
@on_errors('could not find the source survey')
def get_report_survey(report_id):
    report = database.get_report(report_id)
    survey = database.get_report_survey(report)
    return {
        "surveyId": survey.id
    }


@app.route('/data/<int:survey_id>/types', methods=['GET'])
@on_errors('could not get question types')
def get_data_types(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    types = database.get_types(conn)
    conn.close()
    return types


@app.route('/data/<int:survey_id>/questions', methods=['GET'])
@on_errors('could not get question order')
def get_questions(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    questions = database.get_columns(conn)
    conn.close()
    return {
        'questions': questions
    }


@app.route('/report/<int:report_id>/rename', methods=['POST'])
@on_errors('could not rename report')
def rename_report(report_id):
    # uprawnienia
    if 'title' not in request:
        raise error.API('no parameter title')
    report = database.get_report(report_id)
    report.Name = request.json['title']
    #db.session.commit()
    return {
        'message': 'report name has been changed',
        'reportId': report.id,
        'title': request.json['title']
    }


@app.route('/survey/<int:survey_id>/rename', methods=['POST'])
@on_errors('could not rename survey')
def rename_survey(survey_id):
    # uprawnienia
    if 'title' not in request:
        raise error.API('no parameter title')
    survey = database.get_survey(survey_id)
    survey.Name = request.json['title']
    #db.session.commit()
    return {
        'message': 'survey name has been changed',
        'surveyId': survey.id,
        'title': request.json['title']
    }


@app.route('/survey/<int:survey_id>/share', methods=['POST'])
@on_errors('could not share survey')
def share_survey(survey_id):
    json = request.json
    survey = database.get_survey(survey_id)
    perm = database.get_survey_permission(survey, database.get_user())
    if perm != "o":
        raise error.API("you must be the owner to share this survey")
    for p, users in json.items():
        for user in users:
            database.set_survey_permission(survey, database.get_user(user), p)
    return {
        "message": "permissions added"
    }


@app.route('/report/<int:report_id>/share', methods=['POST'])
@on_errors('could not share report')
def share_report(report_id):
    json = request.json
    report = database.get_report(report_id)
    perm = database.get_report_permission(report, database.get_user())
    if perm != "o":
        raise error.API("you must be the owner to share this report")
    for p, users in json.items():
        for user in users:
            database.set_report_permission(report, database.get_user(user), p)
    return {
        "message": "permissions added"
    }


# e.g. {'permission': 'r', 'surveyId': 1}
@app.route('/survey/<int:survey_id>/link', methods=['POST'])
@on_errors('could not create link to survey')
@for_roles('s', 'u')
def link_to_survey(survey_id):
    survey = database.get_survey(survey_id)
    user = database.get_user()
    perm = database.get_survey_permission(survey, user)
    if perm != 'o':
        raise error.API('only the owner can share a survey')
    json = request.json
    grammar.check(grammar.REQUEST_SURVEY_LINK, json)
    link = database.get_permission_link(json['permission'], 's', json['surveyId'])
    return {
        'link': link
    }


# e.g. {'permission': 'r', 'reportId': 1}
@app.route('/report/<int:report_id>/link', methods=['POST'])
@on_errors('could not create link to report')
@for_roles('s', 'u')
def link_to_report(report_id):
    report = database.get_report(report_id)
    user = database.get_user()
    perm = database.get_report_permission(report, user)
    if perm != 'o':
        raise error.API('only the owner can share a report')
    json = request.json
    grammar.check(grammar.REQUEST_REPORT_LINK, json)
    link = database.get_permission_link(json['permission'], 'r', json['reportId'])
    return {
        'link': link
    }


@app.route('/link/<hash>', methods=['GET'])
@on_errors('could not set permission link')
@for_roles('s', 'u')
def set_permission_link(hash):
    return database.set_permission_link(hash, database.get_user())


# {'group': 'nazwa grupy'}
@app.route('/group/all', methods=['DELETE'])
@on_errors('could not delete group')
@for_roles('s', 'u')
def delete_group():
    grammar.check(grammar.REQUEST_GROUP, request.json)
    database.delete_group(request.json['group'])
    return {
        'message': 'group deleted'
    }


@app.route('/group/all', methods=['GET', 'POST'])
@on_errors('could not obtain list of groups')
@for_roles('s', 'u')
def get_groups():
    result = {}
    for group in database.get_groups():
        result[group] = []
        for user in database.get_group_users(group):
            result[group].append(user.id)
    return result


# {'nazwa grupy': [user_id_1, user_id_2, ...], 'nazwa grupy': ...}
@app.route('/group/change', methods=['POST'])
@on_errors('could not add users to groups')
@for_roles('s')
def set_group():
    for group, ids in request.json.items():
        grammar.check([int], ids)
        for id in ids:
            user = database.get_user(id)
            database.set_user_group(user, group)
    return {
        'message': 'users added to groups'
    }


# {'nazwa grupy': [user_id_1, user_id_2, ...], 'nazwa grupy': ...}
@app.route('/group/change', methods=['DELETE'])
@on_errors('could not remove users from groups')
@for_roles('s')
def unset_group():
    for group, ids in request.json.items():
        grammar.check([int], ids)
        for id in ids:
            user = database.get_user(id)
            database.unset_user_group(user, group)
    return {
        'message': 'users removed from groups'
    }


# {'group': 'nazwa grupy'}
@app.route('/group/users', methods=['POST'])
@on_errors('could not obtain group users')
@for_roles('s', 'u')
def get_group_users():
    grammar.check(grammar.REQUEST_GROUP, request.json)
    users = database.get_group_users(request.json['group'])
    return {
        request.json['group']: [user.id for user in users]
    }


@app.route('/user/<int:user_id>/group', methods=['GET', 'POST'])
@on_errors('could not obtain user groups')
@for_roles('s', 'u')
def get_user_groups(user_id):
    result = {}
    user = database.get_user(user_id)
    for group in database.get_user_groups(user):
        result[group] = []
        for user in database.get_group_users(group):
            result[group].append(user.id)
    return result


@app.route('/user',  methods=['GET'])
@on_errors('could not obtain user data')
def get_user_details():
    user = database.get_user()
    return {
        "logged":    True,
        "username":  session['username'],
        "id":        user.id,
        'casLogin':  user.CasLogin,
        'fetchData': user.FetchData,
        'role':      user.Role,
    }

@app.route('/user/<int:user_id>',  methods=['GET'])
@on_errors('could not obtain user data')
@for_roles('s')
def get_user_id_details(user_id):
    user = database.get_user(user_id)
    return {
        "logged":    True,
        "username":  session['username'],
        "id":        user.id,
        'casLogin':  user.CasLogin,
        'fetchData': user.FetchData,
        'role':      user.Role,
    }


@app.route('/user/all', methods=['GET'])
def get_user_list():
    return database.get_users()


@app.route('/users', methods=['GET'])
def get_users():
    return get_user_list()


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


@app.route('/bkg/<path:path>', methods=['GET'])
def get_bkg(path):
    return send_from_directory('bkg', path)


if __name__ == '__main__':
    for d in daemon.LIST:
        threading.Thread(target=d, daemon=True).start()
    app.run()
