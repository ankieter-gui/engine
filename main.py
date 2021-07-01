from flask import send_from_directory, redirect, url_for, request, session, g, render_template
from config import *
import json
import os
import functools
import threading
import database
import grammar
import daemon
import table
import error


def on_errors(details):
    def on_error_decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except error.API as err:
                return err.add_details(details).as_dict()

        return decorated_function

    return on_error_decorator


def for_roles(*roles):
    def for_roles_decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            if database.get_user().Role not in roles:
                raise error.API('insufficient privileges')
            return f(*args, **kwargs)

        return decorated_function

    return for_roles_decorator


@app.route('/api/dashboard', methods=['GET'])
@on_errors('could not get dashboard')
def get_dashboard():
    user = database.get_user()
    user_surveys = database.get_user_surveys(user)
    result = []
    for survey in user_surveys:
        author = database.get_user(survey.AuthorId)
        result.append({
            'type': 'survey',
            'endsOn': survey.EndsOn.timestamp() if survey.EndsOn is not None else None,
            'startedOn': survey.StartedOn.timestamp() if survey.StartedOn is not None else None,
            'id': survey.id,
            'name': survey.Name,
            'sharedTo': database.get_survey_users(survey),
            'ankieterId': survey.AnkieterId,
            'isActive': survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg,
            'userId': user.id,
            'answersCount': database.get_answers_count(survey),
            'authorId': author.id,
        })
    user_reports = database.get_user_reports(user)
    for report in user_reports:
        try:
            survey = database.get_survey(report.SurveyId)
        except:
            continue
        author = database.get_user(report.AuthorId)
        result.append({
            'type': 'report',
            'id': report.id,
            'name': report.Name,
            'sharedTo': database.get_report_users(report),
            'connectedSurvey': {"id": report.SurveyId, "name": survey.Name},
            'backgroundImg': report.BackgroundImg,
            'userId': user.id,
            # 'author': author.Name
            'authorId': author.id,
        })
    return {"objects": result}


@app.route('/api/users', methods=['GET'])
def get_users():
    return get_user_list()


@app.route('/api/user/new', methods=['POST'])
@on_errors('could not create user')
@for_roles('s')
def create_user():
    data = request.json
    user = database.create_user(data["casLogin"],data['pesel'], data["role"])
    return {"id": user.id}


@app.route('/api/user/all', methods=['GET'])
def get_user_list():
    return database.get_users()


@app.route('/api/user/<int:user_id>/group', methods=['GET', 'POST'])
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


@app.route('/api/user/<int:user_id>', methods=['GET'])
@on_errors('could not obtain user data')
@for_roles('s')
def get_user_id_details(user_id):
    return database.get_user(user_id).as_dict()


@app.route('/api/user/<int:user_id>', methods=['DELETE'])
@on_errors('could not delete user')
@for_roles('s')
def delete_user(user_id):
    user = database.get_user(user_id)
    database.delete_user(user)
    return {"delete": user.id}


@app.route('/api/user', methods=['GET'])
@on_errors('could not obtain user data')
def get_user_details():
    return database.get_user().as_dict()


@app.route('/api/dictionary', methods=["GET"])
@on_errors('could not get dictionary')
@for_roles('s', 'u')
def get_dictionary():
    with open(os.path.join(ABSOLUTE_DIR_PATH, "dictionary.json")) as json_file:
        result = json.load(json_file)
    return result


@app.route('/api/survey/new', methods=['POST'])
@on_errors('could not create survey')
@for_roles('s', 'u')
def create_survey():
    user = database.get_user()
    r = request.json
    if not r["name"]:
        raise error.API("survey name can't be blank")
    survey = database.create_survey(user, r["name"])
    return {
        "id": survey.id
    }

@app.route('/api/survey/<int:survey_id>/upload', methods=['POST'])
@on_errors('could not upload survey')
@for_roles('s', 'u')
def upload_survey(survey_id):
    if not request.files['file']:
        raise error.API('empty survey data')

    file = request.files['file']
    name, ext = file.filename.rsplit('.', 1)
    # if ext.lower() != 'xml':
    #     raise error.API('expected a XML file')
    file.save(os.path.join(ABSOLUTE_DIR_PATH, "survey/",f"{survey_id}.xml"))
    return {
        "id": survey_id
    }



@app.route('/api/survey/<int:survey_id>/share', methods=['POST'])
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


@app.route('/api/survey/<int:survey_id>/rename', methods=['POST'])
@on_errors('could not rename survey')
@for_roles('s', 'u')
def rename_survey(survey_id):
    # uprawnienia
    if 'title' not in request:
        raise error.API('no parameter title')
    survey = database.get_survey(survey_id)
    survey.Name = request.json['title']
    # db.session.commit()
    return {
        'message': 'survey name has been changed',
        'surveyId': survey.id,
        'title': request.json['title']
    }


# e.g. {'permission': 'r', 'surveyId': 1}
@app.route('/api/survey/<int:survey_id>/link', methods=['POST'])
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


@app.route('/api/survey/<int:survey_id>', methods=['DELETE'])
@on_errors('could not delete survey')
@for_roles('s', 'u')
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


@app.route('/api/report/new', methods=['POST'])
@on_errors('could not create report')
@for_roles('s', 'u')
def create_report():
    # można pomyśleć o maksymalnej, dużej liczbie raportów dla każdego użytkownika
    # ze względu na bezpieczeństwo.
    grammar.check(grammar.REQUEST_CREATE_SURVEY, request.json)

    data = request.json
    user = database.get_user()
    # czy użytkownik widzi tę ankietę?
    survey = database.get_survey(data["surveyId"])
    report = database.create_report(user, survey, data["title"], user.id)
    with open(f'report/{report.id}.json', 'w') as file:
        json.dump(data, file)
    return {
        "reportId": report.id
    }


@app.route('/api/report/<int:report_id>/users', methods=['GET'])
@on_errors('could not get the report users')
@for_roles('s', 'u')
def get_report_users(report_id):
    report = database.get_report(report_id)
    return database.get_report_users(report)


@app.route('/api/report/<int:report_id>/answers', methods=['GET'])
@on_errors('could not get report answers')
@for_roles('s', 'u')
def get_report_answers(report_id):
    report = database.get_report(report_id)
    survey_xml = report.SurveyId
    result = database.get_answers(survey_xml)
    return result


@app.route('/api/report/<int:report_id>/survey', methods=['GET'])
@on_errors('could not find the source survey')
@for_roles('s', 'u')
def get_report_survey(report_id):
    report = database.get_report(report_id)
    survey = database.get_report_survey(report)
    return {
        "surveyId": survey.id
    }


@app.route('/api/report/<int:report_id>/share', methods=['POST'])
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


@app.route('/api/report/<int:report_id>/rename', methods=['POST'])
@on_errors('could not rename report')
def rename_report(report_id):
    # uprawnienia
    if 'title' not in request.json:
        raise error.API('no parameter title')
    report = database.get_report(report_id)
    rep = database.rename_report(report, request.json['title'])
    return {
        'message': 'report name has been changed',
        'reportId': report.id,
        'title': request.json['title']
    }


# e.g. {'permission': 'r', 'reportId': 1}
@app.route('/api/report/<int:report_id>/link', methods=['POST'])
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


@app.route('/api/report/<int:report_id>/data', methods=['POST'])
@on_errors('could not obtain survey data for the report')
def get_report_data(report_id):
    report = database.get_report(report_id)
    user = database.get_user()

    perm = database.get_report_permission(report, user)
    if perm not in ['r', 'w', 'o']:
        raise error.API('insufficient permissions')

    survey = database.get_report_survey(report)

    conn = database.open_survey(survey)
    result = table.create(request.json, conn)
    conn.close()

    return result


@app.route('/api/report/<int:report_id>/copy', methods=['GET'])
@on_errors('could not copy the report')
def copy_report(report_id):
    data = get_report(report_id)
    if 'error' in data:
        return data
    user = database.get_user()
    report = database.get_report(report_id)
    survey = database.get_report_survey(report)
    report = database.create_report(user, survey, report.Name, report.AuthorId)
    with open(f'report/{report.id}.json', 'w') as file:
        json.dump(data, file)
    return {
        "reportId": report.id
    }


@app.route('/api/report/<int:report_id>', methods=['POST'])
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


@app.route('/api/report/<int:report_id>', methods=['GET'])
@on_errors('could not open the report')
def get_report(report_id):
    with open(f'report/{report_id}.json', 'r') as file:
        data = json.load(file)
    return data


@app.route('/api/report/<int:report_id>', methods=['DELETE'])
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


# {'group': 'nazwa grupy'}
@app.route('/api/group/users', methods=['POST'])
@on_errors('could not obtain group users')
@for_roles('s', 'u')
def get_group_users():
    grammar.check(grammar.REQUEST_GROUP, request.json)
    users = database.get_group_users(request.json['group'])
    return {
        request.json['group']: [user.id for user in users]
    }


# {'nazwa grupy': [user_id_1, user_id_2, ...], 'nazwa grupy': ...}
@app.route('/api/group/change', methods=['POST'])
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
@app.route('/api/group/change', methods=['DELETE'])
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


@app.route('/api/group/all', methods=['GET', 'POST'])
@on_errors('could not obtain list of groups')
@for_roles('s', 'u')
def get_groups():
    result = {}
    for group in database.get_groups():
        result[group] = []
        for user in database.get_group_users(group):
            result[group].append(user.as_dict())
    return result


# {'group': 'nazwa grupy'}
@app.route('/api/group/all', methods=['DELETE'])
@on_errors('could not delete group')
@for_roles('s')
def delete_group():
    grammar.check(grammar.REQUEST_GROUP, request.json)
    database.delete_group(request.json['group'])
    return {
        'message': 'group deleted'
    }


@app.route('/api/data/new', methods=['POST'], defaults={'survey_id': None})
@app.route('/api/data/new/<int:survey_id>', methods=['POST'])
@on_errors('could not save survey data')
def upload_results(survey_id):
    if not request.files['file']:
        raise error.API("empty survey data")

    file = request.files['file']
    name, ext = file.filename.rsplit('.', 1)

    if 'name' in request.form:
        name = request.form['name']
    if ext.lower() != 'csv':
        raise error.API("expected a CSV file")

    if survey_id:
        survey = database.get_survey(survey_id)
    else:
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


@app.route('/api/data/<int:survey_id>/types', methods=['GET'])
@on_errors('could not get question types')
def get_data_types(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    types = database.get_types(conn)
    conn.close()
    return types


@app.route('/api/data/<int:survey_id>/questions', methods=['GET'])
@on_errors('could not get question order')
def get_questions(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    questions = database.get_columns(conn)
    conn.close()
    return {
        'questions': questions
    }


@app.route('/api/data/<int:survey_id>', methods=['POST'])
@on_errors('could not obtain survey data')
def get_data(survey_id):
    survey = database.get_survey(survey_id)
    conn = database.open_survey(survey)
    result = table.create(request.json, conn)
    conn.close()
    return result


@app.route('/api/link/<hash>', methods=['GET'])
@on_errors('could not set permission link')
@for_roles('s', 'u', 'g') # to be tested for 'g'
def set_permission_link(hash):
    perm, object, id = database.set_permission_link(hash, database.get_user())
    return {
        'permission': perm,
        'object': object,
        'id': id,
    }


@app.route('/api/login', methods=['GET', 'POST'])
@on_errors('could not log in')
def login():
    ticket = request.args.get('ticket')
    if not ticket:
        return redirect(CAS_CLIENT.get_login_url())

    user, attributes, pgtiou = CAS_CLIENT.verify_ticket(ticket)

    if not user:
        return '<p>Failed to verify</p>'

    print(user, attributes, pgtiou)
    u = database.get_user(user)
    session['username'] = u.CasLogin
    return redirect('/')


if DEBUG:
    @app.route('/api/login/<string:username>', methods=['GET', 'POST'])
    @on_errors('could not log in')
    def debug_login(username):
        session['username'] = username
        return redirect('/')


@app.route('/api/logout')
@on_errors('could not log out')
def logout():
    session.clear()
    return redirect(CAS_CLIENT.get_logout_url())


@app.route('/bkg/<path:path>', methods=['GET'])
def get_bkg(path):
    return send_from_directory('bkg', path)

@app.route('/<path:path>', methods=['GET'])
def get_static_file(path):
    return send_from_directory('static', path)

@app.route('/')
@app.route('/groups')
@app.route('/reports/<path:text>')
@app.route('/surveysEditor/<path:text>')
@app.route('/surveysEditor')
@app.route('/shared/<path:text>')
def index(text=None):
    return render_template('index.html')


if __name__ == '__main__':
    license = open('LICENSE', 'r')
    print(license.read())
    if DEBUG:
    	print('debug mode on: accounts can be accessed WITHOUT password')
    print('starting deamon threads')

    for d in daemon.LIST:
        threading.Thread(target=d, daemon=True).start()

    if LOCALHOST:
        print(f'the app is hosted on localhost:{APP_PORT}')
        app.run()
    else:
        app.run(ssl_context='adhoc', port=str(APP_PORT), host='0.0.0.0')
