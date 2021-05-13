from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_sqlalchemy import SQLAlchemy
from flask_restful import Api, Resource
from flask_cors import CORS
from flask_jsonpify import jsonify
from cas import CASClient
from setup import *
from os import path
import sqlite3
import os
from request_survey import *

app = Flask(__name__)
api = Api(app)
CORS(app)

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


class Dashboard(Resource):
    def get(self):
        user = User.query.filter_by(CasLogin=session['username']).first()
        survey_permissions = SurveyPermission.query.filter_by(UserId=user.id).all()
        result = {}
        for sp in survey_permissions:
            survey = Survey.query.filter_by(id=sp.SurveyId).first()
            meta = self.get_meta(survey.AnkieterId)
            print(meta)
            #na szybko - do poprawy
            result[survey.AnkieterId] = {
                'survey_id': survey.AnkieterId,
                'user_id': sp.UserId,
                'startedOn': meta[0][0],
                'endsOn': meta[0][1],
                'isActive': meta[0][2],
                'QuestionsAmount': meta[0][3]
            }
        return jsonify(result)

    def get_meta(self, survey_id):
        conn = sqlite3.connect("data/" + str(survey_id) + '.db')
        cur = conn.cursor()
        cur.execute("select * from meta")
        data = cur.fetchall()
        conn.close()
        return data


class RequestSurvey(Resource):
    def post(self, survey_id):
        if not request.json:
            # TODO: return json with errors
            return
        json_request = request.json
        script_absolute_directory_path = os.path.dirname(os.path.realpath(__file__))
        db_absolute_path=path.join(script_absolute_directory_path,"data", survey_id+".db")
        conn = sqlite3.connect(db_absolute_path)
        result = request_survey(json_request, conn)
        conn.close()
        return result


api.add_resource(Dashboard, '/dashboard')
api.add_resource(RequestSurvey, '/data/<survey_id>')


@app.route('/data/types/<survey_id>', methods=['GET'])
def data_types(survey_id):
    conn = sqlite3.connect(f'data/{survey_id}.db')
    types = get_column_types(conn)
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
        return redirect(cas_client.get_login_url())

    user, attributes, pgtiou = cas_client.verify_ticket(ticket)

    if not user:
        return '<p>Failed to verify</p>'

    session['username'] = user
    return redirect(url_for('index'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(cas_client.get_logout_url())


if __name__ == '__main__':
    app.run()
