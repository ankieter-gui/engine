from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_sqlalchemy import SQLAlchemy
from flask_restful import Api, Resource
from flask_cors import CORS
from flask_jsonpify import jsonify
from cas import CASClient
from setup import *
import sqlite3
import pandas

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


def convertCSV(target_id):
    con = sqlite3.connect("survey_data/" + str(target_id) + ".db")
    df = pandas.read_csv("temp/" + str(target_id) + ".csv", sep=',')
    df.to_sql("data", con, if_exists='replace', index=False)

    
class Dashboard(Resource):
    def get(self):
        user = User.query.filter_by(CasLogin=session['username']).first()
        surveys = SurveyPermission.query.filter_by(UserId=user.id).all()
        result = {}
        for s in surveys:
            result[s.SurveyId] = {
                'survey_id': s.SurveyId,
                'user_id': s.UserId
            }
        return jsonify(result)


api.add_resource(Dashboard, '/dashboard')


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
