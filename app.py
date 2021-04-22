from flask import redirect, url_for, request, session, g
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_sqlalchemy import SQLAlchemy
from cas import CASClient
from setup import *
import sqlite3
import sys

app = Flask(__name__)

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


# DATABASE = './master.db'

@app.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return '''<p>Witaj {}</p>'''.format(username)
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        session['username'] = request.form['uname']
        return redirect(url_for('index'))
    return '''
    <form action="{}" method="post">
     <div>
      <label for="uname"><b>Username</b></label>
      <input type="text" placeholder="Enter Username" name="uname" required></br>
      <label for="psw"><b>Password</b></label>
      <input type="password" placeholder="Enter Password" name="psw" required>
      <br><button type="submit">Login</button>
     </div>
    </form>'''.format(url_for('login'))


if __name__ == '__main__':
    app.run()
