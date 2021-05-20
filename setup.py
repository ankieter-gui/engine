import random
import sqlite3
import pandas
import string
import database

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from faker import Faker
from datetime import datetime
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///master.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
columns_number = {}


class User(db.Model):
    __tablename__ = "Users"
    id = db.Column(db.Integer, primary_key=True)
    CasLogin = db.Column(db.String(80), unique=True, nullable=False)
    FetchData = db.Column(db.Boolean, nullable=False)
    Role = db.Column(db.Integer, default=2, nullable=False)


class Group(db.Model):
    __tablename__ = "Groups"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(25), nullable=False)


class Survey(db.Model):
    __tablename__ = "Surveys"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(80), nullable=False)
    AnkieterId = db.Column(db.Integer, unique=True)
    StartedOn = db.Column(db.DateTime, nullable=False)
    EndsOn = db.Column(db.DateTime, nullable=False)
    IsActive = db.Column(db.Integer, nullable=False)
    QuestionCount = db.Column(db.Integer, nullable=False)
    BackgroundImg = db.Column(db.String(50))

class Report(db.Model):
    __tablename__ = "Reports"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(80), nullable=False)
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), nullable=False)


class UserGroup(db.Model):
    __tablename__ = "UserGroups"
    GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)


class SurveyGroup(db.Model):
    __tablename__ = "SurveyGroups"
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), primary_key=True)
    GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)


class ReportGroup(db.Model):
    __tablename__ = "ReportGroups"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True)
    GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)


class SurveyPermission(db.Model):
    __tablename__ = "SurveyPermissions"
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), primary_key=True)
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Type = db.Column(db.Integer, default=2, nullable=False)


class ReportPermission(db.Model):
    __tablename__ = "ReportPermissions"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True)
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Type = db.Column(db.Integer, default=2, nullable=False)


def convert_csv(target_id):
    global columns_number
    con = sqlite3.connect("data/" + str(target_id) + ".db")
    df = pandas.read_csv("temp/" + str(target_id) + ".csv", sep=',')
    columns_number[target_id] = len(df.columns)
    df.to_sql("data", con, if_exists='replace', index=False)
    con.close()


def get_sample_tuples(n, *args):
    from itertools import product
    from functools import reduce
    n = min(n, reduce(lambda a, b: a*b, args))
    args = map(lambda x: range(1, x+1), args)
    s = random.sample(sorted(product(*args)), n)
    return s


if __name__ == "__main__":
    db.drop_all()
    db.create_all()

    for dir in ['data', 'temp', 'report', 'bkg']:
        if not os.path.exists(dir):
            os.makedirs(dir)

    fake = Faker(locale="pl_PL")

    USERS_AMOUNT = 20
    GROUPS_AMOUNT = 3
    REPORTS_AMOUNT = 5

    surveys_amount = 0
    for filename in os.listdir('temp'):
        if filename.endswith(".csv"):
            survey_id = filename.split('.')[0]
            convert_csv(survey_id)
            db.session.add(Survey(
                Name='ankieta testowa',
                AnkieterId=survey_id,
                StartedOn=datetime(2020, 3, random.randint(1, 31)),
                EndsOn=datetime(2021, 6, random.randint(1, 30)),
                IsActive=random.randint(0, 1),
                QuestionCount=columns_number[survey_id]))
            surveys_amount += 1

    for _ in range(USERS_AMOUNT - 1):
        cas_login = ''.join([random.choice(string.digits) for i in range(11)])
        role = random.randint(0, 2)
        db.session.add(User(CasLogin=cas_login, Role=role, FetchData=False))

    for _ in range(GROUPS_AMOUNT):
        group_name = fake.company()
        db.session.add(Group(Name=group_name))

    for i in range(REPORTS_AMOUNT):
        report_name = f'Raport {random.randint(1, 50)}'
        survey_id = random.randint(1, surveys_amount)
        db.session.add(Report(Name=report_name, SurveyId=survey_id))

    for g_id, u_id in get_sample_tuples(18, GROUPS_AMOUNT, USERS_AMOUNT):
        db.session.add(UserGroup(GroupId=g_id, UserId=u_id))

    for s_id, g_id in get_sample_tuples(18, surveys_amount, GROUPS_AMOUNT):
        db.session.add(SurveyGroup(SurveyId=s_id, GroupId=g_id))

    for r_id, g_id in get_sample_tuples(18, REPORTS_AMOUNT, GROUPS_AMOUNT):
        db.session.add(ReportGroup(ReportId=r_id, GroupId=g_id))

    for s_id, u_id in get_sample_tuples(18, surveys_amount, USERS_AMOUNT):
        db.session.add(SurveyPermission(SurveyId=s_id, UserId=u_id, Type=random.randint(0, 2)))

    for r_id, u_id in get_sample_tuples(18, REPORTS_AMOUNT, USERS_AMOUNT):
        db.session.add(ReportPermission(ReportId=r_id, UserId=u_id, Type=random.randint(0, 2)))

    pesel = input('Podaj swój pesel\n')
    user = User.query.filter_by(id=1).first()
    user.CasLogin = pesel
    db.session.commit()

    for survey in Survey.query.all():
        database.set_survey_permission(survey.id, user.id, 0)
