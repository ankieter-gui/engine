from flask_sqlalchemy import SQLAlchemy
from pandas import read_csv
from random import randint
from typing import Literal
from flask import session
from config import *
import sqlite3
import error
import os

db = SQLAlchemy(app)
Role = Literal['s', 'u', 'g']
# user_roles = {'s': 0, 'u': 1, 'g': 2, 0: 's', 1: 's', 2: 'g'}
Permission = Literal['o', 'w', 'r']
# permissions_types = {'o': 0, 'w': 1, 'r': 2, 0: 'o', 1: 'w', 2: 'r'}


class User(db.Model):
    __tablename__ = "Users"
    id = db.Column(db.Integer, primary_key=True)
    CasLogin = db.Column(db.String(80), unique=True, nullable=False)
    FetchData = db.Column(db.Boolean, nullable=False)
    Role = db.Column(db.String, default='g', nullable=False)


class Group(db.Model):
    __tablename__ = "Groups"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(25), nullable=False)


class Survey(db.Model):
    __tablename__ = "Surveys"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(80), nullable=False)
    AnkieterId = db.Column(db.Integer, unique=True)
    StartedOn = db.Column(db.DateTime, nullable=True)
    EndsOn = db.Column(db.DateTime, nullable=True)
    IsActive = db.Column(db.Integer, nullable=True)
    QuestionCount = db.Column(db.Integer, nullable=False)
    BackgroundImg = db.Column(db.String(50), default=None)


class Report(db.Model):
    __tablename__ = "Reports"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(80), nullable=False)
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), nullable=False)
    BackgroundImg = db.Column(db.String(50))


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
    Type = db.Column(db.String, default='r', nullable=False)


class ReportPermission(db.Model):
    __tablename__ = "ReportPermissions"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True)
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Type = db.Column(db.String, default='r', nullable=False)


ADMIN.add_view(ModelView(User, db.session))
ADMIN.add_view(ModelView(Survey, db.session))


def get_user(username: str) -> User:
    return User.query.filter_by(CasLogin=username).first()


def get_user_role(user_id: int) -> Role:
    user = User.query.filter_by(id=user_id).first()
    if user is None:
        raise error.API('no such user')
    return user.Role


def set_user_role(user_id: int, role: Role):
    user = User.query.filter_by(id=user_id).first()
    if user is None:
        raise error.API('no such user')
    user.Role = role
    db.session.commit()


# meta = {"started_on": DateTime, "ends_on": DateTime, "is_active": int}
def set_survey_meta(survey_id: int, name: str, question_count: int, meta: dict):
    survey = Survey.query.filter_by(AnkieterId=survey_id).first()
    if survey is None:
        survey = Survey(Name=name, AnkieterId=survey_id, QuestionCount=question_count)
        db.session.add(survey)
    if name:
        survey.Name = name
    if meta["started_on"]:
        survey.StartedOn = meta["started_on"]
    if meta["ends_on"]:
        survey.EndsOn = meta["ends_on"]
    if meta["is_active"]:
        survey.IsActive = meta["is_active"]
    if survey.BackgroundImg is None:
        bkgs = os.listdir('bkg')
        survey.BackgroundImg = bkgs[randint(0, len(bkgs))]
    db.session.commit()
    print("Survey meta data added")
    return True


def get_survey_permission(survey_id: int, user_id: int) -> Permission:
    sp = SurveyPermission.query.filter_by(SurveyId=survey_id, UserId=user_id).first()
    if sp is None:
        raise error.API('no such survey permission')
    return sp.Type


def set_survey_permission(survey_id: int, user_id: int, permission: Permission):
    sp = SurveyPermission.query.filter_by(SurveyId=survey_id, UserId=user_id).first()
    if sp is None:
        sp = SurveyPermission(SurveyId=survey_id, UserId=user_id)
        db.session.add(sp)
    sp.Type = permission
    db.session.commit()


def get_report_survey(report_id: int) -> int:
    report = Report.query.filter_by(id=report_id).first()
    if report is None:
        raise error.API('no such report')
    return report.SurveyId


def get_report_permission(report_id: int, user_id: int) -> Permission:
    sp = SurveyPermission.query.filter_by(ReportId=report_id, UserId=user_id).first()
    if sp is None:
        raise error.API('no such report permission')
    return sp.Type


def set_report_permission(report_id: int, user_id: int, permission: Permission):
    rp = ReportPermission.query.filter_by(ReportId=report_id, UserId=user_id).first()
    if rp is None:
        rp = ReportPermission(ReportId=report_id, UserId=user_id)
        db.session.add(rp)
    rp.Type = permission
    db.session.commit()


def create_report(user_id: int, survey_id: int, name: int) -> int:
    report = Report(Name=name, SurveyId=survey_id)
    bg = Survey.query.filter_by(id=survey_id)
    db.session.add(report)
    db.session.commit()
    set_report_permission(report.id, user_id, 'o')
    return report.id


def open_survey(survey_id: int) -> sqlite3.Connection:
    script_absolute_directory_path = os.path.dirname(os.path.realpath(__file__))
    db_absolute_path = os.path.join(script_absolute_directory_path, f"data/{survey_id}.db")
    return sqlite3.connect(db_absolute_path)


def get_types(conn: sqlite3.Connection) -> dict[str, str]:
    types = {}
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()

    for row in data:
        types[row[1]] = row[2]
    return types


def get_columns(conn: sqlite3.Connection) -> list[str]:
    columns = []
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()

    for row in data:
        columns.append(row[1])
    return columns


def csv_to_db(survey_id: int):
    try:
        conn = sqlite3.connect(f"data/{survey_id}.db")
        df = read_csv(f"raw/{survey_id}.csv", sep=",")
        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey_id} created succesfully")
        conn.close()
        return True
    except sqlite3.Error as err:
        return err
