from pandas import read_csv
from flask_sqlalchemy import SQLAlchemy
from config import *
import os
import sqlite3
import error

db = SQLAlchemy(app)

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
    StartedOn = db.Column(db.DateTime, nullable=True)
    EndsOn = db.Column(db.DateTime, nullable=True)
    IsActive = db.Column(db.Integer, nullable=True)
    QuestionCount = db.Column(db.Integer, nullable=False)
    BackgroundImg = db.Column(db.String(50))


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
    Type = db.Column(db.Integer, default=2, nullable=False)


class ReportPermission(db.Model):
    __tablename__ = "ReportPermissions"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True)
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Type = db.Column(db.Integer, default=2, nullable=False)

ADMIN.add_view(ModelView(User, db.session))
ADMIN.add_view(ModelView(Survey, db.session))

# set_user_role
# get_user_role
# get_survey_permission
# set_survey_permission

def add_survey_meta(survey_id: int, name: str, meta: dict):
    return


def set_survey_permission(survey_id: int, user_id: int, permission: int):
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


# get_report_permission


def set_report_permission(report_id: int, user_id: int, permission: int):
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
    set_report_permission(report.id, user_id, 0)
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
        cur = conn.cursor()
        df = read_csv(f"raw/{survey_id}.csv", sep=",")
        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey_id} created succesfully")
        conn.close()
        return True
    except sqlite3.Error as err:
        return err
