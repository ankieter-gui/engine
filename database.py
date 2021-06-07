from flask_sqlalchemy import SQLAlchemy
from pandas import read_csv
from typing import Literal
from flask import session
from config import *
import random
import re
import sqlite3
import error
import os

db = SQLAlchemy(app)
Role = Literal['s', 'u', 'g']
Permission = Literal['o', 'w', 'r']


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
    QuestionCount = db.Column(db.Integer, nullable=True)
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


def get_user() -> User:
    return User.query.filter_by(CasLogin=session['username']).first()


def get_survey(id: int) -> Survey:
    survey = Survey.query.filter_by(id=id).first()
    if survey is None:
        raise error.API('no such survey')
    return survey


def get_report(id: int) -> Report:
    report = Report.query.filter_by(id=id).first()
    if report is None:
        raise error.API('no such report')
    return report


def get_users() -> dict:
    users = User.query.all()
    result = []
    for u in users:
        result.append({"CasLogin": u.CasLogin})
    return {"users": result}


def create_survey(user: User, name: str) -> Survey:
    survey = Survey(Name=name, QuestionCount=0)
    db.session.add(survey)
    bkgs = os.listdir(path.join(ABSOLUTE_DIR_PATH,'bkg'))
    survey.BackgroundImg = random.choice(bkgs)
    db.session.commit()
    set_survey_permission(survey, user, 'o')
    return survey


# meta = {"started_on": DateTime, "ends_on": DateTime, "is_active": int}
def set_survey_meta(survey: Survey, name: str, question_count: int, meta: dict):
    if survey is None:
        survey = Survey(Name=name, QuestionCount=question_count)
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
        bkgs = os.listdir(path.join(ABSOLUTE_DIR_PATH, 'bkg'))
        survey.BackgroundImg = random.choice(bkgs)
    db.session.commit()
    print("Survey meta data added")
    return True


def get_survey_permission(survey: Survey, user: User) -> Permission:
    sp = SurveyPermission.query.filter_by(SurveyId=survey.id, UserId=user.id).first()
    if sp is None:
        raise error.API('no such survey permission')
    return sp.Type


def set_survey_permission(survey: Survey, user: User, permission: Permission):
    sp = SurveyPermission.query.filter_by(SurveyId=survey.id, UserId=user.id).first()
    if sp is None:
        sp = SurveyPermission(SurveyId=survey.id, UserId=user.id)
        db.session.add(sp)
    sp.Type = permission
    db.session.commit()


def get_report_survey(report: Report) -> Survey:
    if report is None:
        raise error.API('no such report')
    survey = Report.query.filter_by(id=report.SurveyId).first()
    return survey


def get_report_permission(report: Report, user: User) -> Permission:
    sp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if sp is None:
        raise error.API('no such report permission')
    return sp.Type


def set_report_permission(report: Report, user: User, permission: Permission):
    rp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if rp is None:
        rp = ReportPermission(ReportId=report.id, UserId=user.id)
        db.session.add(rp)
    rp.Type = permission
    db.session.commit()


def create_report(user: User, survey: Survey, name: int) -> Report:
    report = Report(Name=name, SurveyId=survey.id)
    report.BackgroundImg = Survey.query.filter_by(id=survey.id).first().BackgroundImg
    db.session.add(report)
    db.session.commit()
    set_report_permission(report, user, 'o')
    return report


def delete_survey(survey: Survey):
    if survey is None:
        raise error.API('no such survey')
    # db_path = 'data/' + str(survey.id) + '.db'
    # if os.path.exists(db_path):
    #     os.remove(db_path)
    # xml_path = 'survey/' + str(survey.id) + '.xml'
    # if os.path.exists(xml_path):
    #     os.remove(xml_path)
    id = survey.id
    SurveyPermission.query.filter_by(SurveyId=survey.id).delete()
    SurveyGroup.query.filter_by(SurveyId=survey.id).delete()
    Survey.query.filter_by(id=survey.id).delete()
    db.session.commit()
    return {'message': 'survey has been deleted', 'surveyId': id}


def delete_report(report: Report):
    if report is None:
        raise error.API('no such report')
    id = report.id
    ReportPermission.query.filter_by(ReportId=report.id).delete()
    ReportGroup.query.filter_by(ReportId=report.id).delete()
    Report.query.filter_by(id=report.id).delete()
    db.session.commit()
    return {'message': 'report has been deleted', 'reportId': id}


def rename_report(report: Report, request):
    report = Report.query.filter_by(id=report.id).first()
    if report is None:
        raise error.API('no such report')
    if 'title' not in request:
        raise error.API('no parameter title')
    report.Name = request['title']
    db.session.commit()
    return {'message': 'report name has been changed', 'reportId': report.id, 'title': request['title']}


def rename_survey(survey: Survey, request):
    if survey is None:
        raise error.API('no such survey')
    if 'title' not in request:
        raise error.API('no parameter title')
    survey.Name = request['title']
    db.session.commit()
    return {'message': 'survey name has been changed', 'surveyId': survey.id, 'title': request['title']}


def open_survey(survey: Survey) -> sqlite3.Connection:
    return sqlite3.connect(f"data/{survey.id}.db")


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


def get_answers_count(survey: Survey):
    conn = open_survey(survey.id)
    cur = conn.cursor()
    cur.execute("SELECT * FROM data")
    n = len(cur.fetchall())
    conn.close()
    return n


def csv_to_db(survey: Survey, filename: str):
    def shame(vals):
        counts = {}
        for v in vals:
            c = counts.get(v, 0)
            counts[v] = c+1
        if len(counts) == 0:
            return None
        return min(counts, key=counts.get)

    try:
        conn = open_survey(survey)
        df = read_csv(f"raw/{filename}", sep=",")
        df.columns = df.columns.str.replace('</?\w[^>]*>', '', regex=True)

        columns = df.columns.values
        repeats = df.filter(regex=r'\.\d+$').columns.values
        uniques = [c for c in columns if c not in repeats]

        for u in uniques:
            esc = re.escape(u)
            group = list(df.filter(regex=esc+'\.\d+$').columns.values)
            group.append(u)
            df[u] = df[[*group]].aggregate(shame, axis='columns')
            df = df.drop(group[:-1], axis='columns')

        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey.id} created succesfully")
        conn.close()
        return True
    except sqlite3.Error as err:
        return err


if __name__ == '__main__':
    delete_survey(1)
    delete_report(1)
