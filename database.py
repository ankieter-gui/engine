from flask_sqlalchemy import SQLAlchemy
from pandas import read_csv
from typing import Literal, Any
from flask import session
from config import *
import secrets
import random
import re
import sqlite3
import error
import os
from base64 import b32encode

db = SQLAlchemy(app)

Role = Literal['s', 'u', 'g']
Permission = Literal['o', 'w', 'r', 'n']

class User(db.Model):
    __tablename__ = "Users"
    id = db.Column(db.Integer, primary_key=True)
    CasLogin = db.Column(db.String(80), unique=True, nullable=False)
    FetchData = db.Column(db.Boolean, nullable=False)
    Role = db.Column(db.String, default='g', nullable=False)

    def as_dict(self):
        return {
            "id": self.id,
            "casLogin": self.CasLogin,
            "role": self.Role
        }


#class Group(db.Model):
#    __tablename__ = "Groups"
#    id = db.Column(db.Integer, primary_key=True)
#    Name = db.Column(db.String(25), primary_key=True)


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
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Group = db.Column(db.String(25), primary_key=True)
    #GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)


class SurveyGroup(db.Model):
    __tablename__ = "SurveyGroups"
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), primary_key=True)
    Group = db.Column(db.String(25), primary_key=True)
    #GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)


class ReportGroup(db.Model):
    __tablename__ = "ReportGroups"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True)
    Group = db.Column(db.String(25), primary_key=True)
    #GroupId = db.Column(db.Integer, db.ForeignKey('Groups.id'), primary_key=True)


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


class Link(db.Model):
    __tablename__ = "Links"
    id = db.Column(db.Integer, primary_key=True)
    Salt = db.Column(db.String(SALT_LENGTH))
    Type = db.Column(db.String, default='r', nullable=False)
    Object = db.Column(db.String, nullable=False)
    ObjectId = db.Column(db.Integer, nullable=False)


ADMIN.add_view(ModelView(User, db.session))
ADMIN.add_view(ModelView(Survey, db.session))


def get_user(login: Any = "") -> User:
    if not login:
        # zamiast tego blędu, jeśli nie ma loginu, to przydziel gościa
        if 'username' not in session:
            raise error.API('user not logged in')
        login = session['username']
    if type(login) is str:
        user = User.query.filter_by(CasLogin=login).first()
    if type(login) is int:
        user = User.query.filter_by(id=login).first()
    if user is None:
        raise error.API(f'no such user {login}')
    return user


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


def get_permission_link(permission: Permission, object: Literal['s', 'r'], object_id: int) -> str:
    link = Link.query.filter_by(Type=permission, Object=object, ObjectId=object_id).first()
    if link is not None:
        return link.Salt + str(link.id)
    salt = secrets.randbits(5*SALT_LENGTH)
    salt = salt.to_bytes(5*SALT_LENGTH//8+1, byteorder='big')
    link = Link(
        Salt=b32encode(salt).decode('utf-8'),
        Type=permission,
        Object=object,
        ObjectId=object_id
    )
    db.session.add(link)
    db.session.commit()
    return link.Salt + str(link.id)


def set_permission_link(hash: str, user: User):
    salt = hash[:SALT_LENGTH]
    id = str(hash[SALT_LENGTH:])
    link = Link.query.filter_by(Salt=salt, ObjectId=id).first()
    if link is None:
        raise error.API('wrong url')
    object_type = link.Object
    if object_type == 's':
        survey = get_survey(link.ObjectId)
        set_survey_permission(survey, user, link.Type)
    elif object_type == 'r':
        report = get_report(id=link.ObjectId)
        set_report_permission(report, user, link.Type)
    else:
        raise error.API(f'unknown object type "{object_type}"')


def get_report_users(report: Report) -> dict:
    perms = ReportPermission.query.filter_by(ReportId=report.id).all()
    result = {}
    for perm in perms:
        result[perm.UserId] = perm.Type
    return result

def get_users() -> dict:
    users = User.query.all()
    result = []
    for u in users:
        result.append({
            "casLogin": u.CasLogin,
            "id": u.id
        })
    return {"users": result}


def get_groups() -> list[str]:
    user_groups = UserGroup.query.with_entities(UserGroup.Group).distinct()
    return [ug.Group for ug in user_groups]


#def get_group(id: int) -> Group:
#    group = Group.query.filter_by(id=id)
#    if group is None:
#        raise error.API('no such group')
#    return group


#def create_group(name: str) -> Group:
#    group = Group.query.filter_by(Name=name)
#    if group is not None:
#        raise error.API('group already exists')
#    group = Group(Name=name)
#    db.session.add(group)
#    db.session.commit()
#    return group


def set_user_group(user: User, group: str):
    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group).first()
    if user_group is not None:
        return user_group
    user_group = UserGroup(UserId=user.id, Group=group)
    db.session.add(user_group)
    db.session.commit()
    return user_group


def unset_user_group(user: User, group: str):
    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group)
    if user_group is None:
        raise error.API('the user is not in the group')
    user_group.delete()
    db.session.commit()


def get_user_groups(user: User) -> list[str]:
    user_groups = UserGroup.query.filter_by(UserId=user.id).all()
    if user_groups is None:
        return []
    return [user_group.Group for user_group in user_groups]


def get_group_users(group: str) -> list[User]:
    user_groups = UserGroup.query.filter_by(Group=group).all()
    users = []
    for user_group in user_groups:
        user = User.query.filter_by(id=user_group.UserId).first()
        if user is not None:
            users.append(user)
    return users


def delete_group(group: str):
    user_groups = UserGroup.query.filter_by(Group=group).delete()
    db.session.commit()


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
        return 'n'
    return sp.Type


def set_survey_permission(survey: Survey, user: User, permission: Permission):
    sp = SurveyPermission.query.filter_by(SurveyId=survey.id, UserId=user.id).first()
    if sp is None:
        sp = SurveyPermission(SurveyId=survey.id, UserId=user.id)
        db.session.add(sp)
    if permission != "n":
        sp.Type = permission
    else:
        db.session.delete(sp)
    db.session.commit()


def get_report_survey(report: Report) -> Survey:
    if report is None:
        raise error.API('no such report')
    survey = Report.query.filter_by(id=report.SurveyId).first()
    return survey


def get_report_permission(report: Report, user: User) -> Permission:
    sp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if sp is None:
        return 'n'
    return sp.Type


def set_report_permission(report: Report, user: User, permission: Permission):
    rp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if rp is None:
        rp = ReportPermission(ReportId=report.id, UserId=user.id)
        db.session.add(rp)
    if permission != "n":
        rp.Type = permission
    else:
        db.session.delete(rp)
    db.session.commit()


def create_report(user: User, survey: Survey, name: str) -> Report:
    report = Report(Name=name, SurveyId=survey.id)
    report.BackgroundImg = Survey.query.filter_by(id=survey.id).first().BackgroundImg
    db.session.add(report)
    db.session.commit()
    set_report_permission(report, user, 'o')
    return report


def delete_survey(survey: Survey):
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


def delete_report(report: Report):
    id = report.id
    ReportPermission.query.filter_by(ReportId=report.id).delete()
    ReportGroup.query.filter_by(ReportId=report.id).delete()
    Report.query.filter_by(id=report.id).delete()
    db.session.commit()


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
    conn = open_survey(survey)
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
            df[u] = df[group].aggregate(shame, axis='columns')
            df = df.drop(group[:-1], axis='columns')

        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey.id} created succesfully")
        conn.close()
        return True
    except sqlite3.Error as err:
        return err


if __name__ == '__main__':
    delete_survey(get_survey(1))
    delete_report(get_report(1))
