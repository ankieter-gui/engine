from typing import Literal, Any, List, Dict
from flask_sqlalchemy import SQLAlchemy
from base64 import b32encode
from pandas import read_csv, read_excel
from flask import session
from globals import *
import xml.etree.ElementTree as ET
import sqlite3
import secrets
import random
import error
import csv
import re
import os


db = SQLAlchemy(app)

Role = Literal['s', 'u', 'g']
Permission = Literal['o', 'w', 'r', 'n']

PERMISSION_ORDER = ['n', 'r', 'w', 'o']


class User(db.Model):
    __tablename__ = "Users"
    id = db.Column(db.Integer, primary_key=True)
    CasLogin = db.Column(db.String(80), unique=True, nullable=False)
    Pesel = db.Column(db.String(11), nullable=True)
    FetchData = db.Column(db.Boolean, nullable=False)
    Role = db.Column(db.String, default='g', nullable=False)

    def as_dict(self):
        ud = {
            "id":        self.id,
            "casLogin":  self.CasLogin.split('@')[0],
            "fetchData": self.FetchData,
            "role":      self.Role,
            "logged":    self.Role != 'g',
        }
        if DEBUG:
            ud["debug"] = True
        return ud


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
    AuthorId = db.Column(db.Integer, db.ForeignKey('Users.id'))


class Report(db.Model):
    __tablename__ = "Reports"
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(80), nullable=False)
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), nullable=False)
    BackgroundImg = db.Column(db.String(50))
    AuthorId = db.Column(db.Integer, db.ForeignKey('Users.id'))


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
    PermissionType = db.Column(db.String, default='r', nullable=False)
    ObjectType = db.Column(db.String, nullable=False)
    ObjectId = db.Column(db.Integer, nullable=False)


ADMIN.add_view(ModelView(User, db.session))
ADMIN.add_view(ModelView(Survey, db.session))


def get_user(login: Any = "") -> User:
    """Get user.

    Keyword arguments:
    login -- user's cas login, id or guest if empty string (default: "")

    Return value:
    returns User object
    """

    user = None
    if not login:
        # zamiast tego blędu, jeśli nie ma loginu, to przydziel gościa
        if 'username' not in session:
            session['username'] = GUEST_NAME
        if session['username'] == GUEST_NAME:
            return User.query.filter_by(Role='g').first()
        login = session['username']
    if type(login) is str:
        if '@' in login:
            user = User.query.filter_by(CasLogin=login).first()
        elif re.match("[0-9]+", login):
            user = User.query.filter_by(Pesel=login).first()
        else:
            users = get_all_users()
            for u in users["users"]:
                if u["casLogin"].split("@")[0] == login:
                    user = User.query.filter_by(id=u["id"]).first()
    if type(login) is int:
        user = User.query.filter_by(id=login).first()
    if user is None:
        raise error.API(f'no such user {login}')
    return user


def create_user(cas_login: str, pesel: str, role: str) -> User:
    """Create new user.

    Keyword arguments:
    cas_login -- user's cas login
    pesel -- user's pesel
    role -- role of the user (values: 's','u','g')

    Return value:
    returns User object
    """

    user = User(CasLogin=cas_login, Pesel=pesel, Role=role, FetchData=True)
    db.session.add(user)
    db.session.commit()
    return user


def delete_user(user: User):
    """Delete user.

    Keyword arguments:
    user -- User object
    """

    sur_perms = SurveyPermission.query.filter_by(UserId=user.id).all()
    rep_perms = ReportPermission.query.filter_by(UserId=user.id).all()
    groups = UserGroup.query.filter_by(UserId=user.id).all()
    for sp in sur_perms:
        db.session.delete(sp)
    for rp in rep_perms:
        db.session.delete(rp)
    for g in groups:
        db.session.delete(g)
    db.session.delete(user)
    db.session.commit()


def get_survey(id: int) -> Survey:
    """Get survey by given id.

    Keyword arguments:
    id -- id of a survey

    Return value:
    returns Survey object
    """

    survey = Survey.query.filter_by(id=id).first()
    if survey is None:
        raise error.API('no such survey')
    return survey


def get_report(report_id: int) -> Report:
    """Get report by given id.

    Keyword arguments:
    id -- id of a report

    Return value:
    returns Report object
    """

    report = Report.query.filter_by(id=report_id).first()
    if report is None:
        raise error.API('no such report')
    return report


def get_permission_link(permission: Permission, object_type: Literal['s', 'r'], object_id: int) -> str:
    """Get permission link.

    Keyword arguments:
    permission -- perrmision type (values: 'o', 'w', 'r', 'n')
    object_type -- type of an object (values: 's', 'r')
    object_id -- Id of an object

    Return value:
    returns concatenated salt and link id as a string
    """

    link = Link.query.filter_by(PermissionType=permission, ObjectType=object_type, ObjectId=object_id).first()
    if link is not None:
        return link.Salt + str(link.id)

    bits = secrets.randbits(5*SALT_LENGTH)
    salt = bits.to_bytes(5*SALT_LENGTH//8+1, byteorder='big')
    salt = b32encode(salt).decode('utf-8')[:SALT_LENGTH]
    salt = salt.lower()
    print(salt)
    link = Link(
        Salt=salt,
        PermissionType=permission,
        ObjectType=object_type,
        ObjectId=object_id
    )
    db.session.add(link)
    db.session.commit()
    return link.Salt + str(link.id)


def set_permission_link(tag: str, user: User):
    """Set permission using link.

    Keyword arguments:
    tag -- salt of a link
    user -- User object

    Return value:
    returns permission type, object name and object id
    """

    link = get_link_details(tag)
    if link is None:
        raise error.API('wrong url')
    object_type = link.ObjectType
    if object_type == 's':
        object_name = 'survey'
        get_object = get_survey
        get_permission = get_survey_permission
        set_permission = set_survey_permission
    elif object_type == 'r':
        object_name = 'report'
        get_object = get_report
        get_permission = get_report_permission
        set_permission = set_report_permission
    else:
        raise error.API(f'unknown database object type "{object_type}"')

    object = get_object(link.ObjectId)
    perm = get_permission(object, user)
    if PERMISSION_ORDER.index(perm) >= PERMISSION_ORDER.index(link.PermissionType):
        return link.PermissionType, object_name, object.id
    set_permission(object, user, link.PermissionType, bylink=True)
    return link.PermissionType, object_name, object.id


def get_link_details(tag: str) -> Link:
    """Get link details

    Keyword arguments:
    tag -- salt of a link

    Return value:
    returns Link object
    """

    salt = tag[:SALT_LENGTH]
    id = int(tag[SALT_LENGTH:])
    link = Link.query.filter_by(id=id, Salt=salt).first()
    return link


def get_report_users(report: Report) -> dict:
    """Get users having permission to given report

    Keyword arguments:
    report -- report object

    Return value:
    returns dictionary with user's ids and permission type
    """

    perms = ReportPermission.query.filter_by(ReportId=report.id).all()
    result = {}
    for perm in perms:
        result[perm.UserId] = perm.Type
    return result


def get_survey_users(survey: Survey) -> dict:
    """Get users having permission to given survey

    Keyword arguments:
    survey -- Survey object

    Return value:
    returns dictionary with user's ids and permission type
    """

    perms = SurveyPermission.query.filter_by(SurveyId=survey.id).all()
    result = {}
    for perm in perms:
        result[perm.UserId] = perm.Type
    return result


def get_all_users() -> dict:
    """Get all users

    Return value:
    dictionary with cas login and user id
    """

    users = User.query.all()
    result = []
    for u in users:
        result.append({
            "casLogin": u.CasLogin.split('@')[0],
            "id": u.id
        })
    return {"users": result}


def get_groups() -> List[str]:
    """Get all groups

    Return value:
    list with group names
    """

    user_groups = UserGroup.query.with_entities(UserGroup.Group).distinct()
    return [ug.Group for ug in user_groups]


def set_user_group(user: User, group: str) -> UserGroup:
    """Assign user to a group

    Keyword arguments:
    user -- User object
    group -- group name

    Return value:
    returns UserGroup object
    """

    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group).first()
    if user_group is not None:
        return user_group
    user_group = UserGroup(UserId=user.id, Group=group)
    db.session.add(user_group)
    db.session.commit()
    return user_group


def unset_user_group(user: User, group: str):
    """Unset user from a group

    Keyword arguments:
    user -- User object
    group -- group name
    """

    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group)
    if user_group is None:
        raise error.API('the user is not in the group')
    user_group.delete()
    db.session.commit()


def get_user_groups(user: User) -> List[str]:
    """Get all groups for given user

    Keyword arguments:
    user -- User object

    Return value:
    returns List with group names
    """

    user_groups = UserGroup.query.filter_by(UserId=user.id).all()
    if user_groups is None:
        return []
    return [user_group.Group for user_group in user_groups]


def get_user_surveys(user: User) -> List[Survey]:
    """Get surveys for which the user has permissions.
    For superadmin returns all surveys.

    Keyword arguments:
    user -- User object

    Return value:
    returns list of Survey objects
    """

    if user.Role == 's':
        return Survey.query.all()
    user_surveys = SurveyPermission.query.filter_by(UserId=user.id).all()
    surveys = []
    for survey in user_surveys:
        surveys.append(Survey.query.filter_by(id=survey.SurveyId).first())
    if 'surveys' in session:
        for id in session['surveys']:
            surveys.append(Survey.query.filter_by(id=int(id)).first())
    return surveys


def get_user_reports(user: User) -> List[Report]:
    """Get reports for which the user has permissions.
    For superadmin returns all reports.

    Keyword arguments:
    user -- User object

    Return value:
    returns List of Report objects
    """

    if user.Role == 's':
        return Report.query.all()
    user_reports = ReportPermission.query.filter_by(UserId=user.id).all()
    reports = []
    for report in user_reports:
        reports.append(Report.query.filter_by(id=report.ReportId).first())
    if 'reports' in session:
        for id in session['reports']:
            reports.append(Report.query.filter_by(id=int(id)).first())
    return reports


def get_group_users(group: str) -> List[User]:
    """Get users assigned to given group.

    Keyword arguments:
    group -- name of a group

    Return value:
    returns List of User objects
    """

    user_groups = UserGroup.query.filter_by(Group=group).all()
    users = []
    for user_group in user_groups:
        user = User.query.filter_by(id=user_group.UserId).first()
        if user is not None:
            users.append(user)
    return users


def rename_report(report: Report, name: str):
    """Rename report

    Keyword arguments:
    report -- Report object
    name -- new report name
    """

    report.Name = name
    db.session.commit()


def rename_survey(survey: Survey, name: str):
    """Rename survey

    Keyword arguments:
    survey -- Survey object
    name -- new survey name
    """

    survey.Name = name
    db.session.commit()


def delete_group(group: str):
    """Delete group

    Keyword arguments:
    group -- group name
    """

    UserGroup.query.filter_by(Group=group).delete()
    db.session.commit()


def create_survey(user: User, name: str) -> Survey:
    """Create survey by given user

    Keyword arguments:
    user -- User object
    name -- name of a survey

    Return value:
    returns Survey object
    """

    backgrounds = os.listdir(path.join(ABSOLUTE_DIR_PATH, 'bkg'))
    survey = Survey(Name=name, QuestionCount=0, AuthorId=user.id, BackgroundImg=random.choice(backgrounds))
    db.session.add(survey)
    db.session.commit()
    set_survey_permission(survey, user, 'o')
    return survey


# meta = {"started_on": DateTime, "ends_on": DateTime, "is_active": int}
def set_survey_meta(survey: Survey, name: str, question_count: int, meta: dict):
    """Add meta information for given survey.

    Keyword arguments:
    survey -- Survey object
    name -- name of a survey
    question_count -- amount of questions
    meta -- dict {"started_on": DateTime, "ends_on": DateTime, "is_active": int}
    """

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
    """Get permission of given survey for user.

    Keyword arguments:
    survey -- Survey object
    user -- User object

    Return value:
    returns permission type (values: 'o', 'w', 'r', 'n')
    """

    if 'surveys' in session and str(survey.id) in session['surveys']:
        return session['surveys'][str(survey.id)]

    sp = SurveyPermission.query.filter_by(SurveyId=survey.id, UserId=user.id).first()
    if sp is None and user.Role == 's':
        return ADMIN_DEFAULT_PERMISSION
    elif sp is None:
        return 'n'
    return sp.Type


def set_survey_permission(survey: Survey, user: User, permission: Permission, bylink=False):
    """Set permission of given survey for user.

    Keyword arguments:
    survey -- Survey object
    user -- User object
    permission -- permission type (values: 'o', 'w', 'r', 'n')
    bylink -- is set by link (default: False)

    Return value:
    returns permission type (values: 'o', 'w', 'r', 'n')
    """

    if bylink and user.Role == 'g':
        if 'surveys' not in session:
            session['surveys'] = {}
        if PERMISSION_ORDER.index(permission) >= PERMISSION_ORDER.index('r'):
            session['surveys'][survey.id] = 'r'
        return

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
    """Get survey assigned to given report

    Keyword arguments:
    report -- Report object

    Return value:
    returns Survey object
    """

    if report is None:
        raise error.API('no such report')
    survey = Survey.query.filter_by(id=report.SurveyId).first()
    return survey


def get_report_permission(report: Report, user: User) -> Permission:
    """Get permission of given report for user.

    Keyword arguments:
    report -- Report object
    user -- User object

    Return value:
    returns permission type (values: 'o', 'w', 'r', 'n')
    """

    if 'reports' in session and str(report.id) in session['reports']:
        return session['reports'][str(report.id)]

    rp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if rp is None and user.Role == 's':
        return ADMIN_DEFAULT_PERMISSION
    if rp is None:
        return 'n'
    return rp.Type


def set_report_permission(report: Report, user: User, permission: Permission, bylink=False):
    """Set permission of given report for user.

    Keyword arguments:
    report -- Report object
    user -- User object
    permission -- permission type (values: 'o', 'w', 'r', 'n')
    bylink -- is set by link (default: False)

    Return value:
    returns permission type (values: 'o', 'w', 'r', 'n')
    """

    if bylink and user.Role == 'g':
        if 'reports' not in session:
            session['reports'] = {}
        if PERMISSION_ORDER.index(permission) >= PERMISSION_ORDER.index('r'):
            session['reports'][report.id] = 'r'
        return

    rp = ReportPermission.query.filter_by(ReportId=report.id, UserId=user.id).first()
    if rp is None:
        rp = ReportPermission(ReportId=report.id, UserId=user.id)
        db.session.add(rp)
    if permission != "n":
        rp.Type = permission
    else:
        db.session.delete(rp)
    db.session.commit()


def create_report(user: User, survey: Survey, name: str, author: int) -> Report:
    """Create report by given user

    Keyword arguments:
    user -- User object
    survey -- Survey object to be assigned to Report
    name -- report name
    author -- id of an user

    Return value:
    returns Report object
    """

    report = Report(Name=name, SurveyId=survey.id, AuthorId=author)
    report.BackgroundImg = Survey.query.filter_by(id=survey.id).first().BackgroundImg
    db.session.add(report)
    db.session.commit()
    set_report_permission(report, user, 'o')
    return report


def delete_survey(survey: Survey):
    """Delete survey

    Keyword arguments:
    survey -- Survey object
    """

    # db_path = 'data/' + str(survey.id) + '.db'
    # if os.path.exists(db_path):
    #     os.remove(db_path)
    # xml_path = 'survey/' + str(survey.id) + '.xml'
    # if os.path.exists(xml_path):
    #     os.remove(xml_path)
    SurveyPermission.query.filter_by(SurveyId=survey.id).delete()
    SurveyGroup.query.filter_by(SurveyId=survey.id).delete()
    Survey.query.filter_by(id=survey.id).delete()
    db.session.commit()


def delete_report(report: Report):
    """Delete report

    Keyword arguments:
    report -- Report object
    """

    ReportPermission.query.filter_by(ReportId=report.id).delete()
    ReportGroup.query.filter_by(ReportId=report.id).delete()
    Report.query.filter_by(id=report.id).delete()
    db.session.commit()


def open_survey(survey: Survey) -> sqlite3.Connection:
    """Connect to the survey

    Keyword arguments:
    survey -- Survey object

    Return value:
    returns sqlite3.Connection
    """

    return sqlite3.connect(f"data/{survey.id}.db")


def get_answers(survey_id: int) -> Dict:
    """Get answers for given survey

    Keyword arguments:
    survey_id -- id of na Survey

    Return value:
    returns dictionary with answers
    """

    xml = ET.parse(os.path.join(ABSOLUTE_DIR_PATH, f"survey/{survey_id}.xml"))
    result = {}
    questions = ['single', 'multi', 'groupedsingle']
    for q in questions:
        for b in xml.getroot().iter(q):
            header = b.find('header').text
            header = re.sub('</?\w[^>]*>', '', header).strip(' \n')
            if header not in result:
                result[header]={}
                result[header]["question"]=header
                result[header]["type"]=q
                result[header]["sub_questions"]=[]
                result[header]["values"]={}

            if 'defaultValue' in b.attrib:
                result[header]["values"][b.attrib['defaultValue']]="default"
            if q == 'groupedsingle':
                for item in b.find('items'):
                    result[header]["sub_questions"].append(item.attrib['value'].strip(' '))
            if q != "multi":
                for item in b.find('answers'):
                    result[header]["values"][item.attrib['code']]=item.attrib['value'].strip(' ')
            else:
                for item in b.find('answers'):
                    result[header]["sub_questions"].append(item.attrib['value'].strip(' '))
                result[header]["values"]["0"] = "NIE"
                result[header]["values"]["1"] = "TAK"
    return result


def get_dashboard() -> Dict:
    """Get dashboard for user

    Return value:
    returns dictionary with surveys and reports
    """

    user = get_user()
    user_surveys = get_user_surveys(user)
    result = []
    for survey in user_surveys:
        author = get_user(survey.AuthorId)
        result.append({
            'type': 'survey',
            'endsOn': survey.EndsOn.timestamp() if survey.EndsOn is not None else None,
            'startedOn': survey.StartedOn.timestamp() if survey.StartedOn is not None else None,
            'id': survey.id,
            'name': survey.Name,
            'sharedTo': get_survey_users(survey),
            'ankieterId': survey.AnkieterId,
            'isActive': survey.IsActive,
            'questionCount': survey.QuestionCount,
            'backgroundImg': survey.BackgroundImg,
            'userId': user.id,
            'answersCount': get_answers_count(survey),
            'authorId': author.id,
        })
    user_reports = get_user_reports(user)
    for report in user_reports:
        try:
            survey = get_survey(report.SurveyId)
        except:
            continue
        author = get_user(report.AuthorId)
        result.append({
            'type': 'report',
            'id': report.id,
            'name': report.Name,
            'sharedTo': get_report_users(report),
            'connectedSurvey': {"id": report.SurveyId, "name": survey.Name},
            'backgroundImg': report.BackgroundImg,
            'userId': user.id,
            'authorId': author.id,
        })
    return {"objects": result}


def get_types(conn: sqlite3.Connection) -> Dict[str, str]:
    """Get column types of answers

    Keyword arguments:
    conn -- sqlite3.Connection

    Return value:
    returns dictionary with types
    """

    types = {}
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()
    for row in data:
        types[row[1]] = row[2]
    return types


def get_columns(conn: sqlite3.Connection) -> List[str]:
    """Get column names

    Keyword arguments:
    conn -- sqlite3.Connection

    Return value:
    returns list of column names
    """

    columns = []
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()
    for row in data:
        columns.append(row[1])
    return columns


def get_default_values(survey_id: int):
    xml = ET.parse(f"./survey/{survey_id}.xml")
    result = {}
    questions = ["groupedsingle","single","multi"]
    for b in xml.getroot().iter("questions"):
        for e in list(b):
            if e.tag in questions:
                header=re.sub('</?\w[^>]*>', '', e.find("header").text).strip(' \n') 
                result[header]=set(['9999'])
                if 'defaultValue' in e.attrib:
                    result[header].add(e.attrib['defaultValue'])
                result[header]=list(result[header])
    
    return result


def get_answers_count(survey: Survey) -> int:
    """Get answers amount for survey

    Keyword arguments:
    survey -- Survey object

    Return value:
    returns amount of answers
    """

    conn = open_survey(survey)
    cur = conn.cursor()
    cur.execute("SELECT * FROM data")
    n = len(cur.fetchall())
    conn.close()
    return n


def detect_csv_sep(filename: str) -> str:
    sep = ''
    with open(f'raw/{filename}',"r") as csv_file:
        res = csv.Sniffer().sniff(csv_file.read(1024))
        csv_file.seek(0)
        sep = res.delimiter
    return sep


def csv_to_db(survey: Survey, filename: str):
    """Convert csv file to database

    Keyword arguments:
    survey -- Survey object
    filename -- name of a csv file
    """

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
        name, ext = filename.rsplit('.', 1)
        if ext != "csv":
            file = read_excel(f'raw/{name}.{ext}')
            file.to_csv(f'raw/{name}.csv',encoding='utf-8')
            filename = f'{name}.csv'
        separator=detect_csv_sep(filename)
        df = read_csv(f"raw/{filename}", sep=separator)
        df.columns = df.columns.str.replace('</?\w[^>]*>', '', regex=True)

        for column in df.filter(regex="czas wypełniania").columns:
            df.drop(column, axis=1, inplace=True)

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
    except Exception as e:
        raise error.API(str(e) + ' while parsing csv/xlsx')

