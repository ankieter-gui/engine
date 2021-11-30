from typing import Literal, Any, List, Dict
from flask_sqlalchemy import SQLAlchemy
from base64 import b32encode
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
    id = db.Column(db.Integer, primary_key=True) #: User Id
    CasLogin = db.Column(db.String(80), unique=True, nullable=False) #: CAS Login
    Pesel = db.Column(db.String(11), nullable=True) #: PESEL number of the user
    FetchData = db.Column(db.Boolean, nullable=False) #: No use of this value is implemented yet
    Role = db.Column(db.String, default='g', nullable=False) #: The user's role in the system

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
    id = db.Column(db.Integer, primary_key=True) #: Survey Id
    Name = db.Column(db.String(80), nullable=False) #: Title of the survey
    AnkieterId = db.Column(db.Integer, unique=True) #: Id of the Survey in USOS Ankieter
    StartedOn = db.Column(db.DateTime, nullable=True) #: Start date of the survey
    EndsOn = db.Column(db.DateTime, nullable=True) #: End date of the survey
    IsActive = db.Column(db.Integer, nullable=True) #: No use of this value is implemented yet
    QuestionCount = db.Column(db.Integer, nullable=True) #: Number of questions in the survey
    BackgroundImg = db.Column(db.String(50), default=None) #: Filename of the survey's backgroun image in the menu
    AuthorId = db.Column(db.Integer, db.ForeignKey('Users.id')) #: Id of the user who created the survey


class Report(db.Model):
    __tablename__ = "Reports"
    id = db.Column(db.Integer, primary_key=True) #: Report Id
    Name = db.Column(db.String(80), nullable=False) #: Title of the report
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), nullable=False) #: Id of the source survey
    BackgroundImg = db.Column(db.String(50)) #: Filename of the report's background image in the menu
    AuthorId = db.Column(db.Integer, db.ForeignKey('Users.id')) #: Id of the user who created the report


class UserGroup(db.Model):
    __tablename__ = "UserGroups"
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True)
    Group = db.Column(db.String(25), primary_key=True)


class SurveyGroup(db.Model):
    __tablename__ = "SurveyGroups"
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), primary_key=True) #: Id of the survey that belongs to a group
    Group = db.Column(db.String(25), primary_key=True) #: The name of the group


class ReportGroup(db.Model):
    __tablename__ = "ReportGroups"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True) #: Id of the report that belongs to a group
    Group = db.Column(db.String(25), primary_key=True) #: The name of the group


class SurveyPermission(db.Model):
    __tablename__ = "SurveyPermissions"
    SurveyId = db.Column(db.Integer, db.ForeignKey('Surveys.id'), primary_key=True) #: The Id of the survey the permission is to
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True) #: The Id of the user that holds the permission
    Type = db.Column(db.String, default='r', nullable=False) #: The type of the permission


class ReportPermission(db.Model):
    __tablename__ = "ReportPermissions"
    ReportId = db.Column(db.Integer, db.ForeignKey('Reports.id'), primary_key=True) #: The Id of the report the permission is to
    UserId = db.Column(db.Integer, db.ForeignKey('Users.id'), primary_key=True) #: The Id of the user that holds the permission
    Type = db.Column(db.String, default='r', nullable=False) #: The type of the permission


class Link(db.Model):
    __tablename__ = "Links"
    id = db.Column(db.Integer, primary_key=True) #: Link Id
    Salt = db.Column(db.String(SALT_LENGTH)) #: The salt of the link
    PermissionType = db.Column(db.String, default='r', nullable=False) #: Perission granted by the link
    ObjectType = db.Column(db.String, nullable=False) #: Type of the object the permission is to
    ObjectId = db.Column(db.Integer, nullable=False) #: Id of the object the permission is to


ADMIN.add_view(ModelView(User, db.session))
ADMIN.add_view(ModelView(Survey, db.session))


def get_user(login: Any = "") -> User:
    """Get a user object from DB.

    :param login: User's CAS login, id or guest if empty string (default: "")
    :raises error.API: no such user
    :return: User object
    :rtype: User
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
    """Create a new user.

    :param cas_login: New user's cas login
    :type cas_login: str

    :param pesel: New user's PESEL number
    :type pesel: str

    :param role: New user's role (values: 's','u','g')
    :type role: Role

    :return: The new user's User object
    :rtype: User
    """

    user = User(CasLogin=cas_login, Pesel=pesel, Role=role, FetchData=True)
    db.session.add(user)
    db.session.commit()
    return user


def delete_user(user: User):
    """Delete user from Users database and their permissions
    from SurveyPermissions and ReportPermissions.

    :param user: The user to be deleted
    :type user: User
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


def get_survey(survey_id: int) -> Survey:
    """Get survey by given id.

    :param survey_id: Survey's id
    :type survey_id: int
    :raises error.API: no such survey
    :return: Returns survey
    :rtype: Survey
    """

    survey = Survey.query.filter_by(id=survey_id).first()
    if survey is None:
        raise error.API('no such survey')
    return survey


def get_report(report_id: int) -> Report:
    """Get report by given id.

    :param id: Id of a report
    :type id: int
    :raises error.API: no such report
    :return: Requested report object
    :rtype: Report
    """

    report = Report.query.filter_by(id=report_id).first()
    if report is None:
        raise error.API('no such report')
    return report


def get_permission_link(permission: Permission, object_type: Literal['s', 'r'], object_id: int) -> str:
    """Create and obtain a permission link.

    :param permission: Permission type (values: 'o', 'w', 'r', 'n')
    :type permission: Role
    :param object_type: Type of the object shared by the link
    :type object_type: Literal['s', 'r']
    :param object_id: Id of the object
    :type object_id: int

    :return: A concatenated salt and link id as a string
    :rtype: str
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

    :param tag: Salt and id string from the link
    :type tag: str
    :param user: User that will gain the permission
    :type user: User

    :return: Returns permission type, object name and object id
    :rtype: Permission, object, int
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

    :param tag: Salt and id string from the link
    :type tag: str

    :return: Returns a Link object
    :rtype: Link
    """

    salt = tag[:SALT_LENGTH]
    id = int(tag[SALT_LENGTH:])
    link = Link.query.filter_by(id=id, Salt=salt).first()
    return link


def get_report_users(report: Report) -> dict:
    """Get users having permission to the given report

    :param report: The report
    :type report: Report

    :return: Returns a dict with user ids as keys and their permissions under them
    :rtype: dict
    """

    perms = ReportPermission.query.filter_by(ReportId=report.id).all()
    result = {}
    for perm in perms:
        result[perm.UserId] = perm.Type
    return result


def get_survey_users(survey: Survey) -> dict:
    """Get users having permission to given survey

    :param survey: The survey
    :type survey: Survey

    :return: Returns a dict with user ids as keys and their permissions under them
    :rtype: dict
    """

    perms = SurveyPermission.query.filter_by(SurveyId=survey.id).all()
    result = {}
    for perm in perms:
        result[perm.UserId] = perm.Type
    return result


def get_all_users() -> dict:
    """Get all users

    :return: Cas logins and users id.
    :rtype: dict
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
    """Get all groups from UserGroups

    :return: List of all groups
    :rtype: List[str]
    """

    user_groups = UserGroup.query.with_entities(UserGroup.Group).distinct()
    return [ug.Group for ug in user_groups]


def set_user_group(user: User, group_name: str):
    """Set group for user. If already exists do nothing.

    :param user: User
    :type user: User
    :param group_name: Name of a group
    :type group_name: str
    """

    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group_name).first()
    if user_group is None:
        user_group = UserGroup(UserId=user.id, Group=group_name)
        db.session.add(user_group)
        db.session.commit()


def unset_user_group(user: User, group: str):
    """Unset user from a group.

    :param user: User object
    :type user: User
    :param group: Group name
    :type group: str
    """

    user_group = UserGroup.query.filter_by(UserId=user.id, Group=group)
    if user_group is None:
        raise error.API('the user is not in the group')
    user_group.delete()
    db.session.commit()


def get_user_groups(user: User) -> List[str]:
    """Get all groups for given user

    :param user: Given user
    :type user: User
    :return: List of user's groups names
    :rtype: List
    """

    user_groups = UserGroup.query.filter_by(UserId=user.id).all()
    if user_groups is None:
        return []
    return [user_group.Group for user_group in user_groups]


def get_user_surveys(user: User) -> List[Survey]:
    """Get surveys for which the user has permissions.
    For administrators it returns all surveys.

    :param user: User object
    :type user: User

    :return: List of Survey objects
    :rtype: List[Survey]
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
    For administrators it returns all reports.

    :param user: User object
    :type user: User

    :return: List of Report objects
    :rtype: List[Report]
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

    :param group: Name of a group
    :rtype group: str

    :return: Returns List of User objects
    :rtype: List[User]
    """

    user_groups = UserGroup.query.filter_by(Group=group).all()
    users = []
    for user_group in user_groups:
        user = User.query.filter_by(id=user_group.UserId).first()
        if user is not None:
            users.append(user)
    return users


def rename_report(report: Report, name: str):
    """Rename report.

    :param report: The Report object
    :type report: Report
    :param name: New report name
    :type name: str
    """

    report.Name = name
    db.session.commit()


def rename_survey(survey: Survey, name: str):
    """Rename survey.

    :param survey: The Survey object
    :type survey: Survey
    :param name: New survey name
    :type name: str
    """

    survey.Name = name
    db.session.commit()


def delete_group(group: str):
    """Delete a group

    :param group: The name of the group
    :type group: str
    """

    UserGroup.query.filter_by(Group=group).delete()
    db.session.commit()


def create_survey(user: User, name: str) -> Survey:
    """Create survey by given user

    :param user: The creator of the new survey
    :type user: User
    :param name: Name of a survey
    :type name: str

    :return: The object of the new survey
    :rtype: Survey
    """

    backgrounds = os.listdir(path.join(ABSOLUTE_DIR_PATH, 'bkg'))
    survey = Survey(Name=name, QuestionCount=0, AuthorId=user.id, BackgroundImg=random.choice(backgrounds))
    db.session.add(survey)
    db.session.commit()
    set_survey_permission(survey, user, 'o')
    return survey


# meta = {"started_on": DateTime, "ends_on": DateTime, "is_active": int}
def set_survey_meta(survey: Survey, name: str, question_count: int, meta: dict):
    """Add meta information of a given survey.

    :param survey: The survey to be modified
    :type survey: Survey
    :param name: The new name of a survey
    :type name: int
    :param question_count: Number of questions
    :type question_count: int
    :param meta: Other information (started_on, ends_on, is_active)
    :type meta: dict
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
    """Get permission of given user for the survey.

    :param survey: The survey
    :type survey: Survey
    :param user: The user whose permissions are to be checked
    :type user: User
    :return: The user's permissions for the survey
    :rtype: Permission
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
    """Set permission of given user for survey.

    :param survey: The survey
    :type survey: Survey
    :param user: The user whose permissions are to be set
    :type user: User
    :param permission: The user's permissions for the survey
    :type permission: Permission
    :param bylink: Is the permission set because of a link? (default: False)
    :type belink: bool
    """

    # If the permission is set because of a link, and the user is a guest
    # then set it only temporarily, in their session.
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
    """Get survey assigned to the given report

    :param report: Report object
    :type report: Report
    :return: The source survey of the report
    :rtype: Survey
    """

    if report is None:
        raise error.API('no such report')
    survey = Survey.query.filter_by(id=report.SurveyId).first()
    return survey


def get_report_permission(report: Report, user: User) -> Permission:
    """Get permission of given user for the report.

    :param report: The report
    :type report: Report
    :param user: The user whose permissions are to be checked
    :type user: User
    :return: The user's permissions for the report
    :rtype: Permission
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
    """Set permission of given user for report.

    :param report: The report
    :type report: Report
    :param user: The user whose permissions are to be set
    :type user: User
    :param permission: The user's permissions for the report
    :type permission: Permission
    :param bylink: Is the permission set because of a link? (default: False)
    :type belink: bool
    """

    # If the permission is set because of a link, and the user is a guest
    # then set it only temporarily, in their session.
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
    """Create report for a given user

    :param user: The creator of the report
    :type user: User
    :param survey: The source survey of the report
    :type survey: Survey
    :param name: The name of the new report
    :type name: str
    :param author: The database id of the creator
    :type author: int
    :return: The newly created report
    :rtype: Report
    """

    report = Report(Name=name, SurveyId=survey.id, AuthorId=author)
    report.BackgroundImg = Survey.query.filter_by(id=survey.id).first().BackgroundImg
    db.session.add(report)
    db.session.commit()
    set_report_permission(report, user, 'o')
    return report


def delete_survey(survey: Survey):
    """Delete survey

    :param survey: The survey to be deleted
    :type survey: Survey
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

    :param report: The report to be deleted
    :type report: Report
    """

    ReportPermission.query.filter_by(ReportId=report.id).delete()
    ReportGroup.query.filter_by(ReportId=report.id).delete()
    Report.query.filter_by(id=report.id).delete()
    db.session.commit()


def open_survey(survey: Survey) -> sqlite3.Connection:
    """Open an SQLite3 connection to the survey database

    :param survey: The survey
    :type survey: Survey
    :return: A connection to the DB of the survey
    :rtype: sqlite3.Connection
    """

    return sqlite3.connect(f"data/{survey.id}.db")


def get_answers(survey_id: int) -> Dict:
    """Get answers for given survey

    :param survey_id: Id of the survey
    :type survey: Survey
    :return: Answers in the survey
    :rtype: Dict
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

    :return: Returns dictionary with surveys and reports
    :rtype: Dict
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
    """Get types for each column in the database.

    :param conn: Connection to the database
    :type conn: sqlite3.Connection
    :return: A dictionary mapping names of columns to SQL names of their types
    :rtype: Dict[str, str]
    """

    types = {}
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()
    for row in data:
        types[row[1]] = row[2]
    return types


def get_columns(conn: sqlite3.Connection) -> List[str]:
    """Get column names in the order just like it is returned from the DB.

    :param conn: Connection to the database
    :type conn: sqlite3.Connection
    :return: A list of column names in the database.
    :rtype: Lis[str]
    """

    columns = []
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()
    for row in data:
        columns.append(row[1])
    return columns


def get_answers_count(survey: Survey) -> int:
    """Get number of answers in the database for a given survey.

    :param survey: The survey
    :type survey: Survey
    :return: The number of answers
    :rtype: int
    """

    conn = open_survey(survey)
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM data")
        n = len(cur.fetchall())
    except:
        n = 0
    conn.close()
    return n
