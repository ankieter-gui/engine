import random

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from faker import Faker

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///master.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class User(db.Model):
    __tablename__ = "Users"
    id = db.Column(db.Integer, primary_key=True)
    CasLogin = db.Column(db.String(80), unique=True, nullable=False)
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


def remove_duplicates(primary_keys):
    return list(set([i for i in primary_keys]))


if __name__ == "__main__":
    db.create_all()

    fake = Faker(locale="pl_PL")

    USERS_AMOUNT = 30
    GROUPS_AMOUNT = 10
    SURVEYS_AMOUNT = 30
    REPORTS_AMOUNT = 20

    for _ in range(USERS_AMOUNT):
        cas_login = fake.unique.name()
        role = random.randint(0, 2)
        db.session.add(User(CasLogin=cas_login, Role=role))

    for _ in range(GROUPS_AMOUNT):
        group_name = fake.company()
        db.session.add(Group(Name=group_name))

    for i in range(SURVEYS_AMOUNT):
        survey_name = 'Ankieta ' + str(random.randint(1, 50))
        ankieter_id = i + 10
        db.session.add(Survey(Name=survey_name, AnkieterId=ankieter_id))

    for i in range(REPORTS_AMOUNT):
        report_name = 'Raport ' + str(random.randint(1, 50))
        survey_id = random.randint(1, SURVEYS_AMOUNT)
        db.session.add(Report(Name=report_name, SurveyId=survey_id))

    user_group = [(random.randint(1, GROUPS_AMOUNT), random.randint(1, USERS_AMOUNT)) for i in range(20)]
    user_group = remove_duplicates(user_group)
    for primary_key in user_group:
        db.session.add(UserGroup(GroupId=primary_key[0], UserId=primary_key[1]))

    survey_group = [(random.randint(1, SURVEYS_AMOUNT), random.randint(1, GROUPS_AMOUNT)) for i in range(20)]
    survey_group = remove_duplicates(survey_group)
    for primary_key in survey_group:
        db.session.add(SurveyGroup(SurveyId=primary_key[0], GroupId=primary_key[1]))

    report_group = [(random.randint(1, REPORTS_AMOUNT),random.randint(1, GROUPS_AMOUNT)) for i in range(20)]
    report_group = remove_duplicates(report_group)
    for primary_key in report_group:
        db.session.add(ReportGroup(ReportId=primary_key[0], GroupId=primary_key[1]))

    survey_permission = [(random.randint(1, SURVEYS_AMOUNT),random.randint(1, USERS_AMOUNT)) for i in range(20)]
    survey_permission = remove_duplicates(survey_permission)
    for primary_key in survey_permission:
        db.session.add(SurveyPermission(SurveyId=primary_key[0], UserId=primary_key[1], Type=random.randint(0, 2)))

    report_permission = [(random.randint(1, REPORTS_AMOUNT), random.randint(1, USERS_AMOUNT)) for i in range(20)]
    report_permission = remove_duplicates(report_permission)
    for primary_key in report_permission:
        db.session.add(ReportPermission(ReportId=primary_key[0], UserId=primary_key[1], Type=random.randint(0, 2)))

    db.session.commit()
