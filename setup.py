import random
import sqlite3
import pandas

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from faker import Faker
from datetime import datetime
import os
from random_pesel import RandomPESEL
from database import csv_to_db

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


def get_columns_number(df) -> int:
    return len(df.columns)


def remove_duplicates(primary_keys):
    return list(set([i for i in primary_keys]))


def add_permission(pesel, ankieter_id):
    survey = Survey.query.filter_by(AnkieterId=ankieter_id).first()
    user = User.query.filter_by(CasLogin=pesel).first()
    db.session.add(SurveyPermission(SurveyId=survey.id, UserId=user.id, Type=0))

    db.session.commit()


if __name__ == "__main__":
    db.drop_all()
    db.create_all()

    if not os.path.exists('data'):
        os.makedirs('data')
    if not os.path.exists('temp'):
        os.makedirs('temp')
    if not os.path.exists('report'):
        os.makedirs('report')
    if not os.path.exists('bkg'):
        os.makedirs('bkg')

    fake = Faker(locale="pl_PL")

    USERS_AMOUNT = 20
    GROUPS_AMOUNT = 3
    REPORTS_AMOUNT = 5

    pesel = input('Podaj swój pesel\n')
    db.session.add(User(CasLogin=pesel, Role=0, FetchData=False))

    surveys_amount = 0
    for filename in os.listdir('temp'):
        if filename.endswith(".csv"):
            survey_id = filename.split('.')[0]
            csv_to_db(survey_id)
            db.session.add(Survey(
                Name='ankieta testowa',
                AnkieterId=survey_id,
                StartedOn=datetime(2020, 3, random.randint(1, 31)),
                EndsOn=datetime(2021, 6, random.randint(1, 30)),
                IsActive=random.randint(0, 1),
                QuestionCount=columns_number[survey_id]))
            add_permission(pesel, survey_id)
            surveys_amount += 1

    pesel = RandomPESEL()
    for _ in range(USERS_AMOUNT - 1):
        cas_login = pesel.generate()
        role = random.randint(0, 2)
        db.session.add(User(CasLogin=cas_login, Role=role, FetchData=False))

    for _ in range(GROUPS_AMOUNT):
        group_name = fake.company()
        db.session.add(Group(Name=group_name))

    for i in range(REPORTS_AMOUNT):
        report_name = 'Raport ' + str(random.randint(1, 50))
        survey_id = random.randint(1, surveys_amount)
        db.session.add(Report(Name=report_name, SurveyId=survey_id))

    user_group = [(random.randint(1, GROUPS_AMOUNT), random.randint(1, USERS_AMOUNT)) for i in range(20)]
    user_group = remove_duplicates(user_group)
    for primary_key in user_group:
        db.session.add(UserGroup(GroupId=primary_key[0], UserId=primary_key[1]))

    survey_group = [(random.randint(1, surveys_amount), random.randint(1, GROUPS_AMOUNT)) for i in range(20)]
    survey_group = remove_duplicates(survey_group)
    for primary_key in survey_group:
        db.session.add(SurveyGroup(SurveyId=primary_key[0], GroupId=primary_key[1]))

    report_group = [(random.randint(1, REPORTS_AMOUNT), random.randint(1, GROUPS_AMOUNT)) for i in range(20)]
    report_group = remove_duplicates(report_group)
    for primary_key in report_group:
        db.session.add(ReportGroup(ReportId=primary_key[0], GroupId=primary_key[1]))

    survey_permission = [(random.randint(1, surveys_amount), random.randint(2, USERS_AMOUNT)) for i in range(20)]
    survey_permission = remove_duplicates(survey_permission)
    for primary_key in survey_permission:
        db.session.add(SurveyPermission(SurveyId=primary_key[0], UserId=primary_key[1], Type=random.randint(0, 2)))

    report_permission = [(random.randint(1, REPORTS_AMOUNT), random.randint(1, USERS_AMOUNT)) for i in range(20)]
    report_permission = remove_duplicates(report_permission)
    for primary_key in report_permission:
        db.session.add(ReportPermission(ReportId=primary_key[0], UserId=primary_key[1], Type=random.randint(0, 2)))

    db.session.commit()
