from datetime import datetime
from database import *
import faker
import random
import sqlite3
import pandas
import string
import os


def get_survey_quest_num(survey_id: int):
    conn = open_survey(survey_id)
    num = len(get_columns(conn))
    conn.close()
    return num


def get_sample_tuples(n: int, *args: list[int]) -> list[tuple]:
    from itertools import product
    from functools import reduce
    n = min(n, reduce(lambda a, b: a*b, args))
    args = map(lambda x: range(1, x+1), args)
    s = random.sample(sorted(product(*args)), n)
    return s


if __name__ == "__main__":
    for dir in ['data', 'raw', 'report', 'bkg']:
        if not os.path.exists(dir):
            os.makedirs(dir)

    fake = faker.Faker(locale="pl_PL")

    USERS_AMOUNT = 20
    GROUPS_AMOUNT = 3
    REPORTS_AMOUNT = 5

    surveys_amount = 0

    db.drop_all()
    db.create_all()

    for filename in os.listdir('raw'):
        if filename.endswith(".csv"):
            survey_id = filename.split('.')[0]
            csv_to_db(survey_id)
            db.session.add(Survey(
                Name='ankieta testowa',
                AnkieterId=survey_id,
                StartedOn=datetime(2020, 3, random.randint(1, 31)),
                EndsOn=datetime(2021, 6, random.randint(1, 30)),
                IsActive=random.randint(0, 1),
                QuestionCount=get_survey_quest_num(survey_id)))
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
        set_survey_permission(survey.id, 1, 0)
