from app import *
from setup import *
from pandas import read_csv
import os
import sqlite3
import error

# set_user_role
# get_user_role
# get_survey_permission
# set_survey_permission


def get_report_survey(report_id: int) -> int:
    report = Report.query.filter_by(id=report_id).first()
    if report is None:
        raise error.API("no such report")
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
        df = read_csv(f"temp/{survey_id}.csv", sep=",")
        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey_id} created succesfully")
    except sqlite3.Error as e:
        return e
