from app import convertCSV
import sqlite3
from datetime import datetime


def add_meta(survey_id, started_on, ends_on, is_active):
    conn = sqlite3.connect("survey_data/" + str(survey_id) + '.db')
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS meta")

    sql = '''CREATE TABLE meta(
       StartedOn TEXT NOT NULL,
       EndsOn TEXT NOT NULL,
       IsActive INT,
       QuestionsAmount INT
    )'''
    cur.execute(sql)

    sql = "INSERT INTO META VALUES (%s,%s,%d,%d)" % (started_on, ends_on, is_active, 10)
    cur.execute(sql)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    convertCSV(1)
    convertCSV(2)
    convertCSV(3)

    add_meta(1, datetime(2020, 5, 17).timestamp(), datetime(2021, 5, 17).timestamp(), 1)
    add_meta(1, datetime(2020, 3, 18).timestamp(), datetime(2021, 6, 17).timestamp(), 1)
    add_meta(1, datetime(2020, 4, 19).timestamp(), datetime(2021, 7, 17).timestamp(), 0)
