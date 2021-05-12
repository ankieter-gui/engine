from json_response import JsonResponse
import sqlite3
import pandas as pd

SURVEY_ID = 'survey_id'


def request_survey(json_request, conn):
    df = request_columns(json_request, conn)

    return df.to_json()


def request_columns(json_request, conn):
    columns = []
    for get in json_request['get']:
        columns += get
    for by in json_request['by']:
        columns.append(by)
    try:
        columns.remove('*')
    except ValueError:
        pass

    columns_to_select = ', '.join([elem for elem in set(columns)])

    sql = f'SELECT {columns_to_select} FROM data'
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df
