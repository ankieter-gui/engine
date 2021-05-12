from json_response import JsonResponse
import sqlite3
import pandas as pd

SURVEY_ID = 'survey_id'


def request_survey(json_request, survey_id):
    df = request_columns(json_request, survey_id)

    return df.to_json()


def request_columns(json_request, survey_id):
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

    # TODO obsługa błędów (np. czy nazwa kolumny istnieje)
    conn = sqlite3.connect("survey_data/" + str(survey_id) + '.db')
    sql = f'SELECT {columns_to_select} FROM data'
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df
