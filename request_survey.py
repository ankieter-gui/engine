from json_response import JsonResponse
import sqlite3
import pandas as pd

SURVEY_ID = 'survey_id'


def request_survey(request):
    json_data = request.json
    response = JsonResponse()
    if SURVEY_ID not in json_data:
        return response.get_json_error_response(f'Parameter {SURVEY_ID} not found')

    survey_id = json_data[SURVEY_ID]
    df = request_columns(json_data, survey_id)

    return df.to_json()


def request_columns(json_data, survey_id):
    columns = []
    for get in json_data['get']:
        columns += get
    for by in json_data['by']:
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
