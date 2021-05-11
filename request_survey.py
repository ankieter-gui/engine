from json_response import JsonResponse
import sqlite3
import pandas


class RequestSurvey:
    SURVEY_ID = 'survey_id'

    def __init__(self, json_data) -> None:
        self.json_data = json_data

    def execute_request(self):
        response = JsonResponse()
        if self.SURVEY_ID not in self.json_data:
            return response.get_json_error_response(f'Parameter {self.SURVEY_ID} not found')
        if len(self.json_data["get"][0]) != len(self.json_data["as"]):
            return response.get_json_error_response('Wrong amount of given "get" and "as" values')

        data = self.get_data()
        return data
        # return response.get_json_success_response('Success', data)

    def get_data(self):
        conn = sqlite3.connect("survey_data/" + str(self.json_data[self.SURVEY_ID]) + '.db')
        df = pandas.read_sql_query('''
        SELECT stopien_stodiow, avg(ocena), count(ocena)
        FROM data
        GROUP BY stopien_stodiow
        ''', conn)
        # cur = conn.cursor()
        # cur.execute('''
        # SELECT stopien_stodiow, avg(ocena), count(ocena)
        # FROM data
        # GROUP BY stopien_stodiow
        # ''')
        # data = cur.fetchall()
        conn.close()
        return df.to_json()
