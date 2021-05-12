from json_response import JsonResponse
import sqlite3
import pandas as pd
from app import convertCSV
from survey_tools import get_column_types

SURVEY_ID = 'filtered'

class Filter:
    def __init__(self, symbol, arity, *types, beg="", end="", sep=", "):
        self.symbol = symbol
        self.arity = arity
        self.types = types
        self.beg = beg
        self.end = end
        self.sep = sep


FILTERS = {
    ">":  Filter(">",  1,    "INTEGER", "REAL"),
    "<":  Filter("<",  1,    "INTEGER", "REAL"),
    "<=": Filter("<=", 1,    "INTEGER", "REAL"),
    ">=": Filter(">=", 1,    "INTEGER", "REAL"),
    "=":  Filter("=",  1,    "INTEGER", "REAL", "TEXT"),
    "!=": Filter("!=", 1,    "INTEGER", "REAL", "TEXT"),
    "in": Filter("IN", None, "INTEGER", "REAL", "TEXT", beg="(", end=")")
}


def request_survey(json_request, conn):
    df = request_columns(json_request, conn)
    return df.to_json()


def request_columns(json_request, conn):
    columns = []
    types = get_column_types(SURVEY_ID)
    for get in json_request['get']:
        columns += get
    for by in json_request['by']:
        columns.append(by)
    try:
        columns.remove('*')
    except ValueError:
        pass

    columns_to_select = ', '.join([f'"{elem}"' for elem in set(columns)])
    if 'if' in json_request:
        filters = list(map(lambda x: get_sql_filter_of(x, types), json_request['if']))
    else:
        filters = ["TRUE"]
    
    sql = f'SELECT {columns_to_select} FROM data WHERE '+" AND ".join(filters) + ";"
    df = pd.read_sql_query(sql, conn)
    return df


def get_sql_filter_of(json_filter, column_types):
    global FILTERS
    column = json_filter[0]
    operator = json_filter[1]
    args = json_filter[2:]
    if operator not in FILTERS:
        return "OPERATOR DOESN'T EXIST"
    sql_filter = FILTERS[operator]
    col_type = column_types[column]
    if col_type not in sql_filter.types:
        # TODO: obsługa błędów
        return "WRONG DATA TYPE"
    if col_type == "TEXT":
        args = [f'"{arg}"' for arg in args]
    result = '"'+column +"\" " + sql_filter.symbol + " " + sql_filter.beg + sql_filter.sep.join(args) + sql_filter.end
    return result   


# TODO: usunąć po zakończeniu testów
if __name__ == "__main__":
    convertCSV(SURVEY_ID)
    conn = sqlite3.connect("survey_data/"+SURVEY_ID+".db")
    json_request = {
        "get": [["Price"]],
        "as": ["avg"],
        "by": ["Average User Rating"],
        "if": [["Age Rating", "in", "4", "9"]]
    }

    print(request_survey(json_request, conn))
