from json_response import JsonResponse
import sqlite3
from pandas import concat, read_sql_query
from app import convert_csv
import survey
from errors import *

class Filter:
    def __init__(self, symbol, arity, *types, beg='', end='', sep=', '):
        self.symbol = symbol
        self.arity = arity
        self.types = set(types)
        self.beg = beg
        self.end = end
        self.sep = sep


class Aggregator:
    def __init__(self, func, *types):
        self.func = func
        self.types = set(types)


FILTERS = {
    '>':  Filter('>',  1,    'INTEGER', 'REAL'),
    '<':  Filter('<',  1,    'INTEGER', 'REAL'),
    '<=': Filter('<=', 1,    'INTEGER', 'REAL'),
    '>=': Filter('>=', 1,    'INTEGER', 'REAL'),
    '=':  Filter('=',  1,    'INTEGER', 'REAL', 'TEXT'),
    '!=': Filter('!=', 1,    'INTEGER', 'REAL', 'TEXT'),
    'in': Filter('IN', None, 'INTEGER', 'REAL', 'TEXT', beg='(', end=')')
    # TODO: between, notin
}


def share(s): return s.value_counts().to_dict()
AGGREGATORS = {
    'share':  Aggregator(share,    'INTEGER', 'REAL', 'TEXT'),
    'max':    Aggregator('max',    'INTEGER', 'REAL', ),
    'min':    Aggregator('min',    'INTEGER', 'REAL', ),
    'mode':   Aggregator('mode',   'INTEGER', 'REAL', 'TEXT'),
    'mean':   Aggregator('mean',   'INTEGER', 'REAL', ),
    'median': Aggregator('median', 'INTEGER', 'REAL', ),
    'std':    Aggregator('std',    'INTEGER', 'REAL', ),
    'var':    Aggregator('var',    'INTEGER', 'REAL', ),
    'count':  Aggregator('count',  'INTEGER', 'REAL', 'TEXT'),
    'sum':    Aggregator('sum',    'INTEGER', 'REAL', )
}


def get_sql_filter_of(json_filter, types):
    global FILTERS

    column, operator, *args = json_filter
    if operator not in FILTERS:
        raise APIError(f'filter "{operator}" does not exist')

    sql_filter = FILTERS[operator]
    if sql_filter.arity != None and len(args) != sql_filter.arity:
        raise APIError(f'filter "{operator}" expects {sql_filter.arity} arguments, got {len(args)}')

    col_type = types[column]
    if col_type not in sql_filter.types:
        raise APIError(f'filter "{operator}" supports types {", ".join(sql_filter.types)}; got {col_type} (column "{column}")')

    if col_type == "TEXT":
        args = [f'"{arg}"' for arg in args]

    result = f'"{column}" {sql_filter.symbol} {sql_filter.beg}{sql_filter.sep.join(args)}{sql_filter.end}'

    return result


def columns(json_query, conn):
    columns = set()
    for get in json_query['get']:
        columns.update(get)
    columns.update(json_query['by'])
    if '*' in columns:
        columns.remove('*')

    columns_to_select = ', '.join([f'"{elem}"' for elem in columns])
    types = survey.get_types(conn)
    if 'if' in json_query:
        filters = list(map(lambda x: get_sql_filter_of(x, types), json_query['if']))
    else:
        filters = ["TRUE"]

    sql = f'SELECT {columns_to_select} FROM data WHERE '+" AND ".join(filters) + ";"
    df = read_sql_query(sql, conn)
    return df, types

# TODO: convert our aggregator names to pandas
def aggregate(json_query, data, types):
    global AGGREGATORS

    columns = {}
    for get in json_query['get']:
        for i, column in enumerate(get):
            aggr_name = json_query['as'][i]
            if aggr_name not in AGGREGATORS:
                raise APIError(f'no aggregator named "{aggr_name}"')

            aggr = AGGREGATORS[aggr_name]
            col_type = types[column]
            if col_type not in aggr.types:
                raise APIError(f'aggregator "{aggr_name}" supports types {", ".join(aggr.types)}; got {col_type} (column "{column}")')

            if column not in columns:
                columns[column] = []

            columns[column].append(aggr.func)

    if 'by' not in json_query:
        json_query['by'] = ['*']

    result = None
    for group in json_query['by']:
        if group == '*':
            group = [True]*len(data)
        ingroups = data.copy().groupby(group)

        if result is not None:
            result = concat([result, ingroups.aggregate(columns)])
        else:
            result = ingroups.aggregate(columns)
    return result


def reorder(data):
    data.columns = [f'{aggr} {label}' for label, aggr in data.columns]

    result = {}
    result['index'] = list(map(lambda x: x if x is not True else '*', data.index.tolist()))
    for column in data:
        result[column] = data[column].tolist()
    return result


def create(json_query, conn):
    try:
        aggrs = len(json_query['as'])
        for get in json_query['get']:
            if len(get) != aggrs:
                raise APIError(f'got {aggrs} aggregators, but some get clauses have {len(get)} columns')

        data, types = columns(json_query, conn)
        data = aggregate(json_query, data, types)
        table = reorder(data)
    except APIError as err:
        err.add_detail('could not create table')
        raise
    return table


# TODO: usunąć po zakończeniu testów
if __name__ == "__main__":
    SURVEY_ID = '1'
    #convert_csv(SURVEY_ID)
    conn = sqlite3.connect(f'data/{SURVEY_ID}.db')
    '''json_query = {
        "get": [["Price",               "Price"],
                ["Average User Rating", "Average User Rating"]],
        "as": ["mean", "var"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "in", "4", "9"]]
    }

    print(create(json_query, conn))'''

    '''json_query = {
        "get": [["Price", "Age Rating"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "in", "4", "9"]]
    }'''

    json_query = {
        "get": [["Price", "Name"]],
        "as": ["count", "mean"],
        "by": ["Age Rating", '*'],
    }

    try:
        r = create(json_query, conn)
        print(r)
    except APIError as err:
        print(err.as_dict())
