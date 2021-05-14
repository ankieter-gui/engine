from json_response import JsonResponse
import sqlite3
from pandas import concat, read_sql_query
from app import convertCSV
import survey

class Filter:
    def __init__(self, symbol, arity, *types, beg='', end='', sep=', '):
        self.symbol = symbol
        self.arity = arity
        self.types = types
        self.beg = beg
        self.end = end
        self.sep = sep


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
    'share':  share,
    'max':    'max',
    'min':    'min',
    'mode':   'mode',
    'mean':   'mean',
    'median': 'median',
    'std':    'std',
    'var':    'var',
    'count':  'count',
    'sum':    'sum'
}


def get_sql_filter_of(json_filter, column_types):
    global FILTERS

    column, operator, *args = json_filter
    if operator not in FILTERS:
        # TODO: obsługa błędów
        return "OPERATOR DOESN'T EXIST"

    sql_filter = FILTERS[operator]
    if sql_filter.arity != None and len(args) != sql_filter.arity:
        # TODO: obsługa błędów
        return f'OPERATOR {sql_filter.symbol} REQUIRES {sql_filter.arity} ARGUMENTS, BUT {len(args)} WERE GIVEN'

    col_type = column_types[column]
    if col_type not in sql_filter.types:
        # TODO: obsługa błędów
        return "WRONG DATA TYPE"

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
    if 'if' in json_query:
        types = survey.get_data_types(conn)
        filters = list(map(lambda x: get_sql_filter_of(x, types), json_query['if']))
    else:
        filters = ["TRUE"]

    sql = f'SELECT {columns_to_select} FROM data WHERE '+" AND ".join(filters) + ";"
    df = read_sql_query(sql, conn)
    return df

# TODO: convert our aggregator names to pandas
def aggregate(json_query, data):
    global AGGREGATORS

    columns = {}
    for get in json_query['get']:
        for i, column in enumerate(get):
            if column not in columns:
                columns[column] = []
            if json_query['as'][i] in AGGREGATORS:
                columns[column].append(AGGREGATORS[json_query['as'][i]])
            else:
                aggregator = json_query['as']
                return f'NO AGGREGATOR NAMED "{aggregator}"'

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
    df = columns(json_query, conn)
    df = aggregate(json_query, df)
    df = reorder(df)
    return df


# TODO: usunąć po zakończeniu testów
if __name__ == "__main__":
    SURVEY_ID = 'filtered'
    #convertCSV(SURVEY_ID)
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
        "get": [["Price", "Price"]],
        "as": ["count", "mean"],
        "by": ["Age Rating", '*'],
    }

    r = create(json_query, conn)
    print(r)
