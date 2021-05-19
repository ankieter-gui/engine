import sqlite3
from pandas import concat, read_sql_query
from app import convert_csv
import database
import grammar
import error

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
    'max':    Aggregator('max',    'INTEGER', 'REAL'),
    'min':    Aggregator('min',    'INTEGER', 'REAL'),
    'mode':   Aggregator('mode',   'INTEGER', 'REAL', 'TEXT'),
    'mean':   Aggregator('mean',   'INTEGER', 'REAL'),
    'median': Aggregator('median', 'INTEGER', 'REAL'),
    'std':    Aggregator('std',    'INTEGER', 'REAL'),
    'var':    Aggregator('var',    'INTEGER', 'REAL'),
    'count':  Aggregator('count',  'INTEGER', 'REAL', 'TEXT'),
    'sum':    Aggregator('sum',    'INTEGER', 'REAL')
}

def is_str(x):
    return isinstance(x, str)

def is_list_of_str(x):
    return isinstance(x, list) and all(map(is_str, x))

def is_list_of_list_of_str(x):
    return isinstance(x, list) and all(map(is_list_of_str, x))

def typecheck(json_query, types):
    grammar.check(grammar.REQUEST_TABLE, json_query)

    if not all(map(lambda x: len(x) == len(json_query['as']), json_query['get'])):
        raise error.API(f'the number of columns requested by "get" does not equal the number of filters in "as" clause')

    for agg in json_query['as']:
        if agg not in AGGREGATORS:
            raise error.API(f'unknown aggregator "{agg}"')

    if 'by' not in json_query:
        json_query['by'] = ['*']
    for by in json_query['by']:
        if by != '*' and by not in types:
            raise error.API(f'cannot group by "{by}" as there is no such column')

    for get in json_query['get']:
        for i, col in enumerate(get):
            op = json_query['as'][i]
            agg = AGGREGATORS[op]
            if col not in types:
                raise error.API(f'no column "{col}" in the data set')
            if types[col] not in agg.types:
                raise error.API(f'aggregator "{op}" supports {", ".join(agg.types)}; got {types[col]} (column "{col}")')

    if 'if' not in json_query:
        return
    for iff in json_query['if']:
        if len(iff) < 2:
            raise error.API(f'filter "{" ".join(iff)}" is too short')

        col, op, *args = iff
        flt = FILTERS.get(op, None)
        if flt is None:
            raise error.API(f'unknown filter {op}')
        if col not in types:
            raise error.API(f'cannot filter by "{col}" as theres no such column')
        if flt.arity is not None and len(args) != flt.arity:
            raise error.API(f'filter "{op}" expects {flt.arity} arguments; got {len(args)}')
        if types[col] not in flt.types:
            raise error.API(f'filter "{op}" supports {", ".join(flt.types)}; got {types[col]} (column "{col}")')


def get_sql_filter_of(json_filter, types):
    global FILTERS

    column, operator, *args = json_filter

    sql_filter = FILTERS[operator]

    col_type = types[column]

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
    types = database.get_types(conn)
    if 'if' in json_query:
        filters = list(map(lambda x: get_sql_filter_of(x, types), json_query['if']))
    else:
        filters = ["TRUE"]

    sql = f'SELECT {columns_to_select} FROM data WHERE '+" AND ".join(filters) + ";"
    df = read_sql_query(sql, conn)
    return df


def aggregate(json_query, data):
    global AGGREGATORS

    columns = {}
    for get in json_query['get']:
        for i, column in enumerate(get):
            aggr_name = json_query['as'][i]
            aggr = AGGREGATORS[aggr_name]
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
        types = database.get_types(conn)

        typecheck(json_query, types)
        data = columns(json_query, conn)
        data = aggregate(json_query, data)
        table = reorder(data)
    except error.API as err:
        err.add_details('could not create table')
        raise
    return table


# TODO: usunąć po zakończeniu testów
if __name__ == "__main__":
    SURVEY_ID = '1'
    conn = sqlite3.connect(f'data/{SURVEY_ID}.db')

    queries = []

    queries.append({
        "get": [["Price",               "Price"],
                ["Average User Rating", "Average User Rating"]],
        "as": ["mean", "var"],
        "by": ["Age Rating", "*"],
        "if": [["9"]]
    })

    queries.append({
        "get": [["Price", "Age Rating"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Name", ">", "4"]]
    })

    queries.append({
        "get": [["Price", "Age Rating"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", ">", "4", "9"]]
    })

    queries.append({
        "get": [["Price", "Name"]],
        "as": ["count", "mean"],
        "by": ["Age Rating", '*'],
    })

    queries.append({
        "get": [["Price", "Age Rating"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "in", "4", "9"]]
    })

    queries.append({
        "get": [["Price", 4]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "in", "4", "9"]]
    })

    for query in queries:
        try:
            r = create(query, conn)
            print(r)
        except error.API as err:
            print(err.message)
