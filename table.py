from pandas import concat, read_sql_query, DataFrame
import numpy as np
import sqlite3
import database
import grammar
import error

class Filter:
    def __init__(self, symbol, func, arity, *types, beg='', end='', sep=', '):
        self.symbol = symbol
        self.func = func
        self.arity = arity
        self.types = set(types)
        self.beg = beg
        self.end = end
        self.sep = sep


class Aggregator:
    def __init__(self, func, *types):
        self.func = func
        self.types = set(types)


def filter_gt(c):  return lambda n: n if n > c         else np.nan
def filter_lt(c):  return lambda n: n if n < c         else np.nan
def filter_le(c):  return lambda n: n if n <= c        else np.nan
def filter_ge(c):  return lambda n: n if n >= c        else np.nan
def filter_eq(c):  return lambda n: n if n == c        else np.nan
def filter_ne(c):  return lambda n: n if n != c        else np.nan
def filter_in(*c): return lambda n: n if s.isin(c)     else np.nan
def filter_ni(*c): return lambda n: n if not s.isin(c) else np.nan


def cols(s):  return len(s)
def share(s): return s.value_counts().to_dict()
def mode(s):
    s = s.value_counts().to_dict()
    return max(s, key=s.get)


FILTERS = {
    '>':     Filter('>',      filter_gt, 1,    'INTEGER', 'REAL'),
    '<':     Filter('<',      filter_lt, 1,    'INTEGER', 'REAL'),
    '<=':    Filter('<=',     filter_le, 1,    'INTEGER', 'REAL'),
    '>=':    Filter('>=',     filter_ge, 1,    'INTEGER', 'REAL'),
    '=':     Filter('=',      filter_eq, 1,    'INTEGER', 'REAL', 'TEXT'),
    '!=':    Filter('!=',     filter_ne, 1,    'INTEGER', 'REAL', 'TEXT'),
    'in':    Filter('IN',     filter_in, None, 'INTEGER', 'REAL', 'TEXT', beg='(', end=')'),
    'notin': Filter('NOT IN', filter_ni, None, 'INTEGER', 'REAL', 'TEXT', beg='(', end=')')
}


AGGREGATORS = {
    'share':  Aggregator(share,    'INTEGER', 'REAL', 'TEXT'),
    'mode':   Aggregator(mode,     'INTEGER', 'REAL', 'TEXT'),
    'cols':   Aggregator(cols,     'INTEGER', 'REAL', 'TEXT'),
    'count':  Aggregator('count',  'INTEGER', 'REAL', 'TEXT'),
    'max':    Aggregator('max',    'INTEGER', 'REAL'),
    'min':    Aggregator('min',    'INTEGER', 'REAL'),
    'mean':   Aggregator('mean',   'INTEGER', 'REAL'),
    'median': Aggregator('median', 'INTEGER', 'REAL'),
    'std':    Aggregator('std',    'INTEGER', 'REAL'),
    'var':    Aggregator('var',    'INTEGER', 'REAL'),
    'sum':    Aggregator('sum',    'INTEGER', 'REAL')
}


def typecheck(query, types):
    """Check types of survey data

    Keyword arguments:
    query -- survey data
    types -- column types
    """

    grammar.check(grammar.REQUEST_TABLE, query)

    if len(query['get']) == 0 or len(query['get'][0]) == 0:
        raise error.API(f'no columns were requested')

    for i, get in enumerate(query['get']):
        if len(get) != len(query['as']):
            if len(get) != 1:
                raise error.API(f'the number of columns requested by "get" does not equal the number of filters in "as" clause')
            query['get'][i] = get * len(query['as'])

    for agg in query['as']:
        if agg not in AGGREGATORS:
            raise error.API(f'unknown aggregator "{agg}"')

    if 'by' not in query:
        query['by'] = ['*']
    for by in query['by']:
        if not by.startswith('*') and by not in types:
            raise error.API(f'cannot group by "{by}" as there is no such column')

    for get in query['get']:
        for i, col in enumerate(get):
            op = query['as'][i]
            agg = AGGREGATORS[op]
            if col not in types:
                raise error.API(f'no column "{col}" in the data set')
            if types[col] not in agg.types:
                raise error.API(f'aggregator "{op}" supports {", ".join(agg.types)}; got {types[col]} (column "{col}")')

    if 'if' not in query:
        return
    if not query['if']:
        return
    for iff in query['if']:
        if len(iff) < 2:
            raise error.API(f'filter "{" ".join(iff)}" is too short')

        col, op, *args = iff
        flt = FILTERS.get(op, None)
        if flt is None:
            raise error.API(f'unknown filter {op}')
        if type(col) is int and col >= len(query['get'][0]):
            raise error.API(f'cannot filter "{col}" as there\'s no column of that number')
        if type(col) is str and col not in types:
            raise error.API(f'cannot filter by "{col}" as there\'s no such column')
        if flt.arity is not None and len(args) != flt.arity:
            raise error.API(f'filter "{op}" expects {flt.arity} arguments; got {len(args)}')
        if type(col) is int and types[query['get'][0][col]] not in flt.types:
            raise error.API(f'filter "{op}" supports {", ".join(flt.types)}; got {types[query["get"][0][col]]} (column no. {col})')
        if type(col) is str and types[col] not in flt.types:
            raise error.API(f'filter "{op}" supports {", ".join(flt.types)}; got {types[col]} (column "{col}")')


def get_sql_filter_of(json_filter, types):
    column, operator, *args = json_filter

    sql_filter = FILTERS[operator]

    col_type = types[column]

    if col_type == "TEXT":
        args = [f'"{arg}"' for arg in args]
    else:
        args = [f'{arg}' for arg in args]

    result = f'"{column}" {sql_filter.symbol} {sql_filter.beg}{sql_filter.sep.join(args)}{sql_filter.end}'

    return result


def get_pandas_filter_of(json_filter, ctype):
    column, operator, *args = json_filter

    pandas_filter = FILTERS[operator].func

    if ctype == "TEXT":
        args = [f'{arg}' for arg in args]
    else:
        args = [(float(arg) if type(arg) is str else arg) for arg in args]

    result = pandas_filter(*args)

    return result


def columns(query, conn: sqlite3.Connection):
    """Obtain dataframe required to compute the query

    Keyword arguments:
    query -- survey data
    conn -- sqlite3.Connection

    Return value:
    returns dataframe object
    """

    # Create an SQL column name list
    columns = set()
    for get in query['get']:
        columns.update(get)
    columns.update([c for c in query['by'] if not c.startswith('*')])
    columns_to_select = ', '.join([f'"{elem}"' for elem in columns])

    # Create an SQL filter string
    types = database.get_types(conn)
    if 'if' in query and query['if']:
        sql_filters = [f for f in query['if'] if type(f[0]) is not int]
    if sql_filters:
        filters = list(map(lambda x: get_sql_filter_of(x, types), sql_filters))
    else:
        filters = ["TRUE"]
    filters_to_apply = ' AND '.join(filters)

    # Gather the data from the database
    sql = f'SELECT {columns_to_select} FROM data WHERE {filters_to_apply};'
    df = read_sql_query(sql, conn)

    # Apply column-specific filters
    # Obtain column filter list
    filters = {}
    for f in query['if']:
        if type(f[0]) is int:
            if f[0] not in filters:
                filters[f[0]] = []
            filters[f[0]].append(f)

    for get in query['get']:
        for i, column in enumerate(get):
            c = df[[column]]

            name = f'{query["as"][i]} {column}'
            c.columns = [name]

            if i in filters:
                for f in filters[i]:
                    c[name] = c[name].apply(get_pandas_filter_of(f, types[column]))
            df = df.join(c)

    return df


def aggregate(query, data):
    """Aggregate survey data

    Keyword arguments:
    query -- survey data
    data -- dataframe object

    Return value:
    returns aggregated data
    """

    columns = {}
    for get in query['get']:
        for i, column in enumerate(get):
            aggr_name = query['as'][i]
            aggr = AGGREGATORS[aggr_name]
            col_name = f'{aggr_name} {column}'
            if col_name not in columns:
                columns[col_name] = []
            columns[col_name].append(aggr.func)

    if 'by' not in query or not query['by']:
        query['by'] = ['*']

    result = None
    for group in query['by']:
        name = None

        if group.startswith('*'):
            if len(group) > 1:
                name = group[1:]
            group = [True]*len(data)

        ingroups = data.copy().groupby(group).aggregate(columns)

        if name is not None:
            ingroups.index = [name]*len(ingroups.index)
        if result is None:
            result = ingroups
        else:
            result = concat([result, ingroups])
    return result


def reorder(data):
    """Reorder survey data

    Keyword arguments:
    data -- survey data

    Return value:
    returns survey reordered data
    """

    data = data.fillna('nd.')

    data.columns = [f'{aggr} {label}' for label, aggr in data.columns]

    result = {}
    result['index'] = list(map(lambda x: x if x is not True else '*', data.index.tolist()))
    for column in data:
        result[column] = data[column].tolist()
    return result


def create(query, conn: sqlite3.Connection):
    """Create data from survey

    Keyword arguments:
    query -- survey data
    conn -- sqlite3.Connection

    Return value:
    returns survey data
    """

    try:
        types = database.get_types(conn)
        typecheck(query, types)
        data = columns(query, conn)
        data = aggregate(query, data)
        table = reorder(data)
    except error.API as err:
        err.add_details('could not create table')
        raise
    return table


if __name__ == "__main__":
    conn = sqlite3.connect(f'data/1.db')

    queries = []

    queries.append({
        "get": [["Price", "Age Rating"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*Total"],
        "if": [["Age Rating", "in", "4", "9"]]
    })

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
        "get": [["Price", 4]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "in", "4", "9"]]
    })

    queries.append({
        "get": [["Price", "Name"]],
        "as": ["mean", "share"],
        "by": ["Age Rating", "*"],
        "if": [["Age Rating", "notin", "4"]]
    })

    queries.append({
        "as": [],
        "by": [],
        "filter": [],
        "get": []
    })

    for query in queries:
        try:
            r = create(query, conn)
            print(query, "===========")
            print(r)
        except error.API as err:
            print(query, err.message)

    conn.close()
    conn = sqlite3.connect(f'data/2.db')
    r = create({
        "get": [["Na jakim wydziale prowadzony jest kierunek studiów, który oceniał/a P. w tej ankiecie?"]],
        "as": ["max"],
        "by": ["P1 - czas wypełniania"]
    }, conn)
    print(r)
