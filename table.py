import pandas
import numpy
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


def filter_gt(c):  return lambda n: n if pandas.notna(n) and n > c      else pandas.NA
def filter_lt(c):  return lambda n: n if pandas.notna(n) and n < c      else pandas.NA
def filter_le(c):  return lambda n: n if pandas.notna(n) and n <= c     else pandas.NA
def filter_ge(c):  return lambda n: n if pandas.notna(n) and n >= c     else pandas.NA
def filter_eq(c):  return lambda n: n if pandas.notna(n) and n == c     else pandas.NA
def filter_ne(c):  return lambda n: n if pandas.notna(n) and n != c     else pandas.NA
def filter_in(*c): return lambda n: n if pandas.notna(n) and n in c     else pandas.NA
def filter_ni(*c): return lambda n: n if pandas.notna(n) and n not in c else pandas.NA


def rows(s):  return len(s.index.unique())
def count(s): return len(s.dropna().index.unique())
def share(s):
    s = s.value_counts().to_dict()

    # Sometimes, if the original series contained a numeric type, the same type
    # will be used for count values. Here they're converted back to ints,
    # as the rest of the software expects.
    for k, v in s.items():
        s[k] = int(v)

    return s
def mode(s):
    s = s.value_counts().to_dict()
    return max(s, key=s.get)


def tobasetypes(s):
    if isinstance(s.dtype, pandas.core.arrays.string_.StringDtype):
        s = s.astype('string')
    if isinstance(s.dtype, pandas.core.arrays.floating.Float64Dtype):
        s = s.astype('float')
    if isinstance(s.dtype, pandas.core.arrays.integer.Int64Dtype):
        s = s.astype('int')
    s = s.apply(lambda x: int(x) if type(x) is float and x.is_integer() else x)
    s = s.apply(lambda x: 0 if type(x) is float and x != x else x)
    return s


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
    'rows':   Aggregator(rows,     'INTEGER', 'REAL', 'TEXT'),
    'count':  Aggregator(count,    'INTEGER', 'REAL', 'TEXT'),
    'max':    Aggregator('max',    'INTEGER', 'REAL'),
    'min':    Aggregator('min',    'INTEGER', 'REAL'),
    'mean':   Aggregator('mean',   'INTEGER', 'REAL'),
    'median': Aggregator('median', 'INTEGER', 'REAL'),
    'std':    Aggregator('std',    'INTEGER', 'REAL'),
    'var':    Aggregator('var',    'INTEGER', 'REAL'),
    'sum':    Aggregator('sum',    'INTEGER', 'REAL')
}


def applymacros(query):
    # Is there a macro at all?
    if 'macro' not in query:
        return query

    # Is the macro empty?
    if type(query['macro']) is not list or not query['macro']:
        raise error.API('a macro must be a list of strings')

    # Save the macro and its arguments
    macro, *args = query['macro']

    # Detect macro type
    if macro == 'count-answers':
        grammar.check(grammar.REQUEST_TABLE_QUESTION_COUNT, query)
        if len(args) <= 0:
            raise error.API(f'the "{macro}" macro requires at least one argument (empty answer value)')
        if 'except' not in query:
            query['except'] = []
        for q in query['get'][0]:
            query['except'].append([q, 'in', *args])

        # Leave only the first column name
        query['get'] = [[query['get'][0][0]]]
        # And aggregate it as rows
        query['as'] = ['rows']
    else:
        raise error.API(f'unknown macro "{macro}"')
    return query


def typecheck(query, types):
    """Check types of survey data

    Keyword arguments:
    query -- survey data
    types -- column types
    """

    # Check the query grammar
    grammar.check(grammar.REQUEST_TABLE, query)

    # Check if the query is not empty
    if len(query['get']) == 0 or len(query['get'][0]) == 0:
        raise error.API(f'no columns were requested')

    # Check if the number of requested columns equals the number of aggregations
    for i, get in enumerate(query['get']):
        if len(get) != len(query['as']):
            if len(get) != 1:
                raise error.API(f'the number of columns requested by "get" does not equal the number of filters in "as" clause')
            query['get'][i] = get * len(query['as'])

    # Check if all requested aggregations are known
    for agg in query['as']:
        if agg not in AGGREGATORS:
            raise error.API(f'unknown aggregator "{agg}"')

    # Check if values are to be groupped by an existent column
    if 'by' not in query:
        query['by'] = ['*']
    for by in query['by']:
        if not by.startswith('*') and by not in types:
            raise error.API(f'cannot group by "{by}" as there is no such column')

    # Check if joins are correct, and add their types to 'types'
    if 'join' in query and query['join']:
        for join in query['join']:
            if join['name'] in types:
                raise error.API(f'cannot join columns into "{join["name"]}" as the name already exists in the dataset')
            if not join['of']:
                raise error.API(f'requested to create a new column out of an empty column list')

            maintype = types[join['of'][0]]
            for of in join['of']:
                if of not in types:
                    raise error.API(f'requested to join column "{of}", but it does not exist')
                if types[of] != maintype:
                    raise error.API(f'requested to join columns of different types ({types[of]}, {maintype})')

            # If this join is fine, add the missing column types for other checks to succeed
            types[join['name']] = maintype

    # Check if requested column types are appropriate for requested aggregators
    for get in query['get']:
        for i, col in enumerate(get):
            op = query['as'][i]
            agg = AGGREGATORS[op]
            if col not in types:
                raise error.API(f'no column "{col}" in the data set')
            if types[col] not in agg.types:
                raise error.API(f'aggregator "{op}" supports {", ".join(agg.types)}; got {types[col]} (column "{col}")')

    # Check if 'except' and 'if' filter types have appropriate types and number of arguments
    for cond in ['if', 'except']:
        if cond in query and query[cond]:
            for iff in query[cond]:
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

    if ctype == "INTEGER":
        args = [(arg if type(arg) is int else int(arg)) for arg in args]
    elif ctype == "REAL":
        args = [(arg if type(arg) is float else float(arg)) for arg in args]
    else:
        args = [f'{arg}' for arg in args]

    result = pandas_filter(*args)

    return result


def columns(query, types, conn: sqlite3.Connection):
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

    if 'join' in query:
        for join in query['join']:
            columns.discard(join['name'])
            columns.update(join['of'])

    columns_to_select = ', '.join([f'"{elem}"' for elem in columns])

    # types = database.get_types(conn)

    # Create an SQL inclusive filter string
    sql_filters = None
    if 'if' in query and query['if']:
        sql_filters = [f for f in query['if'] if type(f[0]) is not int]
    if sql_filters:
        filters = list(map(lambda x: get_sql_filter_of(x, types), sql_filters))
    else:
        filters = ["TRUE"]
    inclusive_filters = ' AND '.join(filters)


    # Create an SQL exclusive filter string
    sql_filters = None
    if 'except' in query and query['except']:
        sql_filters = [f for f in query['except'] if type(f[0]) is not int]
    if sql_filters:
        filters = list(map(lambda x: get_sql_filter_of(x, types), sql_filters))
    else:
        filters = ["FALSE"]
    exclusive_filters = ' AND '.join(filters)


    # Gather the data from the database
    sql = f'SELECT {columns_to_select} FROM data WHERE ({inclusive_filters}) AND NOT ({exclusive_filters});'
    src = pandas.read_sql_query(sql, conn)
    src = src.convert_dtypes()

    group_names = [c for c in query['by'] if not c.startswith('*')]
    groups = src[group_names]
    dst = src[group_names]
    dst.columns = [f'group {c}' for c in group_names]

    # Perform joins on columns
    if 'join' in query and query['join']:
        for join in query['join']:
            # Append the column with data from source columns
            for column in join['of']:
                part = src[[column]]
                part.columns = [join['name']]
                if join['name'] in dst:
                    part = part.join(groups)
                    dst = dst.append(part)
                else:
                    # If no such column yet, it has to be created, not appended
                    dst = dst.join(part)

    # Apply column-specific filters
    # Obtain column filter list
    filters = {}
    if 'if' in query and query['if']:
        for f in query['if']:
            if type(f[0]) is int:
                if f[0] not in filters:
                    filters[f[0]] = []
                filters[f[0]].append(f)

    for get in query['get']:
        for i, column in enumerate(get):
            if column in src:
                series = src[column]
                dtype = series.dtype
            else:
                series = dst[column]
                dtype = series.dtype

            name = f'{query["as"][i]} {column}'

            if i in filters:
                for f in filters[i]:
                    series = series.apply(get_pandas_filter_of(f, types[column]))

            series = series.astype(dtype)
            dst = dst.assign(**{name: series})

    dst.fillna(pandas.NA, inplace=True)

    return dst


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

    groups = [(g if g.startswith('*') else f'group {g}') for g in query['by']]

    result = None
    for group in groups:
        name = None

        if group.startswith('*'):
            if len(group) > 1:
                name = group[1:]
            group = lambda x: True

        ingroups = data.copy().groupby(group).aggregate(columns)

        if name is not None:
            ingroups.index = [name]*len(ingroups.index)
        if result is None:
            result = ingroups
        else:
            result = pandas.concat([result, ingroups])
    return result


def reorder(data):
    """Reorder survey data

    Keyword arguments:
    data -- survey data

    Return value:
    returns survey reordered data
    """

    data = data.apply(tobasetypes)

    data.columns = [f'{label}' for label, aggr in data.columns]

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
        query = applymacros(query)
        typecheck(query, types)
        data = columns(query, types, conn)
        data = aggregate(query, data)
        table = reorder(data)
    except error.API as err:
        err.add_details('could not create table')
        raise
    return table
