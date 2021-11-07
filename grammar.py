import typing
import error

REQUEST_TABLE = {
    'get':    [[str]],
    'as':     [str],
    'by':     ([str],  'optional'),
    'if':     ([list], 'optional'),
    'except': ([list], 'optional'),
}

REQUEST_CREATE_SURVEY = {
    'surveyId': int,
    'title':    str,
}

REQUEST_CHANGE_PERMISSIONS = {
    'r': ([int], 'optional'),
    'w': ([int], 'optional'),
    'n': ([int], 'optional'),
}

REQUEST_GROUP = {
    'group': str,
}

REQUEST_USER = {
    'userId': int,
}

REQUEST_SURVEY_LINK = {
    'permission': str,
    'surveyId': int
}

REQUEST_REPORT_LINK = {
    'permission': str,
    'reportId': int
}


def analyze(tp: typing.Any, obj: typing.Any) -> str:
    """Analyze object structure.

    Keyword arguments:
    tp -- expected object structure
    obj -- given object

    Return value:
    returns message after analyze
    """

    if type(tp) is type:
        if type(obj) is tp:
            return ''
        else:
            return f'expected {tp.__name__}, got {type(obj).__name__}'
    if type(tp) is list:
        if type(obj) is not list:
            return f'expected {type(tp).__name__}, got {type(obj).__name__}'
        for i, o in enumerate(obj):
            if msg := analyze(tp[0], o):
                return f'in element [{i}]: {msg}'
        return ''
    if type(tp) is dict:
        if type(obj) is not dict:
            return f'expected {type(tp).__name__}, got {type(obj).__name__}'
        for k, t in tp.items():
            if type(t) is tuple:
                t, *params = t
            else:
                params = []
            if k not in obj:
                if 'optional' in params:
                    continue
                return f'expected key \'{k}\''
            if msg := analyze(t, obj[k]):
                return f'in element \'{k}\': {msg}'
        return ''
    return 'unexpected object type'


def check(tp: typing.Any, obj: typing.Any):
    """Validate object structure.

    Keyword arguments:
    tp -- expected object structure
    obj -- given object
    """

    if msg := analyze(tp, obj):
        raise error.API(msg)
