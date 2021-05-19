# at least Python 3.8, because of the use of :=
import typing
import error

REQUEST_TABLE = {
    'get': [[str]],
    'as':  [str],
    'if':  ([[str]], 'optional'),
    'by':  ([str],   'optional'),
}

REQUEST_CREATE_SURVEY = {
    'userId':   int,
    'surveyId': int,
    'title':    str,
}

def analyze(tp: typing.Any, obj: typing.Any) -> str:
    if type(tp) is type:
        if type(obj) is tp:
            return False
        else:
            return f'expected {tp.__name__}, got {type(obj).__name__}'
    if type(tp) is list:
        if type(obj) is not list:
            return f'expected {type(tp).__name__}, got {type(obj).__name__}'
        for i, o in enumerate(obj):
            if msg := analyze(tp[0], o):
                return f'in element [{i}]: {msg}'
        return False
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
        return False
    return 'unexpected object type'


def check(tp: typing.Any, obj: typing.Any):
    if msg := analyze(tp, obj):
        raise error.API(msg)
