# at least Python 3.8, because of the use of :=
import errors

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

def analyze(tp, obj):
    if type(tp) is type:
        if type(obj) is tp:
            return False
        else:
            return f'expected {tp.__name__}, got {type(obj).__name__}'
    if type(tp) is list:
        if type(obj) is not list:
            return f'expected {type(tp).__name__}, got {type(obj).__name__}'
        for i, o in enumerate(obj):
            if err := check(tp[0], o):
                return f'in element [{i}]: {err}'
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
            if err := check(t, obj[k]):
                return f'in element \'{k}\': {err}'
        return False
    return 'unexpected object type'


def check(tp, obj):
    if err := analyze(tp, obj):
        raise errors.APIError(err)
