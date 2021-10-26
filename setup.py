#!/usr/bin/python

from datetime import datetime, timedelta
from itertools import product
from functools import reduce
from typing import List, Dict
from database import *
import random
import string
import os


def get_survey_quest_num(survey: Survey) -> int:
    """Get number of questions for given survey

    Keyword arguments:
    survey -- Survey object

    Return value:
    returns permission type, object name and object id
    """

    conn = open_survey(survey)
    num = len(get_columns(conn))
    conn.close()
    return num


def get_sample_tuples(n: int, *args: int) -> List[tuple]:
    """Generate sample tuples.

    Keyword arguments:
    n -- number of rows to generate

    Return value:
    returns list of generated values
    """

    n = min(n, reduce(lambda a, b: a*b, args))
    args = map(lambda x: range(1, x+1), args)
    s = random.sample(sorted(product(*args)), n)
    return s


def setup():
    db.drop_all()
    db.create_all()

    for dir in ['data', 'raw', 'report', 'bkg']:
        if not os.path.exists(dir):
            os.makedirs(dir)

    bkgs = os.listdir('bkg')

    surveys_amount = 0

    for filename in os.listdir('raw'):
        if filename.endswith(".csv"):
            survey = Survey(
                Name=f'ankieta z {filename}',
                StartedOn=datetime.now(),
                EndsOn=datetime.now() + timedelta(days=56),
                IsActive=random.randint(0, 1),
                BackgroundImg=random.choice(bkgs))
            db.session.add(survey)
            db.session.commit()
            csv_to_db(survey, filename)
            survey.QuestionCount = get_survey_quest_num(survey)
            db.session.commit()
            surveys_amount += 1

    db.session.add(User(CasLogin='admin',
                        Pesel='9999999999',
                        Role='s',
                        FetchData=False))

    db.session.add(User(CasLogin=GUEST_NAME,
                        Pesel='9999999998',
                        Role='g',
                        FetchData=True))

    db.session.commit()

if __name__ == "__main__":
    setup()

