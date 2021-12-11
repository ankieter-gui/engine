from typing import Dict
from pathlib import Path
import xml.etree.ElementTree as ET
import database
import sqlite3
import pandas
import error
import csv
import re

def antimode(vals: pandas.Series):
    """Return one of the rarest of the values in a series.

    :param vals: The series
    :type vals: pandas.Series
    :return: One of the rarest values
    """

    counts = {}
    for v in vals:
        c = counts.get(v, 0)
        counts[v] = c+1
    if len(counts) == 0:
        return None
    return min(counts, key=counts.get)


def nodefaults(defaults: Dict, name: str):
    """Creates a pandas row aggregator that skips default column values and
    leaves a value from the first column with a non-default. It's used when
    a few columns represent the same variable, in that case the value chosen
    by the user is saved in one column, and the others contain defaults.
    Such a group of columns can be joined into one columns with a use of
    aggregator functions returned by this function.

    :param defaults: Default column values as returned from get_default_values
    :type defaults: Dict
    :param name: A name of the group of columns column to be aggregated
    :type name: str
    :return: A pandas row aggregator
    :rtype: function
    """

    def f(vals):
        # Excluding this case noticeably speeds up processing
        if len(vals) == 1:
            return vals[0]

        if name not in defaults:
            return antimode(vals)

        for v in vals:
            if str(v) not in defaults[name]:
                return v
        return vals[0]
    return f


def get_default_values(survey: database.Survey) -> Dict:
    """Get default value string for every question in the survey database

    :param survey: The survey
    :type survey: Survey
    :return: A dict from question name to a set of its defaults
    :rtype: Dict
    """

    xml = ET.parse(f"survey/{survey.id}.xml")
    questions = ["groupedsingle","single","multi"]

    result = {}
    for b in xml.getroot().iter("questions"):
        for e in list(b):
            if e.tag in questions:
                header = re.sub('</?\w[^>]*>', '', e.find("header").text).strip(' \n')
                if header not in result:
                    result[header] = {'9999'}
                if 'defaultValue' in e.attrib:
                    result[header].add(str(e.attrib['defaultValue']))
    return result


def detect_csv_sep(filename: str) -> str:
    """Detect the separator used in a raw source CSV file.

    :param filename: The name of the raw CSV in the raw/ directory
    :type filename: str
    :return: The separator string
    :rtype: str
    """

    sep = ''
    with open(f'raw/{filename}',"r") as csv_file:
        res = csv.Sniffer().sniff(csv_file.read(1024))
        csv_file.seek(0)
        sep = res.delimiter
    return sep


def csv_to_db(survey: database.Survey, filename: str, defaults: dict = {}):
    """Read the source CSV file and save it to a new database

    :param survey: The Survey
    :type survey: Survey
    :param filename: Name of the source CSV file in the raw/ directory
    :type filename: str
    :param defaults: A dict with default values set for each column name
    :type defaults: Dict
    """

    try:
        conn = database.open_survey(survey)
        name, ext = filename.rsplit('.', 1)
        if ext != "csv":
            file = pandas.read_excel(f'raw/{name}.{ext}')
            file.to_csv(f'raw/{name}.csv',encoding='utf-8')
            filename = f'{name}.csv'
        separator = detect_csv_sep(filename)
        df = pandas.read_csv(f"raw/{filename}", sep=separator)
        df.columns = df.columns.str.replace('</?\w[^>]*>', '', regex=True)

        for column in df.filter(regex="czas wypełniania").columns:
            df.drop(column, axis=1, inplace=True)

        def_values = get_default_values(survey)
        columns = df.columns.values
        for k,v in def_values.items():
            for c in columns:
                if re.search(k,c):
                    df[c] = df[c].replace([int(x) for x in v], 9999)

        repeats = df.filter(regex=r'\.\d+$').columns.values
        uniques = [c for c in columns if c not in repeats]

        for u in uniques:
            esc = re.escape(u)
            group = list(df.filter(regex=esc+'\.\d+$').columns.values)
            group.append(u)
            df[u] = df[group].aggregate(nodefaults(defaults, u), axis='columns')
            df = df.drop(group[:-1], axis='columns')

        df.to_sql("data", conn, if_exists="replace")
        print(f"Database for survey {survey.id} created succesfully")
        conn.close()
        return True
    except sqlite3.Error as err:
        return err
    except Exception as e:
        raise error.API(str(e) + ' while parsing csv/xlsx')


def db_to_csv(survey: database.Survey):
    """Convert db data to csv and write csv file into temp directory

    :param survey: The Survey
    :type survey: Survey
    """

    try:
        conn = database.open_survey(survey)
        df = pandas.read_sql_query("SELECT * FROM data", conn, index_col='index')
        Path("temp").mkdir(parents=True, exist_ok=True)
        df.to_csv(f"temp/{survey.id}.csv", encoding='utf-8', index=False)
        conn.close()
    except Exception as e:
        raise error.API(str(e) + ' while parsing db to csv')
