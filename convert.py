from typing import Dict
from pandas import read_csv, read_excel, read_sql_query
from pathlib import Path
import xml.etree.ElementTree as ET
import database
import sqlite3
import error
import csv
import re


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

    def shame(vals):
        counts = {}
        for v in vals:
            c = counts.get(v, 0)
            counts[v] = c+1
        if len(counts) == 0:
            return None
        return min(counts, key=counts.get)

    def nodefaults(group):
        def f(vals):
            # Excluding this case noticeably speeds up processing
            if len(vals) == 1:
                return vals[0]

            if group not in defaults:
                return shame(vals)

            for v in vals:
                if str(v) not in defaults[group]:
                    return v
            return vals[0]
        return f

    try:
        conn = database.open_survey(survey)
        name, ext = filename.rsplit('.', 1)
        if ext != "csv":
            file = read_excel(f'raw/{name}.{ext}')
            file.to_csv(f'raw/{name}.csv',encoding='utf-8')
            filename = f'{name}.csv'
        separator = detect_csv_sep(filename)
        df = read_csv(f"raw/{filename}", sep=separator)
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
            df[u] = df[group].aggregate(nodefaults(u), axis='columns')
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
        df = read_sql_query("SELECT * FROM data", conn, index_col='index')
        Path("temp").mkdir(parents=True, exist_ok=True)
        df.to_csv(f"temp/{survey.id}.csv", encoding='utf-8', index=False)
        conn.close()
    except Exception as e:
        raise error.API(str(e) + ' while parsing db to csv')
