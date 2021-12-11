from typing import Dict, Tuple
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

        # if no result could be chosen, return the value from the first column
        return vals[0]
    return f


def get_default_values(survey: database.Survey) -> Dict:
    """Get default value string for every question in the survey database

    :param survey: The survey
    :type survey: database.Survey
    :return: A dict from question names to a set of its defaults
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


def get_column_mismatches(survey: database.Survey, df: pandas.DataFrame) -> tuple:
    """Check which columns in the schema are not present in the data, and also
    which columns in the data are not present in the schema.

    :param survey: Survey object for which the data was gathered
    :type survey: database.Survey
    :param df: DataFrame containing the compacted data
    :type df: pandas.DataFrame
    :return: A pair of lists of columns that the data lacks, and the extra ones
    :rtype: tuple
    """

    lacking = []
    extra = []

    answers = database.get_answers(survey.id)

    for question, v in answers.items():
        if question not in df.columns.values:
            lacking.append(question)

    for question in df.columns.values:
        if question not in answers:
            extra.append(question)

    return lacking, extra


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


def raw_to_compact(survey: database.Survey, df: pandas.DataFrame, defaults: Dict = {}) -> pandas.DataFrame:
    """Convert a raw Ankieter DataFrame into a compact format suitable for
    data analysis. The change is mainly about joining separate columns that
    in fact represent the same question.

    :param survey: Survey object for which the data was gathered
    :type survey: database.Survey
    :param df: DataFrame containing the raw data
    :type df: pandas.DataFrame
    :param defaults: A dict with default values set for each column name
    :type defaults: Dict
    :return: The compacted data
    :rtype: pandas.DataFrame
    """

    # remove XML tags in question names
    df.columns = df.columns.str.replace('</?\w[^>]*>', '', regex=True)

    # remove all "czas wypełniania" columns
    for column in df.filter(regex="czas wypełniania").columns:
        df.drop(column, axis=1, inplace=True)


    # Get all columns with \.\d+ prefixes, this is how Pandas marks repeated
    # column names
    repeats = df.filter(regex=r'\.\d+$').columns.values

    # Get all column names which are not in 'repeats', these are base names
    # of every column
    uniques = [c for c in df.columns.values if c not in repeats]

    # Now join repeated columns into one named by their base names
    for u in uniques:
        esc = re.escape(u)
        group = list(df.filter(regex=esc+'\.\d+$').columns.values)
        group.append(u)
        df[u] = df[group].aggregate(nodefaults(defaults, u), axis='columns')
        df = df.drop(group[:-1], axis='columns')

    # Convert all remaining defaults to the standard 9999
    for k, v in defaults.items():
        for c in df.columns.values:
            if re.search(k, c):
                df[c] = df[c].replace([int(x) for x in v], 9999)

    return df


def csv_to_db(survey: database.Survey, filename: str, defaults: Dict = {}):
    """Read the source CSV file and save it to a new database

    :param survey: The Survey
    :type survey: Survey
    :param filename: Name of the source CSV file in the raw/ directory
    :type filename: str
    :param defaults: A dict with default values set for each column name
    :type defaults: Dict
    """

    try:
        name, ext = filename.rsplit('.', 1)
        if ext != "csv":
            file = pandas.read_excel(f'raw/{name}.{ext}')
            file.to_csv(f'raw/{name}.csv',encoding='utf-8')
            filename = f'{name}.csv'
        separator = detect_csv_sep(filename)
        df = pandas.read_csv(f"raw/{filename}", sep=separator)

        # convert the data to a format suitable for data analysis
        df = raw_to_compact(survey, df, defaults)

        # if defaults is not empty, there exists an XML to which the data must be adjusted
        if defaults:
            lacking, extra = get_column_mismatches(survey, df)
            if lacking or extra:
                err = error.API('the data is incompatible with survey schema')
                err.data['lacking'] = lacking
                err.data['extra'] = extra
                raise err

        conn = database.open_survey(survey)
        df.to_sql("data", conn, if_exists="replace")
        conn.close()
        return True
    except error.API as err:
        err.add_details('could not save save the data')
        print(str(err))
        raise
    except sqlite3.Error as err:
        raise error.API(str(err))
    except Exception as err:
        raise error.API(str(err) + ' while parsing csv/xlsx')


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
