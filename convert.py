from typing import Dict, Tuple
from pathlib import Path
import xml.etree.ElementTree as ET
import database
import sqlite3
import pandas
import error
import json
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


# DEBUG: this function is too strict for practical use or it has a bug
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
        # if defaults:
        #     lacking, extra = get_column_mismatches(survey, df)
        #     if lacking or extra:
        #         err = error.API('the data is incompatible with survey schema')
        #         err.data['lacking'] = lacking
        #         err.data['extra'] = extra
        #         raise err

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


def json_to_xml(survey: database.Survey, survey_json):
    """Convert survey from JSON format to Ankieter xml format.

    :param survey: The Survey that is edited or created
    :type survey: Survey
    :param survey_json: Survey JSON to be converted
    :type survey_json: Dict
    """

    def write_condition(condition,i):
        c=""
        if i>len(condition):
            return "\n"
        if "value" in condition[i-1]:
            e=condition[i-1]
            v=e["value"]
            a=e["aid"]
            c=f'{(i+2)*" "}<condition aid="{a}" value="{v}"/>'
        t=condition[i-1]["type"]
        return f'{(i+1)*" "}<{t}>\n{c}\n{write_condition(condition,i+1)}\n{(i+1)*" "}</{t}>'

    def write_question(question,p=""):
        id=""
        if "id" in question:
            id=question["id"]
        type=question["questionType"]
        required=str(question["commonAttributes"]["required"]).lower()
        collapsed=str(question["commonAttributes"]["collapsed"]).lower()
        defaultVal="9999"
        if question["commonAttributes"]["overrideDefaultValue"]:
            defaultVal=question["commonAttributes"]["defaultValue"]
        question_line=f'{type}'
        if type in ["groupedsingle","single"]:
            question_line+=f' required="{required}" collapsed="{collapsed}" defaultValue="{defaultVal}"'
        if type=="multi":
            question_line+=f' maxAnswers="{question["maxAnswers"]}" required="{required}" defaultValue="{defaultVal}"'
        print(f'{p}<{question_line} id="{id}">', file=xml_out)
        print(f'{p}  <header><![CDATA[{question["header"]}]]></header>',file=xml_out)
        cond=""
        if "condition" in question:
            if len(question["condition"])>0:
                cond=write_condition(question["condition"],1)
                print(f'{p}  <filter>\n{cond}\n{p}  </filter>',file=xml_out)
        if type in ["multi","single"] and "options" in question:
            answers = question["options"]
            print(f'{p}   <answers>',file=xml_out)
            for ans in answers:
                print(f'{p}    <textitem code="{ans["code"]}" value="{ans["value"]}" rotate="{str(ans["rotate"]).lower()}"/>',file=xml_out)
            print(f'{p}   </answers>',file=xml_out)
        if type == "groupedsingle":
            items=question["questions"]
            answers=question["options"]
            print(f'{p}   <items>',file=xml_out)
            for item in items:
                print(f'{p}    <textitem code="{item["code"]}" value="{item["value"]}" rotate="{str(item["rotate"]).lower()}"/>',file=xml_out)
            print(f'{p}   </items>',file=xml_out)
            print(f'{p}   <answers>',file=xml_out)
            for ans in answers:
                print(f'{p}    <textitem code="{ans["code"]}" value="{ans["value"]}" rotate="{str(ans["rotate"]).lower()}"/>',file=xml_out)
            print(f'{p}   </answers>',file=xml_out)
        print(f'{p}</{type}>\n',file=xml_out)

    with open(f'survey/{survey.id}.xml', "w+", encoding='utf-8') as xml_out:
        print('<?xml version="1.0" encoding="UTF-8"?>\n<questionnaire xsi:noNamespaceSchemaLocation="questionnaire.xsd"\nxmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n',file=xml_out)
        for elem in survey_json["elements"]:
            el=elem
            type=el["questionType"]
            if type=="page":
                el=elem["elements"]
                print(f'<page id="{elem["id"]}">',file=xml_out)
                print(f'<header><![CDATA[{elem["header"]}]]></header>',file=xml_out)
                if "condition" in elem:
                    print(elem["condition"])
                    cond=write_condition(elem["condition"],1)
                    print(f'  <filter>\n{cond}\n  </filter>',file=xml_out)
                print(' <questions>',file=xml_out)
                for q in el:
                    write_question(q," ")
            else:
                write_question(el)
            if type=="page":
                print(' </questions>',file=xml_out)
                print('</page>\n', file=xml_out)
        print('</questionnaire>',file=xml_out)


def xml_to_json(survey: database.Survey):
    """Convert survey from Ankieter xml format to json format

    :param survey: The Survey that is edited or created
    :type survey: Survey
    :return: The survey in json format
    :rtype: Dict
    """

    def write_element(question, res):
        res["header"] = question.find("header").text
        res["id"] = question.get("id","")
        res["questionType"] = question.tag
        res["maxLength"]=int(question.get("maxLength",250))
        res["commonAttributes"] = {
            "showId": True if question.get("showID") == "true" else False,
            "showTip": False,
            "overrideDefaultValue": True if question.get("defaultValue") else False,
            "defaultValue": question.get("defaultValue","9999"),
            "tip": True if question.get("tip") == "true" else False,
            "required": True if question.get("required") == "true" else False,
            "orientation": True if question.get("orientation") == "true" else False,
            "collapsed": True if question.get("collapsed") == "true" else False,
            "showTextField": True if question.get("showTextField") == "true" else False,
            "naLabel": True if question.get("naLabel") == "true" else False,
        }
        if question.tag == "multi":
            res["maxAnswers"] = question.get("maxAnswers", "1")
            res["minAnswers"] = question.get("minAnswers", "1")
            res["showAutoTip"] = True if question.get("showAutoTip")=="true" else False
            res["blocking"] = True if question.get("blocking")=="true" else False

        answers = question.find("answers")
        if answers:
            res["options"] = []
            for answer in answers:
                res["options"].append({
                    "code": answer.get("code", ""),
                    "value": answer.get("value", ""),
                    "rotate": True if answer.get("rotate") == "true" else False
                })
        items = question.find("items")
        if items:
            res["questions"] = []
            for item in items:
                res["questions"].append({
                    "code": item.get("code", ""),
                    "value": item.get("value", ""),
                    "rotate": True if item.get("rotate") == "true" else False
                })


        conditions = question.find("filter")
        if conditions:
            res["condition"]=[]
            for cond in conditions:
                for c in cond.iter():
                    if c.tag in ["and","or","not"]:
                        c_r = {
                            "type": c.tag,
                            "value": c[0].get("value",""),
                            "aid": c[0].get("aid", ""),
                            "invert": True if c[0].get("invert") == "true" else False,
                            "relation": c[0].get("relation","=")
                        }
                        if "value" in c_r:
                            if c_r["value"]=="":
                                del c_r["value"]
                        res["condition"].append(c_r)


        return res

    json_out={"elements": [], "title": ""}

    xml = ET.parse(f"survey/{survey.id}.xml")
    questions = ["page","text","information","groupedsingle","single","multi"]
    json_out["title"]=survey.Name
    for child in xml.getroot():
        result={}
        if child.tag in questions:
            if child.tag == "page":
                result["header"] = ""
                if child.find("header").text:
                    result["header"] = child.find("header").text
                result["id"] = child.get("id","")
                result["elements"]=[]
                result["maxLength"]=int(child.get("maxLength",250))
                result["questionType"] = child.tag
                c = child.find("questions")
                for q in c:
                    sub_question={}
                    result["elements"].append(write_element(q, sub_question))
            else:
                result = write_element(child,result)

            json_out["elements"].append(result)

    json_format=json.dumps(json_out)
    return json_format
