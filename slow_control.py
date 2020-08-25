"""
Docstring for the lolx_test.py module.

This is a test for the final draft of the slow control code.

"""

import os
import json
import csv


def csv_to_json(data_file_path, json_file_path="slow_control.json"):
    """
    Converts the CSV of data to a JSON file with the intention of writing the JSON directly to the
    desired database.

    First, os checks that `csv_file_path` exists. If not, an error code is returned. If it does, the
    csv file is opened and each row is turned into a python dictionary as an intermediary step. The
    id of each document is set to be the same as the timestamp, and the dictionaries are converted to
    a JSON file which is named `json_file_path` and added to the project folder.


    Parameters
    ----------
    self.data_file_path: str
        The name of the csv file containing the data in the project folder.
    self.json_file_path : str
        The name to assign to the data once it's been converted from csv to json
        (The default is 'west_island_update.json')
    Returns
    -------
    int
        Non-zero value indicates error code.


    Notes
    -----
    Even though `csv_file_path` and `json_file_path` are called paths, they're names of files.
    If the csv or json files were ever stored anywhere other than the project folder, the code must be
    changed to redirect.
    """
    data = []

    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chmod(dir_path, 0o777)
    csv_file_path = dir_path + "/" + data_file_path
    if os.path.isfile(csv_file_path):
        with open(csv_file_path, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for rows in csv_reader:
                rows['_id'] = rows.get('timestamp')
                data.append(rows)
        with open(json_file_path, 'w') as json_file:
            json_file.write(json.dumps(data, indent=4))
            return 0
    return 1

def format_and_make_string(code, json_file_path="slow_controljson"):
    """
    Formats the JSON file into a string that can be written to the database using a curl command.

    Each data value is a string, so an escape character is added before each quotation mark to maintain
    structure.


    Parameters
    ----------
    self.json_file_path : str
        The name of the json file
    error_code: int
        Non-zero value indicates, there is no CSV to take from
    Returns
    -------
    str
        This string is a JSON string with escape characters to preserve string fields

    Notes
    -----
    There is an upper limit of 4300 arguments in the string. An easy way to regulate this is to
    remember that in the initial data csv, rows*columns must be less than or equal to 4300.

    The command string is formatted with escape characters, so that quotation marks remain.

    Right now, all data fields are written to the database as strings.
    """
    if code == 0:

        f = open(json_file_path)
        data = json.load(f)
        data_string = {"docs": []}
        for rows in data:
            data_string["docs"].append(rows)

        string = str(data_string).replace("'", "\"").replace('"', '\\"')
        return '"' + string + '"'
    else:
        return ""


def write_to_database(data, database_name):
    """
    Writes the formatted string to the specified database using a curl command.

    The database name and data are entered into the curl command by appending strings. Then the
    curl command is executed in the environment created by the SSH protocol.

    Parameters
    ----------
    data: str
        The formatted string that came from the JSON file
    database_name: str
        The name of the database to be written to
    Returns
    -------
    int
        Non-zero value indicates error code

    Notes
    -----
    The CouchDB bulk docs API makes it possible to write as many documents to the database as desired

    The credentials for the database are built into the curl command allowing the process to be automated
    """

    database = database_name
    if data == "":
        print("CSV file didn't exist")
        return 1

    command = 'curl -X POST http://admin:x3n0ntpc@127.0.0.1:5984//_bulk_docs -d  -H "Content-Type: application/json"'
    command = command[:50] + database + command[50:65] + data + command[65:]
    return command


def cleanup_directory(data_file_path, json_file_path):
    """
    Cleans up the directory by deleting all files where data has already been stored.

    Print statements indicate to the user whether or not the files were there to delete.

    Parameters
    ----------
    self.json_file_path : str
        Gives the name of the json file that contains the now-stored data
    self.csv_file_path : str
        Gives the name of the csv file that stored the now-stored data
    """
    json_file = json_file_path
    if os.path.exists(json_file):
        os.remove(json_file)
        print("JSON deleted")
    else:
        print("JSON already deleted")

    csv_file = data_file_path
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print("CSV deleted")
    else:
        print("CSV already deleted")

