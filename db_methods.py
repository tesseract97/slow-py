import os
import json
import csv

# CREATE


def create_database(database_name):
    """
    Creates a database within the CouchDB instance with the provided name.

    Appends database name to the curl command and then executes the curl command to
    create the database. Sends a zero if the database is created and a one if there's
    an error.

    Parameters
    ----------

    Returns
    -------
    int
        Non-zero value indicates error code
    """
    database = database_name
    command = 'curl -X PUT http://admin:x3n0ntpc@127.0.0.1:5984/'
    command = command + database
    return command


def create_first_view(database_name):
    """
    Creates a design document to initialize the database.

    The point of this function is to create a placeholder design document that makes it easier
    for slow_control.py to add design documents when data is added. Because of this, the design
    document being created simply returns timestamp as a key and value if the document has
    timestamp as an attribute.

    Parameters
    ----------

    """
    database = database_name
    view_func = "function(doc) { if(doc.timestamp) emit(doc.timestamp, doc.timestamp)}"

    json_new_view = {"views": {"show_specs": {"map": view_func}}}

    final = json.dumps(json_new_view)

    command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
    command2 = command + database + '/_design/timestamp'
    command3 = command2 + " -d '" + final + "'"

    return command3

# DESIGN


def find_view_names(data_file_path):
    """
    All CSV headings are returned, so that they can be cross referenced with the view functions
    of the design documents of the database.


    Parameters
    ----------
    data_file_path : str
        Name of the CSV file in the project folder where the data is stored
    Returns
    -------
    int
        Non-zero value indicates an error
    headers : object
        The reader object that has taken all the headings of the CSV file
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = dir_path + "/" + data_file_path
    if os.path.exists(csv_file_path):
        with open(csv_file_path, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            csv_file.close()
        return headers
    else:
        return 1


def return_existing_views(ssh, database):
    """
    Returns all views already in the database to be cross referenced with the headers from the CSV file.

    The database is added to the curl command by appending strings together. All design document names
    are returned and the design documents are all named for the attribute that they return. The function
    checks if headers is a subset of the set of design documents named. Any headers that aren't there
    are added to an array called `missing_views`. Design documents for these headers need to be created.

    Parameters
    ----------
    ssh: class
        The instance of SSHClient that's maintaining the ssh connection
    database: str
        The name of the database to which data will be uploaded.
    Returns
    -------
    views: list
        An array holding all the headers that are already in design documents.
    """

    command = 'curl -X GET http://admin:x3n0ntpc@127.0.0.1:5984//_design_docs'
    command = command[:49] + database + command[49:]

    stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
    views = []
    for line in stdout.readlines()[1:-1]:
        comma_index = line.index("value")
        kept = line[0:(comma_index-2)] + "}"
        views.append(json.loads(kept)["id"][8:])
    return views


def compare_views(headers, db_views):
    if set(headers).issubset(set(db_views)):
        print("Success")
        return 0
    else:
        s = dict()
        for i in range(len(db_views)):
            s[db_views[i]] = 1

        missing_views = []

        for i in range(len(headers)):
            if headers[i] not in s.keys():
                missing_views.append(headers[i])
        return missing_views


def create_views(missing_views, database):
    """
    Creates design documents with view functions for headers from the CSV that didn't already have
    design documents.


    Parameters
    ----------
    missing_views : array.pyi
        Contains all headers that don't have design documents.

    database : str
        The name of the desired database to deposit data

    Notes
    -----
    The commented line in the first paragraph was formatting specifically for the test data I was using,
    isn't necessary in a CSV that follows specifications in the README.md

    """
    if missing_views == 0:
        print("All views have design documents")
        return 0

    view_commands = []
    for new_view in missing_views:
        view_func = "function(doc) {if (doc.timestamp && doc." + str(new_view) + ") emit(doc.timestamp, doc." + \
                    str(new_view) + ")}"
        json_new_view = {"views": {"show_specs": {"map": view_func}}}
        final = json.dumps(json_new_view)
        command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
        command2 = command + database + '/_design/' + new_view
        command3 = command2 + " -d '" + final + "'"
        view_commands.append(command3)
    return view_commands

# DATA


def csv_to_json(data_file_path, json_file_path):
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
            json_file.close()
            csv_file.close()
            return "File does exist"
    return "File doesn't exist"


def format_and_make_string(code, json_file_path):
    """
    Formats the JSON file into a string that can be written to the database using a curl command.

    Each data value is a string, so an escape character is added before each quotation mark to maintain
    structure.


    Parameters
    ----------
    code : int
         A nonzero value indicates an error and stops the code
    json_file_path : str
        The name of the json file

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
    if code == "File does exist":

        f = open(json_file_path)
        data = json.load(f)
        data_string = {"docs": []}
        for rows in data:
            data_string["docs"].append(rows)

        string = str(data_string).replace("'", "\"").replace('"', '\\"')
        f.close()
        return '"' + string + '"'
    else:
        return "File doesn't exist"


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
    if data == "File doesn't exist":
        return "File doesn't exist"

    command = 'curl -X POST http://admin:x3n0ntpc@127.0.0.1:5984//_bulk_docs -d  -H "Content-Type: application/json"'
    command = command[:50] + database + command[50:65] + data + command[65:]
    return command


def cleanup_directory(data_file_path, json_file_path, error_code):
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

    if error_code == 0:
        csv_file = data_file_path
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print("CSV deleted")
        else:
            print("CSV already deleted")
    else:
        if os.path.exists(data_file_path):
            new_name = os.path.splitext(data_file_path)[0] + "_conflict.csv"
            os.close(data_file_path)
            os.renames(data_file_path, new_name)
            print("CSV renamed")
        else:
            print("CSV already deleted")
