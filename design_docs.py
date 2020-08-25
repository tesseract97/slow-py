import os
import json
import csv


def find_view_names(data_file_path):
    """
    All CSV headings are returned, so that they can be cross referenced with the view functions
    of the design documents of the database.


    Parameters
    ----------
    self.data_file_path : str
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

        return headers
    else:
        print("This CSV doesn't exist")
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
    Returns
    -------
    missing_views: iterable
        An array holding all the headers that aren't already in design documents.
    """

    command = 'curl -X GET http://admin:x3n0ntpc@127.0.0.1:5984//_design_docs'
    command = command[:49] + database + command[49:]

    stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
    views = []
    for line in stdout.readlines()[1:-1]:
        comma_index = line.index(",")
        print(type(comma_index))
        kept = line[0, comma_index]
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
    missing_views : array
        Contains all headers that don't have design documents.
    ssh : class
        The instance of SSHClient that's maintaining the ssh connection
    self.database_name : str
        The name of the desired database to deposit data

    Notes
    -----
    The commented line in the first paragraph was formatting specifically for the test data I was using,
    isn't necessary in a CSV that follows specifications in the README.md

    """

    view_commands = []
    for new_view in missing_views:
        view_func = "function(doc) {if (doc.timestamp && doc." + str(new_view) +") emit(doc.timestamp, doc." + str(new_view) + ")}"
        json_new_view = {"views": {"show_specs": {"map": view_func}}}
        final = json.dumps(json_new_view)
        command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
        command2 = command + database + '/_design/' + new_view
        command3 = command2 + " -d '" + final + "'"
        view_commands.append(command3)
    return view_commands
