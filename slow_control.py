"""

"""

import os
import paramiko
import json
import csv


class SlowControlDBConnection:
    """
    Connecting and storing climate data to a slow control database.

    The constructor uses the Paramiko package to make an SSH connection to the LOLX computer
    so all curl commands can be run locally. Then it checks for a data csv file in the project
    folder to write to the database. If found, the data is written to the specified database.
    View documents are written to make the database more robust. Then the ssh connection is closed
    and the csv file and the helper file are deleted to eliminate redundancy.


    Attributes
    ----------
    database_name : str
        Name of the desired database to store data within larger CouchDB instance.
    data_file_path : str
        Name of csv in the project folder that holds all the data.
    json_file_path : str
        Name of the json that will be created in the project folder.
        Default value is 'slow_control_update.json'

    Methods
    -------
    csv_to_json()
        Creates a JSON file from a csv in the project folder.
    format_and_make_string()
        Formats the JSON file and convert to string for upload.
    write_to_database_with_data_string()
        Sends the JSON string to the specified database.
    clean_up_directory()
        Deletes the JSON and CSV files in order to avoid duplication of data.
    DesignDocs()
        Calls the DesignDocs class to write view statements.

    Raises
    ------
    SSHException
        If the ssh connection is not made.

    """

    def __init__(self, database_name: str, data_file_path: str, json_file_path: str = 'slow_control_update.json') -> object:
        self.database_name = database_name
        self.data_file_path = data_file_path
        self.json_file_path = json_file_path
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname='132.206.126.208', port=2020, username='lolx', password='x3n0ntpc')
            self.write_to_database_with_data_string(ssh, self.format_and_make_string(self.csv_to_json()))
            DesignDocs(ssh, database_name)
            ssh.close()
            self.cleanup_directory()
        except paramiko.ssh_exception.SSHException:
            print("ssh exception")

    def csv_to_json(self):
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
        csv_file_path = self.data_file_path
        json_file_path = self.json_file_path
        data = []

        if os.path.exists(csv_file_path):

            with open(csv_file_path, encoding='utf-8-sig') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for rows in csv_reader:
                    rows['_id'] = rows.get('timestamp')
                    data.append(rows)
            with open(json_file_path, 'w') as json_file:
                json_file.write(json.dumps(data, indent=4))
                return 0
            return 1

    def format_and_make_string(self, error_code):
        """
        Formats the JSON file into a string that can be written to the database using a curl command.

        Each data value is a string, so an escape character is added before each quotation mark to maintain
        structure.


        Parameters
        ----------
        self.json_file_path : str
            The name of the json file
        error_code: int
            Non-zero value indicates that there is no CSV to take from

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
        if error_code == 0:

            f = open(self.json_file_path)
            data = json.load(f)
            data_string = {"docs": []}
            for rows in data:
                data_string["docs"].append(rows)

            string = str(data_string).replace("'", "\"").replace('"', '\\"')
            return '"' + string + '"'
        else:
            return ""

    def write_to_database_with_data_string(self, ssh, data):
        """
        Writes the formatted string to the specified database using a curl command.

        The database name and data are entered into the curl command by appending strings. Then the
        curl command is executed in the environment created by the SSH protocol.

        Parameters
        ----------
        ssh : class
           The instance of SSHClient that's maintaining the ssh connection
        data: str
            The formatted string that came from the JSON file
        self.database_name: str
            The name of the desired database.

        Returns
        -------
        int
            Non-zero value indicates error code

        Notes
        -----
        The CouchDB bulk docs API makes it possible to write as many documents to the database as desired

        The credentials for the database are built into the curl command allowing the process to be automated

        """

        database = self.database_name
        # if data doesn't exist, message is sent to user
        if data == 1:
            print("CSV file didn't exist")
            return 1

        command = 'curl -X POST http://admin:x3n0ntpc@127.0.0.1:5984//_bulk_docs -d  -H "Content-Type: application/json"'
        command = command[:50] + database + command[50:65] + data + command[65:]
        print(command)
        stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
        for line in stdout.readlines():
            print("Results: ", line)

    def cleanup_directory(self):
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
        json_file = self.json_file_path
        if os.path.exists(json_file):
            os.remove(json_file)
            print("JSON deleted")
        else:
            print("JSON already deleted")

        csv_file = self.data_file_path
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print("CSV deleted")
        else:
            print("CSV already deleted")


class DesignDocs:
    """
    Helper class that writes design documents to the database so that the headings of the data file will
    correspond to view functions.

    This class first checks the input CSV file for all headings. Then it checks the database for design
    documents that are already there, making sure that all the headings are already included in the view
    functions. If not, the view functions are written to the database in new design documents.

    Attributes
    ----------
    ssh : class
        The instance of SSHClient that's maintaining the ssh connection
    database_name : str
        Name of the desired database to store data within larger CouchDB instance.
    data_file_path : str
        Name of csv in the project folder that holds all the data.

    Methods
    -------
    find_view_names()
    return_views()
    create_views()
    """

    def __init__(self, ssh, database_name, data_file_path):
        self.database_name = database_name
        self.data_file_path = data_file_path
        self.create_views(self.return_views(self.find_view_names(), ssh), ssh)

    def find_view_names(self):
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
        csv_file_path = self.data_file_path

        if os.path.exists(self.data_file_path):

            with open(csv_file_path, encoding='utf-8-sig') as csv_file:
                csv_reader = csv.reader(csv_file)
                headers = next(csv_reader)

            return headers
        else:
            print("This CSV doesn't exist")
            return 1

    def return_views(self, headers, ssh):
        """
        Returns all views already in the database to be cross referenced with the headers from the CSV file.

        The database is added to the curl command by appending strings together. All design document names
        are returned and the design documents are all named for the attribute that they return. The function
        checks if headers is a subset of the set of design documents named. Any headers that aren't there
        are added to an array called `missing_views`. Design documents for these headers need to be created.

        Parameters
        ----------
        headers: iterable
            The reader object that has taken all the headings of the CSV file
        ssh: class
            The instance of SSHClient that's maintaining the ssh connection

        Returns
        -------
        missing_views: iterable
            An array holding all the headers that aren't already in design documents.

        """
        database = self.database_name

        command = 'curl -X GET http://admin:x3n0ntpc@127.0.0.1:5984//_design_docs'
        command = command[:49] + database + command[49:]
        print(command)
        stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
        views = []

        for line in stdout.readlines()[1:-1]:
            views.append(json.loads(line)["id"][8:])

        if set(headers).issubset(set(views)):
            print("Success")
            return 0
        else:
            s = dict()
            for i in range(len(views)):
                s[views[i]] = 1

            missing_views = []

            for i in range(len(headers)):
                if headers[i] not in s.keys():
                    missing_views.append(headers[i])
            return missing_views

    def create_views(self, missing_views, ssh):
        """
        Creates design documents with view functions for headers from the CSV that didn't already have
        design documents.

        The design document is built through appending strings that are either crucial to the structure
        of the design document or variable names and then the curl command is executed using SSH protocol
        to put all the design documents in the database.

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

        database = self.database_name

        for new_view in missing_views:
            #new_view = new_view.replace(" ", "_").replace("°", "").replace("(", "").replace(")", "")
            view_func = "function(doc) { emit(doc.timestamp, doc." + str(new_view) + ")}"

            json_new_view = {"views": {"show_specs": {"map": view_func}}}

            final = json.dumps(json_new_view)

            command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
            command2 = command + database + '/_design/' + new_view
            command3 = command2 + " -d '" + final + "'"

            stdin, stdout, stderr = ssh.exec_command(command3, get_pty=True)
            for line in stdout.readlines():
                print("Results: ", line)



if __name__ == '__main__':
    SlowControlDBConnection("slowcontroldb", 'slow_control_db.csv')
