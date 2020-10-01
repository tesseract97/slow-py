import paramiko
import json
import os

import db_methods as methods


# SSH FUNCTIONS
def ssh_connect(hostname='132.206.126.208', port=2020, username='lolx', password='x3n0ntpc'):
    """
    Creates the initial ssh connection to the CouchDB database on the lolx computer using Paramiko.


    Parameters
    ----------
    hostname : str
        (default is '132.206.126.208')
        Address of the host.
    port : int
        (default is 2020)
        Port of the host.
    username : str
        (default is 'lolx')
        Username to make ssh connection.
    password : str
        (default is 'x3n0ntpc')
        Password to make ssh connection.

    Returns
    -------
    ssh : class
        SSH instance is returned for later access.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)
    return ssh


def ssh_execute(ssh, command):
    """
    Executes ssh command from input string.

    Parameters
    ----------
    ssh : class
        SSH instance that provides access to the database
    command : str
        Desired command as a string that must be executed by paramiko.
    Returns
    -------
    :int
        A nonzero value indicates error on a range with one typically being informational and not program halting
        but two being catastrophic and program halting.
    """
    stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
    for line in stdout.readlines():
        result = json.loads(line)
        result_key = ""
        if type(result) == dict:
            result_key = list(result.keys())[0]
        if type(result) == list:
            result_key = list(result[0].keys())[0]
            result = result[0]
        if result_key == "ok":
            return 0
        if result_key == "error":
            if result[result_key] == "file_exists":
                print("Document already exists")
                return 1
            if result[result_key] == "conflict":
                print("Conflict with document")
                return 1
            if result[result_key] == "not_found":
                print("Database must be created")
                return 1
            if result[result_key] == "compilation_error":
                print("Check the header names in your CSV file and see if they follow convention")
                return 1
            else:
                print("Error: ", result[result_key])
                return 2
        else:
            print("An unknown error has occurred: ", result)
            return 2


def ssh_disconnect(ssh):
    """
    Closes SSH instance.

    Parameters
    ----------
    ssh : class
        SSH instance that provides access to the database
    Returns
    -------
    :int
        A zero value indicates task completion.
    """
    ssh.close()
    return 0


# MAIN FUNCTIONS

def create_database(database_name):
    """
    Creates and initializes database with the create_database and create_first view functions.

    The database needs a dummy view to initialize it in order to easily add custom views later.

    Parameters
    ----------
    database_name: str
        Desired name of database.
    Returns
    -------
    :int
        A nonzero value indicates an error code.
    Throws
    -----
    SSHException
        An issue with the ssh connection occurred.
    Notes
    -----
    The name of database is corrected to be lowercase because that's all that CouchDB allows and it's one of the most
    common initial errors.
    """
    name = database_name.lower()
    db_command = methods.create_database(name)
    view_command = methods.create_first_view(name)
    try:
        ssh = ssh_connect()
        if ssh_execute(ssh, db_command) == 0:
            if ssh_execute(ssh, view_command) == 0:
                print("Successful creation and initialization of database")
        ssh_disconnect(ssh)
    except paramiko.ssh_exception.SSHException:
        return str(1)


def record_data_from_csv(database_name, csv_file):
    """
    Records data from csv into the database with an optional conflict directory.

    Parameters
    ----------
    database_name: str
        Desired name of database.
    csv_file :str
        CSV file name.
    abs_file_path: str
        Absolute file path to CSV.
    conflict_dir: str
        Name of the directory where the CSV will be deposited if any errors occur.
    Returns
    -------
    :int
        A nonzero value indicates an error code with two signifying catastrophic failure and one signifying a failure
        that may be informational instead of breaking the code.
    Throws
    -----
    SSHException
        An issue with the ssh connection occurred.
    Notes
    -----
    The name of database is corrected to be lowercase because that's all that CouchDB allows and it's one of the most
    common initial errors.
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    json_file_path = dir_path + "\\" + os.path.splitext(csv_file)[0] + ".json"
    json_file = methods.csv_to_json(csv_file, json_file_path)
    data = methods.format_and_make_string(json_file, json_file_path)
    data_command = methods.write_to_database(data, database_name)
    headers = methods.find_view_names(csv_file)
    if data_command == 1 or headers == 1:
        print("The CSV file can't be found")
        methods.cleanup_directory(csv_file, json_file_path, 1)
        return 1
    try:
        ssh = ssh_connect()
        name = database_name.lower()
        existing_views = methods.return_existing_views(ssh, name)
        missing_views = methods.compare_views(headers, existing_views)
        if ssh_execute(ssh, data_command) < 2:
            print("Data successfully recorded")
            all_view_commands = methods.create_views(missing_views, name)
            if all_view_commands == 0:
                print("No new views necessary")
                print("Data successfully recorded and indexed")
                methods.cleanup_directory(csv_file, json_file_path, 0)
            else:
                print("Some views necessary")
                clean_up = True
                for command in all_view_commands:
                    if ssh_execute(ssh, command) != 0:
                        clean_up = False
                if clean_up:
                    print("Data successfully recorded and indexed")
                    methods.cleanup_directory(csv_file, json_file_path, 0)
                else:
                    print("View creation unsuccessful, a new conflict document is being created.")
                    methods.cleanup_directory(csv_file, json_file_path, 1)
        else:
            print("Data recording unsuccessful, a new conflict document is being created.")
            methods.cleanup_directory(csv_file, json_file_path, 1)
        ssh_disconnect(ssh)
    except paramiko.ssh_exception.SSHException:
        return 1
    return 'Complete Success'

if __name__ == '__main__':
    create_database("Hello_World")
    record_data_from_csv("hello_world", "climate_data.csv")

