import paramiko
import json
import os

import db_methods as methods


# SSH FUNCTIONS
def ssh_connect(hostname='132.206.126.208', port=2020, username='lolx', password='x3n0ntpc'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)
    return ssh


def ssh_execute(ssh, command):
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
    ssh.close()
    return 0


# MAIN FUNCTIONS

def create_database(database_name):
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
        return 1


def record_data_from_csv(database_name, csv_file):
    json_file_path = os.path.splitext(csv_file)[0] + ".json"
    data_command = methods.write_to_database(methods.format_and_make_string
                                             (methods.csv_to_json
                                              (csv_file, json_file_path), json_file_path), database_name)
    headers = methods.find_view_names(csv_file)
    if data_command == 1 or headers == 1:
        print("The CSV file can't be found")
        methods.cleanup_directory(csv_file, json_file_path, 1)
        return 1
    try:
        ssh = ssh_connect()
        existing_views = methods.return_existing_views(ssh, database_name)
        missing_views = methods.compare_views(headers, existing_views)
        if ssh_execute(ssh, data_command) < 2:
            print("Data successfully recorded")
            all_view_commands = methods.create_views(missing_views, database_name)
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


if __name__ == '__main__':
    create_database("Hello_World")
    record_data_from_csv("hello_world", "climate_data.csv")
