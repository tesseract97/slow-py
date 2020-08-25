import paramiko
import json

import slow_control as data
import create_database as create
import design_docs as design


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
        if type(result) == dict:
            result_key = list(result.keys())[0]
        if type(result) == list:
            result_key = list(result[0].keys())[1]
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
            else:
                print("Error: ", result[result_key])
                return 2
        else:
            print(result_key)
            return 2


def ssh_disconnect(ssh):
    ssh.close()
    return 0


# MAIN FUNCTIONS

def create_database(database_name):
    name = database_name.lower()
    db_command = create.create_database(name)
    view_command = create.create_first_view(name)
    try:
        ssh = ssh_connect()
        if ssh_execute(ssh, db_command) == 0:
            if ssh_execute(ssh, view_command) == 0:
                print("Successful creation and initialization of database")
        ssh_disconnect(ssh)
    except paramiko.ssh_exception.SSHException:
        return 1
    except Exception:
        return 1


def record_data_from_csv(database_name, csv_file):
    data_command = data.write_to_database(data.format_and_make_string(data.csv_to_json(csv_file)), database_name)
    headers = design.find_view_names(csv_file)
    try:
        ssh = ssh_connect()
        existing_views = design.return_existing_views(ssh, database_name)
        missing_views = design.compare_views(headers, existing_views)
        if ssh_execute(ssh, data_command) < 2:
            all_view_commands = design.create_views(missing_views, database_name)
            clean_up = True
            for command in all_view_commands:
                if ssh_execute(ssh, command) != 0:
                    clean_up = False
            print("Data successfully recorded and indexed")
            if clean_up == True:
                data.cleanup_directory(csv_file, json_file_path="west_island_update.json")
        ssh_disconnect(ssh)
    except paramiko.ssh_exception.SSHException:
        return 1
    #except Exception:
        #return 1


if __name__ == '__main__':
    create_database("Hello_World")
    record_data_from_csv("hello_world", "climate_data.csv")
