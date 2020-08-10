"""
"""

import paramiko
import json


class StartDBConnection():
    """
    Creating a new database and initializing it with a design document.

    The constructor uses the Paramiko package to make an SSH connection to the LOLX computer
    so all curl commands can be run locally. The it runs a curl command that creates a database
    with the user entered name. Then it creates a design document to initialize the database
    that returns the timestamp of the document if it has one. This is just a placeholder that
    makes it easier for slow_control.py to add more design documents later.

    Attributes
    ----------
    database_name : str
        The name of the database to be created

    Methods
    -------
    create_database()
        Creates a database with the desired name.
    create_first_view()
        Initializes the database by adding the first design document.

    Raises
    ------
    SSHException
        If the ssh connection is not made.

    """

    def __init__(self, database_name):
        self.database_name = database_name
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname='132.206.126.208', port=2020, username='lolx', password='x3n0ntpc')
            self.create_first_view(ssh, self.create_database(ssh))

            ssh.close()
        except paramiko.ssh_exception.SSHException:
            print("ssh exception")

    def create_database(self, ssh):
        """
        Creates a database within the CouchDB instance with the provided name.

        Appends database name to the curl command and then executes the curl command to
        create the database. Sends a zero if the database is created and a one if there's
        an error.

        Parameters
        ----------
        ssh : class
            The instance of SSHClient that's maintaining the ssh connection

        Returns
        -------
        int
            Non-zero value indicates error code
        """
        database = self.database_name
        command = 'curl -X PUT http://admin:x3n0ntpc@127.0.0.1:5984/'
        command = command + database
        stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
        for line in stdout.readlines():
            print("Results: ", line)
            print("The database has been successfully created")
            return 0
        print("The database has not been successfully created")
        return 1

    def create_first_view(self, ssh, error_code):
        """
        Creates a design document to initialize the database.

        The point of this function is to create a placeholder design document that makes it easier
        for slow_control.py to add design documents when data is added. Because of this, the design
        document being created simply returns timestamp as a key and value if the document has
        timestamp as an attribute.

        Parameters
        ----------
        ssh : class
            The instance of SSHClient that's maintaining the ssh connection
        error_code : int
            The result of the previous function that determines whether this function should be run.

        """
        if error_code ==0:
            database = self.database_name
            view_func = "function(doc) { if(doc.timestamp) emit(doc.timestamp, doc.timestamp)}"

            json_new_view = {"views": {"show_specs": {"map": view_func}}}

            final = json.dumps(json_new_view)

            command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
            command2 = command + database + '/_design/timestamp'
            command3 = command2 + " -d '" + final + "'"

            stdin, stdout, stderr = ssh.exec_command(command3, get_pty=True)
            for line in stdout.readlines():
                print("Results: ", line)
                print("The database has been successfully initialized")
        else:
            print("The database was not successfully created")


if __name__ == '__main__':
    StartDBConnection("west_island_climate_data")

