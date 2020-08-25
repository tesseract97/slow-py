import json

def create_database(database_name):
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
    ssh : class
        The instance of SSHClient that's maintaining the ssh connection
    error_code : int
        The result of the previous function that determines whether this function should be run.

    """
    database = database_name
    view_func = "function(doc) { if(doc.timestamp) emit(doc.timestamp, doc.timestamp)}"

    json_new_view = {"views": {"show_specs": {"map": view_func}}}

    final = json.dumps(json_new_view)

    command = 'curl -X PUT -H "Content-Type: application/json" http://admin:x3n0ntpc@127.0.0.1:5984/'
    command2 = command + database + '/_design/timestamp'
    command3 = command2 + " -d '" + final + "'"

    return command3

