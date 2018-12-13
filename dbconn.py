import psycopg2
import constants

def getDBConn():
    """
    connects to Database and returns connection object
    return: connection object
    """
    # builds the connection string
    str_conn_string = constants.DB_CONN_STRING % (constants.DB_HOST, constants.DB_NAME, constants.DB_USER_NAME, constants.DB_PASSWORD)
	
    # connect to database
    return psycopg2.connect(str_conn_string)

def disableMerge(obj_conn):
    """
    disables nestloop and merge joins in the SQL connection session
    args: obj_conn - connection object
    """

    obj_cursor = obj_conn.cursor()
    obj_cursor.execute("set enable_mergejoin=off;")
    obj_cursor.execute("set enable_nestloop=off;")


def executeSelect(obj_conn, str_query):
    """
    executes the given query and return the output
    args:   obj_conn - connection object
            str_query - query to be executed
    """

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    obj_cursor = obj_conn.cursor()

    # execute the query
    obj_cursor.execute(str_query)
    
    # read the output
    t_output = obj_cursor.fetchall()

    return t_output
