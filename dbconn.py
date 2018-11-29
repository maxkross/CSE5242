import psycopg2
import constants

def getDBConn():

    # builds the connection string
    str_conn_string = constants.DB_CONN_STRING % (constants.DB_HOST, constants.DB_NAME, constants.DB_USER_NAME, constants.DB_PASSWORD)
	
    # try:
    # get a connection, if a connect cannot be made an exception will be raised here
    return psycopg2.connect(str_conn_string)

def disableMerge(obj_conn):
	obj_cursor = obj_conn.cursor()

	obj_cursor.execute("set enable_mergejoin=off;")
	obj_cursor.execute("set enable_nestloop=off;")

def executeSelect(obj_conn, str_query):
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    obj_cursor = obj_conn.cursor()

    # execute our Query
    obj_cursor.execute(str_query)
    
    t_output = obj_cursor.fetchall()

    return t_output
