import sys
import psycopg2
import sys
import pprint
import constants
import confWriter
import libconf

# run the program as python main.py queries\select.sql

# checks for arguments
if len(sys.argv) != 2:
    raise BaseException("FileName not given")

str_file_loc = sys.argv[1]

# print("Reading SQL from file: " + str_file_loc)
try:
    # reads the query from the file
    str_query = ''
    with open(str_file_loc, 'r') as f_query:
        str_query = f_query.read()
    # print("Query: " + str_query)
except FileNotFoundError as e:
    print('File not found at the given location')
 
# builds the connection string
str_conn_string = constants.DB_CONN_STRING % (constants.DB_HOST, constants.DB_NAME, constants.DB_USER_NAME, constants.DB_PASSWORD)

# try:
# get a connection, if a connect cannot be made an exception will be raised here
obj_conn = psycopg2.connect(str_conn_string)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
obj_cursor = obj_conn.cursor()

# execute our Query
obj_cursor.execute("EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) " + str_query)


t_query_plan = obj_cursor.fetchall()

dict_query_plan = dict(t_query_plan[0][0][0])

print(libconf.dumps(dict_query_plan))
confWriter.BaseWriter('config.txt', dict_query_plan["Plan"])

# except BaseException as e:
#     print(str(e))
