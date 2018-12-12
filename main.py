import sys

# ensure the correct version of python is being run
if (sys.version_info < (3,6)):
	print('Incorrect version of python. Please run main.py with python 3.6+')
	exit(1)


import dbconn
import pprint
import constants
import confWriter
import libconf
import json
import psycopg2



# run the program as python3.6 main.py queries\select.sql

# checks for arguments
if len(sys.argv) != 2:
    print('FileName not given. Ex: python3.6 main.py queries/select.sql')
    exit(1)

str_file_loc = sys.argv[1]

print("Reading SQL from file: " + str_file_loc)
try:
    # reads the query from the file
    str_query = ''
    with open(str_file_loc, 'r') as f_query:
        str_query = f_query.read()
    # print("Query: " + str_query)
except FileNotFoundError as e:
    print('File not found at the given location')
    exit(1)
 
try:
	# connects to PostgreSQL
	obj_conn = dbconn.getDBConn()

except psycopg2.OperationalError as e:
	print('Invalid database credentials. Check fields in constants.py.')
	print('Ensure they match the database you are accessing')
	exit(1)

# fetches the query plan
# print("EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) " + str_query)

dbconn.disableMerge(obj_conn)
try:
	t_query_plan = dbconn.executeSelect(obj_conn, "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) " + str_query)
except psycopg2.ProgrammingError as e:
	print('Invalid query. Check your query for errors.\n')
	print(e.pgerror)
	exit(1)
dict_query_plan = dict(t_query_plan[0][0][0])
print('Fetched Query plan from PostgreSQL')

# triggers the conf file writer
confWriter.BaseWriter('config.txt', dict_query_plan["Plan"])
print('Conf file generated')

# except BaseException as e:
#     print(str(e))
