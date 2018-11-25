import sys
import dbconn
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
 

obj_conn = dbconn.getDBConn()

t_query_plan = dbconn.executeSelect(obj_conn, "EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) " + str_query)

dict_query_plan = dict(t_query_plan[0][0][0])

# print(libconf.dumps(dict_query_plan))
confWriter.BaseWriter('config.txt', dict_query_plan["Plan"])

# except BaseException as e:
#     print(str(e))
