#DB_NAME = 'pg_pythia'
#DB_HOST = 'localhost'
#DB_USER_NAME = 'postgres'
#DB_PASSWORD = 'postgre'
#POSTGRE_LOC = 'G:\\PostgreSQL\\11\\bin\\'
#DB_CONN_STRING = "host='%s' dbname='%s' user='%s' password='%s'"

DB_NAME = 'pg_pythia'
DB_HOST = 'localhost'
DB_USER_NAME = 'postgres'
DB_PASSWORD = 'postgre'
DB_CONN_STRING = "host='%s' dbname='%s' user='%s' password='%s'"



JOIN_NODE_TEMPLATE = '''
{node_name}:
{{
    type = "{hashtype}";

    hash:
    {{
        fn = "{hashmethod}";
        buckets = 1048576;
    }};

    tuplesperbucket = {tup_per_bucket};
    buildjattr = {build_attr};
    probejattr = {probe_attr};

    projection = ({columns});
    {add}

    threadgroups = ( [0] );
    allocpolicy = "striped";
}};
'''

SCAN_NODE_TEMPLATE = '''
{node_name}:
{{
    type = "scan";

    filetype = "{file_type}";
    file = "{file_name}";
    schema = ( {schema} );
    projection = ({columns});
}};
'''


SQL_COLUMN_NUMBER = '''
select 
	"column_name", 
	row_number() OVER( partition by "table_name"  order by "ordinal_position") -1 as "column_number"
from 
	"information_schema"."columns"
where 
	"table_name" IN  ({tables});
'''