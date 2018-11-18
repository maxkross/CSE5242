DB_NAME = 'pg_pythia'
DB_HOST = 'localhost'
DB_USER_NAME = 'postgres'
DB_PASSWORD = 'postgre'
POSTGRE_LOC = 'G:\\PostgreSQL\\11\\bin\\'
DB_CONN_STRING = "host='%s' dbname='%s' user='%s' password='%s'"

JOIN_NODE_TEMPLATE = '''
{node_name}:
{{
    type = "{hashtype}";
    hashtable = "{hashmethod}";

    hash:
    {{
        fn = "bytes";
        buckets = 1048576;
    }};

    hash2:
    {{
        fn = "willis";
        buckets = 1048576;
    }};

    tuplesperbucket = {tup_per_bucket};
    buildjattr = {build_attr};
    probejattr = {probe_attr};

    projection = ({columns});

    {add}

}};
'''
JOIN_TEMPLATE = '''
    name: "{node_name}";
    probe:
    {{
        name: "{probe_node_name}";
    }};
    build:
    {{
        name: "{build_node_name}";
    }};
'''

SCAN_NODE_TEMPLATE = '''
{node_name}:
{{
    type = "scan";

    filetype = "{file_type}";
    file = "{file_name}";
    schema = ({schema});
}};
'''