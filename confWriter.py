import libconf
import tree
import constants
import dbconn
scan_counter = 0
join_counter = 0
counter = 0
dict_config = {}
dict_cols = {}

def ScanWriter(dict_query_plan):
	"""
	Function that handles Sequential scan
	"""
	global scan_counter, dict_config, dict_cols, counter
	tree_node = {}
	conf = ''
	
	# generates the node name
	scan_node_name = 'scan' + str(scan_counter)
	tree_node['name'] = scan_node_name
	scan_counter += 1

	# connects to the database
	obj_conn = dbconn.getDBConn()

	t_records = dbconn.executeSelect(obj_conn, constants.SQL_COLUMN_NUMBER.format(tables = "'"+dict_query_plan['Relation Name']+"'"))

	dict_col = dict((dict_query_plan['Relation Name']+'.'+x, y) for x, y in t_records)
	dict_cols = {**dict_cols, **dict_col}

	str_projection = []
	for str_col in dict_query_plan["Output"]:
		if '.' not in str_col:
			str_col = dict_query_plan['Relation Name']+'.'+str_col
		str_projection.append(str(dict_col[str_col.replace('"', '')]))

	if "Filter" in dict_query_plan:
		str_filter_conditions = dict_query_plan["Filter"][1:-1]
		arr_conditions = str_filter_conditions.split('AND')
		tree_node, conf=FilterWriter(arr_conditions, tree_node)

	dict_scan_param = {
		'node_name': scan_node_name,
		'file_type': dict_config[dict_query_plan['Relation Name']+ '_type'],
		'file_name': dict_config[dict_query_plan['Relation Name']],
		'schema': dict_config[dict_query_plan['Relation Name']+ '_schema'],
		'columns': ",".join(str_projection)
	}
	
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan_param)
	return tree_node, conf

def FilterWriter(arr_conditions, dict_scan_node):
	"""
	Function that handles Filters
	"""
	global counter	
	
	if len(arr_conditions) == 1:
		tree_node = {}
		node_name = 'filter' + str(counter)		
		counter += 1 

		condition = arr_conditions[0].strip()
		if '(' == condition[0]:
			condition= condition[1:-1]
		condition = condition.replace('"', '')
		tokens =condition.split(' ')
		dict_filter_params = {
			'node_name': node_name,
			'column': dict_cols[tokens[0]],
			'operator': tokens[1],
			'value': tokens[2]
		}
		conf = constants.FILTER_NODE_TEMPLATE.format(**dict_filter_params)
		
		tree_node["name"] = node_name
		tree_node["input"] = dict_scan_node
		return tree_node, conf

	else:
		tree_node = {}
		node_name = 'filter' + str(counter)		
		counter += 1 

		condition = arr_conditions[0].strip()
		if '(' == condition[0]:
			condition= condition[1:-1]
		condition = condition.replace('"', '')
		tokens =condition.split(' ')
		dict_filter_params = {
			'node_name': node_name,
			'column': dict_cols[tokens[0]],
			'operator': tokens[1],
			'value': tokens[2]
		}
		conf = constants.FILTER_NODE_TEMPLATE.format(**dict_filter_params)
		
		tree_node["name"] = node_name
		arr_conditions.pop(0)
		tree_node["input"], temp_conf = FilterWriter(arr_conditions, dict_scan_node)
		conf += temp_conf
		return tree_node, conf

def HashJoinWriter(dict_query_plan):
	
	global join_counter, dict_cols
	tree_node = {}

	join_node_name = 'join'+str(join_counter)
	join_counter += 1

	probe = dict_query_plan['Plans'][0]	
	build = dict_query_plan['Plans'][1]

	tree_node['name'] = join_node_name
	tree_node['probe'], probe_conf_nodes = GeneralWriter(probe)
	tree_node['build'], build_conf_nodes = GeneralWriter(build)
	
	str_projection = []
	for str_col in dict_query_plan["Output"]:
		if str_col in probe['Output']:
			index = probe['Output'].index(str_col)
			str_projection.append('"P$' + str(index) + '"')
		else:
			index = build['Output'].index(str_col)
			str_projection.append('"B$'+ str(index) + '"')
	
	join_attrs = dict_query_plan["Hash Cond"][1:-1].split(" = ")
	build_join = build['Output'].index(join_attrs[1])
	probe_join = probe['Output'].index(join_attrs[0])
	dict_hash_params = {
		'node_name': join_node_name,
		'hashtype': 'hashjoin',
		'hashmethod': 'modulo',
		'tup_per_bucket':4,
		'build_attr':build_join,
		'probe_attr':probe_join,
		'columns': ",".join(str_projection),
		'add': ''
	}
	conf = constants.JOIN_NODE_TEMPLATE.format(**dict_hash_params)
	conf = conf + probe_conf_nodes + build_conf_nodes	

	return tree_node, conf

def GeneralWriter(dict_query_plan):
	plan_type = dict_query_plan["Node Type"]
	if plan_type == "Seq Scan":
		tree_node, conf_nodes = ScanWriter(dict_query_plan)
	elif plan_type == "Hash Join":
		tree_node, conf_nodes = HashJoinWriter(dict_query_plan)
	elif plan_type == "Hash":
		tree_node, conf_nodes = GeneralWriter(dict_query_plan["Plans"][0])
	else:
		raise BaseException(dict_query_plan["Node Type"] + " is not supported yet") 
	return tree_node, conf_nodes	
		


#Writes out the general structure of the file
def BaseWriter(configFileName, dict_query_plan):
	global dict_config

	# read user conf file
	with open(configFileName) as configFile:
		for line in configFile:
			key, value = line.split(":")
			dict_config[key.strip()] = value.strip()

	# write basic info to pythia conf file
	conf_name = dict_config['conf_name']+'.conf'
	conf_file = open(conf_name,'w')
	conf_file.write('path = \"'+dict_config['path']+'\";\n')
	conf_file.write('buffsize = 1048576;\n\n')

	root_node = {}
	tree_node, conf_nodes = GeneralWriter(dict_query_plan)
	
	root_node["treeroot"] = tree_node
	conf_file.write(conf_nodes)
	conf_file.write(libconf.dumps(root_node).replace(' =', ':'))
	conf_file.close()


