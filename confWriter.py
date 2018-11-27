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
	
	global scan_counter, dict_config, dict_cols, counter
	tree_node = {}
	conf = ''
	
	scan_node_name = 'scan' + str(scan_counter)
	tree_node['name'] = scan_node_name
	scan_counter += 1

	obj_conn = dbconn.getDBConn()

	t_records = dbconn.executeSelect(obj_conn, constants.SQL_COLUMN_NUMBER.format(tables = "'"+dict_query_plan['Relation Name']+"'"))

	dict_col = dict((dict_query_plan['Relation Name']+'.'+x, y) for x, y in t_records)
	dict_cols = {**dict_cols, **dict_col}

	str_projection = []
	for str_col in dict_query_plan["Output"]:
		str_projection.append(str(dict_col[str_col.replace('"', '')]))

	if "Filter" in dict_query_plan:
		tree_node, conf=FilterWriter(dict_query_plan["Filter"], tree_node)

	dict_scan_param = {
		'node_name': scan_node_name,
		'file_type': dict_config[dict_query_plan['Relation Name']+ '_type'],
		'file_name': dict_config[dict_query_plan['Relation Name']],
		'schema': dict_config[dict_query_plan['Relation Name']+ '_schema'],
		'columns': ",".join(str_projection)
	}
	
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan_param)
	# print(tree_node)
	return tree_node, conf

def FilterWriter(str_filter_conditions, dict_scan_node):
	global counter
	str_filter_conditions = str_filter_conditions[1:-1]
	arr_conditions = str_filter_conditions.split('AND')
	
	if len(arr_conditions) == 1:
		node_name = 'filter' + str(counter)
		counter += 1 
		condition = arr_conditions[0].replace('"', '')
		tokens =condition.split(' ')
		dict_filter_params = {
			'node_name': node_name,
			'column': dict_cols[tokens[0]],
			'operator': tokens[1],
			'value': tokens[2]
		}
		conf = constants.FILTER_NODE_TEMPLATE.format(**dict_filter_params)
		
		return {
			'name': node_name,
			'input': dict_scan_node
		}, conf

	else:
		pass

def HashJoinWriter(dict_query_plan):
	
	global join_counter, dict_cols
	tree_node = {}

	join_node_name = 'join'+str(join_counter)
	join_counter += 1

	tree_node['name'] = join_node_name
	tree_node['probe'], probe_conf_nodes = GeneralWriter(dict_query_plan['Plans'][0])
	tree_node['build'], build_conf_nodes = GeneralWriter(dict_query_plan['Plans'][1])
	
	str_projection = []
	for str_col in dict_query_plan["Output"]:
		str_proj_mapping = ''
		str_relation_name = str_col.split('.')[0].replace('"', '')
		
		if str_relation_name == dict_query_plan['Plans'][0]['Relation Name']:
			str_proj_mapping = 'P$'+str(dict_cols[str_col.replace('"', '')])
		elif str_relation_name == dict_query_plan['Plans'][1]['Plans'][0]['Relation Name']:
			str_proj_mapping = 'B$'+str(dict_cols[str_col.replace('"', '')])
		str_projection.append('"'+str_proj_mapping+'"')

	dict_hash_params = {
		'node_name': join_node_name,
		'hashtype': 'hashjoin',
		'hashmethod': 'modulo',
		'tup_per_bucket':4,
		'build_attr':0,
		'probe_attr':0,
		'columns': ",".join(str_projection),
		'add': ''
	}
	
	
	attrs = dict_query_plan['Hash Cond'][1:-1].split(" = ")
	dict_hash_params['build_attr'] = dict_cols[attrs[0].replace('"', '')]
	dict_hash_params['probe_attr'] = dict_cols[attrs[1].replace('"', '')]
	
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


