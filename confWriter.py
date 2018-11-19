
import tree
import constants
counter = 0

def TreeWriter(conf_file,operator_tree):
	conf_file.write(operator_tree.operator+':\n')
	conf_file.write('{\n')
	for child in operator_tree.children:
		conf_file.write(child.operator)
	conf_file.write('};')

def ScanWriter(dict_query_plan,config_dict):

	conf = ''

	node_name = 'scanL'
	scan_params = {
		'node_name': node_name,

		'file_type': config_dict['file_type'],

		'file_name': config_dict['file'],

		'schema': config_dict['scan_schema']
	}

	conf += constants.SCAN_NODE_TEMPLATE.format(**scan_params)

	return conf



def HashJoinWriter(dict_query_plan, config_dict):

	conf = ''

	join_node_name = 'join'+str(0)
	dict_hash_params = {
		'node_name': join_node_name,
		'hashtype': 'hash join',
		'hashmethod': 'cuckoo',
		'tup_per_bucket':3,
		'build_attr':0,
		'probe_attr':0,
		'columns':'"P$2"'
	}

	conf += constants.JOIN_NODE_TEMPLATE.format(**dict_hash_params)

	scan1_node_name = 'scan'+str(1)
	dict_scan1_param = {
		'node_name': scan1_node_name,
		'file_type':'text',
		'file_name': 'filename',
		'schema':0
	}
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan1_param)

	scan2_node_name = 'scan'+str(2)
	dict_scan2_param = {
		'node_name': scan2_node_name,
		'file_type':'text',
		'file_name': 'filename',
		'schema':0
	}
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan2_param)

	dict_join_params = {
		'node_name': join_node_name,
		'probe_node_name': scan1_node_name,
		'build_node_name': scan2_node_name,
	}

	return constants.JOIN_TEMPLATE.format(**dict_join_params), conf

#Writes out the general structure of the file
def BaseWriter(config_file_name,planType,dict_query_plan):
	config_dict = {}
	operator_tree = tree.Node('treeroot')
	with open(config_file_name) as config_file:
		for line in config_file:
			key, value = line.split(":")
			config_dict[key.strip()] = value.strip()
	conf_name = config_dict['conf_name']+'.conf'
	conf_file = open(conf_name,'w')
	conf_file.write('path = \"'+config_dict['path']+'\";\n')
	conf_file.write('buffsize = 1048576;\n\n')

	if(planType == 'Seq Scan'):
		conf_nodes = ScanWriter(dict_query_plan,config_dict)
		conf_file.write(conf_nodes)
		new_node = tree.Node('\tname: \"scanL\";\n')
		operator_tree.AddChild(new_node)
		
	elif planType == 'Hash Join':
		plan_conf, conf_nodes = HashJoinWriter(dict_query_plan, config_file)
		conf_file.write(conf_nodes)
		new_node = tree.Node(plan_conf)
		operator_tree.AddChild(new_node)
	TreeWriter(conf_file,operator_tree)
	
	conf_file.close()


	

