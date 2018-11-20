import libconf
import tree
import constants
scan_counter = 0
join_counter = 0

def ScanWriter(dict_query_plan):
	
	global scan_counter	
	tree_node = {}
	conf = ''
	
	scan_node_name = 'scan' + str(scan_counter)
	tree_node['name'] = scan_node_name
	scan_counter += 1

	dict_scan_param = {
		'node_name': scan_node_name,
		'file_type': 'text',
		'file_name': dict_query_plan['Relation Name'],
		'schema': 0
	}
	
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan_param)
	return tree_node, conf

def HashJoinWriter(dict_query_plan):
	
	global join_counter
	tree_node = {}
	conf = ''

	join_node_name = 'join'+str(join_counter)
	join_counter += 1

	dict_hash_params = {
		'node_name': join_node_name,
		'hashtype': 'hash join',
		'hashmethod': 'modulo',
		'tup_per_bucket':4,
		'build_attr':0,
		'probe_attr':0,
		'columns': dict_query_plan["Output"],
		'add': ''
	}

	conf = constants.JOIN_NODE_TEMPLATE.format(**dict_hash_params)
	
	tree_node['name'] = join_node_name
	tree_node['probe'], probe_conf_nodes = GeneralWriter(dict_query_plan['Plans'][0])
	tree_node['build'], build_conf_nodes = GeneralWriter(dict_query_plan['Plans'][1])
	
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
	return tree_node, conf_nodes	
		


#Writes out the general structure of the file
def BaseWriter(configFileName, dict_query_plan):
	configDict = {}
	operatorTree = tree.Node('treeroot')
	with open(configFileName) as configFile:
		for line in configFile:
			key, value = line.split(":")
			configDict[key.strip()] = value.strip()
	confName = configDict['confName']+'.conf'
	confFile = open(confName,'w')
	confFile.write('path = \"'+configDict['path']+'\";\n')
	confFile.write('buffsize = 1048576;\n\n')

	root_node = {}
	tree_node, conf_nodes = GeneralWriter(dict_query_plan)
	root_node["root"] = tree_node
	confFile.write(conf_nodes)
	confFile.write(libconf.dumps(root_node))
	confFile.close()


	

