import libconf
import tree
import constants
import json
scan_counter = 0
join_counter = 0
config_dict = {}

def ScanWriter(dict_query_plan):
	
	global scan_counter, config_dict
	tree_node = {}
	conf = ''
	
	scan_node_name = 'scan' + str(scan_counter)
	tree_node['name'] = scan_node_name
	scan_counter += 1

	
	dict_scan_param = {
		'node_name': scan_node_name,
		'file_type': config_dict[dict_query_plan['Relation Name']+ '_type'],
		'file_name': config_dict[dict_query_plan['Relation Name']],
		'schema': config_dict[dict_query_plan['Relation Name']+ '_schema'],
		'columns': dict_query_plan["Output"]
	}
	
	conf += constants.SCAN_NODE_TEMPLATE.format(**dict_scan_param)
	return tree_node, conf

def HashJoinWriter(dict_query_plan):
	
	global join_counter
	tree_node = {}

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

	
	tree_node['name'] = join_node_name
	tree_node['probe'], probe_conf_nodes = GeneralWriter(dict_query_plan['Plans'][0])
	tree_node['build'], build_conf_nodes = GeneralWriter(dict_query_plan['Plans'][1])
	
	attrs = dict_query_plan['Hash Cond'][1:-1].split(" = ")
	dict_hash_params['build_attr'] = attrs[0]
	dict_hash_params['probe_attr'] = attrs[1]
	
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
	return tree_node, conf_nodes	
		


#Writes out the general structure of the file
def BaseWriter(configFileName, dict_query_plan):
	global config_dict

	# read user conf file
	with open(configFileName) as configFile:
		for line in configFile:
			key, value = line.split(":")
			config_dict[key.strip()] = value.strip()

	# write basic info to pythia conf file
	conf_name = config_dict['conf_name']+'.conf'
	conf_file = open(conf_name,'w')
	conf_file.write('path = \"'+config_dict['path']+'\";\n')
	conf_file.write('buffsize = 1048576;\n\n')

	root_node = {}
	tree_node, conf_nodes = GeneralWriter(dict_query_plan)
	
	root_node["treeroot"] = tree_node
	conf_file.write(conf_nodes)
	conf_file.write(libconf.dumps(root_node).replace(' =', ':'))
	conf_file.close()


