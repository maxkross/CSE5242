
import tree
import constants
counter = 0

def TreeWriter(confFile,operatorTree):
	confFile.write(operatorTree.operator+':\n')
	confFile.write('{\n')
	for child in operatorTree.children:
		confFile.write(child.operator)
	confFile.write('};')

def ScanWriter(confFile,dict_query_plan,configDict):
	confFile.write('scanL:\n')
	confFile.write('{\n')
	confFile.write('  type = \"scan\";\n\n')
	confFile.write('  filetype = \"'+configDict['fileType']+'\";\n')
	confFile.write('  file = \"'+configDict['file']+'\";\n')
	confFile.write('  schema = ( \"long\", \"long\", \"dec\" );\n')
	confFile.write('};\n\n')


def HashJoinWriter(dict_query_plan, configDict):

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
def BaseWriter(configFileName,planType,dict_query_plan):
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

	if(planType == 'Seq Scan'):
		ScanWriter(confFile,dict_query_plan,configDict)
		newNode = tree.Node('\tname: \"ScanL\";\n')
		operatorTree.AddChild(newNode)
		
	elif planType == 'Hash Join':
		plan_conf, conf_nodes = HashJoinWriter(dict_query_plan, configFile)
		confFile.write(conf_nodes)
		newNode = tree.Node(plan_conf)
		operatorTree.AddChild(newNode)
	TreeWriter(confFile,operatorTree)
	
	confFile.close()


	

