
import tree
def TreeWriter(confFile,operatorTree):
	confFile.write(operatorTree.operator+':\n')
	confFile.write('{\n')
	for child in operatorTree.children:
		confFile.write('\tname: \"'+child.operator+'\";\n')
	confFile.write('};')

def ScanWriter(confFile,dict_query_plan,configDict):
	confFile.write('scanL:\n')
	confFile.write('{\n')
	confFile.write('  type = \"scan\";\n\n')
	confFile.write('  filetype = \"'+configDict['fileType']+'\";\n')
	confFile.write('  file = \"'+configDict['file']+'\";\n')
	confFile.write('  schema = ( \"long\", \"long\", \"dec\" );\n')
	confFile.write('};\n\n')



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
		newNode = tree.Node('scanL')
		operatorTree.AddChild(newNode)
	TreeWriter(confFile,operatorTree)
	confFile.close()


	

