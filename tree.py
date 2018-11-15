class Node:
	def __init__(self, operator):
		self.operator = operator
		self.children = []

	def AddChild(self,operator):
		self.children.append(operator)
