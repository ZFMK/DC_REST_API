import pudb

class CollectionAgentSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'CollectorsName', 'None allowed': False},
			{'colname': 'CollectorsSequence'},
			{'colname': 'DataWithholdingReason'}
		]

	def getSchema(self):
		return self.schema
