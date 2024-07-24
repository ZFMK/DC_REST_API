import pudb

class CollectionSpecimenPartSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'SpecimenPartID'},
			{'colname': 'CollectionName', 'default': 'No collection', 'None allowed': False},
			{'colname': 'CollectionID'},
			{'colname': 'AccessionNumber'},
			{'colname': 'PartSublabel'},
			{'colname': 'PreparationMethod'},
			{'colname': 'MaterialCategory', 'None allowed': False},
			{'colname': 'StorageLocation'},
			{'colname': 'StorageContainer'},
			{'colname': 'Stock'},
			{'colname': 'StockUnit'},
			{'colname': 'ResponsibleName'},
			{'colname': 'ResponsibleAgentURI'},
			{'colname': 'Notes'},
			{'colname': 'DataWithholdingReason'}
		]

	def getSchema(self):
		return self.schema
