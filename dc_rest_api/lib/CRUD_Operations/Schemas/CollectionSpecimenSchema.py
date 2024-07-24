import pudb

class CollectionSpecimenSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'AccessionNumber'},
			{'colname': 'DepositorsAccessionNumber'},
			{'colname': 'DepositorsName'},
			{'colname': 'ExternalIdentifier'},
			{'colname': 'OriginalNotes'},
			{'colname': 'AdditionalNotes'},
			{'colname': 'DataWithholdingReason'}
		]

	def getSchema(self):
		return self.schema
