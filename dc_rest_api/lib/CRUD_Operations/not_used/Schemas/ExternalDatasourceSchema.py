import pudb

class ExternalDatasourceSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'ExternalDatasourceID'},
			{'colname': 'ExternalDatasourceName', 'None allowed': False},
			{'colname': 'ExternalDatasourceVersion'},
			{'colname': 'ExternalDatasourceURI'},
			{'colname': 'ExternalDatasourceInstitution'},
			{'colname': 'InternalNotes'},
			{'colname': 'ExternalAttribute_NameID'}
		]

	def getSchema(self):
		return self.schema
