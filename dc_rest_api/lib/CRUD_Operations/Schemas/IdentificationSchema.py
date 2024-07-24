import pudb

class IdentificationSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'IdentificationSequence'},
			{'colname': 'TaxonomicName', 'default': 'unknown', 'None allowed': False},
			{'colname': 'NameURI'},
			{'colname': 'VernacularTerm'},
			{'colname': 'IdentificationDay'},
			{'colname': 'IdentificationMonth'},
			{'colname': 'IdentificationYear'},
			{'colname': 'IdentificationDateSupplement'},
			{'colname': 'ResponsibleName', 'None allowed': False, 'Minimal string length': 1},
			{'colname': 'ResponsibleAgentURI'},
			{'colname': 'IdentificationCategory'},
			{'colname': 'IdentificationQualifier'},
			{'colname': 'TypeStatus'},
			{'colname': 'TypeNotes'},
			{'colname': 'ReferenceTitle'},
			{'colname': 'ReferenceURI'},
			{'colname': 'ReferenceDetails'},
			{'colname': 'Notes'}
		]

	def getSchema(self):
		return self.schema
