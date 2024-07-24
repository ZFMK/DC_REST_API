import pudb

class IdentificationUnitSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID'},
			{'colname': 'LastIdentificationCache', 'default': 'unknown', 'None allowed': False},
			{'colname': 'LifeStage'},
			{'colname': 'Gender'},
			{'colname': 'NumberOfUnits'},
			{'colname': 'NumberOfUnitsModifier'},
			{'colname': 'UnitIdentifier'},
			{'colname': 'UnitDescription'},
			{'colname': 'DisplayOrder'},
			{'colname': 'Notes'},
			{'colname': 'TaxonomicGroup', 'default': 'unknown', 'None allowed': False},
			{'colname': 'DataWithholdingReason'},
		]

	def getSchema(self):
		return self.schema
