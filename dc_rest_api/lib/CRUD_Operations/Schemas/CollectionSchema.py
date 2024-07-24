import pudb

class CollectionSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionID'},
			#{'colname': 'CollectionSpecimenID'},
			{'colname': 'CollectionName', 'default': 'No collection', 'None allowed': False},
			{'colname': 'CollectionAccronym'},
			{'colname': 'AdministrativeContactName'},
			{'colname': 'AdministrativeContactAgentURI'},
			{'colname': 'Description'},
			{'colname': 'Description_sha'},
			{'colname': 'CollectionOwner'},
			{'colname': 'Type'},
			{'colname': 'Location'},
			{'colname': 'LocationPlan'},
			{'colname': 'LocationPlanWidth'},
			{'colname': 'LocationPlanDate'},
			{'colname': 'LocationGeometry'},
			{'colname': 'LocationHeight'}
		]

	def getSchema(self):
		return self.schema
