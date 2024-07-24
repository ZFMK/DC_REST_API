import pudb

class CollectionEventSchema():
	def __init__(self):
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': '@id'},
			{'colname': 'CollectionEventID'},
			{'colname': 'CollectorsEventNumber'},
			{'colname': 'CollectionDate'},
			{'colname': 'CollectionDay'},
			{'colname': 'CollectionMonth'},
			{'colname': 'CollectionYear'},
			{'colname': 'CollectionDateSupplement'},
			{'colname': 'CollectionDateCategory'},
			{'colname': 'CollectionEndDay'},
			{'colname': 'CollectionEndMonth'},
			{'colname': 'CollectionEndYear'},
			{'colname': 'LocalityDescription'},
			{'colname': 'LocalityDescription_sha', 'compute sha of': 'LocalityDescription'},
			{'colname': 'LocalityVerbatim'},
			{'colname': 'LocalityVerbatim_sha', 'compute sha of': 'LocalityVerbatim'},
			{'colname': 'HabitatDescription'},
			{'colname': 'HabitatDescription_sha', 'compute sha of': 'HabitatDescription'},
			{'colname': 'CollectingMethod'},
			{'colname': 'CollectingMethod_sha', 'compute sha of': 'CollectingMethod'},
			{'colname': 'ReferenceTitle'},
			{'colname': 'ReferenceURI'},
			# {'colname': 'ReferenceDetails'},
			{'colname': 'Notes'},
			{'colname': 'Country'},
			#################
			{'colname': 'State'},
			{'colname': 'StateDistrict'},
			{'colname': 'County'},
			{'colname': 'Municipality'},
			{'colname': 'StreetHouseNumber'},
			#################
			#  lsID = 4
			{'colname': 'Altitude'},
			{'colname': 'Altitude_Accuracy'},
			# lsID = 8
			{'colname': 'WGS84_Lat'},
			{'colname': 'WGS84_Lon'},
			{'colname': 'WGS84_Accuracy'},
			{'colname': 'WGS84_RecordingMethod'},
			# lsID = 14
			{'colname': 'Depth_min_m'},
			{'colname': 'Depth_max_m'},
			{'colname': 'Depth_Accuracy_m'},
			{'colname': 'Depth_RecordingMethod_m'},
			# lsID = 15
			{'colname': 'Height_min_m'},
			{'colname': 'Height_max_m'},
			{'colname': 'Height_Accuracy_m'},
			{'colname': 'Height_RecordingMethod_m'},
			{'colname': 'DataWithholdingReason'},
			{'colname': 'DataWithholdingReasonDate'}
		]

	def getSchema(self):
		return self.schema
