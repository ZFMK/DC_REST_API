# DC REST API
REST API for DiversityCollection database

## Concepts

### json structure

```
'CollectionSpecimens': [
	{
		'CollectionSpecimenID': ,
		'AccessionNumber': ,
		'DepositorsAccessionNumber': ,
		'DepositorsName': ,
		'ExternalIdentifier': ,
		'OriginalNotes': ,
		'AdditionalNotes': ,
		'DataWithholdingReason': ,
		'CollectionAgents': [
			{
				'CollectorsName': ,
				'DataWithholdingReason' : 
			},
			[...] # more CollectionAgents
		],
		'CollectionEvent': {
			'CollectorsEventNumber': ,
			'CollectionDay': ,
			'CollectionMonth': ,
			'CollectionYear': ,
			'CollectionEndDay': ,
			'CollectionEndMonth': ,
			'CollectionEndYear': ,
			'CollectionDateSupplement': ,
			'LocalityDescription': ,
			'LocalityVerbatim': ,
			'HabitatDescription': ,
			'CollectingMethod': ,
			'Notes': ,
			'Country': ,
			'State': ,
			'State_District': ,
			'County': ,
			'Municipality': ,
			'Street_HouseNumber': ,
			'LocalityName': ,
			'Altitude_mNN': ,
			'Altitude_Accuracy': ,
			'WGS84_Lat': ,
			'WGS84_Lon': ,
			'WGS84_Accuracy': ,
			'WGS84_RecordingMethod': ,
			'DataWithholdingReason': 
		},
		'IdentificationUnits': [
			{
				'LifeStage': ,
				'Gender': ,
				'NumberOfUnits': ,
				'UnitIdentifier': ,
				'UnitDescription': ,
				'DisplayOrder': ,
				'DataWithholdingReason': ,
				'IdentificationUnitAnalyses': [
					{
						'AnalysisResult': 
					},
					[...] more IdentificationUnitAnalyses
				],
				'Identifications': [
					{
						'TaxonomicName': ,
						'NameURI': ,
						'VernacularTerm': ,
						'IdentificationDay': ,
						'IdentificationMonth': ,
						'IdentificationYear': ,
						'IdentificationDateSupplement': ,
						'ResponsibleName': ,
						'ResponsibleAgentURI': ,
						'IdentificationCategory': ,
						'IdentificationQualifier': ,
						'TypeStatus': ,
						'TypeNotes': ,
						'ReferenceTitle': ,
						'ReferenceURI': ,
						'ReferenceDetails': ,
						'Notes': 
					},
					[...] # more Identifications
				]
			},
			[...] # more IdentificationUnits
		],
		'CollectionSpecimenParts': [
			{
				'CollectionName': ,
				'AccessionNumber': ,
				'PreparationMethod': ,
				'PartSublabel': ,
				'MaterialCategory': ,
				'StorageLocation': ,
				'Stock': ,
				'StockUnit': ,
				'StorageContainer': ,
				'ResponsibleName': ,
				'ResponsibleAgentURI': ,
				'Notes': ,
				'DataWithholdingReason': 
			},
			[...] # more CollectionSpecimenParts
		]
	},
	[...] # more CollectionSpecimens
]
```


