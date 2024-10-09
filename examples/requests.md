## Examples

### Get token for database authentication. The token could then be used for all subsequent requests:

	curl -k -X POST "https://localhost/dc_rest_api/login" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"username": "testuser",
		"password": "*******"
	}' \
	| json_pp

returns:

	{
		"projects" : [
			"DiversityWorkbench"
		],
		"roles" : [
			"CollectionManager",
			"DataManager",
			"Editor",
			"StorageManager",
			"User"
		],
		"token" : "b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c"
	}


### Create datasets:

#### json file with example data

json with CollectionEvents, Collections, ExternalDatasources, Projects in each CollectionSpecimen:

	{
		"CollectionSpecimens": [
			{
				"CollectionSpecimenID": null,
				"AccessionNumber": "unittest_03_a",
				"DepositorsAccessionNumber": "unittest_03",
				"DepositorsName": "Testuser Depositor 03",
				"ExternalIdentifier": "unittest_03 ExternalIdentifier",
				"OriginalNotes": "unittest_03 OriginalNotes",
				"AdditionalNotes": "unittest_03 AdditionalNotes",
				"DataWithholdingReason": "DataWithholdingReason: Dataset for UnitTest",
				"CollectionAgents": [
					{
						"CollectorsName": "Testuser Collector1",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					},
					{
						"CollectorsName": "Testuser Collector2",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					},
					{
						"CollectorsName": "Testuser Collector3",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					}
				],
				"CollectionEvent": {
					"CollectorsEventNumber": "UnitTest_Event_04",
					"CollectionDay": "10",
					"CollectionMonth": 9,
					"CollectionYear": "2022",
					"CollectionEndDay": 13,
					"CollectionEndMonth": 9,
					"CollectionEndYear": 2022,
					"CollectionDateSupplement": "10.09.2022- 13.09.2022",
					"LocalityDescription": "Münster-Wolbeck, Buchenwald im NWZ Teppes Viertel",
					"LocalityVerbatim": "Westfälische Bucht",
					"HabitatDescription": "aus verpilzten Rinden einer liegenden Buche gesiebt",
					"CollectingMethod": "Gesiebe",
					"Notes": null,
					"CountryCache": "Germany",
					"State": "Nordrhein-Westfalen",
					"State_District": null,
					"County": "Münsterland",
					"Municipality": null,
					"Street_HouseNumber": null,
					"LocalityName": "Münster-Wolbeck, NWZ Teppes Viertel",
					"Altitude_mNN": 70,
					"Altitude_Accuracy": 10,
					"WGS84_Lat": 51.910245,
					"WGS84_Lon": "7.744298",
					"WGS84_Accuracy": 10,
					"WGS84_RecordingMethod": "GPS",
					"Depth_min_m": "10",
					"Depth_max_m": "20",
					"Depth_Accuracy_m": 5,
					"Depth_RecordingMethod_m": "Bathometer",
					"Height_min_m": "2",
					"Height_max_m": "3",
					"Height_Accuracy_m": 5,
					"Height_RecordingMethod_m": "Bandmaß",
					"DataWithholdingReason": "DataWithholdingReason Event: Dataset for UnitTest",
					"DataWithholdingReasonDate": "2021-03-15"
				},
				"CollectionExternalDatasource": {
					"ExternalDatasourceName": "Unittest external datasource 2",
					"ExternalDatasourceVersion": 1,
					"ExternalDatasourceURI": "https://example.com/testuri_2",
					"ExternalDatasourceInstitution": "Unittest",
					"InternalNotes": "Unittest",
					"ExternalAttribute_NameID": "Unittest table 2"
				},
				"IdentificationUnits": [
					{
						"LifeStage": "adult",
						"Gender": "female",
						"NumberOfUnits": 45,
						"UnitIdentifier": "testunit 03",
						"UnitDescription": "testunit 03",
						"DisplayOrder": 1,
						"DataWithholdingReason": "DataWithholdingReason IdentificationUnit: Dataset for UnitTest",
						"IdentificationUnitAnalysis": {
							"AnalysisResult": "CAAAAAGCAAGAATAAAAAAGGATAGGTGCAGAGACTCAAAGGAAGCTGTTCTAAAAAAATGGGATTGACTGTGCTGTATTGTTATAGCTGTATTGTTATAAAACTTTTTCAGTCTAAATTCCAACCCAAGAATAAGGGGTGAAGAATCTACGTCCTGAAATTATTAATGACAACCCGAATTTGTTCTGTATTTTTTTTTCACATAATTAAATTATATAATATAGATAATCTCTAGAAATCCATATTATAGGATAGATTTAAAATTAAGGAATGGAAAAATGCAAGAATTGTTATGAATCGATTCTAAGTTGAAAGCCGAATAAATTTTTGAGTTATTCATAAAAACATTCATACTCACCCCATAGTCTGAACGATCTTTTGAATAAGAGATTAATCGGATGAGAATAAAGATAGAGTCCCGTTCTACATGTCAATACTGACAACAATTAAATTTATAGTAAGAGGAAAATCCGTCGACTTTAAAAATCGTGAGGGTTCAAGTCCCTCTATCCCCATACACTCCCTAACTAGTTATCTTTTCTTTTTCCCAGTACCTAATAGAAGACTTTATAATACTTTTCATCCTTTTAATTGACACAGACTCAAGTTATCTCGTAAAATGGGGAGATGCTGCGGGTAATGGTCGGGATAGCTCAGTTGGTAGAGCAGAGGACTGAAAA"
						},
						"Identifications": [
							{
								"TaxonomicName": "Eisenia veneta (Rosa, 1886)",
								"NameURI": "https://www.gbif.org/species/8861735",
								"VernacularTerm": "Riesen-Rotwurm",
								"IdentificationDay": "17",
								"IdentificationMonth": "05",
								"IdentificationYear": "2024",
								"IdentificationDateSupplement": "",
								"ResponsibleName": "Testuser 1",
								"ResponsibleAgentURI": "https://zfmk.diversityagents.de/0815",
								"IdentificationCategory": "determination",
								"IdentificationQualifier": "",
								"TypeStatus": "paratype",
								"TypeNotes": "",
								"ReferenceTitle": "",
								"ReferenceURI": null,
								"ReferenceDetails": null,
								"Notes": null
							},
							{
								"TaxonomicName": "Dendrobaena veneta (Rosa, 1886)",
								"NameURI": "https://www.gbif.org/species/5739805",
								"VernacularTerm": "Garten Regenwurm",
								"IdentificationDay": "17",
								"IdentificationMonth": "05",
								"IdentificationYear": "2024",
								"IdentificationDateSupplement": "",
								"ResponsibleName": "Testuser 1",
								"ResponsibleAgentURI": "https://zfmk.diversityagents.de/0815",
								"IdentificationCategory": "determination",
								"IdentificationQualifier": "",
								"TypeStatus": "paratype",
								"TypeNotes": "",
								"ReferenceTitle": "",
								"ReferenceURI": null,
								"ReferenceDetails": null,
								"Notes": null
							}
						]
					}
				],
				"CollectionSpecimenParts": [
					{
						"AccessionNumber": "testunit 04 slides",
						"PreparationMethod": "Maceration",
						"PartSublabel": "testunit 04 slides label",
						"MaterialCategory": "microscopic slides",
						"StorageLocation": "Room 15, shelf 22, drawer 12",
						"Stock": 20,
						"StockUnit": null,
						"StorageContainer": "Box 25",
						"ResponsibleName": "a testuser that might be in Collection Agents",
						"ResponsibleAgentURI": "https://zfmk.diversityworkbench/collectionagents/0815",
						"Notes": "something to say about this part",
						"DataWithholdingReason": "withhold by default"
						"Collection": {
							"CollectionName": "A Collection for Unittest",
							"CollectionAccronym": "coll Unittest 1",
							"AdministrativeContactName": "Unittester 1",
							"AdministrativeContactAgentURI": null,
							"Description": "A collection inserted only for unittest",
							"CollectionOwner": "Unittester 1",
							"Type": "room",
							"Location": "Käferhaus"
						}
					}
				],
				"Projects": [
					{
						"Project": "Project 2 for Unittests",
						"ProjectURI": "https://localhost/Projects/02"
					}
				]
			}
		]
	}

This file is available as ./examples/insert_CS_01.json. The data can be inserted with:

	curl -k -X POST "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	--data-binary @examples/insert_CS_01.json

Projects, CollectionEvents, Collections, and ExternalDatasources are given as extra lists in json and become linked to each CollectionSpecimen via the "@id" key and a blank node identifier


	{
		"Projects": [
			{
				"@id": "_:p2",
				"Project": "Project 2 for Unittests",
				"ProjectURI": "https://localhost/Projects/02"
			}
		],
		"CollectionEvents": [
			{
				"@id": "_:ce1",
				"CollectorsEventNumber": "UnitTest_Event_04",
				"CollectionDay": "10",
				"CollectionMonth": 9,
				"CollectionYear": "2022",
				"CollectionEndDay": 13,
				"CollectionEndMonth": 9,
				"CollectionEndYear": 2022,
				"CollectionDateSupplement": "10.09.2022- 13.09.2022",
				"LocalityDescription": "Münster-Wolbeck, Buchenwald im NWZ Teppes Viertel",
				"LocalityVerbatim": "Westfälische Bucht",
				"HabitatDescription": "aus verpilzten Rinden einer liegenden Buche gesiebt",
				"CollectingMethod": "Gesiebe",
				"Notes": null,
				"CountryCache": "Germany",
				"State": "Nordrhein-Westfalen",
				"State_District": null,
				"County": "Münsterland",
				"Municipality": null,
				"Street_HouseNumber": null,
				"LocalityName": "Münster-Wolbeck, NWZ Teppes Viertel",
				"Altitude_mNN": 70,
				"Altitude_Accuracy": 10,
				"WGS84_Lat": 51.910245,
				"WGS84_Lon": "7.744298",
				"WGS84_Accuracy": 10,
				"WGS84_RecordingMethod": "GPS",
				"Depth_min_m": "10",
				"Depth_max_m": "20",
				"Depth_Accuracy_m": 5,
				"Depth_RecordingMethod_m": "Bathometer",
				"Height_min_m": "2",
				"Height_max_m": "3",
				"Height_Accuracy_m": 5,
				"Height_RecordingMethod_m": "Bandmaß",
				"DataWithholdingReason": "DataWithholdingReason Event: Dataset for UnitTest",
				"DataWithholdingReasonDate": "2021-03-15"
			}
		],
		"Collections": [
			{
				"@id":"_:c1",
				"CollectionName": "A Collection for Unittest",
				"CollectionAccronym": "coll Unittest 1",
				"AdministrativeContactName": "Unittester 1",
				"AdministrativeContactAgentURI": null,
				"Description": "A collection inserted only for unittest",
				"CollectionOwner": "Unittester 1",
				"Type": "room",
				"Location": "Käferhaus"
			}
		],
		"CollectionExternalDatasources": [
			{
				"@id":"_:ed8",
				"ExternalDatasourceName": "Unittest external datasource 1",
				"ExternalDatasourceVersion": 1,
				"ExternalDatasourceURI": "https://example.com/testuri_1",
				"ExternalDatasourceInstitution": "Unittest",
				"InternalNotes": "Unittest",
				"ExternalAttribute_NameID": "Unittest table 1"
			}
		],
		"CollectionSpecimens": [
			{
				"CollectionSpecimenID": null,
				"AccessionNumber": "unittest_03_a",
				"DepositorsAccessionNumber": "unittest_03",
				"DepositorsName": "Testuser Depositor 03",
				"ExternalIdentifier": "unittest_03 ExternalIdentifier",
				"OriginalNotes": "unittest_03 OriginalNotes",
				"AdditionalNotes": "unittest_03 AdditionalNotes",
				"DataWithholdingReason": "DataWithholdingReason: Dataset for UnitTest",
				"CollectionAgents": [
					{
						"CollectorsName": "Testuser Collector1",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					},
					{
						"CollectorsName": "Testuser Collector2",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					},
					{
						"CollectorsName": "Testuser Collector3",
						"DataWithholdingReason": "DataWithholdingReason Agent: Dataset for UnitTest"
					}
				],
				"CollectionEvent": {
					"@id": "_:ce1"
				},
				"CollectionExternalDatasource": {
					"@id": "_:ed8"
				},
				"IdentificationUnits": [
					{
						"LifeStage": "adult",
						"Gender": "female",
						"NumberOfUnits": 45,
						"UnitIdentifier": "testunit 03",
						"UnitDescription": "testunit 03",
						"DisplayOrder": 1,
						"DataWithholdingReason": "DataWithholdingReason IdentificationUnit: Dataset for UnitTest",
						"IdentificationUnitAnalysis": {
							"AnalysisResult": "CAAAAAGCAAGAATAAAAAAGGATAGGTGCAGAGACTCAAAGGAAGCTGTTCTAAAAAAATGGGATTGACTGTGCTGTATTGTTATAGCTGTATTGTTATAAAACTTTTTCAGTCTAAATTCCAACCCAAGAATAAGGGGTGAAGAATCTACGTCCTGAAATTATTAATGACAACCCGAATTTGTTCTGTATTTTTTTTTCACATAATTAAATTATATAATATAGATAATCTCTAGAAATCCATATTATAGGATAGATTTAAAATTAAGGAATGGAAAAATGCAAGAATTGTTATGAATCGATTCTAAGTTGAAAGCCGAATAAATTTTTGAGTTATTCATAAAAACATTCATACTCACCCCATAGTCTGAACGATCTTTTGAATAAGAGATTAATCGGATGAGAATAAAGATAGAGTCCCGTTCTACATGTCAATACTGACAACAATTAAATTTATAGTAAGAGGAAAATCCGTCGACTTTAAAAATCGTGAGGGTTCAAGTCCCTCTATCCCCATACACTCCCTAACTAGTTATCTTTTCTTTTTCCCAGTACCTAATAGAAGACTTTATAATACTTTTCATCCTTTTAATTGACACAGACTCAAGTTATCTCGTAAAATGGGGAGATGCTGCGGGTAATGGTCGGGATAGCTCAGTTGGTAGAGCAGAGGACTGAAAA"
						},
						"Identifications": [
							{
								"TaxonomicName": "Eisenia veneta (Rosa, 1886)",
								"NameURI": "https://www.gbif.org/species/8861735",
								"VernacularTerm": "Riesen-Rotwurm",
								"IdentificationDay": "17",
								"IdentificationMonth": "05",
								"IdentificationYear": "2024",
								"IdentificationDateSupplement": "",
								"ResponsibleName": "Testuser 1",
								"ResponsibleAgentURI": "https://zfmk.diversityagents.de/0815",
								"IdentificationCategory": "determination",
								"IdentificationQualifier": "",
								"TypeStatus": "paratype",
								"TypeNotes": "",
								"ReferenceTitle": "",
								"ReferenceURI": null,
								"ReferenceDetails": null,
								"Notes": null
							},
							{
								"TaxonomicName": "Dendrobaena veneta (Rosa, 1886)",
								"NameURI": "https://www.gbif.org/species/5739805",
								"VernacularTerm": "Garten Regenwurm",
								"IdentificationDay": "17",
								"IdentificationMonth": "05",
								"IdentificationYear": "2024",
								"IdentificationDateSupplement": "",
								"ResponsibleName": "Testuser 1",
								"ResponsibleAgentURI": "https://zfmk.diversityagents.de/0815",
								"IdentificationCategory": "determination",
								"IdentificationQualifier": "",
								"TypeStatus": "paratype",
								"TypeNotes": "",
								"ReferenceTitle": "",
								"ReferenceURI": null,
								"ReferenceDetails": null,
								"Notes": null
							}
						]
					}
				],
				"CollectionSpecimenParts": [
					{
						"AccessionNumber": "testunit 04 slides",
						"PreparationMethod": "Maceration",
						"PartSublabel": "testunit 04 slides label",
						"MaterialCategory": "microscopic slides",
						"StorageLocation": "Room 15, shelf 22, drawer 12",
						"Stock": 20,
						"StockUnit": null,
						"StorageContainer": "Box 25",
						"ResponsibleName": "a testuser that might be in Collection Agents",
						"ResponsibleAgentURI": "https://zfmk.diversityworkbench/collectionagents/0815",
						"Notes": "something to say about this part",
						"DataWithholdingReason": "withhold by default",
						"Collection": {
							"@id": "_:c1"
						}
					}
				],
				"Projects": [
					{
						"@id": "_:p2"
					}
				]
			}
		]
	}


This file is available as ./examples/insert_CS_02.json. The data can be inserted with:

	curl -k -X POST "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	--data-binary @examples/insert_CS_02.json


### Read datasets:

	curl -k -X GET "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"CollectionSpecimenIDs": [
			862,
			863
		]
	}'

or with RowGUIDs

	curl -k -X GET "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"RowGUIDs": [
			"49FD423E-F36B-1410-9186-00FFFFFFFFFF",
			"4BFD423E-F36B-1410-9186-00FFFFFFFFFF"
		]
	}'


or with token in submitted json

	curl -k -X GET "https://localhost/dc_rest_api/specimens" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"CollectionSpecimenIDs": [
			862,
			863
		],
		"token": "b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c"
	}'


### Delete datasets:

	curl -k -X DELETE "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"CollectionSpecimenIDs": [
			862,
			863
		]
	}'

or with RowGUIDs

	curl -k -X DELETE "https://localhost/dc_rest_api/specimens?token=b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"RowGUIDs": [
			"49FD423E-F36B-1410-9186-00FFFFFFFFFF",
			"4BFD423E-F36B-1410-9186-00FFFFFFFFFF"
		]
	}'

or with token in submitted json

	curl -k -X DELETE "https://localhost/dc_rest_api/specimens" \
	-H "Content-Type: application/json" -H "Accept: application/json" \
	-d '{
		"RowGUIDs": [
			"49FD423E-F36B-1410-9186-00FFFFFFFFFF",
			"4BFD423E-F36B-1410-9186-00FFFFFFFFFF"
		],
		"token": "b9cxiRFwhFOUOzmdoOKO5ukGyWcl6g8hSnblSiyTPhN-QU-E0MxJVwiWbkE4qsL1c"
	}'


