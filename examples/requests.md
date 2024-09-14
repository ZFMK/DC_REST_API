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


