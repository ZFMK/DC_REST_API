import pudb

import hashlib
import json

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Schemas.CollectionSchema import CollectionSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.CollectionEventSchema import CollectionEventSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.CollectionSpecimenSchema import CollectionSpecimenSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.IdentificationUnitSchema import IdentificationUnitSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.IdentificationSchema import IdentificationSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.CollectionSpecimenPartSchema import CollectionSpecimenPartSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.CollectionAgentSchema import CollectionAgentSchema
from dc_rest_api.lib.CRUD_Operations.Schemas.ExternalDatasourceSchema import ExternalDatasourceSchema


class JSON2Datadicts():

	def __init__(self, json_dicts):
		self.json_dicts = json_dicts
		
		self.schemata = {
			"Collections": CollectionSchema().getSchema(),
			"CollectionEvents": CollectionEventSchema().getSchema(),
			"CollectionSpecimens": CollectionSpecimenSchema().getSchema(),
			"IdentificationUnits": IdentificationUnitSchema().getSchema(),
			"Identifications": IdentificationSchema().getSchema(),
			"CollectionSpecimenParts": CollectionSpecimenPartSchema().getSchema(),
			"CollectionAgents": CollectionAgentSchema().getSchema(),
			"ExternalDatasources": ExternalDatasourceSchema().getSchema(),
		}
		
		self.dicts_with_subdicts = [
			"CollectionSpecimens", 
			"IdentificationUnits", 
			"CollectionSpecimenParts"
		]
		
		self.datadicts = {}
		
		pudb.set_trace()
		self.parseJSONRecursively(self.json_dicts)



	def parseJSONRecursively(self, json_dicts):
		keys_to_parse = [key for key in self.schemata]
		
		for key in json_dicts:
			
			if key in keys_to_parse and (isinstance(json_dicts[key], list) or isinstance(json_dicts[key], tuple)):
				
				self.setDatadicts(self.schemata[key], json_dicts[key])
			
			# only parse elements further that can have sub-elements
				if key in self.dicts_with_subdicts:
					for inner_key in json_dicts[key]:
						if inner_key in keys_to_parse:
							self.parseJSONRecursively(json_dicts[key][inner_key])
			#else:
			#	self.parseJSON(json_dicts[key])
		return


	def setDatadicts(self, schema, json_dicts = []):
		'''
		the set_datadicts method is used by the child objects to compare the data with the data schemes given in each child object
		the data schemes should ensure, that default values are added for missing entries (isn't that defined in database?)
		and that only available columns are set in the data dicts
		'''
		
		entry_num = 0
		
		for json_dict in json_dicts:
			entry_num += 1 
			json_dict['entry_num'] = entry_num
			
			#try:
			values_not_none = 0
			for entry in schema:
				if 'None allowed' in entry and entry['None allowed'] is False:
					if entry['colname'] not in json_dict or json_dict[entry['colname']] is None or json_dict[entry['colname']] == "":
						if 'default' in entry:
							json_dict[entry['colname']] = entry['default']
						else:
							raise ValueError('Can not insert data, field {0} is empty'.format(entry['colname']))
						if 'Minimal string length' in entry:
							if len(json_dict[entry['colname']]) < entry['Minimal string length']:
								raise ValueError('Can not insert data, value in field {0} must have a length of at least {1} letters'.format(entry['colname'], entry['Minimal string length']))
				elif entry['colname'] not in json_dict:
					if 'default' in entry:
						json_dict[entry['colname']] = entry['default']
					else:
						json_dict[entry['colname']] = None
				else:
					# just let the json_dict entry as it is
					pass
				
				if entry['colname'] != 'entry_num' and json_dict[entry['colname']] is not None:
					values_not_none += 1
			
			# check that at least one value in json_dict entries is not None
			if values_not_none < 1:
				raise ValueError('Can not insert data, all fields are empty')
			
			if key not in self.datadicts:
				self.datadicts[key] = []
			self.datadicts[key].append(json_dict)
			#except:
			#	pass
		
		return 


