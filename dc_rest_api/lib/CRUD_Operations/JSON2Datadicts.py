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
		
		self.datadicts = []
		
		self.setExtractedDicts()
		pudb.set_trace()
		self.parseJSON(self.json_dicts)
		self.overwriteJSONWithExtractedDicts()


	def setExtractedDicts(self):
		self.references = {
			"Collection": "Collections",
			"CollectionEvent": "CollectionEvents",
			"CollectionExternalDatasource": "CollectionExternalDatasources",
		}
		
		self.extracted_dicts = {}
		for key in self.references:
			self.extracted_dicts[self.references[key]] = []
		
		for key in self.extracted_dicts:
			if key in self.json_dicts:
				self.extracted_dicts[key] = self.json_dicts[key]
				del self.json_dicts[key]
		
		for key in self.json_dicts:
			if key not in self.extracted_dicts:
				self.extractInternalSubdicts(self.json_dicts[key])
		
		return


	def overwriteJSONWithExtractedDicts(self):
		for key in self.extracted_dicts:
			self.json_dicts[key] = self.extracted_dicts[key]
		return


	def parseJSON(self, json_dicts):
		keys_to_parse = [key for key in self.schemata]
		
		for key in json_dicts:
			
			if isinstance(json_dicts[key], list) or isinstance(json_dicts[key], tuple) and key in keys_to_parse:
				
				self.setDatadicts(self.schemata[key], json_dicts[key])
			
			# only parse elements further that can have sub-elements
				if key in [
							"CollectionSpecimens", 
							"IdentificationUnits", 
							"CollectionSpecimenParts"
						]:
					for inner_key in json_dicts[key]:
						if inner_key in keys_to_parse:
							self.parseJSONRecursively(json_dicts[key][inner_key])
			else:
				self.parseJSON(json_dicts[key])
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
			
			self.datadicts.append(json_dict)
			#except:
			#	pass
		
		return 


	def extractInternalSubdicts(self, subdicts):
		#pudb.set_trace()
		for subdict in subdicts:
			#if isinstance(subdict, dict) or isinstance(subdict[key], list) or isinstance(subdict[key], tuple)
			
			if isinstance(subdict, dict):
				for key in subdict:
					if key in self.references:
						
						# there might be a list of elements or just one (e. g. ProjectProxy Collection)
						
						extracted_ids = [element['@id'] for element in self.extracted_dicts[self.references[key]]]
						
						if isinstance(subdict[key], dict):
							if not '@id' in subdict[key]:
								dict_id = hashlib.sha256(json.dumps(subdict[key]).encode()).hexdigest()
								
								if not dict_id in extracted_ids:
									subdict[key]['@id'] = dict_id
									self.extracted_dicts[self.references[key]].append(dict(subdict[key]))
									extracted_ids.append(dict_id)
								
								subdict[key] = {'@id': dict_id}
						
						elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
							replaced_elements = []
							for element in subdict[key]:
								
								if not '@id' in element and len(element) > 0:
									# check if dict is in extracted dicts
									dict_id = hashlib.sha256(json.dumps(element).encode()).hexdigest()
									
									if not dict_id in extracted_ids:
										element['@id'] = dict_id
										self.extracted_dicts[self.references[key]].append(dict(element))
										extracted_ids.append(dict_id)
									
									replaced_elements.append({'@id': dict_id})
								
								elif '@id' in element and len(element) == 1:
									replaced_elements.append(element)
							
							subdict[key] = replaced_elements
					
					elif subdict[key] is not None and (isinstance(subdict[key], list) or isinstance(subdict[key], tuple) or isinstance(subdict[key], dict)):
						self.extractInternalSubdicts(subdict[key])
		return


	'''
	def expandInternalReferences(self):
		"""
		look up the data for internally referenced Collections, Events, etc. 
		These must be referenced by an '@id' value in the target dictionery and a structure:
		"Collection": {
			"@id": "value"
		}
		in the referencing structure
		"""
		
		for key in ["Collections", 
					"CollectionEvents", 
					"CollectionSpecimens", 
					"IdentificationUnits", 
					"CollectionSpecimenParts"
					]: 
			if key in self.datadicts:
				if key == "CollectionEvents":
					for element in self.datadicts["CollectionEvents"]:
						self.replaceIDWithTarget(element["CollectionEvents"], ["Collection", "CollectionEvent"])
				if key == "CollectionSpecimens":
					for element in self.datadicts["CollectionSpecimens"]:
						self.replaceIDWithTarget(element["CollectionSpecimens"], ["Collection", "CollectionEvent"])
				if key == "IdentificationUnits":
					for element in self.datadicts["IdentificationUnits"]:
						self.replaceIDWithTarget(element["IdentificationUnits"], ["CollectionSpecimen"])
				if key == "CollectionSpecimenParts":
					for element in self.datadicts["CollectionSpecimenParts"]:
						self.replaceIDWithTarget(element["CollectionSpecimenParts"], ["Collection"])


	def replaceIDWithTarget(self, element_list, target_keys = []):
		for element in element_list:
			for target_key in target_keys:
				if target_key in element and len(element[target_key]) == 1 and "@id" in element[target_key]:
					target = self.getTargetDictByInternalID(target_key, element[target_key]['@id'])
					if target is not None:
						element[target_key] = target
					else:
						raise ValueError('referenced internal @id:{0} for {1} can not be found'.format(element[target_key]['@id'], target_key))
		return


	def getTargetDictByInternalID(self, target_key, internal_id):
		target = None
		
		target_list_key = target_key + 's'
		# the targets must be in level 0 of the datadicts
		if target_list_key in self.datadicts:
			for target in self.datadicts[target_list_key]:
				if internal_id in target:
					return target
		return target
	'''
