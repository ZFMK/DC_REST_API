import pudb

import hashlib
import json

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.ReferencedJSON import ReferencedJSON


class FlattenedJSON():

	def __init__(self, json_dicts):
		
		self.json_dicts = json_dicts
		
		self.references = {
			"Projects": "Projects", 
			"Collection": "Collections",
			"CollectionEvent": "CollectionEvents",
			"CollectionExternalDatasource": "CollectionExternalDatasources",
			"CollectionSpecimens": "CollectionSpecimens"
		}
		
		self.referenced_json = ReferencedJSON(self.json_dicts)


	def flatten2ListsOfDicts(self):
		pudb.set_trace()
		# first set all dicts as subdict so that the references are all resolved
		self.referenced_json.self.insertSubdicts()
		
		self.flattened_dicts = {
			'Projects': {},
			'Collections': {},
			'CollectionExternalDatasources': {},
			'CollectionEvents': {},
			'CollectionSpecimens': {},
			#'IdentificationUnits': {},
			#'Identifications': {},
			#'CollectionSpecimenParts': {},
			#'CollectionAgents': {},
		}
		
		self.flattened_keys = [key for key in self.flattened_dicts]
		self.__flatten_dicts(self.json_dicts)
		
		self.__overwriteJSONWithFlattenedDicts()
		
		return


	def __overwriteJSONWithFlattenedDicts(self):
		"""
		this is needed to keep the reference on self.json_dicts
		"""
		for key in self.flattened_dicts:
			if key not in self.json_dicts:
				self.json_dicts[key] = {}
			self.json_dicts[key] = self.flattened_dicts[key]
		return


	def __flatten_dicts(self, subdicts):
		for key in subdicts:
			# run into the leafs and replace them before replacing the parent nodes
			if isinstance(subdicts[key], dict):
				self.__flatten_dicts(subdicts[key])
			
			elif isinstance(subdicts[key], list) or isinstance(subdicts[key], tuple):
				for subdict in subdicts[key]:
					if isinstance(subdict, list) or isinstance(subdict, tuple) or isinstance(subdict, dict):
						self.__flatten_dicts(subdict)
			
			if key in self.references:
				if isinstance(subdicts[key], dict):
					dict_id, copied_dict = self.__calculateSHA(key, subdicts[key])
					self.flattened_dicts[self.references[key]][dict_id] = copied_dict
					subdicts[key] = dict_id
				elif isinstance(subdicts[key], list) or isinstance(subdicts[key], tuple):
					idslist = []
					for subdict in subdicts[key]:
						if isinstance(subdict, dict):
							dict_id, copied_dict = self.__calculateSHA(key, subdict)
							self.flattened_dicts[self.references[key]][dict_id] = copied_dict
							idslist.append(dict_id)
					subdicts[key] = idslist
		
		return


	def __calculateSHA(self, key, json_dict):
		copied_dict = dict(json_dict)
		self.cs_independend_tables = [key for key in self.references]
		if key in self.cs_independend_tables and '@id' in copied_dict:
			del copied_dict['@id']
		dict_id = '_:' + hashlib.sha256(json.dumps(copied_dict).encode()).hexdigest()
		return dict_id, copied_dict


	def insertFlattenedSubdicts(self):
		pudb.set_trace()
		self.__insertFlattenedSubdicts(self.json_dicts)
		# delete the referenced dicts when they have been inserted as subdicts
		for key in self.flattened_keys:
			# do not delete 'CollectionSpecimens' as this is the dict where all subdicts are put in
			#if key != 'CollectionSpecimens':
			if key in self.json_dicts:
				del self.json_dicts[key]
		
		if 'CollectionSpecimens' in self.json_dicts:
			new_specimen_list = []
			for specimen_dict_key in self.json_dicts['CollectionSpecimens']:
				self.json_dicts['CollectionSpecimens'][specimen_dict_key]['@id'] = specimen_dict_key
				new_specimen_list.append(self.json_dicts['CollectionSpecimens'][specimen_dict_key])
			self.json_dicts['CollectionSpecimens'] = new_specimen_list
		
		return


	def __insertFlattenedSubdicts(self, subdict):
		if isinstance(subdict, dict):
			for key in subdict:
				if isinstance(key, str) and key in self.references:
					
					if isinstance(subdict[key], str) and subdict[key] in self.json_dicts[self.references[key]] and isinstance(self.json_dicts[self.references[key]][subdict[key]], dict):
						#if key == 'CollectionEvent':
						#	pudb.set_trace()
						dict_id = str(subdict[key])
						subdict[key] = self.json_dicts[self.references[key]][dict_id]
					
					elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
						for i in range(len(subdict[key])):
							if isinstance(subdict[key][i], str) and subdict[key][i] in self.json_dicts[self.references[key]] and isinstance(self.json_dicts[self.references[key]][subdict[key][i]], dict):
								subdict[key][i] = self.json_dicts[self.references[key]][subdict[key][i]]
							else:
								self.__insertFlattenedSubdicts(subdict[key][i])
					
					else:
						self.__insertFlattenedSubdicts(subdict[key])
				else:
					self.__insertFlattenedSubdicts(subdict[key])
		
		if isinstance(subdict, list) or isinstance(subdict, tuple):
			for element in subdict:
				self.__insertFlattenedSubdicts(element)
		
		return
		





	'''
	def updateIDs(self, key, id_columns = []):
		if not key in self.references:
			raise ValueError('ReferencedJSON.updateIDs: key must be in one of: {0}'.format(', '.join([refkey for refkey in self.references])))
		if len(id_columns) < 1:
			raise ValueError('ReferencedJSON.updateIDs: At least one id column must be given')
		
		self.__updateIDsInReferences(key, id_columns, json_dicts)
		self.__updateIDsInExtractedDicts(key, id_columns, json_dicts)
		return


	def __updateIDsInReferences(self, key, id_columns, subdicts):
		for subdict in subdicts:
			if isinstance(subdict, dict) and key in subdict:
				extracted_ids = [element['@id'] for element in self.extracted_dicts[self.references[key]]]
				
				if isinstance(subdict[key], dict):
					if '@id' in subdict[key] and len(subdict[key]) == 1:
						if subdict[key]['@id'] in extracted_ids:
							for element in self.extracted_dicts[self.references[key]]:
								if subdict[key]['@id'] == element['@id']:
									for id_column in id_columns:
										#if id_column in element:
										subdict[id_column] = element[id_column]
				
				if isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
					for subdictelement in subdict[key]:
						
						if '@id' in subdictelement and len(subdictelement) == 1:
							if subdictelement['@id'] in extracted_ids:
								for element in self.extracted_dicts[self.references[key]]:
									if subdictelement['@id'] == element['@id']:
										for id_column in id_columns:
											if id_column not in subdict:
												subdict[id_column] = []
											subdict[id_column].append(element[id_column])
				#else:
				#	self.__updateIDsInReferences(key, id_columns, subdict[key])
			
			else:
				self.__updateIDsInReferences(key, id_columns, subdict[key])
		return
	'''

