import pudb

import hashlib
import json

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class ReferencedJSON():

	def __init__(self, json_dicts):
		
		self.json_dicts = json_dicts
		self.messages = []

		self.references = {
			"Projects": "Projects", 
			"Collection": "Collections",
			"CollectionEvent": "CollectionEvents",
			"CollectionExternalDatasource": "CollectionExternalDatasources",
			"Analysis": "Analyses",
			"Method": "Methods",
			"Parameter": "Parameters"
		}
		
		self.flat_references = {
			"Projects": "Projects", 
			"Collection": "Collections",
			"CollectionEvent": "CollectionEvents",
			"CollectionExternalDatasource": "CollectionExternalDatasources",
			"CollectionSpecimens": "CollectionSpecimens",
			"Analysis": "Analyses",
			"Method": "Methods",
			"Parameter": "Parameters"
		}
		
		self.flattened_dicts = {
			'Projects': {},
			'Collections': {},
			'CollectionExternalDatasources': {},
			'CollectionEvents': {},
			'CollectionSpecimens': {},
			'Analyses': {},
			'Methods': {},
			'Parameters': {}
		}
		
		self.initExtractedDicts()


	def initExtractedDicts(self):
		self.extracted_dicts = {}
		for key in self.references:
			self.extracted_dicts[self.references[key]] = []


	def insertSubdicts(self):
		for key in self.json_dicts:
			if key not in self.extracted_dicts:
				self.__insertSubdicts(self.json_dicts[key])
		# delete the referenced dicts when they have been inserted as subdicts
		for key in self.extracted_dicts:
			if key in self.json_dicts:
				del self.json_dicts[key]

	def __checkReferencedElementsAvailable(self, key):
		if self.references[key] not in self.json_dicts:
			self.messages.append('List of {0} is referenced in json file but not available in file'.format(self.references[key]))
			raise ValueError



	def __insertSubdicts(self, subdicts):
		# subdicts is a list of dicts
		for subdict in subdicts:
			#if isinstance(subdict, dict) or isinstance(subdict[key], list) or isinstance(subdict[key], tuple)
			
			if isinstance(subdict, dict):
				for key in subdict:
					if key in self.references:
						# @id-key is in subdict: {'CollectionEvent':{'@id': '_:my_key_refernecing_a_collection_event'}}
						if isinstance(subdict[key], dict):
							if '@id' in subdict[key] and len(subdict[key]) == 1:
								self.__checkReferencedElementsAvailable(key)
								for element in self.json_dicts[self.references[key]]:
									if element['@id'] == subdict[key]['@id']:
										subdict[key] = dict(element)
						
						elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
							referenced_elements = []
							
							for refdict in subdict[key]:
								reference_solved = False
								if '@id' in refdict and len(refdict) == 1:
									self.__checkReferencedElementsAvailable(key)
									for element in self.json_dicts[self.references[key]]:
										if element['@id'] == refdict['@id']:
											referenced_elements.append(dict(element))
											reference_solved = True
								if reference_solved is False:
									referenced_elements.append(refdict)
							subdict[key] = referenced_elements
					elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
						self.__insertSubdicts(subdict[key])
			elif isinstance(subdict, list) or isinstance(subdict, tuple):
				self.__insertSubdicts(subdict)
		return


	def extractSubdicts(self):
		self.__setExtractedDicts()
		self.__overwriteJSONWithExtractedDicts()


	def __setExtractedDicts(self):
		
		self.initExtractedDicts()
		
		for key in self.extracted_dicts:
			if key in self.json_dicts:
				self.extracted_dicts[key] = self.json_dicts[key]
				del self.json_dicts[key]
		
		for key in self.json_dicts:
			if key not in self.extracted_dicts:
				self.__extractInternalSubdicts(self.json_dicts[key])
		
		return


	def __overwriteJSONWithExtractedDicts(self):
		for key in self.extracted_dicts:
			self.json_dicts[key] = self.extracted_dicts[key]
		return


	def __extractInternalSubdicts(self, subdicts):
		for subdict in subdicts:
			#if isinstance(subdict, dict) or isinstance(subdict[key], list) or isinstance(subdict[key], tuple)
			
			if isinstance(subdict, dict):
				for key in subdict:
					if key in self.references:
						# there might be a list of elements or just one (e. g. ProjectProxy or Collection)
						
						extracted_ids = [element['@id'] for element in self.extracted_dicts[self.references[key]]]
						
						if isinstance(subdict[key], dict):
							if not '@id' in subdict[key]:
								dict_id = '_:' + hashlib.sha256(json.dumps(subdict[key]).encode()).hexdigest()
								
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
									dict_id = '_:' + hashlib.sha256(json.dumps(element).encode()).hexdigest()
									
									if not dict_id in extracted_ids:
										element['@id'] = dict_id
										self.extracted_dicts[self.references[key]].append(dict(element))
										extracted_ids.append(dict_id)
									
									replaced_elements.append({'@id': dict_id})
								
								elif '@id' in element and len(element) == 1:
									replaced_elements.append(element)
							
							subdict[key] = replaced_elements
					
					elif subdict[key] is not None and (isinstance(subdict[key], list) or isinstance(subdict[key], tuple) or isinstance(subdict[key], dict)):
						self.__extractInternalSubdicts(subdict[key])
			elif isinstance(subdict, list) or isinstance(subdict, tuple) or isinstance(subdict, dict):
				self.__extractInternalSubdicts(subdict)
		return


	def flatten2Dicts(self):
		# first set all dicts as subdict so that the references are all resolved
		# when the user provides a mixed json with some extracted independent tables
		self.insertSubdicts()
		
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


	def __copyMethodHashIntoChildParameters(self, method_dicts):
		for iuam_entry in method_dicts:
			if 'Method' in iuam_entry and isinstance(iuam_entry['Method'], dict):
				copied_dict = dict(iuam_entry['Method'])
				if '@id' in copied_dict:
					del copied_dict['@id']
				methodhash = '_:' + hashlib.sha256(json.dumps(copied_dict).encode()).hexdigest()
				
				if 'Parameters' in iuam_entry:
					for iuamp_entry in iuam_entry['Parameters']:
						if 'Parameter' in iuamp_entry and isinstance(iuamp_entry['Parameter'], dict):
							iuamp_entry['Parameter']['@id_method'] = methodhash
		return


	def __flatten_dicts(self, subdicts):
		for key in subdicts:
			# due to the problem, that Parameter depends on MethodID (i have no idea why) copy the MethodID from the parent Method dict into the child Parameter dict
			# Method dicts have been flattened before because the recursive method runs into the leaves first
			if key == 'Methods':
				self.__copyMethodHashIntoChildParameters(subdicts[key])
			
			# run into the leafs and replace them before replacing the parent nodes
			if isinstance(subdicts[key], dict):
				self.__flatten_dicts(subdicts[key])
			
			elif isinstance(subdicts[key], list) or isinstance(subdicts[key], tuple):
				
				for subdict in subdicts[key]:
					if isinstance(subdict, list) or isinstance(subdict, tuple) or isinstance(subdict, dict):
						self.__flatten_dicts(subdict)
			
			# dicts with the same id will be overwritten, could this be shortened
			if key in self.flat_references:
				
				if isinstance(subdicts[key], dict):
					dict_id, copied_dict = self.__calculateSHA(key, subdicts[key])
					self.flattened_dicts[self.flat_references[key]][dict_id] = copied_dict
					self.flattened_dicts[self.flat_references[key]][dict_id]['@id'] = dict_id
					subdicts[key] = dict_id
				elif isinstance(subdicts[key], list) or isinstance(subdicts[key], tuple):
					idslist = []
					for subdict in subdicts[key]:
						if isinstance(subdict, dict):
							dict_id, copied_dict = self.__calculateSHA(key, subdict)
							self.flattened_dicts[self.flat_references[key]][dict_id] = copied_dict
							self.flattened_dicts[self.flat_references[key]][dict_id]['@id'] = dict_id
							idslist.append(dict_id)
					subdicts[key] = idslist
		
		return


	def __calculateSHA(self, key, json_dict):
		copied_dict = dict(json_dict)
		self.cs_independend_tables = [key for key in self.flat_references]
		if key in self.cs_independend_tables and '@id' in copied_dict:
			del copied_dict['@id']
		dict_id = '_:' + hashlib.sha256(json.dumps(copied_dict).encode()).hexdigest()
		return dict_id, copied_dict


	def get_filtered_result(self, path_list = None, all_data = False):
		
		if path_list is None:
			path_list = []
		
		self.ids_not_ending_with_id = [
			'AnalysisNumber', 'MethodMarker', 'CollectorsName', 'CollectorsSequence',
			'CollectorsEventNumber', 'AccessionNumber', 'IdentificationSequence'
		]
		
		result_dict = {}
		
		if len(path_list) > 0:
			pathes = [path for path in path_list]
			path_dict = {}
			for path in pathes:
				self.__set_path_dict(path, path_dict)
			
			subdict = self.json_dicts
			self.__filter_result_by_path_dict(path_dict, all_data, self.json_dicts, result_dict)
			return result_dict
		
		elif all_data is False:
			self.__filter_result(all_data, self.json_dicts, result_dict)
			return result_dict
		
		else:
			return self.json_dicts


	def __set_path_dict(self, path, path_dict):
		while len(path) > 0:
			path_element = path.pop(0)
			if path_element not in path_dict:
				path_dict[path_element] = {}
				self.__set_path_dict(path, path_dict[path_element])
		return


	def __get_filtered_result_item(self, item, all_data):
		if all_data is True:
			filtered_dict = {}
			for key in item:
				if not isinstance(item[key], dict) and not isinstance(item[key], list):
					filtered_dict[key] = item[key]
			return filtered_dict
		
		else:
			ids_dict = {}
			for key in item:
				if key in self.ids_not_ending_with_id:
					ids_dict[key] = item[key]
				elif key.endswith('ID'):
					ids_dict[key] = item[key]
			return ids_dict


	def __filter_result(self, all_data, subdict, result_dict):
		for key in subdict:
			if isinstance(subdict[key], list):
				if key not in result_dict:
					result_dict[key] = []
				for subdict_item in subdict[key]:
					if isinstance(subdict_item, dict):
						filtered_item = self.__get_filtered_result_item(subdict_item, all_data)
						if len(filtered_item) > 0:
							result_dict[key].append(filtered_item)
							self.__filter_result(all_data, subdict_item, filtered_item)
			elif isinstance(subdict[key], dict):
				filtered_item = self.__get_filtered_result_item(subdict[key], all_data)
				if len(filtered_item) > 0:
					result_dict[key] = filtered_item
					self.__filter_result(all_data, subdict[key], filtered_item)
		return


	def __filter_result_by_path_dict(self, path_dict, all_data, subdict, result_dict):
		for path_name in path_dict:
			if path_name in subdict:
				if path_name not in result_dict:
					result_dict[path_name] = []
				
				if isinstance(subdict[path_name], list):
					for subdict_item in subdict[path_name]:
						if isinstance(subdict_item, dict):
							filtered_item = self.__get_filtered_result_item(subdict_item, all_data)
							if len(filtered_item) > 0:
								result_dict[path_name].append(filtered_item)
								self.__filter_result_by_path_dict(path_dict[path_name], all_data, subdict_item, filtered_item)
				elif isinstance(subdict[path_name], dict):
					filtered_item = self.__get_filtered_result_item(subdict[path_name], all_data)
					if len(filtered_item) > 0:
						result_dict[path_name] = filtered_item
						self.__filter_result_by_path_dict(path_dict[path_name], all_data, subdict[path_name], filtered_item)
		return


	def insertFlattenedSubdicts(self):
		self.__insertFlattenedSubdicts(self.json_dicts)
		# delete the referenced dicts when they have been inserted as subdicts
		for key in self.flattened_keys:
			# do not delete 'CollectionSpecimens' as this is the dict where all subdicts are put in
			if key != 'CollectionSpecimens':
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
			# do not insert empty key:values
			# this must be done with a list of keys, not with for key in subdict as that would change the available keys during the iteration
			keys = [key for key in subdict.keys()]
			for key in keys:
				if subdict[key] is None:
					del subdict[key]
			
			for key in subdict:
				if isinstance(key, str) and key in self.flat_references:
					
					if isinstance(subdict[key], str) and subdict[key] in self.json_dicts[self.flat_references[key]] and isinstance(self.json_dicts[self.flat_references[key]][subdict[key]], dict):
						dict_id = str(subdict[key])
						subdict[key] = self.json_dicts[self.flat_references[key]][dict_id]
					
					elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
						for i in range(len(subdict[key])):
							if isinstance(subdict[key][i], str) and subdict[key][i] in self.json_dicts[self.flat_references[key]] and isinstance(self.json_dicts[self.flat_references[key]][subdict[key][i]], dict):
								subdict[key][i] = self.json_dicts[self.flat_references[key]][subdict[key][i]]
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

