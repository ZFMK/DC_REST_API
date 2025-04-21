import pudb

import hashlib
import json

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class NonReferencedJSON():

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
		self.initExtractedDicts()


	def initExtractedDicts(self):
		# create an empty dict with all referenced key names 
		self.extracted_dicts = {}
		for key in self.references:
			self.extracted_dicts[self.references[key]] = []


	def insertSubdicts(self):
		# iterate over all keys that are not in extracted dicts
		# the extracted dicts will be inserted into the subdicts by 
		# __insertSubdicts()
		for key in self.json_dicts:
			if key not in self.extracted_dicts:
				self.__insertSubdicts(self.json_dicts[key])
		# delete the referenced dicts when they have been inserted as subdicts
		for key in self.extracted_dicts:
			if key in self.json_dicts:
				del self.json_dicts[key]
		return


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


	def __checkReferencedElementsAvailable(self, key):
		if self.references[key] not in self.json_dicts:
			self.messages.append('List of {0} is referenced in json file but not available in file'.format(self.references[key]))
			raise ValueError


