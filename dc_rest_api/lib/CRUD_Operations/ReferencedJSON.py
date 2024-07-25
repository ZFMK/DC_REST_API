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
		
		self.references = {
			"Collection": "Collections",
			"CollectionEvent": "CollectionEvents",
			"CollectionExternalDatasource": "CollectionExternalDatasources",
		}
		
		self.initExtractedDicts()


	def initExtractedDicts(self):
		self.extracted_dicts = {}
		for key in self.references:
			self.extracted_dicts[self.references[key]] = []


	def insertSubdicts(self):
		for key in self.json_dicts:
			if key not in self.extracted_dicts:
				self.__insertRefererencedSubdicts(self.json_dicts[key])


	def __insertRefererencedSubdicts(self, subdicts):
		for subdict in subdicts:
			#if isinstance(subdict, dict) or isinstance(subdict[key], list) or isinstance(subdict[key], tuple)
			
			if isinstance(subdict, dict):
				for key in subdict:
					if key in self.references:
						if isinstance(subdict[key], dict):
							if '@id' in subdict[key] and len(subdict[key]) == 1:
								for element in self.extracted_dicts[self.references[key]]:
									if element['@id'] == subdict[key]['@id']:
										subdict[key] = dict(element)
						
						elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
							referenced_elements = []
							
							for refdict in subdict[key]:
								if '@id' in refdict and len(refdict) == 1:
									for element in self.extracted_dicts[self.references[key]]:
										if element['@id'] == refdict['@id']:
											referenced_elements.append(dict(element))
							subdict[key] = referenced_elements
					elif isinstance(subdict[key], list) or isinstance(subdict[key], tuple):
						self.__insertRefererencedSubdicts(subdict[key])
			elif isinstance(subdict, list) or isinstance(subdict, tuple):
				self.__insertRefererencedSubdicts(subdict)
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


