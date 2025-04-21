import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class LinkIndependentTables():
	# set the IDs from the newly added independent tables in the json dict that links to such an independent table
	# this is a recursive method, so it will work on the different levels of the json dict
	def __init__(self):
		self.references = {
			"Projects": ["Projects", "ProjectID"],
			"Collection": ["Collections", "CollectionID"],
			"CollectionEvent": ["CollectionEvents", "CollectionEventID"],
			"CollectionExternalDatasource": ["CollectionExternalDatasources", "ExternalDatasourceID"],
			"Analysis": ["Analyses", "AnalysisID"],
			"Method": ["Methods", "MethodID"],
			"Parameter": ["Parameters", "ParameterID"]
		}



	def setLinkedIDs(self, data_dicts):
		
		if isinstance(data_dicts, list):
			for data_dict in data_dicts:
				for key in data_dict:
					if key in self.references:
						if key == 'Projects':
							self.__setLinkedProjectIDs(data_dict)
						else:
							self.__setLinkedID(data_dict, key)
					else:
						self.setLinkedIDs(data_dict[key])
				
		elif isinstance(data_dicts, dict):
			for key in data_dicts:
				self.setLinkedIDs(data_dicts[key])
		return


	def __setLinkedID(self, data_dict, key):
		if key == 'Analysis':
			pudb.set_trace()
		
		try:
			ref_id = data_dict[key]
			id_name = self.references[key][1]
			list_name = self.references[key][0]
			data_dict[id_name] = self.json_dict[list_name][ref_id][id_name]
		
		except:
			data_dict[id_name] = None
			pass


	def __setLinkedProjectIDs(self, data_dict):
		try:
			project_ids = data_dict['Projects']
			for project_id in project_ids:
				#if not 'ProjectID' in data_dict:
				#	data_dict['ProjectID'] = []
				data_dict['ProjectID'].append(self.json_dict['Projects'][project_id]['ProjectID'])
		
		except:
			#data_dict['ProjectID'] = []
			pass
		return


	'''
	def setLinkedEventIDs(self, data_dicts):
		for data_dict in data_dicts:
			try:
				ref_id = data_dict['CollectionEvent']
				data_dict['CollectionEventID'] = self.json_dict['CollectionEvents'][ref_id]['CollectionEventID']
			
			except:
				data_dict['CollectionEventID'] = None
				pass
		return


	def setLinkedCollectionIDs(self, data_dicts):
		for data_dict in data_dicts:
			try:
				ref_id = data_dict['Collection']
				data_dict['CollectionID'] = self.json_dict['Collections'][ref_id]['CollectionID']
			
			except:
				data_dict['CollectionID'] = None
				pass
		return


	def setLinkedExternalDatasourceIDs(self, data_dicts):
		for data_dict in data_dicts:
			try:
				ref_id = data_dict['CollectionExternalDatasource']
				data_dict['ExternalDatasourceID'] = self.json_dict['CollectionExternalDatasources'][ref_id]['ExternalDatasourceID']
			
			except:
				data_dict['ExternalDatasourceID'] = None
				pass
		return


	def setLinkedProjectIDs(self, data_dicts):
		for data_dict in data_dicts:
			try:
				project_ids = data_dict['Projects']
				for project_id in project_ids:
					if not 'ProjectID' in data_dict:
						data_dict['ProjectID'] = []
					data_dict['ProjectID'].append(self.json_dict['Projects'][project_id]['ProjectID'])
			
			except:
				data_dict['ProjectID'] = []
				pass
		return


	def setLinkedAnalysisIDs(self, data_dicts):
		pudb.set_trace()
		for data_dict in data_dicts:
			try:
				ref_id = data_dict['Analysis']
				data_dict['AnalysisID'] = self.json_dict['Analyses'][ref_id]['AnaylsisID']
			
			except:
				data_dict['CollectionID'] = None
				pass
		return
	'''

