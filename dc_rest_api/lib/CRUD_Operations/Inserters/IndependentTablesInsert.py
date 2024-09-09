import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Inserters.ProjectInserter import ProjectInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.ExternalDatasourceInserter import ExternalDatasourceInserter


# this class do only work with the flattened dicts

class IndependentTablesInsert():
	def __init__(self, dc_db, json_dict, uid, users_roles = []):
		self.dc_db = dc_db
		
		self.json_dict = json_dict
		
		self.uid = uid
		self.users_roles = users_roles


	def insertIndependentTables(self):
		
		if 'Projects' in self.json_dict:
			try:
				projects = self.json_dict['Projects']
				self.p_inserter = ProjectInserter(self.dc_db, self.uid, self.users_roles)
				self.p_inserter.insertProjectData(projects)
			except:
				self.messages.extend(self.p_inserter.messages)
				pudb.set_trace()
		
		if 'Collections' in self.json_dict:
			try:
				collections = self.json_dict['Collections']
				c_inserter = CollectionInserter(self.dc_db, self.users_roles)
				c_inserter.insertCollectionData(collections)
			except:
				self.messages.extend(c_inserter.messages)
				pudb.set_trace()
		
		if 'CollectionEvents' in self.json_dict:
			try:
				events = self.json_dict['CollectionEvents']
				ce_inserter = CollectionEventInserter(self.dc_db)
				ce_inserter.insertCollectionEventData(events)
			except:
				self.messages.extend(ce_inserter.messages)
		
		if 'CollectionExternalDatasources' in self.json_dict:
			try:
				datasources = self.json_dict['CollectionExternalDatasources']
				ed_inserter = ExternalDatasourceInserter(self.dc_db, self.users_roles)
				ed_inserter.insertExternalDatasourceData(datasources)
			except:
				self.messages.extend(ed_inserter.messages)
				#pudb.set_trace()


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


	def insertCollectionProjects(self, specimens_list):
		self.p_inserter.insertCollectionProjects(specimens_list)
