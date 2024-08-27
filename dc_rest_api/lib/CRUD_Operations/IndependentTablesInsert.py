import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.ProjectInserter import ProjectInserter
from dc_rest_api.lib.CRUD_Operations.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.ExternalDatasourceInserter import ExternalDatasourceInserter


# this class do only work with the flattened dicts

class IndependentTablesInsert():
	def __init__(self, dc_db, uid, users_roles = []):
		self.dc_db = dc_db
		self.uid = uid
		self.users_roles = users_roles


	def insertIndependentTables(self, json_dict):
		self.json_dict = json_dict
		
		if 'Projects' in self.json_dict:
			try:
				projects = self.json_dict['Projects']
				p_inserter = ProjectInserter(self.dc_db, self.uid, self.users_roles)
				p_inserter.insertProjectData(projects)
			except:
				self.messages.extend(p_inserter.messages)
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


	def setEventIDsInParentDicts(self, parent_key):
		for p_id in self.json_dict[parent_key]:
			p_dict = self.json_dict[parent_key][p_id]
			try:
				event_id = p_dict['CollectionEvent']
				p_dict['CollectionEventID'] = self.json_dict['CollectionEvents'][event_id]['CollectionEventID']
			except:
				pass
		return


	def setExternaDatasourceIDsInParentDicts(self, parent_key):
		for p_id in self.json_dict[parent_key]:
			p_dict = self.json_dict[parent_key][p_id]
			try:
				ed_id = p_dict['CollectionExternalDatasource']
				p_dict['ExternalDatasourceID'] = self.json_dict['CollectionExternalDatasources'][ed_id]['ExternalDatasourceID']
			except:
				pass
		return


	def setCollectionIDsInParentDicts(self, parent_key):
		for p_id in self.json_dict[parent_key]:
			p_dict = self.json_dict[parent_key][p_id]
			try:
				collection_id = p_dict['Collection']
				p_dict['CollectionID'] = self.json_dict['Collections'][collection_id]['CollectionID']
			except:
				pass
		return


	def setProjectIDsInParentDicts(self, parent_key):
		for p_id in self.json_dict[parent_key]:
			p_dict = self.json_dict[parent_key][p_id]
			try:
				project_ids = p_dict['Projects']
				for project_id in project_ids:
					if not 'ProjectID' in p_dict:
						p_dict['ProjectID'] = []
					p_dict['ProjectID'].append(self.json_dict['Projects'][project_id]['ProjectID'])
			except:
				pass
		return
