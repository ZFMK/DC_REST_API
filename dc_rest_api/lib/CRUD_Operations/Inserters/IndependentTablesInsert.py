import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('./config.ini')

import logging
import logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')
errorlog = logging.getLogger('error')

from dc_rest_api.lib.CRUD_Operations.Inserters.ProjectInserter import ProjectInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionExternalDatasourceInserter import CollectionExternalDatasourceInserter

from dc_rest_api.lib.CRUD_Operations.Inserters.AnalysisInserter import AnalysisInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.MethodInserter import MethodInserter
from dc_rest_api.lib.CRUD_Operations.Inserters.ParameterInserter import ParameterInserter


# this class do only work with the flattened dicts

class IndependentTablesInsert():
	def __init__(self, dc_db, json_dict, uid, users_roles = []):
		self.dc_db = dc_db
		
		self.json_dict = json_dict
		
		self.uid = uid
		self.users_roles = users_roles
		
		self.references = {
			"Projects": ["Projects", "ProjectID"],
			"Collection": ["Collections", "CollectionID"],
			"CollectionEvent": ["CollectionEvents", "CollectionEventID"],
			"CollectionExternalDatasource": ["CollectionExternalDatasources", "ExternalDatasourceID"],
			"Analysis": ["Analyses", "AnalysisID"],
			"Method": ["Methods", "MethodID"],
			"Parameter": ["Parameters", "ParameterID"]
		}
		
		self.messages = []


	def insertIndependentTables(self):
		# TODO: error handling when user does not have the appropriate rights
		
		if 'Projects' in self.json_dict:
			try:
				projects = self.json_dict['Projects']
				self.p_inserter = ProjectInserter(self.dc_db, self.uid, self.users_roles)
				self.p_inserter.insertProjectData(projects)
			except Exception as e:
				self.messages.extend('Exception while inserting projects')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting projects', exc_info = True)
		
		if 'Collections' in self.json_dict:
			try:
				collections = self.json_dict['Collections']
				c_inserter = CollectionInserter(self.dc_db, self.users_roles)
				c_inserter.insertCollectionData(collections)
			except Exception as e:
				self.messages.extend('Exception while inserting collections')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting collections', exc_info = True)
		
		if 'CollectionEvents' in self.json_dict:
			try:
				events = self.json_dict['CollectionEvents']
				ce_inserter = CollectionEventInserter(self.dc_db)
				ce_inserter.insertCollectionEventData(events)
			except:
				self.messages.extend('Exception while inserting collection events')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting collection events', exc_info = True)
		
		if 'CollectionExternalDatasources' in self.json_dict:
			try:
				datasources = self.json_dict['CollectionExternalDatasources']
				ed_inserter = CollectionExternalDatasourceInserter(self.dc_db, self.users_roles)
				ed_inserter.insertExternalDatasourceData(datasources)
			except:
				self.messages.extend('Exception while inserting collection external datasources')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting collection external datasources', exc_info = True)
		
		if 'Analyses' in self.json_dict:
			try:
				analyses = self.json_dict['Analyses']
				a_inserter = AnalysisInserter(self.dc_db)
				a_inserter.insertAnalysisData(analyses)
			except:
				self.messages.extend('Exception while inserting analyses')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting analyses', exc_info = True)
		
		if 'Methods' in self.json_dict:
			try:
				methods = self.json_dict['Methods']
				m_inserter = MethodInserter(self.dc_db)
				m_inserter.insertMethodData(methods)
			except:
				self.messages.extend('Exception while inserting methods')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting methods', exc_info = True)
		
		# set MethodIDs in self.json_dict[Parameters] because Parameters depend on them
		if 'Parameters' in self.json_dict:
			for pm_id in self.json_dict['Parameters']:
				try:
					m_id = self.json_dict['Parameters'][pm_id]['@id_method']
					self.json_dict['Parameters'][pm_id]['MethodID'] = self.json_dict['Methods'][m_id]['MethodID']
				except:
					self.messages.extend('Exception while inserting parameters')
					errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting parameters, MethodID for parameter could not be found', exc_info = True)
					self.messages.extend('MethodID for parameter {0} could not be found'.format(pm_id))
			try:
				parameters = self.json_dict['Parameters']
				pm_inserter = ParameterInserter(self.dc_db)
				pm_inserter.insertParameterData(parameters)
			except:
				self.messages.extend('Exception while inserting parameters')
				errorlog.error('Exception in InsertDeleteQueue.insert_DC_data(), inserting parameters', exc_info = True)
		
		return


	def insertCollectionProjects(self, specimens_list):
		self.p_inserter.insertCollectionProjects(specimens_list)


	def setLinkedIDs(self, data_dicts):
		# set the DC-ID of the object after it has been inserted into the surrounding object
		# this is neccessary because when the surrounding objects are inserted into the DC
		# they need the IDs of the sub-objects to connect to them
		if isinstance(data_dicts, list):
			for data_dict in data_dicts:
				# get keys in an extra list
				# when using for key in datadict the length of the dict changes and an error is thrown
				keylist = [key for key in data_dict]
				for key in keylist:
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
				if not 'ProjectID' in data_dict:
					data_dict['ProjectIDs'] = []
				data_dict['ProjectIDs'].append(self.json_dict['Projects'][project_id]['ProjectID'])
		
		except:
			#data_dict['ProjectID'] = []
			pass
		return


