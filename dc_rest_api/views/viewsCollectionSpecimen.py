from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from DBConnectors.MSSQLConnector import MSSQLConnector

from dwb_authentication.security import SecurityPolicy
from dwb_authentication.dbsession import DBSession

from dc_rest_api.lib.Authentication.UserLogin import UserLogin
from dc_rest_api.views.RequestParams import RequestParams

from dc_rest_api.lib.CRUD_Operations.ReferencedJSON import ReferencedJSON

from dc_rest_api.lib.CRUD_Operations.ProjectInserter import ProjectInserter
from dc_rest_api.lib.CRUD_Operations.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.ExternalDatasourceInserter import ExternalDatasourceInserter
from dc_rest_api.lib.CRUD_Operations.CollectionSpecimenInserter import CollectionSpecimenInserter

from dc_rest_api.lib.CRUD_Operations.Deleters.CollectionSpecimenDeleter import CollectionSpecimenDeleter

import pudb
import json


class CollectionSpecimensViews():

	def __init__(self, request):
		
		self.request = request
		self.request_params = RequestParams(self.request)
		
		self.messages = []
		
		self.userlogin = UserLogin(self.request)
		self.credentials = self.request_params.credentials
		
		if len(self.request_params.credentials) > 0:
			self.userlogin.handle_credentials(self.credentials)
			self.messages.extend(self.userlogin.get_messages())
		
		self.uid = self.request.authenticated_userid
		
		self.roles = self.request.identity['dwb_roles']
		self.users_projects = self.request.identity['projects']
		self.users_project_ids = [project[0] for project in self.users_projects]


	@view_config(route_name='specimens', accept='application/json', renderer="json", request_method = "POST")
	def insertSpecimensJSON(self):
		jsonresponse = {
			'title': 'API for requests on DiversityCollection database',
			'messages': self.messages
		}
		
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return jsonresponse
		
		security = SecurityPolicy()
		self.dc_db = security.get_mssql_connector(self.request)
		if self.dc_db is None:
			self.messages.append('Can not connect to DiversityCollection server. Please check your credentials')
			return jsonresponse
		
		#pudb.set_trace()
		referenced_json = ReferencedJSON(self.request_params.json_body)
		
		#referenced_json.extractSubdicts()
		#referenced_json.insertSubdicts()
		referenced_json.flatten2ListsOfDicts()
		
		#referenced_json.insertFlattenedSubdicts()
		#pudb.set_trace()
		
		# TODO: move this into parent class for specimen insert and handle errors and messages
		if 'Projects' in self.request_params.json_body:
			try:
				projects = self.request_params.json_body['Projects']
				p_inserter = ProjectInserter(self.dc_db, self.uid, users_roles = self.roles)
				p_inserter.insertProjectData(projects)
			except:
				self.messages.extend(p_inserter.messages)
				pudb.set_trace()
		
		if 'Collections' in self.request_params.json_body:
			try:
				collections = self.request_params.json_body['Collections']
				c_inserter = CollectionInserter(self.dc_db, users_roles = self.roles)
				c_inserter.insertCollectionData(collections)
			except:
				self.messages.extend(c_inserter.messages)
				pudb.set_trace()
		
		if 'CollectionEvents' in self.request_params.json_body:
			try:
				events = self.request_params.json_body['CollectionEvents']
				ce_inserter = CollectionEventInserter(self.dc_db)
				ce_inserter.insertCollectionEventData(events)
			except:
				self.messages.extend(ce_inserter.messages)
		
		if 'CollectionExternalDatasources' in self.request_params.json_body:
			try:
				datasources = self.request_params.json_body['CollectionExternalDatasources']
				ed_inserter = ExternalDatasourceInserter(self.dc_db)
				ed_inserter.insertExternalDatasourceData(datasources)
			except:
				self.messages.extend(ed_inserter.messages)
				pudb.set_trace()
		
		
		if 'CollectionSpecimens' in self.request_params.json_body:
			try:
				specimens = self.request_params.json_body['CollectionSpecimens']
				cs_inserter = CollectionSpecimenInserter(self.dc_db, users_roles = self.roles)
				cs_inserter.insertSpecimenData(specimens)
			except:
				self.messages.extend(cs_inserter.messages)
				pudb.set_trace()
		
		else:
			self.messages.append('Error: no "CollectionSpecimens" array in json data')
		
		referenced_json.insertFlattenedSubdicts()
		
		cs_data = json.loads(json.dumps(self.request_params.json_body['CollectionSpecimens'], default = str))
		
		jsonresponse = {
			'title': 'DC REST API CREATE CollectionSpecimens',
			'messages': self.messages,
			'CollectionSpecimens': cs_data
		}
		
		return jsonresponse


	@view_config(route_name='specimens', accept='application/json', renderer="json", request_method = "DELETE")
	def deleteSpecimensJSON(self):
		jsonresponse = {
			'title': 'API for requests on DiversityCollection database, delete CollectionSpecimens',
			'messages': self.messages
		}
		
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return jsonresponse
		
		security = SecurityPolicy()
		self.dc_db = security.get_mssql_connector(self.request)
		if self.dc_db is None:
			self.messages.append('Can not connect to DiversityCollection server. Please check your credentials')
			return jsonresponse
		
		pudb.set_trace()
		
		specimen_deleter = CollectionSpecimenDeleter(self.dc_db)
		
		deleted = []
		if 'CollectionSpecimenIDs' in self.request_params.json_body:
			specimen_ids = self.request_params.json_body['CollectionSpecimenIDs']
			specimen_deleter.deleteByPrimaryKeys(specimen_ids)
			deleted = specimen_ids
		
		elif 'RowGUIDs' in self.request_params.json_body:
			rowguids = self.request_params.json_body['RowGUIDs']
			specimen_deleter.deleteByRowGUIDs(rowguids)
			deleted = rowguids
			pass
		
		jsonresponse = {
			'title': 'DC REST API DELETE CollectionSpecimens',
			'messages': self.messages,
			'deleted': deleted,
			#'CollectionSpecimens': cs_data
		}
		
		return jsonresponse

