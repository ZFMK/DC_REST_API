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
from dc_rest_api.lib.CRUD_Operations.JSON2Datadicts import JSON2Datadicts

from dc_rest_api.lib.CRUD_Operations.CollectionInserter import CollectionInserter
from dc_rest_api.lib.CRUD_Operations.CollectionEventInserter import CollectionEventInserter
from dc_rest_api.lib.CRUD_Operations.ExternalDatasourceInserter import ExternalDatasourceInserter
from dc_rest_api.lib.CRUD_Operations.CollectionSpecimenInserter import CollectionSpecimenInserter


import pudb
import json


class CollectionSpecimenViews():

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


	@view_config(route_name='specimen', accept='application/json', renderer="json", request_method = "POST")
	def insertSpecimensJSON(self):
		jsonresponse = {
			'title': 'API for requests on DiversityCollection database',
			'messages': self.messages
		}
		
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return jsonresponse
		
		# TODO: move this into extra method and shorten the way to get self.dc_db
		try:
			security = SecurityPolicy()
			token = security.get_token_from_request(self.request)
			dbsession = DBSession()
			username, password = dbsession.get_credentials_from_session(token)
			dc_config = dbsession.get_mssql_connectionparams_by_token(token)
			if dc_config is None:
				self.messages.append('There is no valid database connection for your session, try to login again')
				return jsonresponse
			dc_config['username'] = username
			dc_config['password'] = password
			self.dc_db = MSSQLConnector(config = dc_config)
		except:
			self.messages.append('Can not connect to DiversityCollection server. Please contact server administrator')
			return jsonresponse
		
		pudb.set_trace()
		referenced_json = ReferencedJSON(self.request_params.json_body)
		
		referenced_json.extractSubdicts()
		#referenced_json.insertSubdicts()
		
		#dataparser = JSON2Datadicts(self.request_params.json_body)
		#self.datadicts = dataparser.parseJSON(self.request_params.json_body)
		
		if 'Collections' in self.request_params.json_body:
			collections = self.request_params.json_body['Collections']
			c_inserter = CollectionInserter(self.dc_db, users_roles = self.roles)
			c_inserter.insertCollectionData(collections)
		
		if 'CollectionEvents' in self.request_params.json_body:
			events = self.request_params.json_body['CollectionEvents']
			ce_inserter = CollectionEventInserter(self.dc_db)
			ce_inserter.insertCollectionEventData(events)
		
		if 'CollectionExternalDatasources' in self.request_params.json_body:
			datasources = self.request_params.json_body['CollectionExternalDatasources']
			ed_inserter = ExternalDatasourceInserter(self.dc_db)
			ed_inserter.insertExternalDatasourceData(datasources)
		
		if 'CollectionSpecimens' in self.request_params.json_body:
			#pudb.set_trace()
			#dataparser = JSON2Datadicts(self.request_params.json_body)
			#self.datadicts = dataparser.parseJSON(self.request_params.json_body)
			
			self.payload = self.request_params.json_body['CollectionSpecimens']
			cs_inserter = CollectionSpecimenInserter(self.dc_db, users_roles = self.roles)
			cs_inserter.setSpecimenDicts(self.payload)
			cs_inserter.insertSpecimenData()
		
		else:
			self.messages.append('Error: no "CollectionSpecimens" array in json data')
		
		cs_data = json.loads(json.dumps(self.payload, default = str))
		
		jsonresponse = {
			'title': 'DC REST API Create Resources',
			#'aggregations': aggregations,
			'messages': self.messages,
			'CollectionSpecimens': cs_data
		}
		
		return jsonresponse



