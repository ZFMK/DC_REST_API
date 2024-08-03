from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from dwb_authentication.security import SecurityPolicy
from dwb_authentication.dbsession import DBSession

from dc_rest_api.lib.Authentication.UserLogin import UserLogin
from dc_rest_api.views.RequestParams import RequestParams
from dc_rest_api.lib.CRUD_Operations.ReferencedJSON import ReferencedJSON
from dc_rest_api.lib.CRUD_Operations.ProjectInserter import ProjectInserter

import pudb
import json


class ProjectsViews():

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


	@view_config(route_name='projects', accept='application/json', renderer="json", request_method = "POST")
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
		
		pudb.set_trace()
		referenced_json = ReferencedJSON(self.request_params.json_body)
		
		referenced_json.flatten2ListsOfDicts()
		pudb.set_trace()
		
		if 'Projects' in self.request_params.json_body:
			projects = self.request_params.json_body['Projects']
			p_inserter = ProjectInserter(self.dc_db, users_roles = self.roles)
			p_inserter.insertProjectData(projects)
		
		else:
			self.messages.append('Error: no "Projects" array in json data')
		
		referenced_json.insertFlattenedSubdicts()
		pudb.set_trace()
		
		p_data = json.loads(json.dumps(self.request_params.json_body['Projects'], default = str))
		
		jsonresponse = {
			'title': 'DC REST API Create Projects',
			#'aggregations': aggregations,
			'messages': self.messages,
			'Projects': p_data
		}
		
		return jsonresponse



