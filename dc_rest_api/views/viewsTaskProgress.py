from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from DBConnectors.MSSQLConnector import MSSQLConnector

from dwb_authentication.security import SecurityPolicy
from dwb_authentication.dbsession import DBSession

from dc_rest_api.lib.Authentication.UserLogin import UserLogin
from dc_rest_api.views.RequestParams import RequestParams
from dc_rest_api.lib.ProgressTracker import ProgressTracker

import pudb
import json


class TaskProgressViews():

	def __init__(self, request):
		self.request = request
		self.request_params = RequestParams(self.request)
		
		self.messages = []
		self.messages.extend(self.request_params.messages)
		
		self.userlogin = UserLogin(self.request)
		self.credentials = self.request_params.credentials
		
		if len(self.request_params.credentials) > 0:
			self.userlogin.handle_credentials(self.credentials)
			self.messages.extend(self.userlogin.get_messages())
		
		self.uid = self.request.authenticated_userid


	@view_config(route_name='task_progress', accept='application/json', renderer="json", request_method = "GET")
	def get_task_progress_json(self):
		self.jsonresponse = {
			'title': 'API for requests on DiversityCollection database, ',
			'messages': self.messages
		}
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return self.jsonresponse
		
		task_id = self.request.matchdict['task_id']
		progresstracker = ProgressTracker()
		progress, status = progresstracker.get_progress(task_id)
		
		if status in ('completed', 'failed'):
			route_path = self.request.route_path()
			return HTTPSeeOther('{0}/task_result/{1}'.format(self.request.application_url, task_id), headers={"content_type": "application/json", "accept": "application/json"})
		
		else:
			self.json_response['progress'] = progress
			self.json_response['status'] = status
			self.json_response['message'] = message
			return self.jsonresponse


	@view_config(route_name='task_result', accept='application/json', renderer="json", request_method = "GET")
	def get_task_result(self):
		self.jsonresponse = {
			'title': 'API for requests on DiversityCollection database, ',
			'messages': self.messages
		}
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return self.jsonresponse
		
		task_id = self.request.matchdict['task_id']
		progresstracker = ProgressTracker()
		task_result = progresstracker.get_task_result(task_id)
		if 'status' in task_result and task_result['status'] in ['complete', 'fail']:
			self.jsonresponse.update(task_result)
			return self.jsonresponse
		elif 'status' in task_result and task_result['status'] not in ['complete', 'fail']:
			route_path = self.request.route_path()
			return HTTPSeeOther('{0}/task_progress/{1}'.format(self.request.application_url, task_id), headers={"content_type": "application/json", "accept": "application/json"})
		else:
			self.messages.append('Task with id {0} can not be found'.format(task_id))
