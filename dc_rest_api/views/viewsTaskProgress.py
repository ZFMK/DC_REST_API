from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from DBConnectors.MSSQLConnector import MSSQLConnector

from dwb_authentication.security import SecurityPolicy
from dwb_authentication.dbsession import DBSession

from dc_rest_api.lib.Authentication.UserLogin import UserLogin
from dc_rest_api.views.RequestParams import RequestParams
from dc_rest_api.lib.ProgressTracker.ProgressTracker import ProgressTracker

import pudb
import json
import requests
from datetime import datetime


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
		progress_dict = progresstracker.get_progress(task_id)
		
		if progress_dict['status'] in ('complete', 'failed'):
			self.jsonresponse['status'] = "303"
			self.jsonresponse['location'] = '{0}/task_result/{1}'.format(self.request.application_url, task_id)
			task_result_url = '{0}/task_result/{1}'.format(self.request.application_url, task_id)
			return HTTPSeeOther(location=task_result_url, headers={"status": "303", "Content-Type": "application/json", "Accept": "application/json"})
		
		else:
			self.jsonresponse.update(progress_dict)
			if progress_dict['notification_url'] is not None:
				r = requests.post(progress_dict['notification_url'], headers = {'Accept': 'application/json', 'Content-Type': 'application/json'},
					# auth=(self.dc_api_user, self.dc_api_password),
					verify = self.dc_api_verify_https,
					json={'task_id': task_id, 'message': 'update available'})
			
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
		task_result_dict = progresstracker.get_task_result(task_id)
		
		if 'task_result' in task_result_dict and task_result_dict['task_result']:
			task_result_dict['task_result'] = json.loads(task_result_dict['task_result'])
		for key in task_result_dict:
			if isinstance(task_result_dict[key], datetime):
				task_result_dict[key] = task_result_dict[key].strftime('%Y-%m-%d %H:%M:%S')
		
		if 'status' in task_result_dict and task_result_dict['status'] in ['complete', 'failed']:
			
			if task_result_dict['notification_url'] is not None:
				r = requests.post(task_result_dict['notification_url'], headers = {'Accept': 'application/json', 'Content-Type': 'application/json'},
					verify = self.dc_api_verify_https,
					json={'task_id': task_id, 'message': 'task result available'})
			
			self.jsonresponse.update(task_result_dict)
			return self.jsonresponse
		
		elif 'status' in task_result_dict and task_result_dict['status'] not in ['complete', 'failed']:
			if task_result_dict['notification_url'] is not None:
				r = requests.post(task_result_dict['notification_url'], headers = {'Accept': 'application/json', 'Content-Type': 'application/json'},
					verify = self.dc_api_verify_https,
					json={'task_id': task_id, 'message': 'update available'})
			self.jsonresponse.update(task_result_dict)
			return self.jsonresponse
		else:
			self.messages.append('Task with id {0} can not be found'.format(task_id))
			return self.jsonresponse
