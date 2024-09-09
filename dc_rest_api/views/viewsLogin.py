from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from dwb_authentication.security import SecurityPolicy

from dc_rest_api.lib.Authentication.UserLogin import UserLogin

from dc_rest_api.views.RequestParams import RequestParams


import pudb
import re


class LoginViews(object):
	'''
	copied from https://docs.pylonsproject.org/projects/pyramid/en/latest/quick_tutorial/authentication.html
	'''

	def __init__(self, request):
		
		self.request = request
		
		self.uid = self.request.authenticated_userid
		self.userlogin = UserLogin(self.request)
		self.messages = []


	@view_config(route_name='login', accept='text/html')
	@forbidden_view_config(accept='text/html')
	def login_view(self):
		
		login_url = self.request.route_url('login')
		referrer = self.request.url
		if referrer == login_url:
			referrer = self.request.route_url('help') # never use login form itself as came_from
		came_from = self.request.params.get('came_from', referrer)
		
		self.token = None
		
		if 'form.submitted' in self.request.params:
			
			self.token = self.userlogin.authenticate_user(self.request.params.get('username', ''), self.request.params.get('password', ''))
			
			self.messages.extend(self.userlogin.get_messages())
			
			if self.token is not None:
				return HTTPFound(location=came_from)
		
		responsedict = dict(
			name='Login',
			messages = self.messages,
			url = self.request.application_url + '/login',
			came_from = came_from,
			username = self.request.params.get('username', ''),
			request = self.request
		)
		
		result = render('dc_rest_api:templates/login.pt', responsedict)
		response = Response(result)
		return response
	
	
	@view_config(route_name='logout')
	def logout_view(self):
		logout_url = self.request.route_url('logout')
		referrer = self.request.url
		if referrer == logout_url:
			referrer = self.request.route_url('help') # never use login form itself as came_from
		came_from = self.request.params.get('came_from', referrer)
		
		security = SecurityPolicy()
		security.de_authenticate(self.request)
		
		self.request.session['token'] = None
		return HTTPFound(location = came_from)


	@view_config(route_name='login', accept='application/json', renderer='json')
	def login_view_json(self):
		
		self.userlogin = UserLogin(self.request)
		self.messages = []
		
		request_params = RequestParams(self.request)
		self.credentials = request_params.credentials
		
		token = None
		roles = []
		projects = []
		project_ids = []
		
		# check if there are any authentication data given in request
		# and if so: authenticate the user
		if 'username' in self.credentials and 'password' in self.credentials:
			token = self.userlogin.authenticate_user(self.credentials['username'], self.credentials['password'])
			roles = self.request.identity['dwb_roles']
			project_list = self.request.identity['projects']
			projects = [project[1] for project in project_list]
			project_ids = [project[0] for project in project_list]
		
		self.messages.extend(self.userlogin.get_messages())
		
		response = {
			'token': token,
			'roles': roles,
			'projects': projects
		}
		if len(self.messages) > 0:
			response['messages'] = self.messages
		
		return response


	@view_config(route_name='logout', accept='application/json', renderer='json')
	def logout_view_json(self):
		
		self.messages = []
		
		logout_url = self.request.route_url('logout')
		referrer = self.request.url
		if referrer == logout_url:
			referrer = self.request.route_url('help') # never use login form itself as came_from
		came_from = self.request.params.get('came_from', referrer)
		
		security = SecurityPolicy()
		token = security.get_token_from_request(self.request)
		session_id = security.de_authenticate(self.request)
		
		if session_id is not None:
			self.messages.append('session has been deleted for token {0}'.format(token))
		else:
			self.messages.append('please give a valid session token either as request parameter or in json data')
		
		token = None
		self.request.session['token'] = None
		
		#update the authenticated_userid and thus identity in self.request 
		self.uid = self.request.authenticated_userid
		roles = self.request.identity['dwb_roles']
		projects = [project[0] for project in self.request.identity['projects']]
		
		response = {
			'token': token,
			'roles': roles,
			'projects': projects
		}
		if len(self.messages) > 0:
			response['messages'] = self.messages
		
		return response





