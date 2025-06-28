import pudb
import json
import re


class RequestParams():
	def __init__(self, request):
		self.request = request
		self.messages = []
		self.json_body = {}
		
		self.read_request_params()
		#self.read_search_params()
		self.read_credentials()
		self.read_single_params()
		#self.set_requeststring()
		self.set_response_params()
		
		pass


	def read_request_params(self):
		self.params_dict = self.request.params.dict_of_lists()
		# because request.json_body returns an error when no content is in a request with Content-Type: application/json
		# the availability of json data is first testet with try except
		if not self.request.content_length:
			test = str(self.request.content_length)
			test = str(self.request.content_type)
			return
		try:
				
			if self.request.json_body:
				
				self.json_body = self.request.json_body
				# prepare the params as dict of lists
				for key in self.json_body:
					self.params_dict[key] = []
					if isinstance(self.json_body[key], list) or isinstance(self.json_body[key], tuple):
						self.params_dict[key].extend(self.json_body[key])
					else:
						self.params_dict[key].append(self.json_body[key])
		except ValueError as e:
			
			if hasattr(e, 'lineno') and hasattr(e, 'colno') and hasattr(e, 'pos') and hasattr(e, 'msg'):
				self.messages.append('Can not parse json data: line: {0}, column: {1}, position: {2}: {3}'.format(e.lineno, e.colno, e.pos, e.msg))
			
			else:
				self.messages.append('Valid json data missing')
			
		return


	def read_credentials(self):
		self.credentials = {}
		credentials = ['username', 'password', 'token', 'logout', 'db_accronym']
		for param_name in credentials:
			if param_name in self.params_dict and len(self.params_dict[param_name]) > 0:
				if self.params_dict[param_name][-1]:
					self.credentials[param_name] = self.params_dict[param_name][-1]
			else:
				self.credentials[param_name] = None
		return


	def read_single_params(self):
		single_params = ['notification_url']
		for param in single_params:
			if param in self.params_dict and len(self.params_dict[param]) > 0:
				self.params_dict[param] = self.params_dict[param][-1]
		return


	def set_requeststring(self):
		self.requeststring = ''
		paramslist = []
		for param in self.params_dict:
			if param not in ['username', 'password', 'token', 'logout']:
				for value in self.params_dict[param]:
					paramslist.append('{0}={1}'.format(param, value))
		
		self.requeststring = '&'.join(paramslist)
		return


	def get_token_from_request(self):
		token = None
		if 'token' in self.request.session:
			token = self.request.session['token']
		elif 'token' in self.request.params:
			try:
				token = self.request.params.getone('token')
			except:
				token = None
		else:
			try:
				json_params = self.request.json_body
				if 'token' in json_params:
					if (isinstance (json_params['token'], str)):
						token = json_params['token']
			except:
				token = None
				
		return token


	def set_response_params(self):
		if 'response_pathes' in self.params_dict:
			for response_path in self.params_dict['response_pathes']:
				if not 'response_pathes_list' in self.params_dict:
					self.params_dict['response_pathes_list'] = []
				try:
					self.params_dict['response_pathes_list'].append([path.strip() for path in response_path.split(':')])
				except:
					try:
						self.messages.append('Your request contains an invalid value for parameter response_pathes. Wrong value is {0}'.format(str(response_path)))
					except:
						self.messages.append('Your request contains an invalid value for parameter response_pathes')
		else:
			self.params_dict['response_pathes_list'] = []
		
		if 'all_data' in self.params_dict:
			if self.params_dict['all_data'][-1] in [True, 'True', 'true', '1', 1]:
				self.params_dict['all_data'] = True
			else:
				self.params_dict['all_data'] = False
		else:
			self.params_dict['all_data'] = False
		return


####################################


'''
	def read_search_params(self):
		self.search_params = {}
		
		self.read_stack_queries_params()
		
		exists_params = ['restrict_to_users_projects']
		boolean_params = ['buckets_sort_alphanum']
		simple_params = ['pagesize', 'page', 'sorting_col', 'sorting_dir', 'aggregation', 'tree', 'match_queries_connector', 
							'term_filters_connector', 'buckets_search_term', 'overlay_remaining_all_select', 'buckets_sort_dir'
						]
		complex_params = ['term_filters',]
		list_params = ['open_filter_selectors', 'result_table_columns', 'parent_ids', 'match_query']
		
		for param_name in boolean_params:
			if param_name in self.params_dict:
				if self.params_dict[param_name][-1] in ['false', 'False', '', '0']:
					self.search_params[param_name] = False
				elif not self.params_dict[param_name][-1]:
					self.search_params[param_name] = False
				else:
					self.search_params[param_name] = True
		
		for param_name in exists_params:
			if param_name in self.params_dict:
				self.search_params[param_name] = True
		
		for param_name in simple_params:
			if param_name in self.params_dict and len(self.params_dict[param_name]) > 0:
				self.search_params[param_name] = self.params_dict[param_name][-1]
		
		for param_name in complex_params: 
			if param_name in self.params_dict and len(self.params_dict[param_name]) > 0:
				for searchquery in self.params_dict[param_name]:
					query = searchquery.split(':', 1)
					if len(query) == 2:
						if param_name not in self.search_params:
							self.search_params[param_name] = {}
						if query[0] not in self.search_params[param_name]:
							self.search_params[param_name][query[0]] = []
						self.search_params[param_name][query[0]].append(query[1])
			else:
				self.search_params[param_name] = []
		
		for param_name in list_params:
			if param_name in self.params_dict:
				self.search_params[param_name] = self.params_dict[param_name]
			else:
				self.search_params[param_name] = []
		
		return
'''

