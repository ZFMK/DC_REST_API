from pyramid.response import Response
from pyramid.view import view_config, forbidden_view_config
from pyramid.renderers import render
from pyramid.httpexceptions import HTTPFound, HTTPNotFound, HTTPSeeOther

from dwb_authentication.security import SecurityPolicy
from dwb_authentication.dbsession import DBSession

from dc_rest_api.lib.Authentication.UserLogin import UserLogin
from dc_rest_api.views.RequestParams import RequestParams

from dc_rest_api.lib.CRUD_Operations.ReferencedJSON import ReferencedJSON

from Queues.InsertDeleteQueue import InsertDeleteQueue
from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionSpecimenInserter import CollectionSpecimenInserter

from dc_rest_api.lib.CRUD_Operations.Deleters.CollectionSpecimenDeleter import CollectionSpecimenDeleter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionSpecimenGetter import CollectionSpecimenGetter

import pudb
import json

QUEUE_PATH='dc_ins_del_queue'

class CollectionSpecimensViews():

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
		
		self.roles = self.request.identity['dwb_roles']
		self.users_projects = self.request.identity['projects']
		self.users_project_ids = [project[0] for project in self.users_projects]


	@view_config(route_name='specimens', accept='application/json', renderer="json", request_method = "POST")
	def insertSpecimensJSON(self):
		self.jsonresponse = {
			'title': 'API for requests on DiversityCollection database',
			'messages': self.messages
		}
		#pudb.set_trace()
		if not self.uid:
			self.messages.append('You must be logged in to use the DC REST API. Please send your credentials or a valid session token with your request')
			return self.jsonresponse
		
		security = SecurityPolicy()
		
		self.dc_con_params = security.get_dc_connection_params(self.request)
		# test connection
		
		if self.dc_con_params is None:
			self.messages.append('Can not connect to DiversityCollection server. Please check your credentials')
			return self.jsonresponse
		
		try:
			# this converts self.request_params.json_body to a json that references the dicts indpendent of CollectionSpecimen like CollectionEvents, Projects, Collections
			referenced_json = ReferencedJSON(self.request_params.json_body)
			referenced_json.flatten2Dicts()
		except:
			self.messages.extend(referenced_json.messages)
			return self.jsonresponse
		
		if 'CollectionSpecimens' in self.request_params.json_body:
			try:
				pudb.set_trace()
				# TODO: insert via queue
				queue = InsertDeleteQueue(QUEUE_PATH, auto_commit=True)
				
				request_params = {
					'json_dicts': self.request_params.json_body,
					'uid': self.uid,
					'users_roles': self.roles,
					'notification_url': self.request_params.params_dict.get('notification_url', None)
				}
				task_id = queue.submit_to_insert_queue(self.dc_con_params, request_params, self.request.application_url)
				
				# queue object must be deleted here as the queue otherwise complains about SQLLite called in different threads when the next call
				# of InserDeleteQueue() deletes the exisiting one. 
				# Obviously pyramid holds the old queue object in memory because it loads viewsCollectionSpecimen only once
				del queue
				
				#self.request.response.status = "202"
				#self.request.response.location = '{0}/task_progress/{1}'.format(self.request.application_url, task_id)
				self.jsonresponse['status'] = "303"
				self.jsonresponse['location'] = '{0}/task_progress/{1}'.format(self.request.application_url, task_id)
				self.jsonresponse['task_id'] =  task_id
				
				progress_url = '{0}/task_progress/{1}'.format(self.request.application_url, task_id)
				#return self.jsonresponse
				return HTTPSeeOther(location=progress_url, headers={"status": "303", "Content-Type": "application/json", "Accept": "application/json"})
			except:
				self.messages.extend(queue.messages)
				return self.jsonresponse
			
			
			'''
			referenced_json.insertFlattenedSubdicts()
			filtered_result = referenced_json.get_filtered_result(self.request_params.params_dict['response_pathes_list'], self.request_params.params_dict['all_data'])
			cs_data = json.loads(json.dumps(filtered_result, default = str))
			'''
		
		else:
			self.messages.append('Error: no "CollectionSpecimens" array in json data')
		
		return self.jsonresponse


	@view_config(route_name='specimens', accept='application/json', renderer="json", request_method = "DELETE")
	def deleteSpecimensJSON(self):
		pudb.set_trace()
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
		
		specimen_deleter = CollectionSpecimenDeleter(self.dc_db, self.users_project_ids)
		deleted = []
		if 'CollectionSpecimenIDs' in self.request_params.json_body:
			specimen_ids = list(self.request_params.json_body['CollectionSpecimenIDs'])
			specimen_deleter.deleteByPrimaryKeys(specimen_ids)
			deleted = specimen_ids
		
		elif 'RowGUIDs' in self.request_params.json_body:
			rowguids = list(self.request_params.json_body['RowGUIDs'])
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


	@view_config(route_name='specimens', accept='application/json', renderer="json", request_method = "GET")
	def getSpecimensJSON(self):
		
		jsonresponse = {
			'title': 'API for requests on DiversityCollection database, get CollectionSpecimens',
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
		
		specimen_getter = CollectionSpecimenGetter(self.dc_db, self.users_project_ids)
		
		specimens = []
		
		if 'CollectionSpecimenIDs' in self.request_params.json_body:
			specimen_ids = self.request_params.json_body['CollectionSpecimenIDs']
			specimens = specimen_getter.getByPrimaryKeys(specimen_ids)
		
		elif 'RowGUIDs' in self.request_params.json_body:
			rowguids = self.request_params.json_body['RowGUIDs']
			specimens = specimen_getter.getByRowGUIDs(rowguids)
		
		specimens = json.loads(json.dumps(specimens, default = str))
		
		jsonresponse = {
			'title': 'DC REST API GET CollectionSpecimens',
			'messages': self.messages,
			'CollectionSpecimens': specimens
		}
		
		return jsonresponse
