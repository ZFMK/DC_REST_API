import pudb
from datetime import datetime, date
now = datetime.now()
from uuid import uuid4

import persistqueue
#import tempfile
import math
import traceback
import re

QUEUE_PATH='dc_ins_del_queue' # path to the persistent storage of the queue, relative to the working directory

import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('dc_api')
errorlog = logging.getLogger('error')

from DBConnectors.MSSQLConnector import MSSQLConnector

from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionSpecimenInserter import CollectionSpecimenInserter
from dc_rest_api.lib.CRUD_Operations.Deleters.CollectionSpecimenDeleter import CollectionSpecimenDeleter
from dc_rest_api.lib.ProgressTracker.ProgressTracker import ProgressTracker
from dc_rest_api.lib.CRUD_Operations.Inserters.IndependentTablesInsert import IndependentTablesInsert

from dc_rest_api.lib.EmailNotification.async_email import notify_developers

#from Queues.singleton import singleton
from Queues.asyncfunc import asyncfunc

QUEUE_PATH='dc_ins_del_queue'


@asyncfunc
def insdel_queue_daemon():
	q = InsertDeleteQueue()
	q.run_daemon()
	return


class InsertDeleteQueue(persistqueue.SQLiteQueue):
	"""
	Queue that manages long running INSERT, UPDATE and DELETE tasks on DC
	DELETE must be in the same queue to prevent deletion on referenced data during input?
	Is it enough to rely on SQL Servers table lock here?
	
	If the Insert requires a Deletion (e. g. for Colltables in ASV-Registry), the Deletion must be put on the queue
	before the Insert task
	"""
	
	def __init__(self):
		queue_path = QUEUE_PATH
		persistqueue.SQLiteQueue.__init__(self, queue_path, auto_commit = True)
		self.progress_tracker = ProgressTracker()
		self.messages = []
		self.daemon_is_running = False
		pass


	def run_daemon(self):
		self.daemon_is_running = True
		while self.daemon_is_running:
			[dc_params, request_params, task_id, target] = self.get()
			if target == 'insert':
				self.insert_DC_data(dc_params, request_params, task_id)
			# TODO: implement update
			#elif target == 'update':
			#	self.update_DC_data(dc_params, json_dicts, uid, users_roles, task_id)
			elif target == 'delete':
				self.delete_DC_data(dc_params, request_params, task_id)
		self.daemon_is_running = False


	######### Implementation of wrappers to put items into the queue
	# TODO: is this wrapper needed when there is no other task than to put it onto the queue?
	def submit_to_delete_queue(self, dc_params, request_params, application_url):
		task_id = str(uuid4().hex)
		self.put([dc_params, request_params, task_id, 'delete'])
		self.progress_tracker.insert_new_task(task_id, 'Delete CollectionSpecimens', 0, 'waiting for start') 
		return task_id


	def submit_to_insert_queue(self, dc_params, request_params, application_url):
		task_id = str(uuid4().hex)
		self.put([dc_params, request_params, task_id, 'insert'])
		self.progress_tracker.insert_new_task(task_id, 'Insert CollectionSpecimens', 0, 'waiting for start') 
		return task_id


	######### Implementation of tasks to start by the queue
	def delete_DC_data(self, dc_params, request_params, task_id):
		dc_db = MSSQLConnector(config = dc_params)
		
		ids_list_json = request_params['ids_list_json']
		users_project_ids = request_params['users_project_ids']
		notification_url = request_params['notification_url']
		
		self.progress_tracker.update_progress(task_id, 0, 'delete submission started')
		
		ids_list = []
		
		if 'RowGUIDs' in ids_list_json:
			ids_list = [rowguid for rowguid in ids_list_json['RowGUIDs']]
			ids_key = 'RowGUIDs'
		elif 'CollectionSpecimenIDs' in ids_list_json:
			ids_list = [cs_id for cs_id in ids_list_json['CollectionSpecimenIDs']]
			ids_key = 'CollectionSpecimenIDs'
		
		page = 0
		pagesize = 100
		max_pages = math.ceil(len(ids_list) / pagesize)
		
		task_result = {"CS_IDs": []}
		step_result = {"CS_IDs": []}
		
		try:
			while len(ids_list) > 0:
				ids_batch = []
				ids_batch = ids_list[:pagesize]
				del ids_list[:pagesize]
				
				ids_copy = list(ids_batch)
				
				specimen_deleter = CollectionSpecimenDeleter(dc_db, users_project_ids)
				if ids_key == 'CollectionSpecimenIDs':
					specimen_deleter.deleteByPrimaryKeys(ids_batch)
				elif ids_key == 'RowGUIDs':
					specimen_deleter.deleteByRowGUIDs(ids_batch)
				
				deleted_specimen_ids = specimen_deleter.getListOfDeletedIDs()
				step_result['CS_IDs'] = deleted_specimen_ids['CS_IDs']
				task_result['CS_IDs'].extend(deleted_specimen_ids['CS_IDs'])
				
				page = page + 1
				percent_done = math.floor(page / max_pages * 100) 
				self.progress_tracker.update_progress(task_id, percent_done, status = 'deleting specimens', task_result = task_result, step_result = step_result, message = 'please wait for task to complete')
				
			percent_done = 100
			status = 'complete'
			self.progress_tracker.update_progress(task_id, percent_done, status, message = None)
			self.progress_tracker.set_task_result(task_id, task_result)
		
		except Exception as e:
			#pudb.set_trace()
			errorlog.error('Exception in InsertDeleteQueue.delete_DC_data()', exc_info = True)
			status = 'failed'
			self.progress_tracker.update_progress(task_id, 0, status, ', '.join(self.messages))
			notify_developers('Exception in InsertDeleteQueue.delete_DC_data(): \n{0}'.format(''.join(traceback.format_tb(e.__traceback__))))
		return
	
	
	def insert_DC_data(self, dc_params, request_params, task_id):
		# DC connection must be set here, to prevent that it is ouddated when the task starts
		dc_db = MSSQLConnector(config = dc_params)
		
		json_dicts = request_params['json_dicts']
		uid = request_params['uid']
		users_roles = request_params['users_roles']
		notification_url = request_params['notification_url']
		
		self.progress_tracker.update_progress(task_id, 0, 'insert submission started')
		
		independent_tables = IndependentTablesInsert(dc_db, json_dicts, uid, users_roles)
		independent_tables.insertIndependentTables()
		self.progress_tracker.update_progress(task_id, 20, 'independent tables')
		
		specimen_dicts = json_dicts['CollectionSpecimens']
		specimen_list = [specimen_dicts[cs_id] for cs_id in specimen_dicts]
		independent_tables.setLinkedIDs(specimen_list)
		
		# add the progress value from independent tables insert
		# TODO: refine the progress calculation from independent tables?
		page = 0 + 20
		pagesize = 100
		max_pages = math.ceil(len(specimen_list) / pagesize) + 20
		
		task_result = {"CS_IDs": []}
		step_result = {"CS_IDs": []}
		try:
			while len(specimen_list) > 0:
				specimen_batch = specimen_list[0:pagesize]
				del specimen_list[0:pagesize]
			
				specimen_inserter = CollectionSpecimenInserter(dc_db, uid, users_roles)
				specimen_inserter.insertSpecimenData(specimen_batch, task_id)
				
				independent_tables.insertCollectionProjects(specimen_batch)
				
				inserted_specimen_ids = specimen_inserter.getListOfInsertedIDs()
				step_result['CS_IDs'] = inserted_specimen_ids['CS_IDs']
				task_result['CS_IDs'].extend(inserted_specimen_ids['CS_IDs'])
				
				page = page + 1
				percent_done = math.floor(page / max_pages * 100) 
				self.progress_tracker.update_progress(task_id, percent_done, status = 'inserting specimens', task_result = task_result, step_result = step_result, message = 'please wait for task to complete')
			
			percent_done = 100
			status = 'complete'
			self.progress_tracker.update_progress(task_id, percent_done, status, message = None)
			self.progress_tracker.set_task_result(task_id, task_result)
		
		except Exception as e:
			# TODO
			#pudb.set_trace()
			#self.messages.append(e[0])
			errorlog.error('Exception in InsertDeleteQueue.insert_DC_data()', exc_info = True)
			status = 'failed'
			self.progress_tracker.update_progress(task_id, 0, status, ', '.join(self.messages))
			notify_developers('Exception in InsertDeleteQueue.delete_DC_data(): \n{0}'.format(''.join(traceback.format_tb(e.__traceback__))))
		
		return


if __name__ == "__main__":
	insdel_queue_daemon()

