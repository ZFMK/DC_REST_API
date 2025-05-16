import pudb
from datetime import datetime, date
now = datetime.now()
from uuid import uuid4

import persistqueue
#import tempfile
import math

QUEUE_PATH='dc_ins_del_queue' # path to the persistent storage of the queue, relative to the working directory

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('dc_api')

from DBConnectors.MSSQLConnector import MSSQLConnector

from dc_rest_api.lib.CRUD_Operations.Inserters.CollectionSpecimenInserter import CollectionSpecimenInserter
from dc_rest_api.lib.CRUD_Operations.Deleters.CollectionSpecimenDeleter import CollectionSpecimenDeleter
from dc_rest_api.lib.ProgressTracker import ProgressTracker



def insdel_queue_daemon():
	q = InsertDeleteQueue(QUEUE_PATH, auto_commit=True)
	q.run_daemon()


class InsertDeleteQueue(persistqueue.SQLiteQueue):
	"""
	Queue that manages long running INSERT, UPDATE and DELETE tasks on DC
	DELETE must be in the same queue to prevent deletion on referenced data during input?
	Is it enough to rely on SQL Servers table lock here?
	
	If the Insert requires a Deletion (e. g. for Colltables in ASV-Registry), the Deletion must be put on the queue
	before the Insert task
	"""
	
	def __init__(self, queue_path, auto_commit=True):
		persistqueue.SQLiteQueue.__init__(self, queue_path, auto_commit)
		self.progress_tracker = ProgressTracker() 
		self.insert_pagesize = 100
		self.messages = []
		pass


	def run_daemon(self):
		self.daemonize = True
		while self.daemonize:
			[dc_params, json_dicts, uid, user_roles, task_id, target] = self.get()
			if target == 'insert':
				self.insert_DC_data(dc_params, json_dicts, uid, user_roles, task_id)
			# TODO: implement update
			#elif target == 'update':
			#	self.update_DC_data(dc_params, json_dicts, uid, user_roles, task_id)
			# TODO: implement delete
			#elif target == 'delete':
			#	self.delete_DC_data(dc_params, json_dicts, uid, user_roles, task_id)
		
		return

	######### Implementation of wrappers to put items into the queue
	# TODO: is this wrapper needed when there is no other task than to put it onto the queue?
	def submit_to_insert_queue(dc_params, json_dicts, uid, user_roles, application_url):
		task_id = uuid4()
		self.put(dc_params, json_dicts, uid, user_roles, task_id, 'insert')
		
		return task_id


	######### Implementation of tasks to start by the queue
	def insert_DC_data(self, dc_params, json_dicts, uid, user_roles, task_id):
		# DC connection must be set here, to prevent that it is ouddated when the task starts
		
		dc_db = MSSQLConnector(config = dc_params)
		specimen_inserter = CollectionSpecimenInserter(dc_db, uid, user_roles)
		
		# create smaller batches to import
		len_dicts = len(json_dicts)
		percent_done = 0
		pages = 0
		self.progress_tracker.update_progress(task_id, 0, 'submission started')
		
		task_result = {"CollectionSpecimens": []}
		try:
			while len(json_dicts) > 0:
				json_dicts_batch = json_dicts[0:self.insert_pagesize]
				del json_dicts[0:self.insert_pagesize]
				
				# must be a new instance of CollectionSpecimenInserter?
				specimen_inserter = CollectionSpecimenInserter(dc_db, uid, user_roles)
				specimen_inserter.insertSpecimenData(json_dicts_batch)
				inserted_specimen_ids = specimen_inserter.getInsertedSpecimenIDs()
				task_result["CollectionSpecimens"].extend(inserted_specimen_ids)
				
				pages = pages + self.insert_pagesize
				percent_done = math.floor(pages / len_dicts * 100) 
				
				self.progress_tracker.update_progress(task_id, percent_done, 'processing')
				del specimen_inserter
			
			percent_done = 100
			status = 'complete'
			
			self.progress_tracker.update_progress(task_id, percent_done, status)
			self.progress_tracker.set_task_result(task_result)
		
		except Exception as e:
			self.messages.append(e)
			status = 'failed'
			self.progress_tracker.update_progress(task_id, 0, status, ', '.join(self.messages))
		
		del specimen_inserter
		
		return
