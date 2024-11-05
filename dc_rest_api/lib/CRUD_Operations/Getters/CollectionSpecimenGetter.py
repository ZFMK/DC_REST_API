import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from threading import Thread, Lock

from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.SpecimenPartGetter import SpecimenPartGetter
from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitGetter import IdentificationUnitGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionAgentGetter import CollectionAgentGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionGetter import CollectionGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionEventGetter import CollectionEventGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionProjectGetter import CollectionProjectGetter

from DBConnectors.MSSQLConnector import MSSQLConnector


class CollectionSpecimenGetter(DataGetter):
	def __init__(self, dc_config, users_project_ids = []):
		self.dc_config = dc_config
		self.dc_db = MSSQLConnector(config = self.dc_config)
		
		DataGetter.__init__(self, self.dc_db)
		
		self.withholded = []
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_specimen_temptable'
		
		self.specimens = {}


	def getByPrimaryKeys(self, specimen_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(specimen_ids) > 0:
			cached_ids = specimen_ids[:batchsize]
			del specimen_ids[:batchsize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#cs_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#cs_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#cs_pks_to_get_temptable] (
			[CollectionSpecimenID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionSpecimen] cs
			INNER JOIN [#cs_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		specimens = self.getData()
		
		return specimens


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		specimens = self.getData()
		
		return specimens


	def getData(self):
		self.lock = Lock()
		
		#pudb.set_trace()
		
		cs_getter_thread = Thread(target = self.getSpecimens)
		csp_getter_thread = Thread(target = self.getSpecimenParts)
		iu_getter_thread = Thread(target = self.getIdentificationUnits)
		ca_getter_thread = Thread(target = self.getCollectionAgents)
		c_getter_thread = Thread(target = self.getCollections)
		ce_getter_thread = Thread(target = self.getCollectionEvents)
		cp_getter_thread = Thread(target = self.getCollectionProjects)
		
		cs_getter_thread.start()
		csp_getter_thread.start()
		iu_getter_thread.start()
		ca_getter_thread.start()
		c_getter_thread.start()
		ce_getter_thread.start()
		cp_getter_thread.start()
		
		cs_getter_thread.join()
		csp_getter_thread.join()
		iu_getter_thread.join()
		ca_getter_thread.join()
		c_getter_thread.join()
		ce_getter_thread.join()
		cp_getter_thread.join()
		
		self.insertCSPDict()
		self.insertIUDict()
		self.insertCADict()
		self.insertCDict()
		self.insertCEDict()
		self.insertCPDict()
		
		return self.cs_list


	def getSpecimens(self):
		self.lock.acquire()
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		cs.[CollectionSpecimenID],
		cs.[CollectionEventID],
		cs.[ExternalDatasourceID],
		cs.[ExternalIdentifier],
		cs.[AccessionNumber],
		cs.[AccessionDate],
		cs.[AccessionDay],
		cs.[AccessionMonth],
		cs.[AccessionYear],
		cs.[AccessionDateSupplement],
		cs.[AccessionDateCategory],
		cs.[DepositorsName],
		cs.[DepositorsAgentURI],
		cs.[DepositorsAccessionNumber],
		cs.[LabelTitle],
		cs.[LabelType],
		cs.[LabelTranscriptionState],
		cs.[LabelTranscriptionNotes],
		cs.[ExsiccataURI],
		cs.[ExsiccataAbbreviation],
		cs.[OriginalNotes],
		cs.[AdditionalNotes],
		cs.[Problems],
		cs.[DataWithholdingReason]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		querylog.info(query)
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		cs_rows = self.cur.fetchall()
		self.lock.release()
		
		self.cs_list = []
		for row in cs_rows:
			self.cs_list.append(dict(zip(columns, row)))
		
		return


	def list2dict(self):
		self.cs_dict = {}
		for element in self.cs_list:
			self.cs_dict[element['CollectionSpecimenID']] = element
		return



	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT cs.[CollectionSpecimenID], cs.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		{1}
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		querylog.info(', '.join(self.users_project_ids))
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		{1}
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def getSpecimenParts(self):
		self.lock.acquire()
		
		query = """
		SELECT csp.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionSpecimenPart] csp
		ON cs.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		
		csp_getter = SpecimenPartGetter(self.dc_config, self.users_project_ids)
		csp_getter.getByRowGUIDs(row_guids)
		csp_getter.list2dict()
		self.csp_dict = csp_getter.csp_dict
		
		return


	def insertCSPDict(self):
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in self.csp_dict:
				cs['CollectionSpecimenParts'] = []
				for csp_id in self.csp_dict[cs['CollectionSpecimenID']]:
					cs['CollectionSpecimenParts'].append(self.csp_dict[cs['CollectionSpecimenID']][csp_id])
		
		return


	def getIdentificationUnits(self):
		self.lock.acquire()
		
		query = """
		SELECT iu.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [IdentificationUnit] iu
		ON cs.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		row_guids = [row[0] for row in rows]
		self.lock.release()
		
		iu_getter = IdentificationUnitGetter(self.dc_config, self.users_project_ids)
		iu_getter.getByRowGUIDs(row_guids)
		iu_getter.list2dict()
		self.iu_dict = iu_getter.iu_dict
		
		return


	def insertIUDict(self):
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in self.iu_dict:
				cs['IdentificationUnits'] = []
				for iu_id in self.iu_dict[cs['CollectionSpecimenID']]:
					cs['IdentificationUnits'].append(self.iu_dict[cs['CollectionSpecimenID']][iu_id])
		
		return


	def getCollectionAgents(self):
		self.lock.acquire()
		
		query = """
		SELECT ca.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionAgent] ca
		ON cs.[CollectionSpecimenID] = ca.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		
		ca_getter = CollectionAgentGetter(self.dc_config, self.users_project_ids)
		ca_getter.getByRowGUIDs(row_guids)
		ca_getter.list2dict()
		self.ca_dict = ca_getter.ca_dict
		
		return


	def insertCADict(self):
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in self.ca_dict:
				cs['CollectionAgents'] = []
				for ca_id in self.ca_dict[cs['CollectionSpecimenID']]:
					cs['CollectionAgents'].append(self.ca_dict[cs['CollectionSpecimenID']][ca_id])
		
		return


	def getCollections(self):
		self.lock.acquire()
		
		query = """
		SELECT DISTINCT c.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [Collection] c
		ON cs.[CollectionID] = c.[CollectionID]
		WHERE cs.[CollectionID] IS NOT NULL
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		
		c_getter = CollectionGetter(self.dc_config)
		c_getter.getByRowGUIDs(row_guids)
		c_getter.list2dict()
		self.c_getter = c_getter.c_dict
		
		return


	def insertCDict(self):
		for cs in self.cs_list:
			if 'CollectionID' in cs and cs['CollectionID'] in self.c_dict:
				cs['Collection'] = self.c_dict[cs['CollectionID']]
		
		return


	def getCollectionProjects(self):
		self.lock.acquire()
		
		query = """
		SELECT DISTINCT cp.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		
		cp_getter = CollectionProjectGetter(self.dc_config)
		cp_getter.getByRowGUIDs(row_guids)
		cp_getter.list2dict()
		self.cp_dict = cp_getter.cp_dict
		
		return


	def insertCPDict(self):
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in self.cp_dict:
				cs['Projects'] = []
				for cp_id in self.cp_dict[cs['CollectionSpecimenID']]:
					cs['Projects'].append(self.cp_dict[cs['CollectionSpecimenID']][cp_id])
		
		return



	def getCollectionEvents(self):
		self.lock.acquire()
		
		query = """
		SELECT DISTINCT ce.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [CollectionEvent] ce
		ON ce.[CollectionEventID] = cs.[CollectionEventID]
		WHERE cs.[CollectionEventID] IS NOT NULL
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		self.lock.release()
		
		row_guids = [row[0] for row in rows]
		
		ce_getter = CollectionEventGetter(self.dc_config, self.users_project_ids)
		ce_getter.getByRowGUIDs(row_guids)
		ce_getter.list2dict()
		self.ce_dict = ce_getter.ce_dict
		return


	def insertCEDict(self):
		for cs in self.cs_list:
			if 'CollectionEventID' and cs['CollectionEventID'] in self.ce_dict:
				cs['CollectionEvent'] = self.ce_dict[cs['CollectionEventID']]
		
		return
