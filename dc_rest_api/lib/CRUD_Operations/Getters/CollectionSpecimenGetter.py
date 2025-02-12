import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.SpecimenPartGetter import SpecimenPartGetter
from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitGetter import IdentificationUnitGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionAgentGetter import CollectionAgentGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionGetter import CollectionGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionEventGetter import CollectionEventGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionProjectGetter import CollectionProjectGetter


class CollectionSpecimenGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
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


	def setConnectedTableData(self):
		self.setSpecimenParts()
		self.setIdentificationUnits()
		self.setCollectionAgents()
		self.setCollections()
		self.setCollectionEvents()
		self.setCollectionProjects()
		return


	def getData(self):
		self.setDatabaseURN()
		self.filterAllowedRowGUIDs()
		
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
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setConnectedTableData()
		
		return self.results_list


	def list2dict(self):
		self.cs_dict = {}
		for element in self.results_list:
			self.cs_dict[element['CollectionSpecimenID']] = element
		return



	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
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
		
		return


	def setSpecimenParts(self):
		
		csp_getter = SpecimenPartGetter(self.dc_db, self.users_project_ids)
		csp_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT csp.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionSpecimenPart] csp
		ON cs.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(csp_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		csp_getter.getData()
		csp_getter.list2dict()
		
		for cs in self.results_list:
			if cs['CollectionSpecimenID'] in csp_getter.results_dict:
				cs['CollectionSpecimenParts'] = []
				for csp_id in csp_getter.results_dict[cs['CollectionSpecimenID']]:
					cs['CollectionSpecimenParts'].append(csp_getter.results_dict[cs['CollectionSpecimenID']][csp_id])
		
		return


	def setIdentificationUnits(self):
		
		iu_getter = IdentificationUnitGetter(self.dc_db, self.users_project_ids)
		iu_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT iu.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [IdentificationUnit] iu
		ON cs.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(iu_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		iu_getter.getData()
		iu_getter.list2dict()
		
		for cs in self.results_list:
			if cs['CollectionSpecimenID'] in iu_getter.results_dict:
				cs['IdentificationUnits'] = []
				for iu_id in iu_getter.results_dict[cs['CollectionSpecimenID']]:
					cs['IdentificationUnits'].append(iu_getter.results_dict[cs['CollectionSpecimenID']][iu_id])
		
		return


	def setCollectionAgents(self):
		
		ca_getter = CollectionAgentGetter(self.dc_db, self.users_project_ids)
		ca_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT ca.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionAgent] ca
		ON cs.[CollectionSpecimenID] = ca.[CollectionSpecimenID]
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(ca_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		ca_getter.getData()
		ca_getter.list2dict()
		
		for cs in self.results_list:
			if cs['CollectionSpecimenID'] in ca_getter.results_dict:
				cs['CollectionAgents'] = []
				for ca_id in ca_getter.results_dict[cs['CollectionSpecimenID']]:
					cs['CollectionAgents'].append(ca_getter.results_dict[cs['CollectionSpecimenID']][ca_id])
		
		return


	def setCollections(self):
		
		c_getter = CollectionGetter(self.dc_db)
		c_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT c.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [Collection] c
		ON cs.[CollectionID] = c.[CollectionID]
		WHERE cs.[CollectionID] IS NOT NULL
		;""".format(c_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		c_getter.getData()
		c_getter.list2dict()
		
		for cs in self.results_list:
			if 'CollectionID' in cs and cs['CollectionID'] in c_getter.results_dict:
				cs['Collection'] = c_getter.results_dict[cs['CollectionID']]
		
		return


	def setCollectionProjects(self):
		
		cp_getter = CollectionProjectGetter(self.dc_db)
		cp_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT cp.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		;""".format(cp_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		cp_getter.getData()
		cp_getter.list2dict()
		
		for cs in self.results_list:
			if cs['CollectionSpecimenID'] in cp_getter.results_dict:
				cs['Projects'] = []
				for cp_id in cp_getter.results_dict[cs['CollectionSpecimenID']]:
					cs['Projects'].append(cp_getter.results_dict[cs['CollectionSpecimenID']][cp_id])
		
		return



	def setCollectionEvents(self):
		
		ce_getter = CollectionEventGetter(self.dc_db, self.users_project_ids)
		ce_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT ce.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{1}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [CollectionEvent] ce
		ON ce.[CollectionEventID] = cs.[CollectionEventID]
		WHERE cs.[CollectionEventID] IS NOT NULL
		;""".format(ce_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.setDatabaseURN()
		
		ce_getter.getData()
		ce_getter.list2dict()
		
		for cs in self.results_list:
			if 'CollectionEventID' in cs and cs['CollectionEventID'] in ce_getter.results_dict:
				cs['CollectionEvent'] = ce_getter.results_dict[cs['CollectionEventID']]
		
		return
