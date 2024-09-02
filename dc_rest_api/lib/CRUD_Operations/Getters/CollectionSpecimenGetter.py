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


class CollectionSpecimenGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
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


	def setConnectedTableData(self):
		self.setChildSpecimenParts()
		self.setChildIdentificationUnits()
		self.setChildCollectionAgents()
		self.setChildCollections()
		self.setChildCollectionEvents()
		return


	def getData(self):
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT 
		g_temp.[row_num],
		g_temp.[rowguid_to_get] AS [RowGUID],
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
		
		self.cs_rows = self.cur.fetchall()
		self.rows2list()
		
		#pudb.set_trace()
		self.setConnectedTableData()
		
		return self.cs_list


	def rows2list(self):
		self.cs_list = []
		for row in self.cs_rows:
			self.cs_list.append(dict(zip(self.columns, row)))
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
		
		projectclause = self.getProjectClause()
		
		query = """
		SELECT cs.[CollectionSpecimenID], cs.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildSpecimenParts(self):
		
		csp_getter = SpecimenPartGetter(self.dc_db, self.users_project_ids)
		csp_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT csp.[RowGUID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionSpecimenPart] csp
		ON cs.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(csp_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		csp_getter.getData()
		csp_getter.list2dict()
		
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in csp_getter.csp_dict:
				cs['CollectionSpecimenParts'] = []
				for csp_id in csp_getter.csp_dict[cs['CollectionSpecimenID']]:
					cs['CollectionSpecimenParts'].append(csp_getter.csp_dict[cs['CollectionSpecimenID']][csp_id])
		
		return


	def setChildIdentificationUnits(self):
		
		iu_getter = IdentificationUnitGetter(self.dc_db, self.users_project_ids)
		iu_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT iu.[RowGUID]
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
		
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in iu_getter.iu_dict:
				cs['IdentificationUnits'] = []
				for iu_id in iu_getter.iu_dict[cs['CollectionSpecimenID']]:
					cs['IdentificationUnits'].append(iu_getter.iu_dict[cs['CollectionSpecimenID']][iu_id])
		
		return


	def setChildCollectionAgents(self):
		
		ca_getter = CollectionAgentGetter(self.dc_db, self.users_project_ids)
		ca_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT ca.[RowGUID]
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
		
		for cs in self.cs_list:
			if cs['CollectionSpecimenID'] in ca_getter.ca_dict:
				cs['CollectionAgents'] = []
				for ca_id in ca_getter.ca_dict[cs['CollectionSpecimenID']]:
					cs['CollectionAgents'].append(ca_getter.ca_dict[cs['CollectionSpecimenID']][ca_id])
		
		return


	def setChildCollections(self):
		
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
		
		for cs in self.cs_list:
			if 'CollectionID' in cs and cs['CollectionID'] in c_getter.c_dict:
				cs['Collection'] = c_getter.c_dict[cs['CollectionID']]
		
		return


	def setChildCollectionEvents(self):
		
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
		
		ce_getter.getData()
		ce_getter.list2dict()
		
		for cs in self.cs_list:
			if 'CollectionEventID' and cs['CollectionEventID'] in ce_getter.ce_dict:
				cs['CollectionEvent'] = ce_getter.ce_dict[cs['CollectionEventID']]
		
		return
