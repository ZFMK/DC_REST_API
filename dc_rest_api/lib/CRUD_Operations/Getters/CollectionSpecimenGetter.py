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
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimens = self.getData()
		
		self.setConnectedTableData()
		
		return specimens


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimens = self.getData()
		
		self.setConnectedTableData()
		
		return specimens


	def setConnectedTableData(self):
		self.setChildSpecimenParts()
		self.setChildIdentificationUnits()
		self.setChildCollectionAgents()
		self.setChildCollections()
		self.setChildCollectionEvents()
		return


	def getData(self):
		
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
		
		id_lists = []
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionSpecimenPart] csp
		ON cs.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append((row[0], row[1]))
		
		csp_getter = SpecimenPartGetter(self.dc_db, self.users_project_ids)
		csp_getter.getByPrimaryKeys(id_lists)
		csp_getter.list2dict()
		
		for specimen_id in csp_getter.csp_dict:
			for cs in self.cs_list:
				if specimen_id == cs['CollectionSpecimenID']:
					if 'CollectionSpecimenParts' not in cs:
						cs['CollectionSpecimenParts'] = []
						for csp_id in csp_getter.csp_dict[specimen_id]:
							cs['CollectionSpecimenParts'].append(csp_getter.csp_dict[specimen_id][csp_id])
		
		return


	def setChildIdentificationUnits(self):
		
		id_lists = []
		query = """
		SELECT iu.[CollectionSpecimenID], iu.[IdentificationUnitID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [IdentificationUnit] iu
		ON cs.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append((row[0], row[1]))
		
		iu_getter = IdentificationUnitGetter(self.dc_db, self.users_project_ids)
		iu_getter.getByPrimaryKeys(id_lists)
		iu_getter.list2dict()
		
		for specimen_id in iu_getter.iu_dict:
			for cs in self.cs_list:
				if specimen_id == cs['CollectionSpecimenID']:
					if 'IdentificationUnits' not in cs:
						cs['IdentificationUnits'] = []
						for iu_id in iu_getter.iu_dict[specimen_id]:
							cs['IdentificationUnits'].append(iu_getter.iu_dict[specimen_id][iu_id])
		
		return


	def setChildCollectionAgents(self):
		
		id_lists = []
		query = """
		SELECT ca.[CollectionSpecimenID], ca.[CollectorsName]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionAgent] ca
		ON cs.[CollectionSpecimenID] = ca.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append((row[0], row[1]))
		
		ca_getter = CollectionAgentGetter(self.dc_db, self.users_project_ids)
		ca_getter.getByPrimaryKeys(id_lists)
		ca_getter.list2dict()
		
		for specimen_id in ca_getter.ca_dict:
			for cs in self.cs_list:
				if specimen_id == cs['CollectionSpecimenID']:
					if 'CollectionAgents' not in cs:
						cs['CollectionAgents'] = []
						for ca_id in ca_getter.ca_dict[specimen_id]:
							cs['CollectionAgents'].append(ca_getter.ca_dict[specimen_id][ca_id])
		
		return


	def setChildCollections(self):
		
		id_lists = []
		query = """
		SELECT DISTINCT cs.[CollectionID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		WHERE cs.[CollectionID] IS NOT NULL
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append((row[0]))
		
		c_getter = CollectionGetter(self.dc_db)
		c_getter.getByPrimaryKeys(id_lists)
		c_getter.list2dict()
		
		for collection_id in c_getter.c_dict:
			for cs in self.cs_list:
				if 'CollectionID' in cs and collection_id == cs['CollectionID']:
					cs['Collection'] = c_getter.c_dict[collection_id]
		
		return


	def setChildCollectionEvents(self):
		
		id_lists = []
		query = """
		SELECT DISTINCT cs.[CollectionEventID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_get]
		WHERE cs.[CollectionEventID] IS NOT NULL
		;""".format(self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append((row[0]))
		
		ce_getter = CollectionEventGetter(self.dc_db, self.users_project_ids)
		ce_getter.getByPrimaryKeys(id_lists)
		ce_getter.list2dict()
		
		for event_id in ce_getter.ce_dict:
			for cs in self.cs_list:
				if 'CollectionEventID' in cs and event_id == cs['CollectionEventID']:
					cs['CollectionEvent'] = ce_getter.ce_dict[event_id]
		
		return
