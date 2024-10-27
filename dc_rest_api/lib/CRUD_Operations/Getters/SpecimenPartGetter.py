import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionGetter import CollectionGetter

class SpecimenPartGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.withholded = []
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_csp_temptable'



	def getByPrimaryKeys(self, csp_ids):
		self.createGetTempTable()
		
		batchsize = 1000
		while len(csp_ids) > 0:
			cached_ids = csp_ids[:batchsize]
			del csp_ids[:batchsize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#csp_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#csp_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[SpecimenPartID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#csp_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[SpecimenPartID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionSpecimenPart] csp
			INNER JOIN [#csp_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
			AND pks.[SpecimenPartID] = csp.[SpecimenPartID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		specimenparts = self.getData()
		
		return specimenparts


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		specimenparts = self.getData()
		
		return specimenparts



	def getData(self):
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		csp.[CollectionSpecimenID],
		csp.[SpecimenPartID],
		csp.[CollectionID],
		csp.[AccessionNumber],
		csp.[PartSublabel],
		csp.[PreparationMethod],
		csp.[MaterialCategory],
		csp.[StorageLocation],
		csp.[StorageContainer],
		csp.[Stock],
		csp.[StockUnit],
		csp.[ResponsibleName],
		csp.[ResponsibleAgentURI],
		csp.[Notes],
		csp.[DataWithholdingReason]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.csp_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildCollections()
		
		return self.csp_list


	def rows2list(self):
		self.csp_list = []
		for row in self.csp_rows:
			self.csp_list.append(dict(zip(self.columns, row)))
		
		return


	def list2dict(self):
		self.csp_dict = {}
		for element in self.csp_list:
			if element['CollectionSpecimenID'] not in self.csp_dict:
				self.csp_dict[element['CollectionSpecimenID']] = {}
				
			self.csp_dict[element['CollectionSpecimenID']][element['SpecimenPartID']] = element 


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID], csp.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON csp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (csp.[DataWithholdingReason] IS NOT NULL AND csp.[DataWithholdingReason] != '') 
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1], row[2]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [CollectionSpecimen] cs ON csp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (csp.[DataWithholdingReason] IS NOT NULL AND csp.[DataWithholdingReason] != '') 
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildCollections(self):
		
		c_getter = CollectionGetter(self.dc_db)
		c_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT c.[RowGUID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [{1}] rg_temp
		ON csp.[RowGUID] = rg_temp.[rowguid_to_get]
		INNER JOIN [Collection] c
		ON csp.[CollectionID] = c.[CollectionID]
		WHERE csp.[CollectionID] IS NOT NULL
		;""".format(c_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		c_getter.getData()
		c_getter.list2dict()
		
		for csp in self.csp_list:
			if csp['CollectionID'] in c_getter.c_dict:
				csp['Collection'] = c_getter.c_dict[csp['CollectionID']]
		
		return








