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
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimenparts = self.getData()
		
		self.setChildCollections()
		
		return specimenparts


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimenparts = self.getData()
		
		self.setChildCollections()
		
		return specimenparts



	def getData(self):
		
		query = """
		SELECT
		g_temp.[row_num],
		g_temp.[rowguid_to_get] AS [RowGUID],
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
		
		projectclause = self.getProjectClause()
		
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID], csp.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON csp.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE csp.[DataWithholdingReason] IS NOT NULL AND csp.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append(dict(zip(columns, row)))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON csp.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE csp.[DataWithholdingReason] IS NOT NULL AND csp.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildCollections(self):
		
		id_lists = []
		query = """
		SELECT DISTINCT csp.[CollectionID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [{0}] rg_temp
		ON csp.[RowGUID] = rg_temp.[rowguid_to_get]
		WHERE csp.[CollectionID] IS NOT NULL
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
			for csp in self.csp_list:
				if collection_id == csp['CollectionID']:
					csp['Collection'] = c_getter.c_dict[collection_id]
		
		return








