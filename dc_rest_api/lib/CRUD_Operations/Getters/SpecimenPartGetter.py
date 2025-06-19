import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionGetter import CollectionGetter
from dc_rest_api.lib.CRUD_Operations.Getters.CollectionSpecimenRelationGetter import CollectionSpecimenRelationGetter


class SpecimenPartGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
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
				INDEX [SpecimenPartID_idx] ([SpecimenPartID])
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
		self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
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
		csp.[DataWithholdingReason],
		iuip.[IdentificationUnitID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenPart] csp
		ON csp.[RowGUID] = g_temp.[rowguid_to_get]
		LEFT JOIN IdentificationUnitInPart iuip
		ON iuip.CollectionSpecimenID = csp.CollectionSpecimenID
			AND iuip.SpecimenPartID = csp.SpecimenPartID
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildCollections()
		# pudb.set_trace()
		self.setCollectionSpecimenRelations()
		
		return self.results_list


	def list_2_iu_part_dict(self):
		# 2 different dicts for parts: parts with units
		self.results_dict = {}
		for element in self.results_list:
			if element['CollectionSpecimenID'] not in self.results_dict:
				self.results_dict[element['CollectionSpecimenID']] = {}
			if 'IdentificationUnitID' in element and element['IdentificationUnitID'] is not None:
				if element['IdentificationUnitID'] not in self.results_dict[element['CollectionSpecimenID']]:
					self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = {}
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['SpecimenPartID']] = element
		return


	def list_2_cs_part_dict(self):
		# 2 different dicts for parts: parts without units
		self.results_dict = {}
		for element in self.results_list:
			if element['CollectionSpecimenID'] not in self.results_dict:
				self.results_dict[element['CollectionSpecimenID']] = {}
			if 'IdentificationUnitID' in element and element['IdentificationUnitID'] is not None:
				pass
			else:
				self.results_dict[element['CollectionSpecimenID']][element['SpecimenPartID']] = element
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
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
		
		return


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
		
		for csp in self.results_list:
			if csp['CollectionID'] in c_getter.results_dict:
				csp['Collection'] = c_getter.results_dict[csp['CollectionID']]
		
		return


	def setCollectionSpecimenRelations(self):
		csrel_getter = CollectionSpecimenRelationGetter(self.dc_db)
		csrel_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT csrel.[RowGUID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [{1}] rg_temp
		ON csp.[RowGUID] = rg_temp.[rowguid_to_get]
		LEFT JOIN IdentificationUnitInPart iuip
		ON iuip.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		AND iuip.[SpecimenPartID] = csp.[SpecimenPartID]
		INNER JOIN [CollectionSpecimenRelation] csrel
		ON csrel.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		AND csrel.[SpecimenPartID] = csp.[SpecimenPartID]
		AND (csrel.[IdentificationUnitID] = iuip.[IdentificationUnitID]
			OR (csrel.[IdentificationUnitID] IS NULL AND iuip.[IdentificationUnitID] IS NULL))
		;""".format(csrel_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.setDatabaseURN()
		
		csrel_getter.getData()
		csrel_getter.list2dict()
		
		for csp_dict in self.results_list:
			cs_id = csp_dict['CollectionSpecimenID']
			csp_id = csp_dict['SpecimenPartID']
			
			if 'IdentificationUnitID' in csp_dict and csp_dict['IdentificationUnitID'] is not None:
				iu_id = csp_dict['IdentificationUnitID']
				if cs_id in csrel_getter.results_dict:
					for csrel_id in csrel_getter.results_dict[cs_id]:
						if 'IdentificationUnitID' in csrel_getter.results_dict[cs_id][csrel_id] and 'SpecimenPartID' in csrel_getter.results_dict[cs_id][csrel_id]:
							if csrel_getter.results_dict[cs_id][csrel_id]['IdentificationUnitID'] == iu_id and csrel_getter.results_dict[cs_id][csrel_id]['SpecimenPartID'] == csp_id:
								if 'CollectionSpecimenRelations' not in csp_dict[cs_id]:
									csp_dict['CollectionSpecimenRelations'] = []
								csp_dict['CollectionSpecimenRelations'].append(csrel_getter.results_dict[cs_id][csrel_id])
			elif cs_id in csrel_getter.results_dict:
				for csrel_id in csrel_getter.results_dict[cs_id]:
					if 'SpecimenPartID' in csrel_getter.results_dict[cs_id][csrel_id] and csrel_getter.results_dict[cs_id][csrel_id]['SpecimenPartID'] == csp_id:
						if 'IdentificationUnitID' not in csrel_getter.results_dict[cs_id][csrel_id] or csrel_getter.results_dict[cs_id]['IdentificationUnitID'] is None:
							if 'CollectionSpecimenRelations' not in csp_dict[cs_id]:
								csp_dict[cs_id]['CollectionSpecimenRelations'] = []
							csp_dict[cs_id]['CollectionSpecimenRelations'].append(csrel_getter.results_dict[cs_id][csrel_id])
		
		return





