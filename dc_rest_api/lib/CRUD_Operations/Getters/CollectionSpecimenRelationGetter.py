import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter


class CollectionSpecimenRelationGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_csr_temptable'



	def getByPrimaryKeys(self, csr_ids):
		self.createGetTempTable()
		# primary key: CollectionSpecimenID, RelatedSpecimenURI
		# needed ids for request:
		# CollectionSpecimenID
		# possible ids:
		# IdentificationUnitID, SpecimenPartID, RelatedSpecimenCollectionID
		
		batchsize = 500
		while len(csr_ids) > 0:
			cached_ids = csr_ids[:batchsize]
			del csr_ids[:batchsize]
			placeholders = ['(?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#csr_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#csr_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT,
				[SpecimenPartID] INT,
				[RelatedSpecimenCollectionID] INT,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
				INDEX [RelatedSpecimenCollectionID_idx] ([RelatedSpecimenCollectionID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#csr_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			[RelatedSpecimenCollectionID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionSpecimenRelation] csr
			INNER JOIN [#csr_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = csr.[CollectionSpecimenID]
			AND (pks.[IdentificationUnitID] = csr.[IdentificationUnitID] OR (pks.[IdentificationUnitID] IS NULL AND csr.[IdentificationUnitID] IS NULL))
			AND (pks.[SpecimenPartID] = csr.[SpecimenPartID] OR (pks.[SpecimenPartID] IS NULL AND csr.[SpecimenPartID] IS NULL))
			AND (pks.[RelatedSpecimenCollectionID] = csr.[RelatedSpecimenCollectionID] OR (pks.[RelatedSpecimenCollectionID] IS NULL AND csr.[RelatedSpecimenCollectionID] IS NULL))
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		specimenrelations = self.getData()
		
		return specimenrelations


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		specimenrelations = self.getData()
		
		return specimenrelations


	def setConnectedTableData(self):
		self.setCollections()
		return


	def getData(self):
		self.setDatabaseURN()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		csr.[CollectionSpecimenID],
		csr.[RelatedSpecimenURI],
		csr.[IdentificationUnitID],
		csr.[SpecimenPartID],
		csr.[RelatedSpecimenCollectionID],
		csr.[RelationType],
		csr.[RelatedSpecimenDisplayText],
		csr.[RelatedSpecimenDescription],
		csr.[Notes],
		csr.[IsInternalRelationCache]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimenRelation] csr
		ON csr.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.results_list


	def list2dict(self):
		self.results_dict = {}
		for element in self.results_list:
			if element['CollectionSpecimenID'] not in self.results_dict:
				self.results_dict[element['CollectionSpecimenID']] = {}
			if element['RelatedSpecimenURI'] not in self.results_dict[element['CollectionSpecimenID']]:
				self.results_dict[element['CollectionSpecimenID']][element['RelatedSpecimenURI']] = {}
			
			self.results_dict[element['CollectionSpecimenID']][element['RelatedSpecimenURI']] = element 
		
		return













