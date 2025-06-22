import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter


class CollectionDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		self.prohibited = []
		self.delete_temptable = '#collection_to_delete'


	def deleteByPrimaryKeys(self, collection_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#collection_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#collection_pks_to_delete_temptable] (
			[CollectionID] INT NOT NULL,
			INDEX [CollectionID_idx] ([CollectionID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 2000
		while len(collection_ids) > 0:
			cached_ids = collection_ids[:pagesize]
			del collection_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			INSERT INTO [#collection_pks_to_delete_temptable] (
			[CollectionID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
		
		
		# must be out of the while loop that fills the #collection_pks_to_delete_temptable,
		# otherwise RowGUIDs are inserted more than once
		query = """
		INSERT INTO [{0}] ([rowguid_to_delete])
		SELECT DISTINCT c.[RowGUID]
		FROM [Collection] c
		INNER JOIN [#collection_pks_to_delete_temptable] pks
		ON pks.[CollectionID] = c.[CollectionID]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.filterOtherwiseConnectedCollections()
		self.checkRowGUIDsUniqueness('Collection')
		self.deleteFromTable('Collection')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.filterOtherwiseConnectedCollections()
		self.checkRowGUIDsUniqueness('Collection')
		
		self.deleteFromTable('Collection')
		return


	def filterOtherwiseConnectedCollections(self):
		
		query = """
		DELETE rg_temp
		FROM [{0}] rg_temp
		INNER JOIN [Collection] c
		ON c.[RowGUID] = rg_temp.[rowguid_to_delete]
		RIGHT JOIN Collection c_p
		ON c_p.CollectionID = c.CollectionParentID
		RIGHT JOIN CollectionSpecimen cs
		ON cs.CollectionID = c.CollectionID
		RIGHT JOIN CollectionSpecimenPart csp
		ON csp.CollectionID = c.CollectionID
		RIGHT JOIN CollectionSpecimenRelation csrel
		ON csrel.CollectionID = c.CollectionID
		RIGHT JOIN [Transaction] ta
		ON ta.FromCollectionID = c.CollectionID OR ta.ToCollectionID = c.CollectionID OR ta.AdministratingCollectionID = c.CollectionID
		RIGHT JOIN CollectionImage ci
		ON ci.CollectionID = c.CollectionID
		RIGHT JOIN CollectionManager cm
		ON cm.AdministratingCollectionID = c.CollectionID
		RIGHT JOIN CollectionTask ct
		ON ct.CollectionID = c.CollectionID
		RIGHT JOIN CollectionUser cu
		ON cu.CollectionID = c.CollectionID
		WHERE c.CollectionID IS NOT NULL
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return



