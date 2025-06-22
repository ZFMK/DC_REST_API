import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
# from dc_rest_api.lib.CRUD_Operations.Deleters.CollectionDeleter import CollectionDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitAnalysisDeleter import IdentificationUnitAnalysisDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.SpecimenPartDeleter import SpecimenPartDeleter

class CollectionSpecimenRelationDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#csrel_to_delete'


	def deleteByPrimaryKeys(self, specimen_rel_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#csrel_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
	
		query = """
		CREATE TABLE [#csrel_pks_to_delete_temptable] (
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT,
			[SpecimenPartID] INT,
			[CollectionID] INT,
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
			INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
			INDEX [CollectionID_idx] ([CollectionID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 500
		while len(specimen_rel_ids) > 0:
			cached_ids = specimen_rel_ids[:pagesize]
			del specimen_rel_ids[:pagesize]
			placeholders = ['(?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			INSERT INTO [#csrel_pks_to_delete_temptable] (
			[CollectionSpecimenID], [IdentificationUnitID], [SpecimenPartID], [CollectionID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
		
		# must be out of the while loop that fills the #event_pks_to_delete_temptable,
		# otherwise RowGUIDs are inserted more than once
		query = """
		INSERT INTO [{0}] ([rowguid_to_delete])
		SELECT DISTINCT [RowGUID]
		FROM [CollectionSpecimenRelation] csrel
		INNER JOIN [#csrel_pks_to_delete_temptable] pks
		ON pks.[CollectionSpecimenID] = csrel.[CollectionSpecimenID]
		AND (pks.[IdentificationUnitID] = csrel.[IdentificationUnitID] OR (pks.[IdentificationUnitID] IS NULL AND csrel.[IdentificationUnitID] IS NULL))
		AND (pks.[SpecimenPartID] = csrel.[SpecimenPartID] OR (pks.[SpecimenPartID] IS NULL AND csrel.[SpecimenPartID] IS NULL))
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenRelation')
		self.prohibited = self.filterAllowedRowGUIDs('CollectionSpecimenRelation', ['CollectionSpecimenID'])
		self.deleteFromTable('CollectionSpecimenRelation')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenRelation')
		self.prohibited = self.filterAllowedRowGUIDs('CollectionSpecimenRelation', ['CollectionSpecimenID'])
		
		self.deleteFromTable('CollectionSpecimenRelation')
		return


