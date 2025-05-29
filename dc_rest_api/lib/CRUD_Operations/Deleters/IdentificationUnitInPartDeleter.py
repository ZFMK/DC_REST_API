import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter

class IdentificationUnitInPartDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#iuip_to_delete'


	def deleteByPrimaryKeys(self, iuip_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#iuip_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
	
		query = """
		CREATE TABLE [#iuip_pks_to_delete_temptable] (
			[CollectionSpecimenID] INT NOT NULL,
			[SpecimenPartID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
			INDEX [IdentificationUnitID_idx] ([IdentificationUnitID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 600
		while len(iuip_ids) > 0:
			cached_ids = iuip_ids[:pagesize]
			del iuip_ids[:pagesize]
			placeholders = ['(?, ?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			INSERT INTO [#iuip_pks_to_delete_temptable] (
			[CollectionSpecimenID], [SpecimenPartID], [IdentificationUnitID]
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
		FROM [IdentificationUnitInPart] iuip
		INNER JOIN [#iuip_pks_to_delete_temptable] pks
		ON pks.[CollectionSpecimenID] = iuip.[CollectionSpecimenID] 
			AND pks.[SpecimenPartID] = iuip.[SpecimenPartID] 
			AND pks.[IdentificationUnitID] = iuip.[IdentificationUnitID]
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitInPart')
		self.prohibited = self.filterAllowedRowGUIDs('IdentificationUnitInPart', ['CollectionSpecimenID', 'SpecimenPartID', 'IdentificationUnitID'])
		self.deleteFromTable('IdentificationUnitInPart')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitInPart')
		self.prohibited = self.filterAllowedRowGUIDs('IdentificationUnitInPart', ['CollectionSpecimenID', 'SpecimenPartID', 'IdentificationUnitID'])
		self.deleteFromTable('IdentificationUnitInPart')
		return







