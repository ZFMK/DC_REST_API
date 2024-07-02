import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_importer.DCImporter.DCDeleter import DCDeleter

class IdentificationDeleter(DCDeleter):
	def __init__(self, dc_db):
		DCDeleter.__init__(self, dc_db)
		
		self.delete_temptable = '#identifications_to_delete'


	def deleteByPrimaryKeys(self, specimen_unit_ids):
		self.createDeleteTempTable()
		
		pagesize = 1000
		while len(specimen_unit_ids) > 0:
			cached_ids = specimen_unit_ids[:pagesize]
			del specimen_unit_ids[:pagesize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			DROP TABLE IF EXISTS [#i_pks_to_delete_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#i_pks_to_delete_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#i_pks_to_delete_temptable] (
			[CollectionSpecimenID], [IdentificationUnitID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_delete])
			SELECT [RowGUID] FROM [Identification] i
			INNER JOIN [#i_pks_to_delete_temptable] pks
			ON pks.[CollectionSpecimenID] = i.[CollectionSpecimenID] AND pks.[IdentificationUnitID] = i.[IdentificationUnitID]
			;""".format(self.delete_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		self.checkRowGUIDsUniqueness('Identification')
		self.deleteFromTable('Identification')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('Identification')
		
		self.deleteFromTable('Identification')
		return








