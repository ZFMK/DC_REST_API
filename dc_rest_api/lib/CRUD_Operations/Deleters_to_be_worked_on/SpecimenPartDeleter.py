import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_importer.DCImporter.DCDeleter import DCDeleter
from dc_importer.DCImporter.IdentificationUnitInPartDeleter import IdentificationUnitInPartDeleter


class SpecimenPartDeleter(DCDeleter):
	def __init__(self, dc_db):
		DCDeleter.__init__(self, dc_db)
		
		self.delete_temptable = '#csp_to_delete'


	def deleteByPrimaryKeys(self, specimen_part_ids):
		self.createDeleteTempTable()
		
		pagesize = 1000
		while len(specimen_part_ids) > 0:
			cached_ids = specimen_part_ids[:pagesize]
			del specimen_part_ids[:pagesize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			DROP TABLE IF EXISTS [#csp_pks_to_delete_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#csp_pks_to_delete_temptable] (
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
			INSERT INTO [#csp_pks_to_delete_temptable] (
			[CollectionSpecimenID], [SpecimenPartID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_delete])
			SELECT [RowGUID] FROM [CollectionSpecimenPart] csp
			INNER JOIN [#csp_pks_to_delete_temptable] pks
			ON pks.[CollectionSpecimenID] = csp.[CollectionSpecimenID] AND pks.[SpecimenPartID] = csp.[SpecimenPartID]
			;""".format(self.delete_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenPart')
		self.deleteChildIdentificationUnitsInPart()
		self.deleteFromTable('CollectionSpecimenPart')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenPart')
		self.deleteChildIdentificationUnitsInPart()
		
		self.deleteFromTable('CollectionSpecimenPart')
		return


	def deleteChildIdentificationUnitsInPart(self):
		iuip_id_lists = []
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID], iuip.[IdentificationUnitID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [IdentificationUnitInPart] iuip
		ON csp.[CollectionSpecimenID] = iuip.[CollectionSpecimenID] AND csp.[SpecimenPartID] = iuip.[SpecimenPartID]
		INNER JOIN [{0}] rg_temp
		ON csp.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			iuip_id_lists.append(row)
		
		iuip_deleter = IdentificationUnitInPartDeleter(self.dc_db)
		iuip_deleter.deleteByPrimaryKeys(iuip_id_lists)
		
		return





