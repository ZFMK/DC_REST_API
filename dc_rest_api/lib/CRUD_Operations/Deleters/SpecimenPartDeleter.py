import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitInPartDeleter import IdentificationUnitInPartDeleter


class SpecimenPartDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		self.prohibited = []
		self.delete_temptable = '#csp_to_delete'


	def deleteByPrimaryKeys(self, csp_ids):
		self.createDeleteTempTable()
		
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
			[IdentificationUnitID] INT,
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
			INDEX [IdentificationUnitID_idx] ([IdentificationUnitID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 600
		while len(csp_ids) > 0:
			cached_ids = csp_ids[:pagesize]
			del csp_ids[:pagesize]
			placeholders = ['(?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			INSERT INTO [#csp_pks_to_delete_temptable] (
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
		SELECT DISTINCT csp.[RowGUID]
		FROM [CollectionSpecimenPart] csp
		INNER JOIN [#csp_pks_to_delete_temptable] pks
		ON pks.[CollectionSpecimenID] = csp.[CollectionSpecimenID] 
		AND pks.[SpecimenPartID] = csp.[SpecimenPartID]
		LEFT JOIN [IdentificationUnitInPart] iuip
		ON csp.[CollectionSpecimenID] = iuip.[CollectionSpecimenID]
		AND csp.[SpecimenPartID] = iuip.[SpecimenPartID]
		WHERE iuip.[IdentificationUnitID] = pks.[IdentificationUnitID] 
		OR (iuip.[IdentificationUnitID] IS NULL AND pks.[IdentificationUnitID] IS NULL)
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenPart')
		self.prohibited = self.filterAllowedRowGUIDs('CollectionSpecimenPart', ['CollectionSpecimenID', 'SpecimenPartID'])
		self.deleteChildIdentificationUnitsInPart()
		self.deleteFromTable('CollectionSpecimenPart')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimenPart')
		self.prohibited = self.filterAllowedRowGUIDs('CollectionSpecimenPart', ['CollectionSpecimenID', 'SpecimenPartID'])
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
		
		iuip_deleter = IdentificationUnitInPartDeleter(self.dc_db, self.users_project_ids)
		iuip_deleter.deleteByPrimaryKeys(iuip_id_lists)
		
		return


