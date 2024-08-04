import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.SpecimenPartDeleter import SpecimenPartDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitDeleter import IdentificationUnitDeleter

class CollectionSpecimenDeleter(DCDeleter):
	def __init__(self, dc_db):
		DCDeleter.__init__(self, dc_db)
		
		self.delete_temptable = '#cs_to_delete'


	def deleteByPrimaryKeys(self, specimen_ids):
		self.createDeleteTempTable()
		
		pagesize = 1000
		while len(specimen_ids) > 0:
			cached_ids = specimen_ids[:pagesize]
			del specimen_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#cs_pks_to_delete_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#cs_pks_to_delete_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#cs_pks_to_delete_temptable] (
			[CollectionSpecimenID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_delete])
			SELECT [RowGUID] FROM [CollectionSpecimen] cs
			INNER JOIN [#cs_pks_to_delete_temptable] pks
			ON pks.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
			;""".format(self.delete_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimen')
		self.deleteChildSpecimenParts()
		self.deleteChildIdentificationUnits()
		self.deleteFromTable('CollectionSpecimen')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('CollectionSpecimen')
		self.deleteChildSpecimenParts()
		self.deleteChildIdentificationUnits()
		
		self.deleteFromTable('CollectionSpecimen')
		return


	def deleteChildSpecimenParts(self):
		id_lists = []
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [CollectionSpecimenPart] csp
		ON cs.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		csp_deleter = SpecimenPartDeleter(self.dc_db)
		csp_deleter.deleteByPrimaryKeys(id_lists)
		
		return


	def deleteChildIdentificationUnits(self):
		id_lists = []
		query = """
		SELECT cs.[CollectionSpecimenID], iu.[IdentificationUnitID]
		FROM [CollectionSpecimen] cs
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		INNER JOIN [{0}] rg_temp
		ON cs.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		iu_deleter = IdentificationUnitDeleter(self.dc_db)
		iu_deleter.deleteByPrimaryKeys(id_lists)
		
		return





