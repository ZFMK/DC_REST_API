import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationDeleter import IdentificationDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitAnalysisDeleter import IdentificationUnitAnalysisDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.SpecimenPartDeleter import SpecimenPartDeleter

class IdentificationUnitDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#iu_to_delete'


	def deleteByPrimaryKeys(self, specimen_unit_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#iu_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
	
		query = """
		CREATE TABLE [#iu_pks_to_delete_temptable] (
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [IdentificationUnitID_idx] ([IdentificationUnitID])
		)
		;"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 1000
		while len(specimen_unit_ids) > 0:
			cached_ids = specimen_unit_ids[:pagesize]
			del specimen_unit_ids[:pagesize]
			placeholders = ['(?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			INSERT INTO [#iu_pks_to_delete_temptable] (
			[CollectionSpecimenID], [IdentificationUnitID]
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
		FROM [IdentificationUnit] iu
		INNER JOIN [#iu_pks_to_delete_temptable] pks
		ON pks.[CollectionSpecimenID] = iu.[CollectionSpecimenID] AND pks.[IdentificationUnitID] = iu.[IdentificationUnitID]
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.checkRowGUIDsUniqueness('IdentificationUnit')
		self.prohibited = self.filterAllowedRowGUIDs('IdentificationUnit', ['CollectionSpecimenID', 'IdentificationUnitID'])
		self.deleteChildIdentifications()
		self.deleteChildIdentificationUnitAnalyses()
		self.deleteChildSpecimenParts()
		self.deleteFromTable('IdentificationUnit')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('IdentificationUnit')
		self.prohibited = self.filterAllowedRowGUIDs('IdentificationUnit', ['CollectionSpecimenID', 'IdentificationUnitID'])
		self.deleteChildIdentifications()
		
		self.deleteFromTable('IdentificationUnit')
		return


	def deleteChildIdentifications(self):
		id_lists = []
		query = """
		SELECT [CollectionSpecimenID], [IdentificationUnitID]
		FROM [IdentificationUnit] iu
		INNER JOIN [{0}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		i_deleter = IdentificationDeleter(self.dc_db, self.users_project_ids)
		i_deleter.deleteByPrimaryKeys(id_lists)
		
		return


	def deleteChildIdentificationUnitAnalyses(self):
		id_lists = []
		query = """
		SELECT iua.[CollectionSpecimenID], iua.[IdentificationUnitID], iua.[AnalysisID], iua.[AnalysisNumber]
		FROM [IdentificationUnitAnalysis] iua
		INNER JOIN IdentificationUnit iu
		ON iu.[CollectionSpecimenID] = iua.[CollectionSpecimenID]
		AND iu.[IdentificationUnitID] = iua.[IdentificationUnitID]
		INNER JOIN [{0}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		iua_deleter = IdentificationUnitAnalysisDeleter(self.dc_db, self.users_project_ids)
		iua_deleter.deleteByPrimaryKeys(id_lists)
		
		return


	def deleteChildSpecimenParts(self):
		id_lists = []
		query = """
		SELECT csp.[CollectionSpecimenID], csp.[SpecimenPartID], iuip.[IdentificationUnitID]
		FROM [IdentificationUnit] iu
		INNER JOIN [IdentificationUnitInPart] iuip
		ON iuip.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
		AND iuip.[IdentificationUnitID] = iu.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimenPart] csp
		ON iuip.[CollectionSpecimenID] = csp.[CollectionSpecimenID]
		AND iuip.[SpecimenPartID] = csp.[SpecimenPartID]
		INNER JOIN [{0}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		csp_deleter = SpecimenPartDeleter(self.dc_db, self.users_project_ids)
		csp_deleter.deleteByPrimaryKeys(id_lists)
		
		return
