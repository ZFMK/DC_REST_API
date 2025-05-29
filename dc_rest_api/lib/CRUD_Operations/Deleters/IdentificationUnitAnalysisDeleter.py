import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitAnalysisMethodDeleter import IdentificationUnitAnalysisMethodDeleter

class IdentificationUnitAnalysisDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#iua_to_delete'


	def deleteByPrimaryKeys(self, specimen_unit_analysis_ids):
		self.createDeleteTempTable()
		
		query = """
		DROP TABLE IF EXISTS [#iua_pks_to_delete_temptable]
		"""
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
	
		query = """
		CREATE TABLE [#iua_pks_to_delete_temptable] (
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) COLLATE {0} NOT NULL,
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
			INDEX [AnalysisID_idx] ([AnalysisID]),
			INDEX [AnalysisNumber_idx] ([AnalysisNumber])
		)
		;""".format(self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		pagesize = 500
		while len(specimen_unit_analysis_ids) > 0:
			cached_ids = specimen_unit_analysis_ids[:pagesize]
			del specimen_unit_analysis_ids[:pagesize]
			placeholders = ['(?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			INSERT INTO [#iua_pks_to_delete_temptable] (
			[CollectionSpecimenID], [IdentificationUnitID], [AnalysisID], [AnalysisNumber]
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
		FROM [IdentificationUnitAnalysis] iua
		INNER JOIN [#iua_pks_to_delete_temptable] pks
		ON pks.[CollectionSpecimenID] = iua.[CollectionSpecimenID] 
		AND pks.[IdentificationUnitID] = iua.[IdentificationUnitID]
		AND pks.[AnalysisID] = iua.[AnalysisID]
		AND pks.[AnalysisNumber] = iua.[AnalysisNumber]
		;""".format(self.delete_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysis')
		self.deleteChildIdentificationUnitAnalysisMethods()
		self.deleteFromTable('IdentificationUnitAnalysis')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysis')
		self.deleteChildIdentificationUnitAnalysisMethods()
		
		self.deleteFromTable('IdentificationUnitAnalysis')
		return


	def deleteChildIdentificationUnitAnalysisMethods(self):
		id_lists = []
		query = """
		SELECT iuam.[CollectionSpecimenID], iuam.[IdentificationUnitID], iuam.[AnalysisID], iuam.[AnalysisNumber], iuam.[MethodID], iuam.[MethodMarker]
		FROM [IdentificationUnitAnalysisMethod] iuam
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON iuam.[CollectionSpecimenID] = iua.[CollectionSpecimenID]
		AND iuam.[IdentificationUnitID] = iua.[IdentificationUnitID]
		AND iuam.[AnalysisID] = iua.[AnalysisID]
		AND iuam.[AnalysisNumber] = iua.[AnalysisNumber]
		INNER JOIN [{0}] rg_temp
		ON iua.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(list(row))
		
		iuam_deleter = IdentificationUnitAnalysisMethodDeleter(self.dc_db, self.users_project_ids)
		iuam_deleter.deleteByPrimaryKeys(id_lists)
		
		return





