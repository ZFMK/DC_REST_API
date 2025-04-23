import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter
from dc_rest_api.lib.CRUD_Operations.Deleters.IdentificationUnitAnalysisMethodParameterDeleter import IdentificationUnitAnalysisMethodParameterDeleter

class IdentificationUnitAnalysisMethodDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#iuam_to_delete'


	def deleteByPrimaryKeys(self, iuam_ids):
		self.createDeleteTempTable()
		
		pagesize = 300
		while len(iuam_ids) > 0:
			cached_ids = iuam_ids[:pagesize]
			del iuam_ids[:pagesize]
			placeholders = ['(?, ?, ?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			DROP TABLE IF EXISTS [#iuam_pks_to_delete_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iuam_pks_to_delete_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) COLLATE {0} NOT NULL,
				[MethodID] INT NOT NULL,
				[MethodMarker] NVARCHAR(50) COLLATE {0} NOT NULL, 
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
				INDEX [MethodID_idx] ([MethodID]),
				INDEX [MethodMarker_idx] ([MethodMarker])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iuam_pks_to_delete_temptable] (
			[CollectionSpecimenID], [IdentificationUnitID], [AnalysisID], [AnalysisNumber], [MethodID], [MethodMarker]
			)
			VALUES {0}
			;""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_delete])
			SELECT [RowGUID] FROM [IdentificationUnitAnalysisMethod] iuam
			INNER JOIN [#iuam_pks_to_delete_temptable] pks
			ON pks.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] 
			AND pks.[IdentificationUnitID] = iuam.[IdentificationUnitID]
			AND pks.[AnalysisID] = iuam.[AnalysisID]
			AND pks.[AnalysisNumber] = iuam.[AnalysisNumber]
			AND pks.[MethodID] = iuam.[MethodID]
			AND pks.[MethodMarker] = iuam.[MethodMarker]
			;""".format(self.delete_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysisMethod')
		self.deleteChildIdentificationUnitAnalysisMethodParameters()
		#deleteUnconnectedMethods()
		#self.deleteUnconnectedMethodForAnalysis()
		
		self.deleteFromTable('IdentificationUnitAnalysisMethod')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysisMethod')
		self.deleteChildIdentificationUnitAnalysisMethodParameters()
		#deleteUnconnectedMethods()
		#self.deleteUnconnectedMethodForAnalysis()
		
		self.deleteFromTable('IdentificationUnitAnalysisMethod')
		return


	def deleteChildIdentificationUnitAnalysisMethodParameters(self):
		id_lists = []
		query = """
		SELECT iuamp.[CollectionSpecimenID], iuamp.[IdentificationUnitID], iuamp.[AnalysisID], iuamp.[AnalysisNumber], iuamp.[MethodID], iuamp.[MethodMarker], iuamp.[ParameterID]
		FROM [IdentificationUnitAnalysisMethodParameter] iuamp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuamp.[CollectionSpecimenID] = iuam.[CollectionSpecimenID]
		AND iuamp.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		AND iuamp.[AnalysisID] = iuam.[AnalysisID]
		AND iuamp.[AnalysisNumber] = iuam.[AnalysisNumber]
		AND iuamp.[MethodID] = iuam.[MethodID]
		AND iuamp.[MethodMarker] = iuam.[MethodMarker]
		INNER JOIN [{0}] rg_temp
		ON iuam.[RowGUID] = rg_temp.[rowguid_to_delete]
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(list(row))
		
		iuamp_deleter = IdentificationUnitAnalysisMethodParameterDeleter(self.dc_db, self.users_project_ids)
		iuamp_deleter.deleteByPrimaryKeys(id_lists)
		
		return


	'''
	def deleteChildMethodForAnalysis(self):
		id_lists = []
		query = """
		SELECT mfa.[AnalysisID], mfa.[MethodID]
		FROM [IdentificationUnitAnalysisMethod] iuam
		INNER JOIN [{0}] rg_temp
		ON iuam.[RowGUID] = rg_temp.[rowguid_to_delete]
		INNER JOIN [MethodForAnalysis] mfa
		ON iuam.[AnalysisID] = mfa.[AnalysisID]
		AND iuam.[MethodID] = mfa.[MethodID]
		LEFT JOIN [IdentificationUnitAnalysisMethod] other_iuam
		ON other_iuam.[AnalysisID] = mfa.[AnalysisID]
		AND other_iuam.[MethodID] = mfa.[MethodID]
		AND iuam.[RowGUID] != other_iuam.[RowGUID]
		WHERE other_iuam.[RowGUID] IS NULL
		;""".format(self.delete_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		rows = self.cur.fetchall()
		for row in rows:
			id_lists.append(row)
		
		iuam_deleter = IdentificationUnitAnalysisMethodDeleter(self.dc_db, self.users_project_ids)
		iuam_deleter.deleteByPrimaryKeys(id_lists)
		
		return
	'''


