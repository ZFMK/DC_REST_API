import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Deleters.DCDeleter import DCDeleter


class IdentificationUnitAnalysisMethodParameterDeleter(DCDeleter):
	def __init__(self, dc_db, users_project_ids = []):
		DCDeleter.__init__(self, dc_db, users_project_ids)
		
		self.prohibited = []
		self.delete_temptable = '#iuamp_to_delete'


	def deleteByPrimaryKeys(self, iuamp_ids):
		self.createDeleteTempTable()
		
		pagesize = 200
		while len(iuamp_ids) > 0:
			cached_ids = iuamp_ids[:pagesize]
			del iuamp_ids[:pagesize]
			placeholders = ['(?, ?, ?, ?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids in cached_ids:
				values.extend(ids)
			
			query = """
			DROP TABLE IF EXISTS [#iuamp_pks_to_delete_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iuamp_pks_to_delete_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) COLLATE {0} NOT NULL,
				[MethodID] INT NOT NULL,
				[MethodMarker] NVARCHAR(50) COLLATE {0} NOT NULL,
				[ParameterID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
				INDEX [MethodID_idx] ([MethodID]),
				INDEX [MethodMarker_idx] ([MethodMarker]),
				INDEX [ParameterID_idx] ([ParameterID])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iuamp_pks_to_delete_temptable] (
				[CollectionSpecimenID], [IdentificationUnitID], [AnalysisID], [AnalysisNumber], [MethodID], [MethodMarker], [ParameterID]
			)
			VALUES {0}
			;""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_delete])
			SELECT [RowGUID] FROM [IdentificationUnitAnalysisMethodParameter] iuamp
			INNER JOIN [#iuamp_pks_to_delete_temptable] pks
			ON pks.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID] 
			AND pks.[IdentificationUnitID] = iuamp.[IdentificationUnitID]
			AND pks.[AnalysisID] = iuamp.[AnalysisID]
			AND pks.[AnalysisNumber] = iuamp.[AnalysisNumber]
			AND pks.[MethodID] = iuamp.[MethodID]
			AND pks.[MethodMarker] = iuamp.[MethodMarker]
			AND pks.[ParameterID] = iuamp.[ParameterID]
			;""".format(self.delete_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysisMethodParameter')
		self.deleteFromTable('IdentificationUnitAnalysisMethodParameter')
		
		return


	def deleteByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createDeleteTempTable()
		self.fillDeleteTempTable()
		
		self.checkRowGUIDsUniqueness('IdentificationUnitAnalysisMethodParameter')
		self.deleteFromTable('IdentificationUnitAnalysisMethodParameter')
		return





