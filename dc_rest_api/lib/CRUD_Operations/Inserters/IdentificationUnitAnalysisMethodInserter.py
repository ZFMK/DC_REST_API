import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationUnitAnalysisMethodParameterInserter import IdentificationUnitAnalysisMethodParameterInserter

class IdentificationUnitAnalysisMethodInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iuam_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'AnalysisID', 'None allowed': False},
			{'colname': 'AnalysisNumber', 'None allowed': False},
			{'colname': 'MethodID', 'None allowed': False},
			{'colname': 'MethodMarker', 'None allowed': False},
			{'colname': 'DatabaseURN'}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def setIdentificationUnitAnalysisMethodDicts(self, json_dicts = []):
		self.iuam_dicts = []
		iuam_count = 1
		for iuam_dict in json_dicts:
			iuam_dict['entry_num'] = iuam_count
			iuam_count += 1
			self.iuam_dicts.append(iuam_dict)
		return


	def insertIdentificationUnitAnalysisMethodData(self):
		
		self.__createIdentificationUnitAnalysisMethodTempTable()
		
		self.json2temp.set_datadicts(self.iuam_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertMethodForAanalysis()
		self.__insertIdentificationUnitAnalysisMethods()
		
		# not needed, all IDs are set by insert into temptable
		#self.__updateIUAMTempTable()
		#self.__updateIUAMDicts()
		
		iuamparameters = []
		for iuam_dict in self.iuam_dicts:
			
			if 'Parameters' in iuam_dict:
				for iuamp_dict in iuam_dict['Parameters']:
					iuamp_dict['CollectionSpecimenID'] = iuam_dict['CollectionSpecimenID']
					iuamp_dict['IdentificationUnitID'] = iuam_dict['IdentificationUnitID']
					iuamp_dict['AnalysisID'] = iuam_dict['AnalysisID']
					iuamp_dict['AnalysisNumber'] = iuam_dict['AnalysisNumber']
					iuamp_dict['MethodID'] = iuam_dict['MethodID']
					iuamp_dict['MethodMarker'] = iuam_dict['MethodMarker']
					iuamparameters.append(iuamp_dict)
		
		iuamp_inserter = IdentificationUnitAnalysisMethodParameterInserter(self.dc_db)
		iuamp_inserter.setIdentificationUnitAnalysisMethodParameterDicts(iuamparameters)
		iuamp_inserter.insertIdentificationUnitAnalysisMethodParameterData()
		
		return


	def __createIdentificationUnitAnalysisMethodTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT NOT NULL,
		[IdentificationUnitID] INT NOT NULL,
		[AnalysisID] INT NOT NULL,
		[AnalysisNumber] NVARCHAR(50) COLLATE {1} NOT NULL,
		[MethodID] INT NOT NULL,
		[MethodMarker] NVARCHAR(50) COLLATE {1}, 
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [AnalysisID_idx] ([AnalysisID]),
		INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
		INDEX [MethodID_idx] ([MethodID]),
		INDEX [MethodMarker_idx] ([MethodMarker]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertMethodForAanalysis(self):
		
		query = """
		INSERT INTO [MethodForAnalysis] (
			MethodID,
			AnalysisID
		)
		SELECT DISTINCT iuam_temp.MethodID, iuam_temp.AnalysisID
		FROM [{0}] iuam_temp
		LEFT JOIN [MethodForAnalysis] mfa
		ON iuam_temp.MethodID = mfa.MethodID
			AND iuam_temp.AnalysisID = mfa.AnalysisID
		WHERE mfa.MethodID IS NULL
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertIdentificationUnitAnalysisMethods(self):
		
		query = """
		INSERT INTO [IdentificationUnitAnalysisMethod] 
		(
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[RowGUID]
		)
		SELECT 
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[RowGUID]
		FROM [{0}] iuam_temp
		ORDER BY iuam_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return

	# not needed, all IDs are set by insert into temptable
	'''
	def __updateIUAMTempTable(self):
		# update the IdentificationUnitAnalysisIDs in iu_temptable
		query = """
		UPDATE iuam_temp
		SET 
		iuam_temp.[CollectionSpecimenID] = iuam.[CollectionSpecimenID],
		iuam_temp.[IdentificationUnitID] = iuam.[IdentificationUnitID],
		iuam_temp.[AnalysisID] = iuam.[AnalysisID],
		iuam_temp.[AnalysisNumber] = iuam.[AnalysisNumber],
		iuam_temp.[MethodID] = iuam.[MethodID],
		iuam_temp.[MethodMarker] = iuam.[MethodMarker]
		FROM [{0}] iuam_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuam_temp.[RowGUID] = iuam.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateIUAMDicts(self):
		iuam_ids = self.getIDsForIUAMDicts()
		for iuam_dict in self.iuam_dicts:
			entry_num = iuam_dict['entry_num']
			iuam_dict['RowGUID'] = iuam_ids[entry_num]['RowGUID']
			iuam_dict['IdentificationUnitID'] = iuam_ids[entry_num]['IdentificationUnitID']
			iuam_dict['AnalysisID'] = iuam_ids[entry_num]['AnalysisID']
			iuam_dict['AnalysisNumber'] = iuam_ids[entry_num]['AnalysisNumber']
			iuam_dict['MethodID'] = iuam_ids[entry_num]['MethodID']
			iuam_dict['MethodMarker'] = iuam_ids[entry_num]['MethodMarker']
			
		return


	def getIDsForIUAMDicts(self):
		query = """
		SELECT 
			iuam_temp.[entry_num],
			iuam.CollectionSpecimenID,
			iuam.IdentificationUnitID,
			iuam.[AnalysisID],
			iuam.[AnalysisNumber],
			iuam.[MethodID],
			iuam.[MethodMarker],
			iuam.[RowGUID]
		FROM [IdentificationUnitAnalysisMethod] iuam
		INNER JOIN [{0}] iuam_temp
		ON iuam_temp.[RowGUID] = iuam.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		iuam_ids = {}
		for row in rows:
			if not row[0] in iuam_ids:
				iuam_ids[row[0]] = {}
			iuam_ids[row[0]]['CollectionSpecimenID'] = row[1]
			iuam_ids[row[0]]['IdentificationUnitID'] = row[2]
			iuam_ids[row[0]]['AnalysisID'] = row[3]
			iuam_ids[row[0]]['AnalysisNumber'] = row[4]
			iuam_ids[row[0]]['MethodID'] = row[5]
			iuam_ids[row[0]]['MethodMarker'] = row[6]
			iuam_ids[row[0]]['RowGUID'] = row[7]
		
		return iuam_ids
	'''
