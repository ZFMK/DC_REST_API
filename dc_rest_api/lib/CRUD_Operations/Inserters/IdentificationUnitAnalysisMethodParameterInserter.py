import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

class IdentificationUnitAnalysisMethodParameterInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iuamp_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'AnalysisID', 'None allowed': False},
			{'colname': 'AnalysisNumber', 'None allowed': False},
			{'colname': 'MethodID', 'None allowed': False},
			{'colname': 'MethodMarker', 'None allowed': False},
			{'colname': 'ParameterID', 'None allowed': False},
			{'colname': 'Value'},
			{'colname': 'DatabaseURN'}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def setIdentificationUnitAnalysisMethodParameterDicts(self, json_dicts = []):
		self.iuamp_dicts = []
		iuamp_count = 1
		for iuamp_dict in json_dicts:
			iuamp_dict['entry_num'] = iuamp_count
			iuamp_count += 1
			self.iuamp_dicts.append(iuamp_dict)
		return


	def insertIdentificationUnitAnalysisMethodParameterData(self):
		
		self.__createIdentificationUnitAnalysisMethodParameterTempTable()
		
		self.json2temp.set_datadicts(self.iuamp_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentificationUnitAnalysisMethodParameters()
		
		self.__updateIUAMPTempTable()
		self.__updateIUAMPDicts()
		return


	def __createIdentificationUnitAnalysisMethodParameterTempTable(self):
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
		[MethodMarker] NVARCHAR(50) COLLATE {1} NOT NULL,
		[ParameterID] INT NOT NULL,
		[Value] NVARCHAR(MAX) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [AnalysisID_idx] ([AnalysisID]),
		INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
		INDEX [MethodID_idx] ([MethodID]),
		INDEX [MethodMarker_idx] ([MethodMarker]),
		INDEX [ParameterID_idx] ([ParameterID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertIdentificationUnitAnalysisMethodParameters(self):
		
		query = """
		INSERT INTO [IdentificationUnitAnalysisMethodParameter] 
		(
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[ParameterID],
			[Value],
			[RowGUID]
		)
		SELECT 
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[ParameterID],
			[Value],
			[RowGUID]
		FROM [{0}] iuamp_temp
		ORDER BY iuamp_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateIUAMPTempTable(self):
		
		query = """
		UPDATE iuamp_temp
		SET 
		iuamp_temp.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID],
		iuamp_temp.[IdentificationUnitID] = iuamp.[IdentificationUnitID],
		iuamp_temp.[AnalysisID] = iuamp.[AnalysisID],
		iuamp_temp.[AnalysisNumber] = iuamp.[AnalysisNumber],
		iuamp_temp.[MethodID] = iuamp.[MethodID],
		iuamp_temp.[MethodMarker] = iuamp.[MethodMarker],
		iuamp_temp.[ParameterID] = iuamp.[ParameterID]
		FROM [{0}] iuamp_temp
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
		ON iuamp_temp.[RowGUID] = iuamp.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateIUAMPDicts(self):
		iuamp_ids = self.getIDsForIUAMPDicts()
		for iuamp_dict in self.iuamp_dicts:
			entry_num = iuamp_dict['entry_num']
			iuamp_dict['RowGUID'] = iuamp_ids[entry_num]['RowGUID']
			iuamp_dict['IdentificationUnitID'] = iuamp_ids[entry_num]['IdentificationUnitID']
			iuamp_dict['AnalysisID'] = iuamp_ids[entry_num]['AnalysisID']
			iuamp_dict['AnalysisNumber'] = iuamp_ids[entry_num]['AnalysisNumber']
			iuamp_dict['MethodID'] = iuamp_ids[entry_num]['MethodID']
			iuamp_dict['MethodMarker'] = iuamp_ids[entry_num]['MethodMarker']
			iuamp_dict['ParameterID'] = iuamp_ids[entry_num]['ParameterID']
			
		return


	def getIDsForIUAMPDicts(self):
		query = """
		SELECT 
			iuamp_temp.[entry_num],
			iuamp.CollectionSpecimenID,
			iuamp.IdentificationUnitID,
			iuamp.[AnalysisID],
			iuamp.[AnalysisNumber],
			iuamp.[MethodID],
			iuamp.[MethodMarker],
			iuamp.[ParameterID],
			iuamp.[RowGUID]
		FROM [IdentificationUnitAnalysisMethodParameter] iuamp
		INNER JOIN [{0}] iuamp_temp
		ON iuamp_temp.[RowGUID] = iuamp.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		iuamp_ids = {}
		for row in rows:
			if not row[0] in iuamp_ids:
				iuamp_ids[row[0]] = {}
			iuamp_ids[row[0]]['CollectionSpecimenID'] = row[1]
			iuamp_ids[row[0]]['IdentificationUnitID'] = row[2]
			iuamp_ids[row[0]]['AnalysisID'] = row[3]
			iuamp_ids[row[0]]['AnalysisNumber'] = row[4]
			iuamp_ids[row[0]]['MethodID'] = row[5]
			iuamp_ids[row[0]]['MethodMarker'] = row[6]
			iuamp_ids[row[0]]['ParameterID'] = row[7]
			iuamp_ids[row[0]]['RowGUID'] = row[8]
		
		return iuamp_ids

