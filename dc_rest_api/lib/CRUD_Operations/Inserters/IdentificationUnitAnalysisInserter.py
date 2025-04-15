import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

from dc_rest_api.lib.CRUD_Operations.Inserters.IdentificationUnitAnalysisMethodInserter import IdentificationUnitAnalysisMethodInserter

class IdentificationUnitAnalysisInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#iua_temptable'
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'IdentificationUnitID', 'None allowed': False},
			{'colname': 'SpecimenPartID'},
			{'colname': 'AnalysisID'},
			{'colname': 'AnalysisNumber'},
			{'colname': 'DatabaseURN'},
			{'colname': 'AnalysisInstanceNotes'},
			{'colname': 'ExternalAnalysisURI'},
			{'colname': 'ResponsibleName'},
			{'colname': 'ResponsibleAgentURI'},
			{'colname': 'AnalysisDate'},
			{'colname': 'AnalysisResult'},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def setIdentificationUnitAnalysisDicts(self, json_dicts = []):
		self.iua_dicts = []
		iua_count = 1
		for iua_dict in json_dicts:
			iua_dict['entry_num'] = iua_count
			iua_count += 1
			self.iua_dicts.append(iua_dict)
		return


	def insertIdentificationUnitAnalysisData(self):
		self.__createIdentificationUnitAnalysisTempTable()
		
		self.json2temp.set_datadicts(self.iua_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentificationUnitAnalyses()
		self.__updateIUATempTable()
		self.__updateIUADicts()
		
		iuamethods = []
		for iua_dict in self.iua_dicts:
			# The key 'Methods' is used in json file instead of 'IdentificationUnitAnalysisMethods' because then Methods can be linked
			# from different subtables i. e. IUA, CollectionEventMethod, CollectionSpecimenProcessingMethod
			
			# if 'IdentificationUnitAnalysisMethods' in iua_dict:
			#	for iuam_dict in iuam_dict['IdentificationUnitAnalysisMethods']:
			
			if 'Methods' in iua_dict:
				for iuam_dict in iua_dict['Methods']:
					iuam_dict['CollectionSpecimenID'] = iua_dict['CollectionSpecimenID']
					iuam_dict['IdentificationUnitID'] = iua_dict['IdentificationUnitID']
					iuam_dict['AnalysisID'] = iua_dict['AnalysisID']
					iuam_dict['AnalysisNumber'] = iua_dict['AnalysisNumber']
					iuamethods.append(iuam_dict)
		
		iuam_inserter = IdentificationUnitAnalysisMethodInserter(self.dc_db)
		iuam_inserter.setIdentificationUnitAnalysisMethodDicts(iuamethods)
		iuam_inserter.insertIdentificationUnitAnalysisMethodData()
		
		return


	def __createIdentificationUnitAnalysisTempTable(self):
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
		[AnalysisNumber] NVARCHAR(50),
		[SpecimenPartID] INT DEFAULT NULL,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[AnalysisInstanceNotes] NVARCHAR(MAX) COLLATE {1},
		[ExternalAnalysisURI] VARCHAR(255) COLLATE {1},
		[ResponsibleName] NVARCHAR(255) COLLATE {1},
		[ResponsibleAgentURI] VARCHAR(255) COLLATE {1},
		[AnalysisDate] NVARCHAR(50) COLLATE {1},
		[AnalysisResult] NVARCHAR(MAX) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		PRIMARY KEY ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
		INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
		INDEX [AnalysisID_idx] ([AnalysisID]),
		INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertIdentificationUnitAnalyses(self):
		
		query = """
		INSERT INTO [IdentificationUnitAnalysis] 
		(
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[SpecimenPartID],
			[RowGUID],
			[Notes],
			[ExternalAnalysisURI],
			[ResponsibleName],
			[ResponsibleAgentURI],
			[AnalysisDate],
			[AnalysisResult]
		)
		SELECT 
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			ISNULL([AnalysisNumber], ROW_NUMBER() OVER(PARTITION BY [CollectionSpecimenID], [IdentificationUnitID], [AnalysisID] ORDER BY [entry_num] ASC)) AS [AnalysisNumber],
			[SpecimenPartID],
			[RowGUID],
			[AnalysisInstanceNotes],
			[ExternalAnalysisURI],
			[ResponsibleName],
			[ResponsibleAgentURI],
			[AnalysisDate],
			[AnalysisResult]
		FROM [{0}] iua_temp
		ORDER BY iua_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateIUATempTable(self):
		# update the IdentificationUnitAnalysisIDs in iu_temptable
		query = """
		UPDATE iua_temp
		SET 
		iua_temp.[CollectionSpecimenID] = iua.[CollectionSpecimenID],
		iua_temp.[IdentificationUnitID] = iua.[IdentificationUnitID],
		iua_temp.[AnalysisID] = iua.[AnalysisID],
		iua_temp.[AnalysisNumber] = iua.[AnalysisNumber],
		iua_temp.[SpecimenPartID] = iua.[SpecimenPartID]
		FROM [{0}] iua_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON iua_temp.[RowGUID] = iua.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateIUADicts(self):
		iua_ids = self.getIDsForIUADicts()
		for iua_dict in self.iua_dicts:
			entry_num = iua_dict['entry_num']
			iua_dict['RowGUID'] = iua_ids[entry_num]['RowGUID']
			iua_dict['IdentificationUnitID'] = iua_ids[entry_num]['IdentificationUnitID']
			iua_dict['AnalysisID'] = iua_ids[entry_num]['AnalysisID']
			iua_dict['AnalysisNumber'] = iua_ids[entry_num]['AnalysisNumber']
			iua_dict['SpecimenPartID'] = iua_ids[entry_num]['SpecimenPartID']
			
		return


	def getIDsForIUADicts(self):
		query = """
		SELECT iua_temp.[entry_num], iua.CollectionSpecimenID, iua.IdentificationUnitID, iua.[AnalysisID], iua.[AnalysisNumber], iua.[SpecimenPartID], iua.[RowGUID]
		FROM [IdentificationUnitAnalysis] iua
		INNER JOIN [{0}] iua_temp
		ON iua_temp.[RowGUID] = iua.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		iua_ids = {}
		for row in rows:
			if not row[0] in iua_ids:
				iua_ids[row[0]] = {}
			iua_ids[row[0]]['CollectionSpecimenID'] = row[1]
			iua_ids[row[0]]['IdentificationUnitID'] = row[2]
			iua_ids[row[0]]['AnalysisID'] = row[3]
			iua_ids[row[0]]['AnalysisNumber'] = row[4]
			iua_ids[row[0]]['SpecimenPartID'] = row[5]
			iua_ids[row[0]]['RowGUID'] = row[6]
		
		return iua_ids
