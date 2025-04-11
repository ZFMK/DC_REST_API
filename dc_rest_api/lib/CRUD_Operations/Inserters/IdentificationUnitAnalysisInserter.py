import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

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
			{'colname': 'AnalysisNumber', 'None allowed': False},
			{'colname': 'DatabaseURN'},
			{'colname': 'AnalysisInstanceNotes'},
			{'colname': 'ExternalAnalysisURI'},
			{'colname': 'ResponsibleName'},
			{'colname': 'ResponsibleNameURI'},
			{'colname': 'AnalysisDate'},
			{'colname': 'AnalysisResult'},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertIdentificationUnitAnalysisData(self):
		self.__createIdentificationUnitAnalysisTempTable()
		
		self.json2temp.set_datadicts(self.iua_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertIdentificationUnitAnalyses()
		self.__updateIUATempTable()
		
		self.__updateIUADicts()
		
		
		
		'''
		identifications = []
		for iu_dict in self.iu_dicts:
			if 'Identifications' in iu_dict:
				for i_dict in iu_dict['Identifications']:
					i_dict['CollectionSpecimenID'] = iu_dict['CollectionSpecimenID']
					i_dict['IdentificationUnitID'] = iu_dict['IdentificationUnitID']
					identifications.append(i_dict)
		
		i_inserter = IdentificationInserter(self.dc_db)
		i_inserter.setIdentificationDicts(identifications)
		i_inserter.insertIdentificationData()
		'''
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
		[AnalysisiNumber] INT NOT NULL,
		[SpecimenPartID] INT DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[AnalysisInstanceNotes] NVARCHAR(MAX) COLLATE {1},
		[ExternalAnalysisURI] VARCHAR(255) COLLATE {1},
		[ResponsibleName] NVARCHAR(255) COLLATE {1},
		[ResponsibleAgentURI] VARCHAR(255) COLLATE {1},
		[AnalysisDate] NVARCHAR(50) COLLATE {1},
		[AnalysisResult] NVARCHAR(MAX) COLLATE {1},
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
			[AnalysisiNumber],
			[SpecimenPartID],
			[RowGUID],
			[AnalysisInstanceNotes],
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
			[AnalysisiNumber],
			[SpecimenPartID],
			[RowGUID],
			[Notes],
			[ExternalAnalysisURI],
			[ResponsibleName],
			[ResponsibleAgentURI],
			[AnalysisDate],
			[AnalysisResult],
			
		FROM [{0}] iu_temp
		ORDER BY iu_temp.[entry_num]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
