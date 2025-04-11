import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.AnalysisMatcher import AnalysisMatcher
#from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

class AnalysisInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		self.temptable = '#analysis_temptable'
		self.unique_analyses_temptable = '#unique_a_temptable'
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			{'colname': 'DatabaseURN'},
			{'colname': 'DisplayText'},
			{'colname': 'Description'},
			{'colname': 'Notes'},
			{'colname': 'MeasurementUnit'},
			{'colname': 'AnalysisURI'},
			{'colname': 'OnlyHierarchy', 'default': 0},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertAnalysisData(self, json_dicts = []):
		self.a_dicts = json_dicts
		
		self.__createAnalysisTempTable()
		
		self.json2temp.set_datadicts(self.a_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addAnalysisSHA()
		
		self.analysis_matcher = AnalysisMatcher(self.dc_db, self.temptable)
		self.analysis_matcher.matchExistingAnalyses()
		
		self.createNewAnalyses()
		
		self.__updateAnalysisDicts()
		
		return


	def __createAnalysisTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[@id] VARCHAR(100) COLLATE {1} NOT NULL,
		[AnalysisID] INT,
		[AnalysisParentID] INT,
		[RowGUID] UNIQUEIDENTIFIER,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[MeasurementUnit] NVARCHAR(50) COLLATE {1},
		[AnalysisURI] VARCHAR(255) COLLATE {1},
		[OnlyHierarchy] BIT,
		[analysis_sha] VARCHAR(64) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [Analysis_idx] ([AnalysisID]),
		INDEX [AnalysisParent_idx] ([AnalysisParentID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addAnalysisSHA(self):
		query = """
		UPDATE a_temp
		SET [analysis_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description],
			[Description_sha],
			[AnalysisURI],
			[MeasurementUnit],
			[OnlyHierarchy]
		)), 2)
		FROM [{0}] a_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def createNewAnalyses(self):
		# insert only one version of each event when the same event occurres multiple times in json data
		self.__setUniqueAnalysesTempTable()
		self.__insertNewAnalyses()
		
		self.__updateAnalysisIDsInTempTable()
		return


	def __setUniqueAnalysesTempTable(self):
		"""
		create a table that contains only one version of each analysis to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_analyses_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[AnalysisID] INT,
			[AnalysisParentID] INT,
			[DisplayText] NVARCHAR(50) COLLATE {1},
			[Description] NVARCHAR(MAX) COLLATE {1},
			[Notes] NVARCHAR(MAX) COLLATE {1},
			[MeasurementUnit] NVARCHAR(50) COLLATE {1},
			[AnalysisURI] VARCHAR(255) COLLATE {1},
			[OnlyHierarchy] BIT,
			 -- 
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			[analysis_sha] VARCHAR(64) COLLATE {1},
			INDEX [analysis_sha_idx] ([analysis_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_analyses_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[analysis_sha]
		)
		SELECT DISTINCT
			[analysis_sha]
		FROM [{1}] a_temp
		WHERE a_temp.[AnalysisID] IS NULL
		;""".format(self.unique_analyses_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET 
			[DisplayText] = a_temp.[DisplayText],
			[Description] = a_temp.[Description],
			[Notes] = a_temp.[Notes],
			[MeasurementUnit] = a_temp.[MeasurementUnit],
			[AnalysisURI] = a_temp.[AnalysisURI],
			[OnlyHierarchy] = a_temp.[OnlyHierarchy]
		FROM [{0}] ue_temp
		INNER JOIN [{1}] a_temp
		ON ue_temp.[analysis_sha] = a_temp.[analysis_sha]
		;""".format(self.unique_analyses_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertNewAnalyses(self):
		query = """
		INSERT INTO [Analysis] (
			[DisplayText],
			[Description],
			[Notes],
			[MeasurementUnit],
			[AnalysisURI],
			[OnlyHierarchy],
			[RowGUID]
		)
		SELECT
			ue_temp.[DisplayText],
			ue_temp.[Description],
			ue_temp.[Notes],
			ue_temp.[MeasurementUnit],
			ue_temp.[AnalysisURI],
			ue_temp.[OnlyHierarchy],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_analyses_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# TODO: what is the reason for updating the ids in ue_temp here?
		query = """
		UPDATE ue_temp
		SET ue_temp.[AnalysisID] = a.[AnalysisID],
		ue_temp.[AnalysisParentID] = a.[AnalysisParentID]
		FROM [{0}] ue_temp
		INNER JOIN [Analysis] a
		ON a.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_analyses_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateAnalysisIDsInTempTable(self):
		query = """
		UPDATE a_temp
		SET a_temp.[AnalysisID] = a.[AnalysisID],
		a_temp.[AnalysisParentID] = a.[AnalysisParentID],
		a_temp.[RowGUID] = a.[RowGUID]
		FROM [{0}] a_temp
		INNER JOIN [{1}] ue_temp
		ON a_temp.[analysis_sha] = ue_temp.[analysis_sha]
		INNER JOIN [Analysis] a
		ON ue_temp.[RowGUID] = a.[RowGUID]
		;""".format(self.temptable, self.unique_analyses_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateAnalysisDicts(self):
		a_ids = self.getIDsForAnalysisDicts()
		for dict_id in self.a_dicts:
			a_dict = self.a_dicts[dict_id]
			a_dict['AnalysisID'] = a_ids[dict_id]['AnalysisID']
			a_dict['AnalysisParentID'] = a_ids[dict_id]['AnalysisParentID']
			a_dict['RowGUID'] = a_ids[dict_id]['RowGUID']
		return


	def getIDsForAnalysisDicts(self):
		query = """
		SELECT 
			a_temp.[@id],
			a.[AnalysisID],
			a.[AnalysisParentID],
			a.[RowGUID]
		FROM [Analysis] a
		INNER JOIN [{0}] a_temp
		ON a_temp.[RowGUID] = a.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		a_ids = {}
		for row in rows:
			if not row[0] in a_ids:
				a_ids[row[0]] = {}
			a_ids[row[0]]['AnalysisID'] = row[1]
			a_ids[row[0]]['AnalysisParentID'] = row[1]
			a_ids[row[0]]['RowGUID'] = row[2]
		
		return a_ids
