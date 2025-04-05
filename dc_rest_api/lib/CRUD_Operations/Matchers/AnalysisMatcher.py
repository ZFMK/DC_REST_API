import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class CollectionMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_analyses'


	def matchExistingCollections(self):
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		self.__addSHAOnPrefiltered()
		
		self.__matchPrefilteredToTempTable()


	def __createPrefilteredTempTable(self):
		
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[AnalysisID] INT,
		[AnalysisParentID] INT,
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description_sha] VARCHAR(64),
		[AnalysisURI] VARCHAR(50) COLLATE {1},
		[MeasurementUnit] NVARCHAR(50) COLLATE {1},
		[OnlyHierarchy] BIT,
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[analysis_sha] VARCHAR(64) COLLATE {1},
		INDEX [analysis_sha_idx] ([analysis_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Collections by DiplayText and AnalysisURI
		
		query = """
		INSERT INTO [{0}] (
			[DisplayText],
			[Description_sha],
			[AnalysisURI],
			[MeasurementUnit],
			[OnlyHierarchy]
		)
		SELECT
			a.[DisplayText],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', a.[Description_sha]), 2) AS [Description_sha],
			a.[AnalysisURI],
			a.[MeasurementUnit],
			a.[OnlyHierarchy]
		FROM [Analysis] a
		INNER JOIN [{1}] a_temp
		ON ((a_temp.[DiplayText] = a.[DiplayText]) OR (a_temp.[DiplayText] IS NULL AND a.[DiplayText] IS NULL))
		AND ((a_temp.[AnalysisURI] = a.[AnalysisURI]) OR (a_temp.[AnalysisURI] IS NULL AND a.[AnalysisURI] IS NULL))
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __addSHAOnPrefiltered(self):
		query = """
		UPDATE pf
		SET [analysis_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description_sha],
			[AnalysisURI],
			[MeasurementUnit],
			[OnlyHierarchy]
		)), 2)
		FROM [{0}] pf
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE a_temp
		SET a_temp.[AnalysisID] = pf.[AnalysisID],
		a_temp.[AnalysisParentID] = pf.[AnalysisParentID],
		a_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] a_temp
		INNER JOIN [{1}] pf
		ON pf.[analysis_sha] = a_temp.[analysis_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
