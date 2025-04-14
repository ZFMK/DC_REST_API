import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class ParameterMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_parameters'


	def matchExistingMethods(self):
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
		[ParameterID] INT,
		[MethodID] INT NOT NULL,
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64) COLLATE {1},
		[ParameterURI] VARCHAR(255) COLLATE {1},
		[DefaultValue] NVARCHAR(MAX) COLLATE {1},
		[DefaultValue_sha] VARCHAR(64) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[parameter_sha] VARCHAR(64) COLLATE {1},
		INDEX [parameter_sha_idx] ([parameter_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Collections by DisplayText and MethodURI
		# parameter depends on MethodID, so it must be included here
		query = """
		INSERT INTO [{0}] (
			p.[MethodID],
			[DisplayText],
			[Description_sha],
			[ParameterURI],
			[DefaultValue_sha],
			[RowGUID]
		)
		SELECT
			p.[MethodID],
			p.[DisplayText],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', p.[Description]), 2) AS [Description_sha],
			p.[ParameterURI],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', p.[DefaultValue]), 2) AS [DefaultValue_sha],
			p.[RowGUID]
		FROM [Parameter] p
		INNER JOIN [{1}] p_temp
		ON ((p_temp.[DisplayText] = p.[DisplayText]) OR (p_temp.[DisplayText] IS NULL AND p.[DisplayText] IS NULL))
		AND ((p_temp.[ParameterURI] = p.[ParameterURI]) OR (p_temp.[ParameterURI] IS NULL AND p.[ParameterURI] IS NULL))
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __addSHAOnPrefiltered(self):
		# parameter depends on MethodID, so it must be included here
		query = """
		UPDATE pf
		SET [parameter_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[MethodID],
			[DisplayText],
			[Description_sha],
			[ParameterURI],
			[DefaultValue_sha]
		)), 2)
		FROM [{0}] pf
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE p_temp
		SET 
		p_temp.[ParameterID] = pf.[ParameterID],
		 -- MethodID must have been inserted into #parameters_temptable by ParameterInserter
		 -- p_temp.[MethodID] = pf.[MethodID],
		p_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] p_temp
		INNER JOIN [{1}] pf
		ON pf.[parameter_sha] = p_temp.[parameter_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
