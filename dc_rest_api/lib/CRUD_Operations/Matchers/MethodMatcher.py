import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class MethodMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_methods'


	def matchExistingMethods(self):
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		
		self.addMethodSHA(self.prefiltered_temptable)
		
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
		[MethodID] INT,
		[MethodParentID] INT,
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64) COLLATE {1},
		[MethodURI] VARCHAR(255) COLLATE {1},
		[OnlyHierarchy] BIT,
		[ForCollectionEvent] BIT,
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[method_sha] VARCHAR(64) COLLATE {1},
		INDEX [method_sha_idx] ([method_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Collections by DisplayText and MethodURI
		
		query = """
		INSERT INTO [{0}] (
			[MethodID],
			[DisplayText],
			[Description_sha],
			[MethodURI],
			[OnlyHierarchy],
			[ForCollectionEvent],
			[RowGUID]
		)
		SELECT
			m.[MethodID],
			m.[DisplayText],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', m.[Description]), 2) AS [Description_sha],
			m.[MethodURI],
			m.[OnlyHierarchy],
			m.[ForCollectionEvent],
			m.[RowGUID]
		FROM [Method] m
		INNER JOIN [{1}] m_temp
		ON ((m_temp.[DisplayText] = m.[DisplayText]) OR (m_temp.[DisplayText] IS NULL AND m.[DisplayText] IS NULL))
		AND ((m_temp.[MethodURI] = m.[MethodURI]) OR (m_temp.[MethodURI] IS NULL AND m.[MethodURI] IS NULL))
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def addMethodSHA(self, tablename):
		query = """
		UPDATE t
		SET [method_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description_sha],
			[MethodURI],
			[OnlyHierarchy],
			[ForCollectionEvent]
		)), 2)
		FROM [{0}] t
		;""".format(tablename)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE m_temp
		SET m_temp.[MethodID] = pf.[MethodID],
		m_temp.[MethodParentID] = pf.[MethodParentID],
		m_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] m_temp
		INNER JOIN [{1}] pf
		ON pf.[method_sha] = m_temp.[method_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
