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
		
		self.prefiltered_temptable = '#prefiltered_methods'


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
		[MethodID] INT,
		[MethodParentID] INT,
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64),
		[MethodURI] VARCHAR(50) COLLATE {1},
		[MethodTypeNotes] NVARCHAR(MAX) COLLATE {1},
		[OnlyHierarchy] BIT,
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
		# first match all existing Collections by DiplayText and MethodURI
		
		query = """
		INSERT INTO [{0}] (
			[DisplayText],
			[Description_sha],
			[MethodURI],
			[MethodTypeNotes],
			[OnlyHierarchy]
		)
		SELECT
			m.[DisplayText],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', m.[Description_sha]), 2) AS [Description_sha],
			m.[MethodURI],
			m.[MethodTypeNotes],
			m.[OnlyHierarchy]
		FROM [Method] a
		INNER JOIN [{1}] m_temp
		ON ((m_temp.[DiplayText] = m.[DiplayText]) OR (m_temp.[DiplayText] IS NULL AND m.[DiplayText] IS NULL))
		AND ((m_temp.[MethodURI] = m.[MethodURI]) OR (m_temp.[MethodURI] IS NULL AND m.[MethodURI] IS NULL))
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __addSHAOnPrefiltered(self):
		query = """
		UPDATE pf
		SET [method_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description],
			[Description_sha],
			[MethodURI],
			[MethodTypeNotes],
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
