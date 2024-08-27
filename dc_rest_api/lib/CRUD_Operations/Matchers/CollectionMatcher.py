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
		
		self.prefiltered_temptable = 'prefiltered_collections'


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
		[CollectionID] INT,
		[CollectionName] NVARCHAR(255) COLLATE {1},
		[CollectionAcronym] NVARCHAR(10) COLLATE {1},
		[AdministrativeContactName] NVARCHAR(500) COLLATE {1},
		[AdministrativeContactAgentURI] VARCHAR(255) COLLATE {1},
		[Description_sha] VARCHAR(64),
		[Location] NVARCHAR(255) COLLATE {1},
		[LocationParentID] INT,
		[LocationPlan] VARCHAR(500) COLLATE {1},
		[LocationPlanWidth] FLOAT,
		[LocationPlanDate] DATETIME,
		[LocationGeometry] GEOMETRY,
		[LocationHeight] FLOAT,
		[CollectionOwner] NVARCHAR(255) COLLATE {1},
		[Type] NVARCHAR(50) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[collection_sha] VARCHAR(64) COLLATE {1},
		INDEX [collection_sha_idx] ([collection_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Collections by CollectionName and CollectionAcronym
		
		query = """
		INSERT INTO [{0}] (
			[CollectionID],
			[CollectionName],
			[CollectionAcronym],
			[AdministrativeContactName],
			[AdministrativeContactAgentURI],
			[Description_sha],
			[Location],
			[LocationParentID],
			[LocationPlan],
			[LocationPlanWidth],
			[LocationPlanDate],
			[LocationGeometry],
			[LocationHeight],
			[CollectionOwner],
			[RowGUID]
		)
		SELECT 
			c.[CollectionID],
			c.[CollectionName],
			c.[CollectionAcronym],
			c.[AdministrativeContactName],
			c.[AdministrativeContactAgentURI],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', c.[Description]), 2) AS [Description_sha],
			c.[Location],
			c.[LocationParentID],
			c.[LocationPlan],
			c.[LocationPlanWidth],
			c.[LocationPlanDate],
			c.[LocationGeometry],
			c.[LocationHeight],
			c.[CollectionOwner],
			c.[RowGUID]
		FROM [Collection] c
		INNER JOIN [{1}] c_temp
		ON ((c_temp.[CollectionName] = c.[CollectionName]) OR (c_temp.[CollectionName] IS NULL AND c.[CollectionName] IS NULL))
		AND ((c_temp.[CollectionAcronym] = c.[CollectionAcronym]) OR (c_temp.[CollectionAcronym] IS NULL AND c.[CollectionAcronym] IS NULL))
		 -- WHERE c_temp.[CollectionID] IS NULL
		;""".format(self.prefiltered_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __addSHAOnPrefiltered(self):
		query = """
		UPDATE pf
		SET [collection_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[CollectionName],
			[CollectionAcronym],
			[AdministrativeContactName],
			[AdministrativeContactAgentURI],
			[Description_sha],
			[Location],
			[LocationParentID],
			[LocationPlan],
			[LocationPlanWidth],
			[LocationPlanDate],
			[LocationGeometry].STAsText(),
			[LocationHeight],
			[CollectionOwner]
		)), 2)
		FROM [{0}] pf
		;""".format(self.prefiltered_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __matchPrefilteredToTempTable(self):
		query = """
		UPDATE c_temp
		SET c_temp.[CollectionID] = pf.[CollectionID],
		c_temp.[RowGUID] = pf.[RowGUID]
		FROM [{0}] c_temp
		INNER JOIN [{1}] pf
		ON pf.[collection_sha] = c_temp.[collection_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
