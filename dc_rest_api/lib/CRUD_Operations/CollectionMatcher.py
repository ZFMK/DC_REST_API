import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class CollectionEventMatcher():
	def __init__(self, dc_db, temptable):
		self.dc_db = dc_db
		self.temptable = temptable
		
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.prefiltered_temptable = '#prefiltered_collections'


	def matchExistingCollections(self):
		
		self.__createPrefilteredTempTable()
		self.__matchIntoPrefiltered()
		self.__addSHAOnPrefiltered()
		
		self.__matchPrefilteredToTempTable()


	def __createPrefilteredTempTable(self):
		# create a temptable that contains all events that
		# match in CollectionEvent table columns
		# use this prefiltered table to check the matching of EventLocalisations
		
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
		[LocationPlanWidth] VARCHAR(500) FLOAT,
		[LocationPlanDate] DATETIME,
		[LocationGeometry] GEOMETRY,
		[LocationHeight] FLOAT,
		[CollectionOwner] NVARCHAR(255) COLLATE {1},
		[Type] NVARCHAR(50) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER NOT NULL,
		 -- 
		[collection_sha] VARCHAR(64),
		INDEX [collection_sha_idx] ([collection_sha])
		)
		;""".format(self.prefiltered_temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __matchIntoPrefiltered(self):
		# first match all existing Collections by CollectionName and CollectionAccronym
		
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
			[CollectionOwner]
		)
		SELECT 
			[CollectionID],
			[CollectionName],
			[CollectionAcronym],
			[AdministrativeContactName],
			[AdministrativeContactAgentURI],
			CONVERT(VARCHAR(64), HASHBYTES('sha2_256', [Description]), 2) AS [Description_sha],
			[Location],
			[LocationParentID],
			[LocationPlan],
			[LocationPlanWidth],
			[LocationPlanDate],
			[LocationGeometry],
			[LocationHeight],
			[CollectionOwner]
		FROM [Collection] c
		INNER JOIN [{1}] c_temp
		ON ((c_temp.[CollectionName] = c.[CollectionName]) OR (c_temp.[CollectionName] IS NULL AND c.[CollectionName] IS NULL))
		AND ((c_temp.[CollectionAccronym] = c.[CollectionAccronym]) OR (c_temp.[CollectionAccronym] IS NULL AND c.[CollectionAccronym] IS NULL))
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
			 -- [LocationPlanWidth],
			 -- [LocationPlanDate],
			 -- [LocationGeometry],
			 -- [LocationHeight],
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
		ON pf.[collection_sha] = ce_temp.[collection_sha]
		;""".format(self.temptable, self.prefiltered_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
