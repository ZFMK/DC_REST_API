import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.CollectionMatcher import CollectionMatcher



class CollectionInserter():
	def __init__(self, dc_db, users_roles = []):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#collection_temptable'
		self.unique_collections_temptable = '#unique_c_temptable'
		
		self.users_roles = users_roles
		
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			# do not add CollectionID as it should be set by comparison
			#{'colname': 'CollectionID'},
			#{'colname': 'CollectionSpecimenID'},
			#{'colname': 'SpecimenPartID'},
			{'colname': 'DatabaseURN'},
			{'colname': 'CollectionName', 'default': 'No collection', 'None allowed': False},
			{'colname': 'CollectionAcronym'},
			{'colname': 'AdministrativeContactName'},
			{'colname': 'AdministrativeContactAgentURI'},
			{'colname': 'Description'},
			{'colname': 'Description_sha', 'compute sha of': 'Description'},
			{'colname': 'CollectionOwner'},
			{'colname': 'Type'},
			{'colname': 'Location'},
			{'colname': 'LocationPlan'},
			{'colname': 'LocationPlanWidth'},
			{'colname': 'LocationPlanDate'},
			{'colname': 'LocationGeometry'},
			{'colname': 'LocationHeight'}
		]
		
		self.json2temp = JSON2TempTable(self.dc_db, self.schema)
		self.messages = []


	def insertCollectionData(self, json_dicts = []):
		
		self.c_dicts = json_dicts
		
		self.__createCollectionTempTable()
		
		self.json2temp.set_datadicts(self.c_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addCollectionSHA()
		
		self.collection_matcher = CollectionMatcher(self.dc_db, self.temptable)
		self.collection_matcher.matchExistingCollections()
		
		self.createNewCollections()
		
		#self.__insertCollectionIDsInCollectionSpecimen()
		#self.__insertCollectionIDsInSpecimenPart()
		
		self.__updateCollectionDicts()
		return


	def __createCollectionTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[@id] VARCHAR(100) COLLATE {1} NOT NULL,
		[CollectionID] INT,
		[CollectionSpecimenID] INT,
		[SpecimenPartID] INT,
		[RowGUID] UNIQUEIDENTIFIER,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[CollectionName] NVARCHAR(255) COLLATE {1},
		[CollectionAcronym] NVARCHAR(10) COLLATE {1},
		[AdministrativeContactName] NVARCHAR(500) COLLATE {1},
		[AdministrativeContactAgentURI] VARCHAR(255) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64),
		[Location] NVARCHAR(255) COLLATE {1},
		[LocationParentID] INT,
		[LocationPlan] VARCHAR(500) COLLATE {1},
		[LocationPlanWidth] FLOAT,
		[LocationPlanDate] DATETIME,
		[LocationGeometry] GEOMETRY,
		[LocationHeight] FLOAT,
		[CollectionOwner] NVARCHAR(255) COLLATE {1},
		[DisplayOrder] SMALLINT,
		[Type] NVARCHAR(50) COLLATE {1},
		[collection_sha] VARCHAR(64) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [CollectionID_idx] (CollectionID),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
		INDEX [RowGUID_idx] (RowGUID)
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addCollectionSHA(self):
		query = """
		UPDATE c_temp
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
		FROM [{0}] c_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def createNewCollections(self):
		# insert only one version of each event when the same event occurres multiple times in json data
		self.__setUniqueCollectionsTempTable()
		self.__insertNewCollections()
		
		self.__updateCollectionIDsInTempTable()
		return



	def __setUniqueCollectionsTempTable(self):
		"""
		create a table that contains only one version of each collection to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_collections_temptable)
		
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
			[Description] NVARCHAR(MAX) COLLATE {1},
			[Location] NVARCHAR(255) COLLATE {1},
			[LocationParentID] INT,
			[LocationPlan] VARCHAR(500) COLLATE {1},
			[LocationPlanWidth] FLOAT,
			[LocationPlanDate] DATETIME,
			[LocationGeometry] GEOMETRY,
			[LocationHeight] FLOAT,
			[CollectionOwner] NVARCHAR(255) COLLATE {1},
			[Type] NVARCHAR(50) COLLATE {1},
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			 -- 
			[collection_sha] VARCHAR(64) COLLATE {1},
			INDEX [collection_sha_idx] ([collection_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_collections_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[collection_sha]
		)
		SELECT DISTINCT
			[collection_sha]
		FROM [{1}] c_temp
		WHERE c_temp.[CollectionID] IS NULL
		;""".format(self.unique_collections_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET 
			[CollectionName] = c_temp.[CollectionName],
			[CollectionAcronym] = c_temp.[CollectionAcronym],
			[AdministrativeContactName] = c_temp.[AdministrativeContactName],
			[AdministrativeContactAgentURI] = c_temp.[AdministrativeContactAgentURI],
			[Description] = c_temp.[Description],
			[Location] = c_temp.[Location],
			[LocationParentID] = c_temp.[LocationParentID],
			[LocationPlan] = c_temp.[LocationPlan],
			[LocationPlanWidth] = c_temp.[LocationPlanWidth],
			[LocationPlanDate] = c_temp.[LocationPlanDate],
			[LocationHeight] = c_temp.[LocationHeight],
			[CollectionOwner] = c_temp.[CollectionOwner]
		FROM [{0}] ue_temp
		INNER JOIN [{1}] c_temp
		ON ue_temp.[collection_sha] = c_temp.[collection_sha]
		;""".format(self.unique_collections_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertNewCollections(self):
		query = """
		INSERT INTO [Collection] (
			[CollectionName],
			[CollectionAcronym],
			[AdministrativeContactName],
			[AdministrativeContactAgentURI],
			[Description],
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
			ue_temp.[CollectionName],
			ue_temp.[CollectionAcronym],
			ue_temp.[AdministrativeContactName],
			ue_temp.[AdministrativeContactAgentURI],
			ue_temp.[Description],
			ue_temp.[Location],
			ue_temp.[LocationParentID],
			ue_temp.[LocationPlan],
			ue_temp.[LocationPlanWidth],
			ue_temp.[LocationPlanDate],
			ue_temp.[LocationGeometry],
			ue_temp.[LocationHeight],
			ue_temp.[CollectionOwner],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_collections_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET ue_temp.[CollectionID] = c.[CollectionID]
		FROM [{0}] ue_temp
		INNER JOIN [Collection] c
		ON c.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_collections_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateCollectionIDsInTempTable(self):
		query = """
		UPDATE c_temp
		SET c_temp.[CollectionID] = c.[CollectionID],
		c_temp.[RowGUID] = c.[RowGUID]
		FROM [{0}] c_temp
		INNER JOIN [{1}] ue_temp
		ON c_temp.[collection_sha] = ue_temp.[collection_sha]
		INNER JOIN [Collection] c
		ON ue_temp.[RowGUID] = c.[RowGUID]
		;""".format(self.temptable, self.unique_collections_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	'''
	def __insertCollectionIDsInCollectionSpecimen(self):
		query = """
		UPDATE cs
		SET cs.[CollectionID] = c_temp.[CollectionID]
		FROM CollectionSpecimen cs
		INNER JOIN [{0}] c_temp 
		ON c_temp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		WHERE c_temp.[CollectionID] IS NOT NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''


	'''
	def __insertCollectionIDsInSpecimenPart(self):
		query = """
		UPDATE csp
		SET csp.[CollectionID] = c_temp.[CollectionID]
		FROM CollectionSpecimenPart csp
		INNER JOIN [{0}] c_temp
		ON c_temp.[SpecimenPartID] = csp.[SpecimenPartID]
		WHERE c_temp.[CollectionID] IS NOT NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	'''


	def __updateCollectionDicts(self):
		c_ids = self.getIDsForCollectionDicts()
		for dict_id in self.c_dicts:
			c_dict = self.c_dicts[dict_id]
			c_dict['CollectionID'] = c_ids[dict_id]['CollectionID']
			c_dict['RowGUID'] = c_ids[dict_id]['RowGUID']
		return


	def getIDsForCollectionDicts(self):
		query = """
		SELECT 
			c_temp.[@id],
			c.[CollectionID],
			c.[RowGUID]
		FROM [Collection] c
		INNER JOIN [{0}] c_temp
		ON c_temp.[RowGUID] = c.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		c_ids = {}
		for row in rows:
			if not row[0] in c_ids:
				c_ids[row[0]] = {}
			c_ids[row[0]]['CollectionID'] = row[1]
			c_ids[row[0]]['RowGUID'] = row[2]
		
		return c_ids


















