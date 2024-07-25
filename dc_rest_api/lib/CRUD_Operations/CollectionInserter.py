import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.CollectionMatcher import CollectionMatcher



class CollectionInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#collection_temptable'
		self.unique_collections_temptable = 'unique_c_temptable'
		
		
		self.schema = [
			{'colname': 'entry_num', 'None allowed': False},
			{'colname': 'CollectionID'},
			{'colname': 'CollectionSpecimenID'},
			{'colname': 'CollectionName', 'default': 'No collection', 'None allowed': False},
			{'colname': 'CollectionAccronym'},
			{'colname': 'AdministrativeContactName'},
			{'colname': 'AdministrativeContactAgentURI'},
			{'colname': 'Description'},
			{'colname': 'Description_sha'},
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


	def insertCollectionData(self):
		
		self.__createCollectionTempTable()
		
		self.json2temp.set_datadicts(self.c_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addCollectionSHA()
		
		self.collection_matcher = CollectionMatcher(self.dc_db, self.temptable)
		self.collection_matcher.matchExistingCollections()
		
		self.createNewCollections()
		
		self.__insertCollectionIDsInCollectionSpecimen()
		
		self.__updateCollectionDicts()
		return


	def setCollectionDicts(self, json_dicts = []):
		self.c_dicts = []
		c_count = 1
		for c_dict in json_dicts:
			c_dict['entry_num'] = c_count
			c_count += 1
			self.c_dicts.append(c_dict)
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
		[entry_num] INT NOT NULL,
		[CollectionSpecimenID] INT,
		[CollectionName] NVARCHAR(255) COLLATE {1},
		[CollectionAcronym] NVARCHAR(10) COLLATE {1},
		[AdministrativeContactName] NVARCHAR(500) COLLATE {1},
		[AdministrativeContactAgentURI] VARCHAR(255) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64),
		[Location] NVARCHAR(255) COLLATE {1},
		[LocationParentID] INT,
		[LocationPlan] VARCHAR(500) COLLATE {1},
		[LocationPlanWidth] VARCHAR(500) FLOAT,
		[LocationPlanDate] DATETIME,
		[LocationGeometry] GEOMETRY,
		[LocationHeight] FLOAT,
		[CollectionOwner] NVARCHAR(255) COLLATE {1},
		[DisplayOrder] SMALLINT,
		[Type] NVARCHAR(50) COLLATE {1},
		[RowGUID] UNIQUEIDENTIFIER,
		[collection_sha] VARCHAR(64),
		PRIMARY KEY ([entry_num]),
		INDEX [entry_num_idx] ([entry_num]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [CollectionID_idx] (CollectionID),
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
			 -- [LocationPlanWidth],
			 -- [LocationPlanDate],
			 -- [LocationGeometry],
			 -- [LocationHeight],
			[CollectionOwner]
		)), 2)
		FROM [{0}] ce_temp
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
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			 -- 
			[collection_sha] VARCHAR(64),
			INDEX [collection_sha_idx] ([collection_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_collections_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
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
		SELECT DISTINCT
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
		FROM [{1}] c_temp
		WHERE c_temp.[CollectionID] IS NULL
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
		SELECT DISTINCT -- insert only one version of each collection when the same collection occurs multiple times in json data
			ue_temp.[CollectionName],
			ue_temp.[CollectionAcronym],
			ue_temp.[AdministrativeContactName],
			ue_temp.[AdministrativeContactAgentURI],
			ue_temp.[Description_sha],
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
		;""".format(self.unique_events_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateCollectionIDsInTempTable(self):
		query = """
		UPDATE c_temp
		SET c_temp.CollectionID = c.CollectionID
		FROM [{0}] c_temp
		INNER JOIN [Collection] c
		ON c_temp.[RowGUID] = c.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __insertCollectionIDsInCollectionSpecimen(self):
		query = """
		UPDATE cs
		SET cs.[CollectionID] = ce_temp.[CollectionID]
		FROM CollectionSpecimen cs
		INNER JOIN [{0}] c_temp 
		ON c_temp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		WHERE c_temp.[CollectionID] IS NOT NULL
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCollectionDicts(self):
		c_ids = self.getIDsForCollectionDicts()
		for c_dict in self.c_dicts:
			entry_num = c_dict['entry_num']
			c_dict['CollectionID'] = c_ids[entry_num]['CollectionID']
			c_dict['RowGUID'] = c_ids[entry_num]['RowGUID']
		return


	def getIDsForCollectionDicts(self):
		query = """
		SELECT 
			c_temp.[entry_num],
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

################################################












	'''
	def __setExistingCollections(self):
		query = """
		UPDATE c_temp
		SET c_temp.[CollectionID] = eds.[CollectionID],
		c_temp.[RowGUID] = eds.[RowGUID]
		FROM [{0}] c_temp
		INNER JOIN Collection c
		ON (
				c_temp.[CollectionName] = c.[CollectionName]
				AND ((c_temp.[CollectionAcronym] = c.[CollectionAcronym]) OR (c_temp.[CollectionAcronym] ICollectionAcronymS NULL AND c.[CollectionAcronym] IS NULL))
				AND ((c_temp.[AdministrativeContactName] = c.[AdministrativeContactName]) OR (c_temp.[AdministrativeContactName] IS NULL AND c.[AdministrativeContactName] IS NULL))
				AND ((c_temp.[AdministrativeContactAgentURI] = c.[AdministrativeContactAgentURI]) OR (c_temp.[AdministrativeContactAgentURI] IS NULL AND c.[AdministrativeContactAgentURI] IS NULL))
				AND ((c_temp.[Description_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', c_temp[Description]), 2)) OR (c_temp.[Description] IS NULL AND c.[Description] IS NULL))
				AND ((c_temp.[Location] = c.[Location]) OR (c_temp.[Location] IS NULL AND c.[Location] IS NULL))
				AND ((c_temp.[CollectionOwner] = c.[CollectionOwner]) OR (c_temp.[CollectionOwner] IS NULL AND c.[CollectionOwner] IS NULL))
				AND ((c_temp.[Type] = c.[Type]) OR (c_temp.[Type] IS NULL AND c.[Type] IS NULL))
			)
		WHERE c_temp.[CollectionID] IS NULL
		;
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
	'''



