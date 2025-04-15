import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.MethodMatcher import MethodMatcher


class MethodInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		self.temptable = '#method_temptable'
		self.unique_methods_temptable = '#unique_method_temptable'
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			{'colname': 'DatabaseURN'},
			{'colname': 'DisplayText'},
			{'colname': 'Description'},
			{'colname': 'Description_sha', 'compute sha of': 'Description'},
			{'colname': 'MethodURI'},
			{'colname': 'Notes'},
			{'colname': 'OnlyHierarchy', 'default': 0},
			{'colname': 'ForCollectionEvent', 'default': 0},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertMethodData(self, json_dicts = []):
		self.m_dicts = json_dicts
		self.__createMethodTempTable()
		
		self.json2temp.set_datadicts(self.m_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addMethodSHA()
		
		self.method_matcher = MethodMatcher(self.dc_db, self.temptable)
		self.method_matcher.matchExistingMethods()
		
		self.createNewMethods()
		
		self.__updateMethodDicts()
		
		return


	def __createMethodTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[@id] VARCHAR(100) COLLATE {1} NOT NULL,
		[MethodID] INT,
		[MethodParentID] INT,
		[RowGUID] UNIQUEIDENTIFIER,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64) COLLATE {1},
		[MethodURI] VARCHAR(255) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[OnlyHierarchy] BIT,
		[ForCollectionEvent] BIT,
		[method_sha] VARCHAR(64) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [MethodID_idx] ([MethodID]),
		INDEX [MethodParentID_idx] ([MethodParentID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addMethodSHA(self):
		query = """
		UPDATE m_temp
		SET [method_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description_sha],
			[MethodURI],
			[OnlyHierarchy],
			[ForCollectionEvent]
		)), 2)
		FROM [{0}] m_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def createNewMethods(self):
		# insert only one version of each event when the same method occurres multiple times in json data
		self.__setUniqueMethodsTempTable()
		self.__insertNewMethods()
		
		self.__updateMethodIDsInTempTable()
		return


	def __setUniqueMethodsTempTable(self):
		"""
		create a table that contains only one version of each method to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_methods_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[MethodID] INT,
			[MethodParentID] INT,
			[DisplayText] NVARCHAR(50) COLLATE {1},
			[Description] NVARCHAR(MAX) COLLATE {1},
			[Notes] NVARCHAR(MAX) COLLATE {1},
			[MethodURI] VARCHAR(255) COLLATE {1},
			[OnlyHierarchy] BIT,
			[ForCollectionEvent] BIT,
			 -- 
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			[method_sha] VARCHAR(64) COLLATE {1},
			INDEX [method_sha_idx] ([method_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_methods_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[method_sha]
		)
		SELECT DISTINCT
			[method_sha]
		FROM [{1}] m_temp
		WHERE m_temp.[MethodID] IS NULL
		;""".format(self.unique_methods_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET 
			[DisplayText] = m_temp.[DisplayText],
			[Description] = m_temp.[Description],
			[Notes] = m_temp.[Notes],
			[MethodURI] = m_temp.[MethodURI],
			[OnlyHierarchy] = m_temp.[OnlyHierarchy],
			[ForCollectionEvent] = m_temp.[ForCollectionEvent]
		FROM [{0}] ue_temp
		INNER JOIN [{1}] m_temp
		ON ue_temp.[method_sha] = m_temp.[method_sha]
		;""".format(self.unique_methods_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertNewMethods(self):
		query = """
		INSERT INTO [Method] (
			[DisplayText],
			[Description],
			[Notes],
			[MethodURI],
			[OnlyHierarchy],
			[ForCollectionEvent],
			[RowGUID]
		)
		SELECT
			ue_temp.[DisplayText],
			ue_temp.[Description],
			ue_temp.[Notes],
			ue_temp.[MethodURI],
			ue_temp.[OnlyHierarchy],
			ue_temp.[ForCollectionEvent],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_methods_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# TODO: what is the reason for updating the ids in ue_temp here?
		query = """
		UPDATE ue_temp
		SET ue_temp.[MethodID] = m.[MethodID],
		ue_temp.[MethodParentID] = m.[MethodParentID]
		FROM [{0}] ue_temp
		INNER JOIN [Method] m
		ON m.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_methods_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateMethodIDsInTempTable(self):
		query = """
		UPDATE m_temp
		SET m_temp.[MethodID] = m.[MethodID],
		m_temp.[MethodParentID] = m.[MethodParentID],
		m_temp.[RowGUID] = m.[RowGUID]
		FROM [{0}] m_temp
		INNER JOIN [{1}] ue_temp
		ON m_temp.[method_sha] = ue_temp.[method_sha]
		INNER JOIN [Method] m
		ON ue_temp.[RowGUID] = m.[RowGUID]
		;""".format(self.temptable, self.unique_methods_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateMethodDicts(self):
		m_ids = self.getIDsForMethodDicts()
		for dict_id in self.m_dicts:
			m_dict = self.m_dicts[dict_id]
			m_dict['MethodID'] = m_ids[dict_id]['MethodID']
			m_dict['MethodParentID'] = m_ids[dict_id]['MethodParentID']
			m_dict['RowGUID'] = m_ids[dict_id]['RowGUID']
		return


	def getIDsForMethodDicts(self):
		query = """
		SELECT 
			m_temp.[@id],
			m.[MethodID],
			m.[MethodParentID],
			m.[RowGUID]
		FROM [Method] m
		INNER JOIN [{0}] m_temp
		ON m_temp.[RowGUID] = m.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		m_ids = {}
		for row in rows:
			if not row[0] in m_ids:
				m_ids[row[0]] = {}
			m_ids[row[0]]['MethodID'] = row[1]
			m_ids[row[0]]['MethodParentID'] = row[2]
			m_ids[row[0]]['RowGUID'] = row[3]
		
		return m_ids
