import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.Inserters.JSON2TempTable import JSON2TempTable
from dc_rest_api.lib.CRUD_Operations.Matchers.ParameterMatcher import ParameterMatcher

from dc_rest_api.lib.CRUD_Operations.Matchers.ParameterMatcher import ParameterMatcher


class ParameterInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		self.temptable = '#parameter_temptable'
		self.unique_parameters_temptable = '#unique_parameter_temptable'
		
		self.schema = [
			{'colname': '@id', 'None allowed': False},
			{'colname': 'MethodID', 'None allowed': False},
			{'colname': 'DatabaseURN'},
			{'colname': 'DisplayText'},
			{'colname': 'Description'},
			{'colname': 'Description_sha', 'compute sha of': 'Description'},
			{'colname': 'ParameterURI'},
			{'colname': 'Description'},
			{'colname': 'DefaultValue_sha', 'compute sha of': 'DefaultValue'},
			{'colname': 'Notes'},
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertParameterData(self, json_dicts = []):
		self.p_dicts = json_dicts
		self.__createParameterTempTable()
		
		self.json2temp.set_datadicts(self.p_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__addParameterSHA()
		
		self.parameter_matcher = ParameterMatcher(self.dc_db, self.temptable)
		self.parameter_matcher.matchExistingParameters()
		
		self.createNewParameters()
		
		self.__updateParameterDicts()
		
		return


	def __createParameterTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[@id] VARCHAR(100) COLLATE {1} NOT NULL,
		[ParameterID] INT,
		[MethodID] INT NOT NULL,
		[RowGUID] UNIQUEIDENTIFIER,
		[DatabaseURN] NVARCHAR(500) COLLATE {1},
		[DisplayText] NVARCHAR(50) COLLATE {1},
		[Description] NVARCHAR(MAX) COLLATE {1},
		[Description_sha] VARCHAR(64) COLLATE {1},
		[ParameterURI] VARCHAR(255) COLLATE {1},
		[DefaultValue] NVARCHAR(MAX) COLLATE {1},
		[DefaultValue_sha] VARCHAR(64) COLLATE {1},
		[Notes] NVARCHAR(MAX) COLLATE {1},
		[parameter_sha] VARCHAR(64) COLLATE {1},
		PRIMARY KEY ([@id]),
		INDEX [ParameterID_idx] ([ParameterID]),
		INDEX [MethodID_idx] ([MethodID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __addParameterSHA(self):
		query = """
		UPDATE p_temp
		SET [parameter_sha] = CONVERT(VARCHAR(64), HASHBYTES('sha2_256', CONCAT(
			[DisplayText],
			[Description_sha],
			[ParameterURI],
			[DefaultValue_sha]
		)), 2)
		FROM [{0}] p_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def createNewParameters(self):
		# insert only one version of each event when the same parameter occurres multiple times in json data
		self.__setUniqueParametersTempTable()
		self.__insertNewParameters()
		
		self.__updateParameterIDsInTempTable()
		return


	def __setUniqueParametersTempTable(self):
		"""
		create a table that contains only one version of each parameter to be inserted
		"""
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.unique_parameters_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[ParameterID] INT,
			[MethodID] INT NOT NULL,
			[DisplayText] NVARCHAR(50) COLLATE {1},
			[Description] NVARCHAR(MAX) COLLATE {1},
			[ParameterURI] VARCHAR(255) COLLATE {1},
			[DefaultValue] NVARCHAR(MAX) COLLATE {1},
			[Notes] NVARCHAR(MAX) COLLATE {1},
			 -- 
			[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
			[parameter_sha] VARCHAR(64) COLLATE {1},
			INDEX [parameter_sha_idx] ([parameter_sha]),
			INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.unique_parameters_temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [{0}] (
			[parameter_sha]
		)
		SELECT DISTINCT
			[parameter_sha]
		FROM [{1}] p_temp
		WHERE p_temp.[ParameterID] IS NULL
		;""".format(self.unique_parameters_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE ue_temp
		SET 
			 -- MethodID has been inserted before
			 -- [MethodID] = p_temp.[MethodID]
			[DisplayText] = p_temp.[DisplayText],
			[Description] = p_temp.[Description],
			[ParameterURI] = p_temp.[ParameterURI],
			[DefaultValue] = p_temp.[DefaultValue],
			[Notes] = p_temp.[Notes]
		FROM [{0}] ue_temp
		INNER JOIN [{1}] p_temp
		ON ue_temp.[parameter_sha] = p_temp.[parameter_sha]
		;""".format(self.unique_parameters_temptable, self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertNewParameters(self):
		query = """
		INSERT INTO [Parameter] (
			[MethodID],
			[DisplayText],
			[Description],
			[ParameterURI],
			[DefaultValue],
			[Notes],
			[RowGUID]
		)
		SELECT
			ue_temp.[MethodID],
			ue_temp.[DisplayText],
			ue_temp.[Description],
			ue_temp.[ParameterURI],
			ue_temp.[DefaultValue],
			ue_temp.[Notes],
			ue_temp.[RowGUID]
		FROM [{0}] ue_temp
		;""".format(self.unique_parameters_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		# TODO: what is the reason for updating the ids in ue_temp here?
		query = """
		UPDATE ue_temp
		SET ue_temp.[ParameterID] = p.[ParameterID]
		 -- ue_temp.[MethodID] = p.[MethodID]
		FROM [{0}] ue_temp
		INNER JOIN [Parameter] p
		ON p.[RowGUID] = ue_temp.[RowGUID]
		;""".format(self.unique_parameters_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateParameterIDsInTempTable(self):
		query = """
		UPDATE p_temp
		SET p_temp.[ParameterID] = p.[ParameterID],
		 -- p_temp.[MethodID] = p.[MethodID],
		p_temp.[RowGUID] = p.[RowGUID]
		FROM [{0}] p_temp
		INNER JOIN [{1}] ue_temp
		ON p_temp.[parameter_sha] = ue_temp.[parameter_sha]
		INNER JOIN [Parameter] p
		ON ue_temp.[RowGUID] = p.[RowGUID]
		;""".format(self.temptable, self.unique_parameters_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def __updateParameterDicts(self):
		p_ids = self.getIDsForParameterDicts()
		for dict_id in self.p_dicts:
			p_dict = self.p_dicts[dict_id]
			p_dict['ParameterID'] = p_ids[dict_id]['ParameterID']
			p_dict['MethodID'] = p_ids[dict_id]['MethodID']
			p_dict['RowGUID'] = p_ids[dict_id]['RowGUID']
		return


	def getIDsForParameterDicts(self):
		query = """
		SELECT 
			p_temp.[@id],
			p.[ParameterID],
			p.[MethodID],
			p.[RowGUID]
		FROM [Parameter] p
		INNER JOIN [{0}] p_temp
		ON p_temp.[RowGUID] = p.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		p_ids = {}
		for row in rows:
			if not row[0] in p_ids:
				p_ids[row[0]] = {}
			p_ids[row[0]]['ParameterID'] = row[1]
			p_ids[row[0]]['MethodID'] = row[2]
			p_ids[row[0]]['RowGUID'] = row[3]
		
		return p_ids
