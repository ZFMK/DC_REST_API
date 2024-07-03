import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')

from dc_rest_api.lib.CRUD_Operations.JSON2TempTable import JSON2TempTable

class CollectionAgentInserter():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.temptable = '#agent_temptable'
		
		self.schema = [
			{'colname': 'agent_num', 'None allowed': False},
			{'colname': 'CollectionSpecimenID', 'None allowed': False},
			{'colname': 'CollectorsName', 'None allowed': False},
			{'colname': 'CollectorsSequence'},
			{'colname': 'DataWithholdingReason'}
		]
		
		self.json2temp = JSON2TempTable(dc_db, self.schema)


	def insertCollectionAgentData(self):
		
		self.__createAgentTempTable()
		
		self.json2temp.set_datadicts(self.ca_dicts)
		self.json2temp.fill_temptable(self.temptable)
		
		self.__insertCollectors()
		self.__updateCATempTable()
		self.__updateCADicts()
		
		return


	def setCollectionAgentDicts(self, json_dicts = []):
		self.ca_dicts = []
		ca_count = 1
		for ca_dict in json_dicts:
			ca_dict['agent_num'] = ca_count
			ca_count += 1
			self.ca_dicts.append(ca_dict)
		return


	def __createAgentTempTable(self):
		# drop table is only neccessary when testing?
		# in tests i first test it without seperator for collectors and then with separator
		# TODO: think about design of it and whether separator usage is good at all
		query = """
		DROP TABLE IF EXISTS [{0}];
		""".format(self.temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
		[agent_num] INT NOT NULL,
		[CollectionSpecimenID] INT DEFAULT NULL,
		[CollectorsName] VARCHAR(255) COLLATE {1} NOT NULL,
		[CollectorsSequence] DATETIME2 DEFAULT NULL,
		[RowGUID] UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
		[DataWithholdingReason] VARCHAR(255) COLLATE {1},
		PRIMARY KEY ([agent_num]),
		INDEX [CollectorsName_idx] ([CollectorsName]),
		INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
		INDEX [RowGUID_idx] ([RowGUID])
		)
		;""".format(self.temptable, self.collation)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCATempTable(self):
		query = """
		UPDATE ca_temp
		SET ca_temp.[CollectorsSequence] = ca.[CollectorsSequence]
		FROM [{0}] ca_temp
		INNER JOIN [CollectionAgent] ca ON ca.[RowGUID] = ca_temp.[RowGUID]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __insertCollectors(self):
		query = """
		INSERT INTO [CollectionAgent] 
		(
			[CollectionSpecimenID],
			[CollectorsName],
			[CollectorsSequence],
			[RowGUID],
			[DataWithholdingReason]
		)
		SELECT 
			[CollectionSpecimenID],
			[CollectorsName],
			ISNULL([CollectorsSequence], DATEADD(ms, (ca_temp.[agent_num] * 200), SYSDATETIME())) AS [CollectorsSequence],
			[RowGUID],
			[DataWithholdingReason]
		FROM [{0}] ca_temp
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def __updateCADicts(self):
		ca_ids = self.getIDsForCADicts()
		for ca_dict in self.ca_dicts:
			agent_num = ca_dict['agent_num']
			ca_dict['CollectorsSequence'] = ca_ids[agent_num]['CollectorsSequence']
			ca_dict['RowGUID'] = ca_ids[agent_num]['RowGUID']
		return


	def getIDsForCADicts(self):
		query = """
		SELECT ca_temp.[agent_num], ca.[CollectorsSequence], ca.[RowGUID]
		FROM [CollectionAgent] ca
		INNER JOIN [{0}] ca_temp
		ON ca_temp.[RowGUID] = ca.[RowGUID] 
		;""".format(self.temptable)
		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		
		ca_ids = {}
		for row in rows:
			if not row[0] in ca_ids:
				ca_ids[row[0]] = {}
			ca_ids[row[0]]['CollectorsSequence'] = row[1]
			ca_ids[row[0]]['RowGUID'] = row[2]
		
		return ca_ids


