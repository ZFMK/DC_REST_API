import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


class CollectionSpecimens():
	def __init__(self, dc_db):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		
		'''
		self.ids_temptable = '#getter_ids_temptable'
		#self.temptable = '#get_specimen_temptable'
		
		self.max_page = 1
		self.pagesize = 1000
		'''


	def getDataPage(self, page):
		if page <= self.max_page:
			first_row = (page - 1) * self.pagesize + 1
			last_row = page * self.pagesize
		
		query = """
		SELECT 
		ids_temp.[row_num],
		ids_temp.[CollectionSpecimenID],
		ids_temp.[RowGUID],
		cs.[CollectionEventID],
		cs.[ExternalDatasourceID],
		cs.[ExternalIdentifier],
		cs.[AccessionNumber],
		cs.[AccessionDate],
		cs.[AccessionDay],
		cs.[AccessionMonth],
		cs.[AccessionYear],
		cs.[AccessionDateSupplement],
		cs.[AccessionDateCategory],
		cs.[DepositorsName],
		cs.[DepositorsAgentURI],
		cs.[DepositorsAccessionNumber],
		cs.[LabelTitle],
		cs.[LabelType],
		cs.[LabelTranscriptionState],
		cs.[LabelTranscriptionNotes],
		cs.[ExsiccataURI],
		cs.[ExsiccataAbbreviation],
		cs.[OriginalNotes],
		cs.[AdditionalNotes],
		cs.[Problems],
		cs.[DataWithholdingReason]
		FROM [{0}] ids_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[CollectionSpecimenID] = ids_temp.[CollectionSpecimenID]
		WHERE ids_temp.[row_num] BETWEEN ? AND ?
		;""".format(self.ids_temptable)
		querylog.info(query)
		self.cur.execute(query, [first_row, last_row])
		self.columns = [column[0] for column in self.cur.description]
		
		self.cs_rows = self.cur.fetchall()
		self.rows2dict()
		


	def rows2dict(self):
		cs_list = []
		for row in self.cs_rows:
			cs_list.append(dict(zip(self.columns, row)))
		
		self.cs_dict = {}
		for element in cs_list:
			self.cs_dict[element['CollectionSpecimenID']] = element
		
		return


#############################################


	'''
	def createIDsTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.ids_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[row_num] INT IDENTITY,
			[CollectionSpecimenID] INT UNIQUE,
			[RowGUID] VARCHAR(64) COLLATE {1} UNIQUE,
			[CollectionEventID] INT,
			[ExternalDatasourceID] INT,
			[AccessionNumber] NVARCHAR(50) COLLATE {1},
			PRIMARY KEY ([row_num])
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [CollectionEventID] ([CollectionEventID]),
			INDEX [ExternalDatasourceID] ([ExternalDatasourceID]),
			INDEX [AccessionNumber] ([AccessionNumber]),
			INDEX [RowGUID] ([RowGUID])
		)
		;""".format(self.ids_temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return



	def fillIDsTempTableByPrimaryKeys(self, specimen_ids):
		self.createIDsTempTable()
		
		# insert of with placeholders is limited to 2100 values
		pagesize = 1000
		while len(specimen_ids) > 0:
			cached_ids = specimen_ids[:pagesize]
			del specimen_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			INSERT INTO [{0}] ([CollectionSpecimenID])
			VALUES {1}
			;""".format(self.ids_temptable)
			querylog.info(query)
			self.cur.execute(query, [values])
			self.con.commit()
			
		query = """
		UPDATE ids_temp
		SET ids_temp.[RowGUID] = cs.[RowGUID]
		FROM [{0}] ids_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[CollectionSpecimenID] = ids_temp.[CollectionSpecimenID]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def fillIDsTempTableByRowGUIDs(self, rowguids):
		self.createIDsTempTable()
		
		# insert of with placeholders is limited to 2100 values
		pagesize = 1000
		while len(rowguids) > 0:
			cached_ids = rowguids[:pagesize]
			del rowguids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			INSERT INTO [{0}] ([RowGUID])
			VALUES {1}
			;""".format(self.ids_temptable)
			querylog.info(query)
			self.cur.execute(query, [values])
			self.con.commit()
			
		query = """
		UPDATE ids_temp
		SET ids_temp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		FROM [{0}] ids_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[RowGUID] = ids_temp.[RowGUID]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return


	def set_max_page(self):
		query = """
		SELECT COUNT(CollectionSpecimenID) FROM [{0}]
		;""".format(self.ids_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		row = self.cur.fetchone()
		self.rownumber = row[0]
		
		self.max_page = math.ceil(self.rownumber / self.pagesize)
		if self.max_page < 1:
			self.max_page = 1
		
		return
	'''







	##########################################
	'''
	def createDataTempTable(self):
		query = """
		DROP TABLE IF EXISTS [{0}]
		;""".format(self.temptable)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		CREATE TABLE [{0}] (
			[row_num] INT IDENTITY,
			[CollectionSpecimenID] INT NOT NULL UNIQUE,
			[RowGUID] VARCHAR(64) COLLATE {1},
			[CollectionEventID] INT,
			[ExternalDatasourceID] INT,
			[ExternalIdentifier] NVARCHAR(100) COLLATE {1},
			[AccessionNumber] NVARCHAR(50) COLLATE {1},
			[AccessionDate] DATETIME,
			[AccessionDay] TINYINT,
			[AccessionMonth] TINYINT,
			[AccessionYear] SMALLINT,
			[AccessionDateSupplement] NVARCHAR(255) COLLATE {1},
			[AccessionDateCategory] NVARCHAR(50) COLLATE {1},
			[DepositorsName] NVARCHAR(255) COLLATE {1},
			[DepositorsAgentURI] VARCHAR(255) COLLATE {1},
			[DepositorsAccessionNumber] NVARCHAR(50) COLLATE {1},
			[LabelTitle] NVARCHAR(MAX) COLLATE {1},
			[LabelType] NVARCHAR(50) COLLATE {1},
			[LabelTranscriptionState] NVARCHAR(50) COLLATE {1},
			[LabelTranscriptionNotes] NVARCHAR(MAX) COLLATE {1},
			[ExsiccataURI] VARCHAR(255) COLLATE {1},
			[ExsiccataAbbreviation] NVARCHAR(255) COLLATE {1},
			[OriginalNotes] NVARCHAR(MAX) COLLATE {1},
			[AdditionalNotes] NVARCHAR(MAX) COLLATE {1},
			[Problems] NVARCHAR(255) COLLATE {1},
			[DataWithholdingReason] NVARCHAR(255),
			INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
			INDEX [CollectionEventID] ([CollectionEventID]),
			INDEX [ExternalDatasourceID] ([ExternalDatasourceID]),
			INDEX [AccessionNumber] ([AccessionNumber]),
			INDEX [RowGUID] ([RowGUID])
		) 
		;""".format(self.temptable, self.collation)
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return
	'''
























