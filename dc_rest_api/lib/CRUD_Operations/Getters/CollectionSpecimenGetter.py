import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter


class CollectionSpecimenGetter(DataGetter):
	def __init__(self, dc_db, users_project_ids = [], page = 1, pagesize = 1000):
		DataGetter.__init__(self, dc_db, page, pagesize)
		
		self.withholded = []
		self.users_project_ids = users_project_ids
		self.get_temptable = 'get_specimen_temptable'


	def getByPrimaryKeys(self, specimen_ids):
		self.createGetTempTable()
		
		pagesize = 1000
		while len(specimen_ids) > 0:
			cached_ids = specimen_ids[:pagesize]
			del specimen_ids[:pagesize]
			placeholders = ['(?)' for _ in cached_ids]
			values = [value for value in cached_ids]
			
			query = """
			DROP TABLE IF EXISTS [#cs_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#cs_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#cs_pks_to_get_temptable] (
			[CollectionSpecimenID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [CollectionSpecimen] cs
			INNER JOIN [#cs_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		#self.getChildSpecimenParts()
		#self.getChildIdentificationUnits()
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimens = self.getDataPage()
		
		return specimens


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		#self.deleteChildSpecimenParts()
		#self.deleteChildIdentificationUnits()
		
		self.withholded = self.filterAllowedRowGUIDs()
		specimens = self.getDataPage()
		
		return specimens



	def getDataPage(self):
		if self.page <= self.max_page:
			first_row = (self.page - 1) * self.pagesize + 1
			last_row = self.page * self.pagesize
		
		query = """
		SELECT 
		g_temp.[row_num],
		g_temp.[rowguid_to_get] AS [RowGUID],
		cs.[CollectionSpecimenID],
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
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.[RowGUID] = g_temp.[rowguid_to_get]
		WHERE g_temp.[row_num] BETWEEN ? AND ?
		;""".format(self.get_temptable)
		querylog.info(query)
		self.cur.execute(query, [first_row, last_row])
		self.columns = [column[0] for column in self.cur.description]
		
		self.cs_rows = self.cur.fetchall()
		self.rows2dict()
		return self.cs_dict


	def rows2dict(self):
		cs_list = []
		for row in self.cs_rows:
			cs_list.append(dict(zip(self.columns, row)))
		
		self.cs_dict = {}
		for element in cs_list:
			self.cs_dict[element['CollectionSpecimenID']] = element
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		withholded = []
		
		projectclause = self.getProjectClause()
		
		query = """
		SELECT cs.[CollectionSpecimenID], cs.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {1}
		;""".format(self.get_temptable, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append(dict(zip(columns, row)))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [CollectionSpecimen] cs
		ON cs.RowGUID = g_temp.[rowguid_to_get]
		LEFT JOIN [CollectionProject] cp
		ON cs.[CollectionSpecimenID] = cp.[CollectionSpecimenID]
		WHERE cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '' {2}
		;""".format(self.get_temptable, placeholderstring, projectclause)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded




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
























