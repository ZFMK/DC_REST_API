import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter


class AnalysisMethodParameterFilterIDs():
	"""
	IDs in Analysis, Methods, and Parameters that should be used in getData() method 
	Create Temptables with the according IDs
	"""
	def __init__(self, dc_db, fieldname):
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		self.collation = self.dc_db.collation
		
		self.fieldname = fieldname
		
		self.amp_filter_ids = {}
		self.amp_lists = []
		
		if fieldname == 'Barcodes':
			self.setBarcodeAMPFilterIDS()
		elif fieldname == 'FOGS':
			self.setFOGSAMPFilterIDS()
		elif fieldname == 'MAM_Measurements':
			self.setMamAMPFilterIDS()
		
		self.amp_filter_temptable = '#temp_amp_filter'
		
		self.set_amp_filter_lists()
		self.create_amp_filter_temptable()


	def setBarcodeAMPFilterIDS(self):
		self.amp_filter_ids = {
		'161': {
				'12': ['62', '86', '87'],
				'16': ['73', '63', '64', '65', '66', '71', '72', '74', '75', '84', '91', '92']
			}
		}


	def setFOGSAMPFilterIDS(self):
		self.amp_filter_ids = {
		'327': {
				'23': ['140', '141', '150', '164', '165', '166', '167'],
				'25': ['144', '145', '146', '148']
			}
		}


	def setMamAMPFilterIDS(self):
		self.amp_filter_ids = {
			'210': {},
			'220': {},
			'230': {},
			'240': {},
			'250': {},
			'260': {},
			'270': {},
			'280': {},
			'290': {},
			'293': {},
			'294': {},
			'295': {},
			'296': {},
			'299': {},
			'301': {},
			'302': {},
			'303': {}
		}


	def set_amp_filter_lists(self):
		self.amp_lists = []
		
		for analysis_id in self.amp_ids:
			if len(self.amp_ids[analysis_id]) <= 0:
				self.amp_lists.append((analysis_id, None, None))
			else:
				for method_id in self.amp_ids[analysis_id]:
					if len(self.amp_ids[analysis_id][method_id]) <= 0:
						self.amp_lists.append((analysis_id, method_id, None))
					else:
						for parameter_id in self.amp_ids[analysis_id][method_id]:
							self.amp_lists.append((analysis_id, method_id, parameter_id))
		return


	def create_amp_filter_temptable(self):
		# create a table that holds the wanted combinations of:
		# AnalysisID
		#	MethodID
		#		ParameterID
		# it is needed because the MethodIDs and ParameterIDs are not guarantied to be unique
		# for different analyses and methods
		
		
		query = """
		DROP TABLE IF EXISTS [#temp_amp_filter]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [{0}] (
			[AnalysisID] INT NOT NULL,
			[MethodID] INT,
			[ParameterID] INT,
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_MethodID] ([MethodID]),
			INDEX [idx_ParameterID] ([ParameterID])
		)
		;""".format(self.amp_filter_temptable)
		self.cur.execute(query)
		self.con.commit()
		
		placeholders = ['(?, ?, ?)' for _ in self.amp_lists]
		values = []
		for amp_list in self.amp_lists:
			values.extend(amp_list)
		
		query = """
		INSERT INTO [{0}] (
			[AnalysisID],
			[MethodID],
			[ParameterID]
		)
		VALUES {1}
		;""".format(self.amp_filter_temptable, ', '.join(placeholders))
		self.cur.execute(query, values)
		self.con.commit()
		
		return



class IdentificationUnitAnalysisGetter(DataGetter):
	def __init__(self, dc_db, amp_ids, fieldname, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.amp_ids = amp_ids
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iua_temptable'
		
		self.withholded = []



	def getByPrimaryKeys(self, iua_ids):
		self.createGetTempTable()
		
		batchsize = 400
		while len(iua_ids) > 0:
			cached_ids = iua_ids[:batchsize]
			del iua_ids[:batchsize]
			placeholders = ['(?, ?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#iua_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iua_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0}
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber])
			)
			;"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iua_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			[AnalysisID],
			[AnalysisNumber]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [IdentificationUnitAnalysis] iua
			INNER JOIN [#iua_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = iua.[CollectionSpecimenID]
			AND pks.[IdentificationUnitID] = iua.[IdentificationUnitID]
			AND pks.[SpecimenPartID] = iua.[SpecimenPartID]
			AND pks.[AnalysisID] = iua.[AnalysisID]
			AND pks.[AnalysisNumber] = iua.[AnalysisNumber]
			;""".format(self.get_temptable)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		identificationunitanalyses = self.getData()
		
		return identificationunitanalyses


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		identificationunitanalyses = self.getData()
		
		return identificationunitanalyses



	def getData(self):
		










	def 
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iua.[CollectionSpecimenID],
		iua.[IdentificationUnitID],
		iua.[SpecimenPartID],
		iua.[AnalysisID],
		iua.[AnalysisNumber],
		iua.[RowGUID],
		
		iua.[AnalysisResult],
		iua.[ExternalAnalysisURI],
		iua.[ResponsibleName],
		iua.[AnalysisDate],
		iua.[Notes],
		
		
		
		
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON iua.[RowGUID] = g_temp.[rowguid_to_get]
		;""".format(self.get_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.iu_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildIdentifications()
		
		return self.iu_list


	def rows2list(self):
		self.iu_list = []
		for row in self.iu_rows:
			self.iu_list.append(dict(zip(self.columns, row)))
		return


	def list2dict(self):
		self.iu_dict = {}
		for element in self.iu_list:
			if element['CollectionSpecimenID'] not in self.iu_dict:
				self.iu_dict[element['CollectionSpecimenID']] = {}
				
			self.iu_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = element 
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		# the withholded variable keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT iua.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON iua.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iua.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iua.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iua.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		rows = self.cur.fetchall()
		for row in rows:
			withholded.append((row[0], row[1], row[2]))
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON iua.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iua.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iua.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iua.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return withholded


	def setChildIdentifications(self):
		
		i_getter = IdentificationGetter(self.dc_db, self.users_project_ids)
		i_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT i.[RowGUID]
		FROM [IdentificationUnit] iu
		INNER JOIN [Identification] i
		ON iu.[CollectionSpecimenID] = i.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = i.[IdentificationUnitID]
		INNER JOIN [{1}] rg_temp
		ON iu.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(i_getter.get_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		i_getter.getData()
		i_getter.list2dict()
		
		for iu in self.iu_list:
			if iu['CollectionSpecimenID'] in i_getter.i_dict and iu['IdentificationUnitID'] in i_getter.i_dict[iu['CollectionSpecimenID']]:
				if 'Identifications' not in iu:
					iu['Identifications'] = []
				for i_id in i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']]:
					iu['Identifications'].append(i_getter.i_dict[iu['CollectionSpecimenID']][iu['IdentificationUnitID']][i_id])
		
		return











