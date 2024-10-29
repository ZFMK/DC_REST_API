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
	"""
	IdentificationUnitAnalysisGetter needs to filter the IDs of the wanted methods and parameters, otherwise the mass of data belonging to an analysis might overwelm the 
	servers capacity
	The filtering is configured in AnalysisMethodParameterFilterIDs. AnalysisMethodParameterFilterIDs also generates a temporary table with the IDs that is used here
	to join against the Method and Parameter tables.
	Analyses, Methods and Parameters are requested separately because otherwise with a big join data like AnalysisResult are selected in multiple rows
	and thus cause a large overhead of data transfer
	"""
	
	def __init__(self, dc_db, fieldname, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iua_temptable'
		
		amp_filters = AnalysisMethodParameterFilterIDs(dc_db, self.fieldname)
		self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
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
				[SpecimenPartID] INT DEFAULT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0}
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [SpecimenPartID_idx] ([SpecimenPartID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber])
			)
			;""".format(self.collation)
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
			AND (pks.[SpecimenPartID] = iua.[SpecimenPartID] OR pks.[SpecimenPartID] IS NULL AND iua.[SpecimenPartID] IS NULL)
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



	def create_analyses_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_analysis_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_analysis_ids] (
			[analysis_pk] INT IDENTITY PRIMARY KEY,
			[RowGUID] uniqueidentifier,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[SpecimenPartID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
			INDEX [idx_RowGUID] ([RowGUID]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_SpecimenPartID] ([SpecimenPartID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber])
		)
		;""".format(self.collation)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_analysis_ids] (
			[RowGUID],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			[AnalysisID],
			[AnalysisNumber]
		)
		SELECT 
		DISTINCT
		iua.[RowGUID],
		iua.[CollectionSpecimenID],
		iua.[IdentificationUnitID],
		iua.[SpecimenPartID],
		iua.[AnalysisID],
		iua.[AnalysisNumber]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
			ON (g_temp.[rowguid_to_get] = iua.[RowGUID])
		INNER JOIN [{1}] amp_filter
		ON amp_filter.AnalysisID = iua.AnalysisID
		;""".format(self.get_temptable, self.amp_filter_temptable)
		
		self.cur.execute(query)
		self.con.commit()


	def create_methods_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_method_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_method_ids] (
			[method_pk] INT IDENTITY PRIMARY KEY,
			[analysis_pk] INT NOT NULL,
			[RowGUID] UNIQUEIDENTIFIER,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
			[MethodID] INT NOT NULL,
			[MethodMarker] NVARCHAR(50) NOT NULL COLLATE {0},
			INDEX [idx_analysis_pk] ([analysis_pk]),
			INDEX [idx_RowGUID] ([RowGUID]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber]),
			INDEX [idx_MethodID] ([MethodID]),
			INDEX [idx_MethodMarker] ([MethodMarker])
		)
		;""".format(self.collation)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_method_ids] (
			[analysis_pk],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[RowGUID]
		)
		SELECT 
		DISTINCT
		a_temp.analysis_pk,
		a_temp.CollectionSpecimenID,
		a_temp.IdentificationUnitID,
		a_temp.AnalysisID,
		a_temp.AnalysisNumber,
		iuam.MethodID,
		iuam.MethodMarker,
		iuam.[RowGUID]
		FROM [#temp_analysis_ids] a_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON (
			a_temp.CollectionSpecimenID = iuam.CollectionSpecimenID 
			AND a_temp.IdentificationUnitID = iuam.IdentificationUnitID
			AND a_temp.AnalysisID = iuam.AnalysisID
			AND a_temp.AnalysisNumber = iuam.AnalysisNumber COLLATE {0}
		)
		INNER JOIN [#temp_amp_filter] amp_filter
		ON (
			amp_filter.AnalysisID = iuam.AnalysisID
			AND amp_filter.MethodID = iuam.MethodID
		)
		;""".format(self.collation)
		
		self.cur.execute(query)
		self.con.commit()


	def set_analyses(self):
		query = """
		SELECT 
		DISTINCT
		a_temp.analysis_pk,
		a_temp.[RowGUID],
		a_temp.CollectionSpecimenID,
		a_temp.IdentificationUnitID,
		a_temp.SpecimenPartID,
		a_temp.[AnalysisID],
		a_temp.AnalysisNumber,
		iua.Notes AS AnalysisInstanceNotes,
		iua.ExternalAnalysisURI,
		iua.ResponsibleName,
		iua.[AnalysisDate],
		iua.AnalysisResult,
		iua.AnalysisID,
		a.DisplayText AS AnalysisDisplay,
		a.Description AS AnalysisDescription,
		a.MeasurementUnit,
		a.Notes AS AnalysisTypeNotes
		FROM [#temp_analysis_ids] a_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
		ON (
			a_temp.[RowGUID] = iua.[RowGUID]
		)
		INNER JOIN [Analysis] a
		ON iua.AnalysisID = a.AnalysisID
		ORDER BY a_temp.analysis_pk, a_temp.[idshash], iua.AnalysisID, iua.AnalysisNumber
		;"""
		
		log_query.debug(query)
		
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		self.keys_dict = {}
		
		self.analyses_dict = {}
		
		self.analyses = []
		for row in rows:
			self.analyses.append(dict(zip(columns, row)))
		
		for analysis in self.analyses:
			specimen_id = analysis['CollectionSpecimenID']
			iu_id = analysis['IdentificationUnitID']
			#part_id = analysis['SpecimenPartID']
			
			if specimen_id not in self.analyses_dict:
				self.analyses_dict[specimen_id] = {}
			
			self.analyses_dict[specimen_id][iu_id] = analysis
			
			'''
			for key in analysis:
				if key not in ['CollectionSpecimenID']:
					self.analyses_dict['CollectionSpecimenID'][key] = analysis[key]
			'''
		
		return


	def set_methods(self):
		query = """
		SELECT 
		DISTINCT
		m_temp.method_pk,
		m_temp.analysis_pk,
		m_temp.[idshash] AS [_id],
		iuam.AnalysisID,
		iuam.AnalysisNumber,
		iuam.MethodID,
		iuam.MethodMarker,
		m.DisplayText AS MethodDisplay,
		m.Description AS MethodDescription,
		m.Notes AS MethodTypeNotes
		FROM [#temp_method_ids] m_temp
		INNER JOIN [#temp_analysis_ids] a_temp
		ON a_temp.analysis_pk = m_temp.analysis_pk
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON (
			a_temp.CollectionSpecimenID = iuam.CollectionSpecimenID 
			AND m_temp.IdentificationUnitID = iuam.IdentificationUnitID
			AND m_temp.AnalysisID = iuam.AnalysisID
			AND m_temp.AnalysisNumber = iuam.AnalysisNumber COLLATE DATABASE_DEFAULT
			AND m_temp.MethodID = iuam.MethodID
			AND m_temp.MethodMarker = iuam.MethodMarker COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [MethodForAnalysis] mfa
		ON (
			iuam.MethodID = mfa.MethodID
			AND iuam.AnalysisID = mfa.AnalysisID
		)
		INNER JOIN [Method] m
		ON mfa.MethodID = m.MethodID
		ORDER BY m_temp.[idshash], iuam.AnalysisID, iuam.AnalysisNumber, iuam.MethodID, iuam.MethodMarker
		;"""
		
		log_query.info(query)
		
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		self.methods_dict = {}
		
		methods = []
		for row in rows:
			methods.append(dict(zip(columns, row)))
		
		for method in methods:
			idshash = method['_id']
			analysis_pk = method['analysis_pk']
			method_pk = method['method_pk']
			
			self.methods_dict[method_pk] = {}
			
			self.methods_dict[method_pk]['MethodDisplay'] = method['MethodDisplay']
			self.methods_dict[method_pk]['MethodDescription'] = method['MethodDescription']
			
			'''
			for key in method:
				if key not in ('_id', 'method_pk', 'analysis_pk', 'AnalysisID', 'AnalysisNumber'):
					self.methods_dict[method_pk][key] = method[key]
			'''
			
			self.keys_dict[idshash][analysis_pk][method_pk] = {}
			
		return



	def getData(self):
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		self.create_analyses_temptable()
		self.create_methods_temptable()
		
		










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











