import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter


class IdentificationUnitAnalysisMethodParameterGetter(DataGetter):
	"""
	IdentificationUnitAnalysisGetter needs to filter the IDs of the wanted methods and parameters, otherwise the mass of data belonging to an analysis might overwelm the 
	servers capacity
	The filtering is configured in AnalysisMethodParameterFilter. AnalysisMethodParameterFilter also generates a temporary table with the IDs that is used here
	to join against the Method and Parameter tables.
	Analyses, Methods and Parameters are requested separately because otherwise with a big join data like AnalysisResult are selected in multiple rows
	and thus cause a large overhead of data transfer
	"""
	
	def __init__(self, dc_db, fieldname, users_project_ids = []):
		DataGetter.__init__(self, dc_db)
		
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iuamp_temptable'
		
		amp_filters = AnalysisMethodParameterFilter(dc_db, self.fieldname)
		self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
		self.withholded = []



	def getByPrimaryKeys(self, iuamp_ids):
		# does this make sense here when the number of IDs is rapidly increasing with every sub table of IdentificationUnitAnalysis?
		
		self.createGetTempTable()
		
		batchsize = 300
		while len(iuamp_ids) > 0:
			cached_ids = iuamp_ids[:batchsize]
			del iuamp_ids[:batchsize]
			placeholders = ['(?, ?, ?, ?, ?, ?, ?)' for _ in cached_ids]
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
				[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
				[MethodID] INT NOT NULL,
				[MethodMarker] NVARCHAR(50) NOT NULL COLLATE {0},
				[ParameterID] INT NOT NULL,
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
				INDEX [MethodID_idx] ([MethodID]),
				INDEX [MethodMarker_idx] ([MethodMarker]),
				INDEX [ParameterID_idx] ([ParameterID])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iuamp_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[ParameterID]
			)
			VALUES {0}
			""".format(', '.join(placeholders))
			querylog.info(query)
			self.cur.execute(query, values)
			self.con.commit()
			
			query = """
			INSERT INTO [{0}] ([rowguid_to_get])
			SELECT [RowGUID] FROM [IdentificationUnitAnalysisMethodParameter] iuamp
			INNER JOIN [#iuamp_pks_to_get_temptable] pks
			ON pks.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID]
			AND pks.[IdentificationUnitID] = iuamp.[IdentificationUnitID]
			AND pks.[AnalysisID] = iuamp.[AnalysisID]
			AND pks.[AnalysisNumber] = iuamp.[AnalysisNumber] COLLATE {1}
			AND pks.[MethodID] = iuamp.[MethodID]
			AND pks.[MethodMarker] = iuamp.[MethodMarker] COLLATE {1}
			AND pks.[ParameterID] = iuamp.[ParameterID]
			;""".format(self.get_temptable, self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		
		
		
		iuamps = self.getData()
		
		return iuamps


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		
		
		
		iuamps = self.getData()
		
		return iuamps



	def create_iuamp_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_iuamp_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_iuamp_ids] (
			[parameter_pk] INT IDENTITY PRIMARY KEY,
			[RowGUID] uniqueidentifier,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[SpecimenPartID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
			[MethodID] INT NOT NULL,
			[MethodMarker] NVARCHAR(50) NOT NULL COLLATE {0},
			[ParameterID] INT NOT NULL,
			INDEX [idx_RowGUID] ([RowGUID]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_SpecimenPartID] ([SpecimenPartID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber]),
			INDEX [MethodID_idx] ([MethodID]),
			INDEX [MethodMarker_idx] ([MethodMarker]),
			INDEX [ParameterID_idx] ([ParameterID])
		)
		;""".format(self.collation)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_iuamp_ids] (
			[RowGUID],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[SpecimenPartID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker],
			[ParameterID]
		)
		SELECT 
		DISTINCT
		iuamp.[RowGUID],
		iuamp.[CollectionSpecimenID],
		iuamp.[IdentificationUnitID],
		iuamp.[SpecimenPartID],
		iuamp.[AnalysisID],
		iuamp.[AnalysisNumber],
		iuamp.[MethodID],
		iuamp.[MethodMarker],
		iuamp.[ParameterID]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
			ON (g_temp.[rowguid_to_get] = iuamp.[RowGUID])
		INNER JOIN [{1}] amp_filter
		ON amp_filter.AnalysisID = iuamp.AnalysisID AND amp.MethodID = iuamp.MethodID AND amp.ParameterID = iuamp.ParameterID
		;""".format(self.get_temptable, self.amp_filter_temptable)
		
		self.cur.execute(query)
		self.con.commit()






	def getData(self):
		self.setDatabaseURN()
		self.withholded = self.filterAllowedRowGUIDs()
		
		self.create_iuamp_temptable()
		
		
		










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











