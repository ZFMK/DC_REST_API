import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

#from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisMethodParameterGetter import IdentificationUnitAnalysisMethodParameterGetter


class IdentificationUnitAnalysisMethodGetter(DataGetter):
	"""
	IdentificationUnitAnalysisGetter needs to filter the IDs of the wanted methods and parameters, otherwise the mass of data belonging to an analysis might overwelm the 
	servers capacity
	The filtering is configured in AnalysisMethodParameterFilter. AnalysisMethodParameterFilter also generates a temporary table with the IDs that is used here
	to join against the Method and Parameter tables.
	Analyses, Methods and Parameters are requested separately because otherwise with a big join data like AnalysisResult are selected in multiple rows
	and thus cause a large overhead of data transfer
	"""
	
	def __init__(self, dc_db, fieldname, users_project_ids = [], amp_filter_temptable = None, withhold_set_before = False):
		DataGetter.__init__(self, dc_db)
		
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iuam_temptable'
		
		self.amp_filter_temptable = amp_filter_temptable
		if self.amp_filter_temptable is None:
			amp_filters = AnalysisMethodParameterFilter(dc_db, self.fieldname)
			self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
		self.withhold_set_before = withhold_set_before
		
		self.withholded = []


	def getByPrimaryKeys(self, iuam_ids):
		# does this make sense here when the number of IDs is rapidly increasing with every sub table of IdentificationUnitAnalysis?
		
		self.createGetTempTable()
		
		batchsize = 400
		while len(iuam_ids) > 0:
			cached_ids = iuam_ids[:batchsize]
			del iuam_ids[:batchsize]
			placeholders = ['(?, ?, ?, ?, ?, ?)' for _ in cached_ids]
			values = []
			for ids_list in cached_ids:
				values.extend(ids_list)
			
			query = """
			DROP TABLE IF EXISTS [#iuam_pks_to_get_temptable]
			"""
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
			query = """
			CREATE TABLE [#iuam_pks_to_get_temptable] (
				[CollectionSpecimenID] INT NOT NULL,
				[IdentificationUnitID] INT NOT NULL,
				[AnalysisID] INT NOT NULL,
				[AnalysisNumber] NVARCHAR(50) NOT NULL COLLATE {0},
				[MethodID] INT NOT NULL,
				[MethodMarker] NVARCHAR(50) NOT NULL COLLATE {0},
				INDEX [CollectionSpecimenID_idx] ([CollectionSpecimenID]),
				INDEX [IdentificationUnitID_idx] ([IdentificationUnitID]),
				INDEX [AnalysisID_idx] ([AnalysisID]),
				INDEX [AnalysisNumber_idx] ([AnalysisNumber]),
				INDEX [MethodID_idx] ([MethodID]),
				INDEX [MethodMarker_idx] ([MethodMarker])
			)
			;""".format(self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
			
			query = """
			INSERT INTO [#iuam_pks_to_get_temptable] (
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber],
			[MethodID],
			[MethodMarker]
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
			AND pks.[AnalysisID] = iua.[AnalysisID]
			AND pks.[AnalysisNumber] = iua.[AnalysisNumber] COLLATE {1}
			AND pks.[MethodID] = iuamp.[MethodID]
			AND pks.[MethodMarker] = iuamp.[MethodMarker] COLLATE {1}
			;""".format(self.get_temptable, self.collation)
			querylog.info(query)
			self.cur.execute(query)
			self.con.commit()
		
		iuams = self.getData()
		
		return iuams


	def getByRowGUIDs(self, row_guids = []):
		self.row_guids = row_guids
		
		self.createGetTempTable()
		self.fillGetTempTable()
		
		iuams = self.getData()
		
		return iuams


	def getData(self):
		self.setDatabaseURN()
		if self.withhold_set_before is not True:
			self.withholded = self.filterAllowedRowGUIDs()
		
		query = """
		SELECT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iuam.[CollectionSpecimenID],
		iuam.[IdentificationUnitID],
		iuam.[AnalysisID],
		iuam.[AnalysisNumber],
		iuam.MethodID,
		iuam.MethodMarker,
		m.DisplayText AS MethodDisplay,
		m.Description AS MethodDescription,
		m.Notes AS MethodTypeNotes
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
			ON g_temp.[rowguid_to_get] = iuam.[RowGUID]
		INNER JOIN [MethodForAnalysis] mfa
			ON iuam.MethodID = mfa.MethodID AND iuam.AnalysisID = mfa.AnalysisID
		INNER JOIN [Method] m
			ON mfa.MethodID = m.MethodID
		INNER JOIN [{1}] amp_filter
			ON amp_filter.AnalysisID = iuam.AnalysisID AND amp_filter.MethodID = iuam.MethodID
		;""".format(self.get_temptable, self.amp_filter_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.iuam_rows = self.cur.fetchall()
		self.rows2list()
		
		#self.setChildParameters()
		
		return self.iuam_list


	def rows2list(self):
		self.iuam_list = []
		for row in self.iuam_rows:
			self.iuam_list.append(dict(zip(self.columns, row)))
		return


	def list2dict(self):
		self.iuam_dict = {}
		for element in self.iuam_list:
			if element['CollectionSpecimenID'] not in self.iuam_dict:
				self.iuam_dict[element['CollectionSpecimenID']] = {}
			if element['IdentificationUnitID'] not in self.iuam_dict[element['CollectionSpecimenID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = {}
			if element['AnalysisID'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']] = {}
			if element['AnalysisNumber'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']] = {}
			if element['MethodID'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']] = {}
			if element['MethodMarker'] not in self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']]:
				self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']] = {}
				
			self.iuam_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']] = element 
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		# the withholded list keeps the IDs and RowGUIDs of the withholded rows
		withholded = []
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		SELECT iuam.[RowGUID]
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuam.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iuam.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
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
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iuam.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iuam.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
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











