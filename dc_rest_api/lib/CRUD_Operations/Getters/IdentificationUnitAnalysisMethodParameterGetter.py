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
	
	def __init__(self, dc_db, fieldname, users_project_ids = [], amp_filter_temptable = None, withhold_set_before = False):
		DataGetter.__init__(self, dc_db)
		
		self.fieldname = fieldname
		
		self.users_project_ids = users_project_ids
		self.get_temptable = '#get_iuamp_temptable'
		
		self.amp_filter_temptable = amp_filter_temptable
		if self.amp_filter_temptable is None:
			amp_filters = AnalysisMethodParameterFilter(dc_db, self.fieldname)
			self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
		self.withhold_set_before = withhold_set_before


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


	def getData(self):
		
		self.setDatabaseURN()
		if self.withhold_set_before is not True:
			self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iuamp.[CollectionSpecimenID],
		iuamp.[IdentificationUnitID],
		iuamp.[AnalysisID],
		iuamp.[AnalysisNumber],
		iuamp.MethodID,
		iuamp.MethodMarker,
		iuamp.ParameterID,
		iuamp.[Value] AS ParameterValue,
		COALESCE(p.DisplayText, CAST(p.ParameterID AS VARCHAR(50))) AS ParameterDisplay,
		p.Description AS ParameterDescription,
		p.Notes AS ParameterNotes
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
			ON g_temp.[rowguid_to_get] = iuamp.[RowGUID]
		INNER JOIN [{1}] amp_filter
		ON amp_filter.AnalysisID = iuamp.AnalysisID
			AND amp_filter.MethodID = iuamp.MethodID
			AND amp_filter.ParameterID = iuamp.ParameterID
		INNER JOIN [Parameter] p
		ON p.MethodID = iuamp.MethodID AND p.ParameterID = iuamp.ParameterID
		;""".format(self.get_temptable, self.amp_filter_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		return self.results_list


	def list2dict(self):
		self.results_dict = {}
		for element in self.results_list:
			if element['CollectionSpecimenID'] not in self.results_dict:
				self.results_dict[element['CollectionSpecimenID']] = {}
			if element['IdentificationUnitID'] not in self.results_dict[element['CollectionSpecimenID']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']] = {}
			if element['AnalysisID'] not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']] = {}
			if element['AnalysisNumber'] not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']] = {}
			if element['MethodID'] not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']] = {}
			if element['MethodMarker'] not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']] = {}
			#if element['MethodMarker'] not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['ParameterID']]:
			#	self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']][element['ParameterID']] = {}
				
			self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']][element['MethodID']][element['MethodMarker']][element['ParameterID']] = element 
		
		return



	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
		query = """
		DELETE g_temp
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
		ON iuamp.RowGUID = g_temp.[rowguid_to_get]
		INNER JOIN [IdentificationUnit] iu
		ON iu.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = iuamp.[IdentificationUnitID]
		INNER JOIN [CollectionSpecimen] cs ON iuamp.[CollectionSpecimenID] = cs.[CollectionSpecimenID]
		{1}
		WHERE (iu.[DataWithholdingReason] IS NOT NULL AND iu.[DataWithholdingReason] != '')
		OR (cs.[DataWithholdingReason] IS NOT NULL AND cs.[DataWithholdingReason] != '')
		{2}
		;""".format(self.get_temptable, projectjoin, projectwhere)
		
		querylog.info(query)
		self.cur.execute(query, self.users_project_ids)
		self.con.commit()
		
		return












