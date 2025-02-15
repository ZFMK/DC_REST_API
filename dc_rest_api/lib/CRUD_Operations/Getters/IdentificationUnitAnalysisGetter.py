import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisMethodGetter import IdentificationUnitAnalysisMethodGetter


class IdentificationUnitAnalysisGetter(DataGetter):
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
		self.get_temptable = '#get_iua_temptable'
		
		self.amp_filter_temptable = amp_filter_temptable
		if self.amp_filter_temptable is None:
			amp_filters = AnalysisMethodParameterFilter(dc_db, self.fieldname)
			self.amp_filter_temptable = amp_filters.amp_filter_temptable
		
		# there is no withhold in any of the iua tables, but in CollectionSpecimen and IdentificationUnit
		# so it must be checked if the data are allowed when only the subtables of IdentificationUnit are requested
		# the withhold_set_before flag shows whether the withhold has been checked in a parent class or not
		
		self.withhold_set_before = withhold_set_before


	def getByPrimaryKeys(self, iua_ids):
		# does this make sense here when the number of IDs is rapidly increasing with every 
		# sub table of IdentificationUnitAnalysis?
		
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



	def getData(self):
		self.setDatabaseURN()
		if self.withhold_set_before is not True:
			self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iua.CollectionSpecimenID,
		iua.IdentificationUnitID,
		iua.SpecimenPartID,
		iua.[AnalysisID],
		iua.AnalysisNumber,
		iua.Notes AS AnalysisInstanceNotes,
		iua.ExternalAnalysisURI,
		iua.ResponsibleName,
		iua.[AnalysisDate],
		iua.AnalysisResult,
		iua.AnalysisID,
		COALESCE(a.DisplayText, CAST(a.AnalysisID AS VARCHAR(50))) AS AnalysisDisplay,
		a.Description AS AnalysisDescription,
		a.MeasurementUnit,
		a.Notes AS AnalysisTypeNotes
		FROM [{0}] g_temp
		INNER JOIN [IdentificationUnitAnalysis] iua
			ON g_temp.[rowguid_to_get] = iua.[RowGUID]
		INNER JOIN [Analysis] a
			ON iua.AnalysisID = a.AnalysisID
		INNER JOIN [{1}] amp_filter
			ON amp_filter.AnalysisID = iua.AnalysisID
		ORDER BY iua.CollectionSpecimenID, iua.IdentificationUnitID, iua.AnalysisID, iua.AnalysisNumber
		;""".format(self.get_temptable, self.amp_filter_temptable)
		self.cur.execute(query)
		self.columns = [column[0] for column in self.cur.description]
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildMethods()
		
		return self.results_list


	def list2dict(self):
		#pudb.set_trace()
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
			
			self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']] = element 
		
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
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
		
		return


	def setChildMethods(self):
		
		iuam_getter = IdentificationUnitAnalysisMethodGetter(self.dc_db, self.fieldname, self.users_project_ids, self.amp_filter_temptable, withhold_set_before = True)
		iuam_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT iuam.[RowGUID]
		FROM [IdentificationUnitAnalysis] iua
		INNER JOIN [IdentificationUnitAnalysisMethod] iuam
		ON iua.[CollectionSpecimenID] = iuam.[CollectionSpecimenID] AND iua.[IdentificationUnitID] = iuam.[IdentificationUnitID]
		AND iua.[AnalysisID] = iuam.[AnalysisID] AND iua.[AnalysisNumber] = iuam.[AnalysisNumber]
		INNER JOIN [{1}] amp_filter
			ON amp_filter.AnalysisID = iuam.AnalysisID AND amp_filter.MethodID = iuam.MethodID
		INNER JOIN [{2}] rg_temp
		ON iua.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(iuam_getter.get_temptable, self.amp_filter_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		iuam_getter.getData()
		iuam_getter.list2dict()
		
		for iua in self.results_list:
			if (iua['CollectionSpecimenID'] in iuam_getter.results_dict 
			and iua['IdentificationUnitID'] in iuam_getter.results_dict[iua['CollectionSpecimenID']] 
			and iua['AnalysisID'] in iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']]
			and iua['AnalysisNumber'] in iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']][iua['AnalysisID']]):
				if 'Methods' not in iua:
					iua['Methods'] = {}
				
				for method_id in iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']][iua['AnalysisID']][iua['AnalysisNumber']]:
					
					for iuam_id in iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']][iua['AnalysisID']][iua['AnalysisNumber']][method_id]:
						method_display = iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']][iua['AnalysisID']][iua['AnalysisNumber']][method_id][iuam_id]['MethodDisplay']
						if method_display not in iua['Methods']:
							iua['Methods'][method_display] = []
						iua['Methods'][method_display].append(iuam_getter.results_dict[iua['CollectionSpecimenID']][iua['IdentificationUnitID']][iua['AnalysisID']][iua['AnalysisNumber']][method_id][iuam_id])
		
		return











