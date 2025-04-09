import pudb

import logging, logging.config
logging.config.fileConfig('logging.conf')
querylog = logging.getLogger('query')


from dc_rest_api.lib.CRUD_Operations.Getters.DataGetter import DataGetter
from dc_rest_api.lib.CRUD_Operations.Getters.AnalysisMethodParameterFilter import AnalysisMethodParameterFilter

from dc_rest_api.lib.CRUD_Operations.Getters.IdentificationUnitAnalysisMethodParameterGetter import IdentificationUnitAnalysisMethodParameterGetter


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
			self.filterAllowedRowGUIDs()
		
		query = """
		SELECT DISTINCT
		g_temp.[rowguid_to_get] AS [RowGUID],
		g_temp.[DatabaseURN],
		iuam.[CollectionSpecimenID],
		iuam.[IdentificationUnitID],
		iuam.[AnalysisID],
		iuam.[AnalysisNumber],
		iuam.MethodID,
		iuam.MethodMarker,
		COALESCE(m.DisplayText, CAST(m.MethodID AS VARCHAR(50))) AS DisplayText,
		m.Description,
		m.Notes
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
		
		self.results_rows = self.cur.fetchall()
		self.rows2list()
		
		self.setChildParameters()
		
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
			
			if 'Methods' not in self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]:
				self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]['Methods'] = []
			
			method_dict = {}
			
			# do not forget to add the Parameters list with key Parameters
			for key in ('MethodID', 'MethodMarker', 'Parameters'):
				if key in element:
					method_dict[key] = element[key]
			
			method_dict['Method'] = {}
			for key in ('MethodID', 'DisplayText', 'Description', 'Notes'):
				if key in element:
					method_dict['Method'][key] = element[key]
			
			self.results_dict[element['CollectionSpecimenID']][element['IdentificationUnitID']][element['AnalysisID']][element['AnalysisNumber']]['Methods'].append(method_dict)
			
		return


	def filterAllowedRowGUIDs(self):
		# this methods checks if the connected Specimen is in one of the users projects or if the Withholding column is empty
		
		projectjoin, projectwhere = self.getProjectJoinForWithhold()
		
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
		
		return


	def setChildParameters(self):
		
		iuamp_getter = IdentificationUnitAnalysisMethodParameterGetter(self.dc_db, self.fieldname, self.users_project_ids, self.amp_filter_temptable, withhold_set_before = True)
		iuamp_getter.createGetTempTable()
		
		query = """
		INSERT INTO [{0}] ([rowguid_to_get])
		SELECT DISTINCT iuamp.[RowGUID]
		FROM [IdentificationUnitAnalysisMethod] iuam
		INNER JOIN [IdentificationUnitAnalysisMethodParameter] iuamp
		ON iuam.[CollectionSpecimenID] = iuamp.[CollectionSpecimenID] AND iuam.[IdentificationUnitID] = iuamp.[IdentificationUnitID]
		AND iuam.[AnalysisID] = iuamp.[AnalysisID] AND iuam.[AnalysisNumber] = iuamp.[AnalysisNumber]
		AND iuam.[MethodID] = iuamp.[MethodID] AND iuam.[MethodMarker] = iuamp.[MethodMarker]
		INNER JOIN [{1}] amp_filter
			ON amp_filter.AnalysisID = iuam.AnalysisID AND amp_filter.MethodID = iuam.MethodID
		INNER JOIN [{2}] rg_temp
		ON iuam.[RowGUID] = rg_temp.[rowguid_to_get]
		;""".format(iuamp_getter.get_temptable, self.amp_filter_temptable, self.get_temptable)
		
		querylog.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		iuamp_getter.getData()
		iuamp_getter.list2dict()
		
		for iuam in self.results_list:
			if (iuam['CollectionSpecimenID'] in iuamp_getter.results_dict 
			and iuam['IdentificationUnitID'] in iuamp_getter.results_dict[iuam['CollectionSpecimenID']] 
			and iuam['AnalysisID'] in iuamp_getter.results_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']]
			and iuam['AnalysisNumber'] in iuamp_getter.results_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']]
			and iuam['MethodID'] in iuamp_getter.results_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']]
			and iuam['MethodMarker'] in iuamp_getter.results_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']][iuam['MethodID']]):
				
				iuam['Parameters'] = iuamp_getter.results_dict[iuam['CollectionSpecimenID']][iuam['IdentificationUnitID']][iuam['AnalysisID']][iuam['AnalysisNumber']][iuam['MethodID']][iuam['MethodMarker']]['Parameters']
		
		return











